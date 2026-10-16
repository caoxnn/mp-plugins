"""站点限速（Transmission）—— 按 tracker 给种子上传限速 + peer 上限，定时巡检、只在不同时改一次。

来源：把 go_rpc（/suzhu/go_rpc，Flask + 自己的 Transmission RPC 客户端）搬成 MoviePilot 插件。
差别只在「下载器从哪来」：插件用 MP 里已经配好的下载器（`DownloaderHelper`），不再自己存
RPC 地址/账号；规则表挪到插件详情页上维护。

口径（2026-10-16 用户拍板）：
  · 只做 **Transmission**（qB 实例直接跳过并记一笔）—— 刷流不用 tr，不会有交叉。
  · **保留 peer 上限**：MP 那层的 `update_torrent()` 只认下载/上传限速 + 分享率 + 做种时间，
    没有 peer-limit → 这一项走底层 `instance.trc`（transmission_rpc 客户端，MP 的 Transmission
    包装类里就挂在 `.trc` 上；实测那版 `change_torrent(..., peer_limit=)` 支持）。
  · 「限速一次」= 先读当前值，和目标不一致才写（同一目标的种**批量一次**发下去），不反复设。
  · 上传限速留空/0 = 不限速；peer 上限留空 = 这一项不动。
"""
import datetime
import threading
import traceback
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.log import logger
from app.plugins import _PluginBase
from app.sdk.config import settings
from app.sdk.services import DownloaderHelper

lock = threading.Lock()

# 一次巡检里每种下载器最多记多少条改种明细（详情页/日志用）
MAX_ROWS = 200
# 详情页「最近巡检」保留条数
HISTORY_KEEP = 30


def _as_int(value: Any) -> Optional[int]:
    """把 "10" / 10 / "10 " 统一成 int；空/非法 → None。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    """安全读属性：transmission_rpc 的 Torrent 属性在字段没请求时会抛 KeyError，
    getattr 的默认值救不了它 —— 这里统一兜住。"""
    try:
        value = getattr(obj, name)
    except Exception:  # noqa: BLE001
        return default
    return default if value is None else value


class TrackerLimit(_PluginBase):
    # 插件名称
    plugin_name = "站点限速"
    # 插件描述
    plugin_desc = "按 tracker 给 Transmission 里的种子上传限速 + peer 上限；每 N 小时巡检一次，只在不同时改一次。"
    # 插件图标
    plugin_icon = "Transmission_A.png"
    # 插件版本
    plugin_version = "0.1.1"
    # 插件作者
    plugin_author = "nc17"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "trackerlimit_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled = False
    _notify = False
    _onlyonce = False
    _cron: Optional[str] = None
    # 要管的 Transmission 实例名（MP 里配的名字）
    _downloaders: List[str] = []
    # 规则表：{f"{下载器}|{tracker关键字}": {...}}，存 data/rules
    _rules: Dict[str, Any] = {}
    # 最近巡检记录
    _history: List[dict] = []

    def init_plugin(self, config: dict = None):
        self._rules = self.get_data("rules") or {}
        self._history = self.get_data("history") or []

        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron") or "0 */6 * * *"
            self._downloaders = list(config.get("downloaders") or [])

        self.stop_service()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("站点限速：立即运行一次")
            self._scheduler.add_job(
                func=self.apply_now,
                trigger="date",
                run_date=datetime.datetime.now(
                    tz=pytz.timezone(settings.TZ)
                ) + datetime.timedelta(seconds=3),
            )
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def __update_config(self):
        """回写配置（只回写配置页上的项，规则表存在 data 里）"""
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "downloaders": self._downloaders,
        })

    # ---------------------------------------------------------------- 下载器
    @staticmethod
    def downloader_names() -> List[str]:
        """MP 里配好的所有下载器名（配置页下拉/多选用）"""
        try:
            return sorted(DownloaderHelper().get_configs().keys())
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：读取下载器列表失败 - {err}")
            return []

    @staticmethod
    def __transmission_instances() -> Dict[str, Any]:
        """只取 Transmission 实例：{名字: MP 包装对象}"""
        result: Dict[str, Any] = {}
        try:
            services = DownloaderHelper().get_services()
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：读取下载器实例失败 - {err}")
            return result
        for name, info in (services or {}).items():
            if "transmission" not in str(getattr(info, "type", "") or "").lower():
                continue
            instance = getattr(info, "instance", None)
            if instance is not None:
                result[name] = instance
        return result

    # ------------------------------------------------------------------ 规则
    @staticmethod
    def rule_key(downloader: str, tracker: str) -> str:
        return f"{(downloader or '').strip()}|{(tracker or '').strip().lower()}"

    def __save_rules(self):
        self.save_data("rules", self._rules)

    def __rules_of(self, downloader: str) -> List[dict]:
        """某台下载器下启用的规则，按关键字长度倒序（长关键字更具体，优先命中）"""
        rows = [dict(v, _key=k) for k, v in self._rules.items()
                if v.get("downloader") == downloader and v.get("enabled", True)]
        rows.sort(key=lambda x: len(str(x.get("tracker") or ""), ), reverse=True)
        return rows

    @staticmethod
    def __match_rule(rules: List[dict], announces: List[str]) -> Optional[dict]:
        """用 tracker 域名片段去匹配这个种的所有 announce 地址"""
        haystack = "\n".join(a.lower() for a in announces if a)
        if not haystack:
            return None
        for rule in rules:
            key = str(rule.get("tracker") or "").strip().lower()
            if key and key in haystack:
                return rule
        return None

    # ------------------------------------------------------------------ 巡检
    @staticmethod
    def __announces(torrent: Any) -> List[str]:
        out: List[str] = []
        trackers = _attr(torrent, "trackers", None) or []
        for one in trackers:
            announce = None
            if isinstance(one, dict):
                announce = one.get("announce")
            else:
                fields = getattr(one, "fields", None)
                if isinstance(fields, dict):
                    announce = fields.get("announce")
                if not announce:
                    announce = _attr(one, "announce", None)
            if announce:
                out.append(str(announce))
        return out

    def scan_and_apply(self, apply: bool = True) -> dict:
        """一轮巡检：读所有种 → 按 tracker 匹配规则 → 只改不一致的（同目标批一次）。

        :param apply: False = 只看不改（干跑，详情页用）
        """
        started = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        summary: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"),
            "downloaders": [],
            "scanned": 0,
            "matched": 0,
            "changed": 0,
            "errors": [],
            "rows": [],
        }
        if not self._downloaders:
            summary["errors"].append("配置页里没勾选要管的 Transmission 下载器")
            self.__record(summary)
            return summary

        instances = self.__transmission_instances()
        for name in self._downloaders:
            entry: Dict[str, Any] = {"name": name, "scanned": 0, "matched": 0, "changed": 0}
            summary["downloaders"].append(entry)

            instance = instances.get(name)
            if instance is None:
                msg = f"{name}：MP 里没有这个下载器实例（或不是 Transmission）"
                logger.warn(f"站点限速：{msg}")
                entry["error"] = msg
                summary["errors"].append(msg)
                continue

            trc = getattr(instance, "trc", None)
            if trc is None:
                msg = f"{name}：拿不到底层客户端（instance.trc 为空），跳过 peer 相关设置"
                logger.warn(f"站点限速：{msg}")
                entry["error"] = msg
                summary["errors"].append(msg)
                continue

            rules = self.__rules_of(name)
            if not rules:
                entry["note"] = "这台没有启用的规则"
                continue

            try:
                # ⚠️ transmission_rpc 的 arguments 是**字段名列表**（不是 {"fields": [...]}）：
                #    传错会只请求 ["fields","id","hashString"]，读 trackers/peer-limit 就 KeyError。
                torrents = trc.get_torrents(arguments=[
                    "name", "uploadLimit", "uploadLimited", "peer-limit", "trackers",
                ])
            except Exception as err:  # noqa: BLE001
                msg = f"{name}：拉种子列表失败 - {err}"
                logger.error(f"站点限速：{msg}")
                entry["error"] = msg
                summary["errors"].append(msg)
                continue

            entry["scanned"] = summary["scanned"] = summary["scanned"] + len(torrents)
            # 目标 → 该目标的 hash 列表（同目标一批发下去）
            groups: Dict[Tuple[bool, int, Optional[int]], List[str]] = {}
            hash2name: Dict[str, str] = {}
            for torrent in torrents:
                hash_string = str(_attr(torrent, "hash_string", "") or "")
                if not hash_string:
                    continue
                rule = self.__match_rule(rules, self.__announces(torrent))
                if not rule:
                    continue
                entry["matched"] = summary["matched"] = summary["matched"] + 1

                want_upload = _as_int(rule.get("upload_kbps"))
                want_peer = _as_int(rule.get("peer_limit"))
                if want_upload and want_upload > 0:
                    target_upload_limited, target_upload = True, int(want_upload)
                else:                      # 留空/0 = 不限速
                    target_upload_limited, target_upload = False, 0
                target_peer = int(want_peer) if want_peer and want_peer > 0 else None

                cur_limited = bool(_attr(torrent, "upload_limited", False))
                cur_upload = int(_attr(torrent, "upload_limit", 0) or 0)
                cur_peer = _as_int(_attr(torrent, "peer_limit", None))

                diff = (cur_limited != target_upload_limited)
                if target_upload_limited and cur_upload != target_upload:
                    diff = True
                if target_peer is not None and cur_peer != target_peer:
                    diff = True
                if not diff:
                    continue

                groups.setdefault(
                    (target_upload_limited, target_upload, target_peer), []
                ).append(hash_string)
                hash2name[hash_string] = str(_attr(torrent, "name", "") or "")

            for (target_limited, target_upload, target_peer), hashes in groups.items():
                if not apply:
                    entry["changed"] = summary["changed"] = summary["changed"] + len(hashes)
                    for h in hashes[:MAX_ROWS]:
                        summary["rows"].append({
                            "downloader": name,
                            "name": hash2name.get(h, h),
                            "upload": target_upload if target_limited else "-",
                            "peer": target_peer if target_peer is not None else "-",
                            "dry": True,
                        })
                    continue
                kwargs: Dict[str, Any] = {
                    "ids": hashes,
                    "upload_limited": target_limited,
                    "upload_limit": target_upload,
                }
                if target_peer is not None:
                    kwargs["peer_limit"] = target_peer
                try:
                    trc.change_torrent(**kwargs)
                except Exception as err:  # noqa: BLE001
                    msg = f"{name}：改限速失败（{len(hashes)} 个种）- {err}"
                    logger.error(f"站点限速：{msg}")
                    entry["error"] = msg
                    summary["errors"].append(msg)
                    continue
                entry["changed"] = summary["changed"] = summary["changed"] + len(hashes)
                for h in hashes[:MAX_ROWS]:
                    summary["rows"].append({
                        "downloader": name,
                        "name": hash2name.get(h, h),
                        "upload": target_upload if target_limited else "不限速",
                        "peer": target_peer if target_peer is not None else "-",
                    })
                logger.info(
                    f"站点限速：{name} 应用 {len(hashes)} 个种 "
                    f"（上传={'不限' if not target_limited else str(target_upload) + ' KB/s'}，"
                    f"peer={target_peer if target_peer is not None else '不动'}）"
                )

        if not apply:
            summary["dry"] = True
        self.__record(summary)
        if self._notify and summary["changed"] and apply:
            try:
                detail = "；".join(
                    f"{d['name']} 改 {d.get('changed', 0)}" for d in summary["downloaders"]
                )
                self.systemmessage.put(
                    f"巡检完成：{detail}（共 {summary['changed']} 个种）",
                    title="站点限速",
                )
            except Exception as err:  # noqa: BLE001
                logger.error(f"站点限速：发通知失败 - {err}")
        return summary

    def apply_now(self):
        """定时任务 / 立即运行入口"""
        with lock:
            try:
                return self.scan_and_apply(apply=True)
            except Exception as err:  # noqa: BLE001
                logger.error(f"站点限速：巡检异常 - {err} - {traceback.format_exc()}")
                return {"errors": [str(err)]}

    def __record(self, summary: dict):
        """把这一轮记进历史（详情页用）"""
        self._history.insert(0, {
            "time": summary.get("time"),
            "scanned": summary.get("scanned", 0),
            "matched": summary.get("matched", 0),
            "changed": summary.get("changed", 0),
            "errors": summary.get("errors") or [],
            "downloaders": [
                {"name": d.get("name"), "scanned": d.get("scanned", 0),
                 "matched": d.get("matched", 0), "changed": d.get("changed", 0),
                 "error": d.get("error") or d.get("note")}
                for d in summary.get("downloaders") or []
            ],
            "rows": (summary.get("rows") or [])[:MAX_ROWS],
        })
        self._history = self._history[:HISTORY_KEEP]
        self.save_data("history", self._history)

    # ---------------------------------------------------------------- 基类接口
    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/apply_now",
            "endpoint": self.api_apply_now,
            "methods": ["GET", "POST"],
            "summary": "立即巡检一次",
            "description": "按规则表巡检并应用限速（同一目标批量改一次）",
        }, {
            "path": "/dry_run",
            "endpoint": self.api_dry_run,
            "methods": ["GET", "POST"],
            "summary": "干跑一次",
            "description": "只列出会被改的种，不真改",
        }, {
            "path": "/save_rule",
            "endpoint": self.api_save_rule,
            "methods": ["POST"],
            "summary": "保存一条规则",
            "description": "下载器 + tracker 关键字 + 上传限速 + peer 上限",
        }, {
            "path": "/del_rule",
            "endpoint": self.api_del_rule,
            "methods": ["POST"],
            "summary": "删除一条规则",
            "description": "按 key 删除",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时巡检"""
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "TrackerLimitScan",
                    "name": "站点限速巡检",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.apply_now,
                    "kwargs": {},
                }]
            except Exception as err:  # noqa: BLE001
                logger.error(f"站点限速：定时任务配置错误 - {err}")
        return []

    # -------------------------------------------------------------------- API
    def api_apply_now(self, apikey: str = None) -> schemas.Response:
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        try:
            result = self.apply_now() or {}
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：立即巡检失败 - {err} - {traceback.format_exc()}")
            return schemas.Response(success=False, message=f"巡检失败：{err}")
        message = f"巡检完成：扫描 {result.get('scanned', 0)}，命中 {result.get('matched', 0)}，改 {result.get('changed', 0)}"
        if result.get("errors"):
            message += f"；错误：{'；'.join(str(e) for e in result['errors'])[:200]}"
        return schemas.Response(success=True, message=message)

    def api_dry_run(self, apikey: str = None) -> schemas.Response:
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        try:
            with lock:
                result = self.scan_and_apply(apply=False) or {}
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：干跑失败 - {err} - {traceback.format_exc()}")
            return schemas.Response(success=False, message=f"干跑失败：{err}")
        message = (f"干跑：扫描 {result.get('scanned', 0)}，命中 {result.get('matched', 0)}，"
                   f"会被改 {result.get('changed', 0)}（没实际改）")
        if result.get("errors"):
            message += f"；错误：{'；'.join(str(e) for e in result['errors'])[:200]}"
        return schemas.Response(success=True, message=message)

    def api_save_rule(self, apikey: str = None, rule_downloader: str = None,
                      rule_tracker: str = None, rule_upload: str = None,
                      rule_peer: str = None, rule_enabled: bool = True) -> schemas.Response:
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        downloader = str(rule_downloader or "").strip()
        tracker = str(rule_tracker or "").strip()
        if not downloader or not tracker:
            return schemas.Response(success=False, message="下载器和 tracker 关键字都要填")
        upload = _as_int(rule_upload)
        peer = _as_int(rule_peer)
        key = self.rule_key(downloader, tracker)
        self._rules[key] = {
            "downloader": downloader,
            "tracker": tracker,
            "upload_kbps": upload,
            "peer_limit": peer,
            "enabled": bool(rule_enabled),
            "updated": datetime.datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.__save_rules()
        logger.info(f"站点限速：保存规则 {key} → 上传 {upload or '不限'} / peer {peer if peer else '不动'}")
        return schemas.Response(
            success=True,
            message=f"已保存：{downloader} · {tracker} → 上传 {upload or '不限速'} / peer {peer if peer else '不动'}",
        )

    def api_del_rule(self, apikey: str = None, key: str = None) -> schemas.Response:
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        if key and key in self._rules:
            self._rules.pop(key, None)
            self.__save_rules()
            return schemas.Response(success=True, message=f"已删除 {key}")
        return schemas.Response(success=False, message="没找到这条规则")

    # ------------------------------------------------------------------- 配置页
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch",
                                     "props": {"model": "enabled", "label": "启用插件"}}
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch",
                                     "props": {"model": "notify", "label": "改动时发通知"}}
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch",
                                     "props": {"model": "onlyonce", "label": "立即运行一次"}}
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VTextField",
                                     "props": {"model": "cron", "label": "巡检周期（cron）",
                                               "placeholder": "0 */6 * * *",
                                               "density": "compact", "variant": "outlined"}}
                                ]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {"component": "VSelect",
                                     "props": {"model": "downloaders", "label": "要管的下载器（只支持 Transmission）",
                                               "multiple": True, "chips": True,
                                               "items": self.downloader_names(), "clearable": True,
                                               "density": "compact", "variant": "outlined"}}
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {"component": "VAlert",
                                     "props": {"type": "info", "variant": "tonal", "density": "compact",
                                               "text": "规则表在插件详情页上维护（下载器 · tracker 关键字 · 上传限速 · peer 上限）。"
                                                       "只对 Transmission 生效，qB 实例会自动跳过。"}}
                                ]
                            }
                        ]
                    },
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "cron": "0 */6 * * *",
            "downloaders": [],
        }

    # ------------------------------------------------------------------- 详情页
    def get_page(self) -> List[dict]:
        names = self.downloader_names()
        last = self._history[0] if self._history else None

        # ---- 状态卡
        status_bits = [
            f"启用：{'是' if self._enabled else '否'}",
            f"周期：{self._cron or '未设置'}",
            f"管 {len(self._downloaders)} 台：{('、'.join(self._downloaders)) or '（未选）'}",
            f"规则 {len([r for r in self._rules.values() if r.get('enabled', True)])} 条",
        ]
        if last:
            status_bits.append(
                f"上次巡检：{last.get('time')} · 扫描 {last.get('scanned', 0)} · "
                f"改 {last.get('changed', 0)}" + (f" · 错误 {len(last.get('errors') or [])}" if last.get("errors") else "")
            )
        status_card = {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "站点限速（Transmission）"},
                {"component": "VCardText", "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 10}, "content": [
                            {"component": "VText", "props": {"class": "text-body-2",
                                                             "text": " ｜ ".join(status_bits)}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                            {"component": "VBtn",
                             "props": {"color": "primary", "variant": "flat", "block": True, "size": "small"},
                             "text": "立即巡检",
                             "events": {"click": {
                                 "api": f"plugin/{self.__class__.__name__}/apply_now?apikey={settings.API_TOKEN}",
                                 "method": "post"}}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 10}, "content": [
                            {"component": "VText", "props": {
                                "class": "text-caption text-medium-emphasis",
                                "text": "「立即巡检」= 读当前限速 → 只改与规则不一致的种（同一目标的种一次批量改）。"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                            {"component": "VBtn",
                             "props": {"color": "secondary", "variant": "tonal", "block": True, "size": "small"},
                             "text": "干跑",
                             "events": {"click": {
                                 "api": f"plugin/{self.__class__.__name__}/dry_run?apikey={settings.API_TOKEN}",
                                 "method": "post"}}}]},
                    ]},
                ]},
            ]
        }

        # ---- 规则表
        rule_rows = []
        for key, rule in sorted(self._rules.items(),
                                key=lambda kv: (kv[1].get("downloader") or "", kv[1].get("tracker") or "")):
            upload = _as_int(rule.get("upload_kbps"))
            peer = _as_int(rule.get("peer_limit"))
            rule_rows.append({
                "component": "VRow",
                "props": {"align": "center"},
                "content": [
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VChip", "props": {"color": "primary", "size": "small", "variant": "tonal"},
                         "content": [{"component": "VText",
                                      "props": {"text": str(rule.get("downloader") or "")}}]}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 4}, "content": [
                        {"component": "VText", "props": {"text": str(rule.get("tracker") or "")}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText",
                         "props": {"text": (f"上传 {upload} KB/s" if upload else "上传不限速")}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText",
                         "props": {"text": (f"peer {peer}" if peer else "peer 不动")}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 1}, "content": [
                        {"component": "VChip",
                         "props": {"color": "success" if rule.get("enabled", True) else "default",
                                   "size": "small", "variant": "tonal"},
                         "content": [{"component": "VText",
                                      "props": {"text": "启用" if rule.get("enabled", True) else "停用"}}]}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 1}, "content": [
                        {"component": "VBtn",
                         "props": {"size": "small", "color": "error", "variant": "tonal"},
                         "text": "删除",
                         "events": {"click": {
                             "api": f"plugin/{self.__class__.__name__}/del_rule?apikey={settings.API_TOKEN}",
                             "method": "post",
                             "params": {"key": key}}}}]},
                ]
            })

        add_card = {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "添加 / 覆盖一条规则（下载器 + tracker 关键字）"},
                {"component": "VCardText", "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSelect",
                             "props": {"model": "rule_downloader", "label": "下载器",
                                       "items": names, "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_tracker", "label": "tracker 关键字",
                                       "placeholder": "例如 tracker.xingyungept.org / mteam",
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_upload", "label": "上传限速 KB/s",
                                       "placeholder": "留空=不限速",
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_peer", "label": "peer 上限",
                                       "placeholder": "留空=不动",
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                            {"component": "VBtn",
                             "props": {"color": "primary", "variant": "flat", "block": True},
                             "text": "保存规则",
                             "events": {"click": {
                                 "api": f"plugin/{self.__class__.__name__}/save_rule?apikey={settings.API_TOKEN}",
                                 "method": "post"}}}]},
                    ]},
                ]},
                {"component": "VCardText", "props": {"class": "pt-0"}, "content": rule_rows or [
                    {"component": "VText",
                     "props": {"class": "text-caption", "text": "（还没有规则）"}}]},
            ]
        }

        page: List[dict] = [status_card, add_card]

        # ---- 最近一轮改了什么
        if last and last.get("rows"):
            detail_rows = []
            for row in last["rows"][:50]:
                detail_rows.append({
                    "component": "VRow",
                    "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                            {"component": "VText", "props": {"class": "text-caption",
                                                             "text": str(row.get("downloader") or "")}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 7}, "content": [
                            {"component": "VText", "props": {"class": "text-caption",
                                                             "text": str(row.get("name") or "")}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VText", "props": {"class": "text-caption",
                                                             "text": f"上传 {row.get('upload')}"}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 1}, "content": [
                            {"component": "VText", "props": {"class": "text-caption",
                                                             "text": f"peer {row.get('peer')}"}}]},
                    ]
                })
            page.append({
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mb-4"},
                "content": [
                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                     "text": f"上一轮改动（{last.get('time')}，共 {last.get('changed', 0)} 个种）"},
                    {"component": "VCardText", "content": detail_rows},
                ]
            })

        # ---- 历史
        if self._history:
            hist_rows = [{
                "component": "VRow",
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": str(h.get("time") or "")}}]},
                    {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": f"扫描 {h.get('scanned', 0)}"}}]},
                    {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": f"命中 {h.get('matched', 0)}"}}]},
                    {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": f"改 {h.get('changed', 0)}"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VText", "props": {
                            "class": "text-caption text-medium-emphasis",
                            "text": ("错误：" + "；".join(h.get("errors") or [])[:80]) if h.get("errors") else "OK"}}]},
                ]
            } for h in self._history[:10]]
            page.append({
                "component": "VCard",
                "props": {"variant": "tonal"},
                "content": [
                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "最近巡检记录"},
                    {"component": "VCardText", "content": hist_rows},
                ]
            })
        return page

    def stop_service(self):
        """退出插件"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
            self._scheduler = None
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：停止服务出错 - {err}")
