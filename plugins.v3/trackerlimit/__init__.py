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
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from fastapi import Request

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


SITE_GROUPS: Dict[str, str] = {  # ← 从 go_rpc 的 site_groups 默认表原样搬来（域名 → 站群别名）
    #   规则表按这个别名匹配：这样 tra1.m-team.cc→mteam、tracker.hdkyl.in→hdkylin、
    #   tracker.totheglory.im→ttg、tracker.cinefiles.info→audiences、chdbits.xyz/tracker.ptchdbits.co→rainbowisland
    #   都能命中；不在表里的域名以「域名本身」当别名（所以表里那些 tracker.xxx 的写法照样生效）。
    "1337x.st": "1337x",
    "1337x.to": "1337x",
    "13city.org": "13city",
    "audiences.me": "audiences",
    "audiobookbay.fi": "audiobookbay",
    "audiobookbay.is": "audiobookbay",
    "audiobookbay.lu": "audiobookbay",
    "audiobookbay.se": "audiobookbay",
    "audiobookbay.ws": "audiobookbay",
    "cf.hdkylin.com": "hdkylin",
    "chdbits.xyz": "rainbowisland",
    "duckboobee.org": "march",
    "e-hentai.org": "ehentai",
    "exhentai.org": "ehentai",
    "ext.to": "exttorrents",
    "extranet.torrentbay.st": "exttorrents",
    "hddolby.com": "hddolby",
    "hdkyl.in": "hdkylin",
    "hdsky.me": "hdsky",
    "ihdbits.me": "ihdbits",
    "keepfrds.com": "keepfrds",
    "longpt.org": "longpt",
    "marchcms.org": "march",
    "na.hdkylin.com": "hdkylin",
    "nexushd.org": "nexushd",
    "on.springsunday.net": "springsunday",
    "ourbits.club": "ourbits",
    "plus.ptdream.net": "ptdream",
    "pornbits.net": "pornbits",
    "pornbits.org": "pornbits",
    "pt.gtk.pw": "ptgtk",
    "pt.gtkpw.xyz": "ptgtk",
    "pt.hdupt.com": "hdupt",
    "pt.keepfrds.com": "keepfrds",
    "pt.newworld.plus": "ihdbits",
    "pt.ourhelp.club": "ourbits",
    "pt.upxin.net": "hdupt",
    "ptdream.net": "ptdream",
    "pterclub.net": "pter",
    "rutracker.net": "rutracker",
    "rutracker.org": "rutracker",
    "search.extto.com": "exttorrents",
    "springsunday.net": "springsunday",
    "t.audiences.me": "audiences",
    "theaudiobookbay.se": "audiobookbay",
    "tleechreload.org": "torrentleech",
    "tlgetin.cc": "torrentleech",
    "tokyo-tosho.net": "tokyotosho",
    "torrentleech.cc": "torrentleech",
    "torrentleech.me": "torrentleech",
    "torrentleech.org": "torrentleech",
    "totheglory.im": "ttg",
    "tra1.m-team.cc": "mteam",
    "tracker.cinefiles.info": "audiences",
    "tracker.hddolby.com": "hddolby",
    "tracker.hdkyl.in": "hdkylin",
    "tracker.hdsky.me": "hdsky",
    "tracker.hhanclub.net": "hhanclub",
    "tracker.pandapt.net": "pandapt",
    "tracker.piggo.me": "piggo",
    "tracker.ptchdbits.co": "rainbowisland",
    "tracker.pterclub.net": "pter",
    "tracker.qingwa.pro": "qingwa",
    "tracker.qingwapt.com": "qingwa",
    "tracker.qingwapt.org": "qingwa",
    "tracker.rainbowisland.co": "rainbowisland",
    "tracker.totheglory.im": "ttg",
    "v6.nexushd.org": "nexushd",
    "wiiii.dpdns.org": "longpt",
    "www.13city.org": "13city",
    "www.hddolby.com": "hddolby",
    "www.hdkyl.in": "hdkylin",
    "www.hdkylin.top": "hdkylin",
    "www.nexushd.org": "nexushd",
    "www.ourbits.club": "ourbits",
    "www.ptdream.net": "ptdream",
    "www.tleechreload.org": "torrentleech",
    "www.tlgetin.cc": "torrentleech",
    "www.tokyotosho.info": "tokyotosho",
    "www.torrentleech.cc": "torrentleech",
    "www.torrentleech.me": "torrentleech",
    "www.torrentleech.org": "torrentleech",
    "x1337x.cc": "1337x",
    "x1337x.eu": "1337x",
    "x1337x.ws": "1337x",
    "zmpt.cc": "zmpt",
}


# 站点显示名：关键字片段 → 中文名（名称取自 go_rpc 的 site_names.json 与 venus 的站点模块名）
SITE_LABELS: Dict[str, str] = {
    "m-team": "01.馒头", "mteam": "01.馒头",
    "hdsky": "02.天空",
    "chdbits": "03.彩虹岛", "rainbowisland": "03.彩虹岛",
    "totheglory": "04.听听歌", "ttg": "04.听听歌",
    "ourbits": "05.我堡",
    "springsunday": "06.春天",
    "pterclub": "07.猫站", "pter": "07.猫站",
    "hdhome": "08.家园",
    "keepfrds": "09.月月",
    "audiences": "10.观众", "cinefiles": "10.观众",
    "hhanclub": "11.憨憨",
    "hddolby": "13.杜比",
    "ubits": "14.UB",
    "piggo": "15.猪猪",
    "agsvpt": "16.末日",
    "qingwa": "17.青蛙",
    "hdkyl": "18.麒麟",
    "nicept": "老师",
    "btschool": "学校",
    "x-anime": "萝莉",
    "cspt": "财神",
    "zmpt": "织梦",
    "pandapt": "熊猫",
    "xingyunge": "星云",
    "cangbao": "藏宝",
    "crabpt": "蟹黄堡",
    "hdarea": "HDarea",
    "hdtime": "HDTime",
    "luckpt": "LuckyPT",
    "tangpt": "唐PT",
    "march": "MARCH",
}

class TrackerLimit(_PluginBase):
    # 插件名称
    plugin_name = "站点限速"
    # 插件描述
    plugin_desc = "按 tracker 给 Transmission 里的种子上传限速 + peer 上限；每 N 小时巡检一次，只在不同时改一次。"
    # 插件图标
    plugin_icon = "Transmission_A.png"
    # 插件版本
    plugin_version = "0.2.1"
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

    _mp_name_cache: Optional[Dict[str, str]] = None
    _mp_name_at: float = 0.0

    @classmethod
    def __mp_site_names(cls) -> Dict[str, str]:
        """MP 站点索引里的显示名：{小写 站点id / 域名: 名字}，10 分钟缓存。

        站点名一律按 MP 的规矩来：宿主内置站点索引 `SitesHelper().get_indexsites()`
        （域名 → {id, name, url}，共 190+ 站，名称就是 MP 界面上显示的那个），
        不自己起名字；MP 没收录的站才退回关键字本身。
        """
        now = time.time()
        if cls._mp_name_cache is not None and now - cls._mp_name_at < 600:
            return cls._mp_name_cache
        table: Dict[str, str] = {}
        try:
            from app.application.site.sites import SitesHelper  # noqa: PLC0415
            catalog = SitesHelper().get_indexsites() or {}
            if isinstance(catalog, dict):
                for domain, info in catalog.items():
                    if not isinstance(info, dict):
                        continue
                    name = str(info.get("name") or "").strip()
                    if not name:
                        continue
                    host = str(domain or "").strip().lower()
                    if host:
                        table.setdefault(host, name)
                        for prefix in ("www.", "pt.", "api.", "kp.", "t.", "xp."):
                            if host.startswith(prefix):
                                table.setdefault(host[len(prefix):], name)
                    site_id = str(info.get("id") or "").strip().lower()
                    if site_id:
                        table.setdefault(site_id, name)
        except Exception as err:  # noqa: BLE001
            logger.debug(f"站点限速：读 MP 站点索引失败（就按关键字显示）- {err}")
        cls._mp_name_cache, cls._mp_name_at = table, now
        return table

    @classmethod
    def __label(cls, tracker: Any) -> str:
        """规则关键字 → 站点名：先按 MP 站点索引，再按自家中文名表，最后原样显示关键字"""
        key = str(tracker or "").lower()
        if not key:
            return str(tracker or "")
        table = cls.__mp_site_names()
        if key in table:
            return table[key]
        hosts = {host for host, group in SITE_GROUPS.items() if group == key} | {key}
        for host in hosts:
            if host in table:
                return table[host]
            if "." in host and host.split(".", 1)[1] in table:
                return table[host.split(".", 1)[1]]
        for host in hosts:
            for domain, name in table.items():
                if "." in domain and host.endswith("." + domain):
                    return name
        for word in sorted(SITE_LABELS, key=len, reverse=True):
            if word in key:
                return re.sub(r"^\d\d\.", "", SITE_LABELS[word])
        return str(tracker or "")

    @classmethod
    def __suffix(cls, downloader: Any, tracker: Any) -> str:
        """表单字段用的稳定后缀：只留字母数字，其它换 _（前端 model 名不能带点/斜杠）"""
        return re.sub(r"[^0-9a-zA-Z]+", "_", f"{downloader}__{tracker}")

    @staticmethod
    def __hosts(announces: List[str]) -> List[str]:
        """把 announce 列表变成小写域名列表"""
        hosts: List[str] = []
        for one in announces:
            try:
                host = (urlparse(str(one)).hostname or "").lower()
            except Exception:  # noqa: BLE001
                host = ""
            if host:
                hosts.append(host)
        return hosts

    @classmethod
    def __match_rule(cls, rules: List[dict], announces: List[str]) -> Optional[dict]:
        """先按【站群别名】精确匹配（tra1.m-team.cc → mteam），再退回子串匹配。

        ★ 2026-10-16：原来只做子串匹配，实测比 go_rpc 漏一大截 —— go_rpc 先做
        `域名 → 站群别名` 映射（site_groups 表）再按别名查规则，所以 tra1.m-team.cc、
        tracker.hdkyl.in、tracker.totheglory.im、tracker.cinefiles.info、chdbits.xyz
        这些"别名和域名长得不一样"的站照样命中；朴素子串匹配全都漏（馒头 358、麒麟 690、
        TTG 195、观众 164、彩虹岛 431 个种）。现在把那张表原样搬进来。
        """
        hosts = cls.__hosts(announces)
        if not hosts:
            return None
        exact_keys = set(hosts) | {SITE_GROUPS.get(host, host) for host in hosts}
        # 第一轮：精确（域名 or 站群别名），命中多个时取最长的关键字
        best: Optional[dict] = None
        for rule in rules:
            key = str(rule.get("tracker") or "").strip().lower()
            if key and key in exact_keys:
                if best is None or len(key) > len(str(best.get("tracker") or "").strip().lower()):
                    best = rule
        if best is not None:
            return best
        # 第二轮：子串兜底（表里写 cspt.top 也能命中 tracker.cspt.top）
        text = "\n".join(hosts)
        for rule in rules:
            key = str(rule.get("tracker") or "").strip().lower()
            if key and key in text:
                if best is None or len(key) > len(str(best.get("tracker") or "").strip().lower()):
                    best = rule
        return best

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
        }, {
            "path": "/save_rules",
            "endpoint": self.api_save_rules,
            "methods": ["POST"],
            "summary": "保存整张规则表",
            "description": "页面「保存全部」：up_*/pw_* 字段一次性回写（留空=不动、0=不限速/不限制）",
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

    async def api_save_rules(self, request: Request = None, apikey: str = None) -> schemas.Response:
        """面板「保存全部」：整张规则表的限速 / peer 一次回写。

        ⚠️ 插件 API 是宿主用 FastAPI 按函数签名注册的路由，**签名里没有的参数会被 422 挡掉**，
        所以这里不收固定参数，改成自己从请求里捞：query（前端表单就发这里）+ JSON body + 表单。
        字段名 = `up_<后缀>` / `pw_<后缀>`（后缀 = 下载器__关键字 里的非字母数字换成 _）。
        约定：哪一栏留空 = 这一项不动；填 0 = 不限速 / 不限制。
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        kwargs: Dict[str, Any] = {}
        if request is not None:
            try:
                kwargs.update({k: v for k, v in request.query_params.items()})
            except Exception:  # noqa: BLE001
                pass
            for reader in ("json", "form"):
                try:
                    got = await getattr(request, reader)()
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(got, dict):
                    for k, v in got.items():
                        if not isinstance(v, (dict, list)):
                            kwargs[k] = v
        changed: List[str] = []
        bad: List[str] = []
        for key, rule in list(self._rules.items()):
            dl = str(rule.get("downloader") or "")
            tracker = str(rule.get("tracker") or "")
            suffix = self.__suffix(dl, tracker)
            for name, field in (("upload_kbps", f"up_{suffix}"), ("peer_limit", f"pw_{suffix}")):
                if field not in kwargs:
                    continue
                raw = kwargs.get(field)
                if raw is None or str(raw).strip() == "":
                    continue                     # 留空 = 不动
                want = _as_int(raw)
                if want is None or want < 0:
                    bad.append(f"{self.__label(tracker)}（{tracker}）的 {name}")
                    continue
                now = _as_int(rule.get(name)) or 0
                if want != now:
                    rule[name] = want
                    changed.append(f"{self.__label(tracker)}｜{dl}｜{name} {now} → {want}")
        if changed:
            self.__save_rules()
            logger.info(f"站点限速：面板保存了 {len(changed)} 项 - {'；'.join(changed[:10])}")
        message = f"已保存 {len(changed)} 项改动" if changed else "没有变化（数值和现在一样）"
        if bad:
            message += f"；填的不是数字，没保存：{'、'.join(bad[:5])}"
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

        # ---- 规则表：站点名 + 上传限速 + peer 上限（直接改，改完点「保存全部」）
        groups: Dict[str, List[Tuple[str, dict]]] = {}
        for key, rule in self._rules.items():
            groups.setdefault(str(rule.get("downloader") or ""), []).append((key, rule))

        rule_blocks: List[dict] = []
        for dl in sorted(groups):
            rule_blocks.append({
                "component": "VRow",
                "props": {"align": "center"},
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 5}, "content": [
                        {"component": "VText", "props": {"class": "text-caption text-medium-emphasis",
                                                          "text": f"下载器 {dl}（{len(groups[dl])} 条）"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [
                        {"component": "VText", "props": {"class": "text-caption text-medium-emphasis",
                                                          "text": "上传限速 KB/s"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [
                        {"component": "VText", "props": {"class": "text-caption text-medium-emphasis",
                                                          "text": "peer 上限"}}]},
                ]
            })
            for key, rule in sorted(groups[dl],
                                    key=lambda kv: (self.__label(kv[1].get("tracker")),
                                                    str(kv[1].get("tracker") or ""))):
                tracker = str(rule.get("tracker") or "")
                upload = _as_int(rule.get("upload_kbps"))
                peer = _as_int(rule.get("peer_limit"))
                suffix = self.__suffix(dl, tracker)
                # 当前值灌进插件数据 —— 前端表单拿它当默认值（model 字段名 = up_/pw_ + 后缀）
                self.save_data(f"up_{suffix}", "" if upload is None else upload)
                self.save_data(f"pw_{suffix}", "" if peer is None else peer)
                rule_blocks.append({
                    "component": "VRow",
                    "props": {"align": "center"},
                    "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 5}, "content": [
                            {"component": "VText", "props": {"class": "text-body-2 font-weight-medium",
                                                             "text": self.__label(tracker)}},
                            {"component": "VText", "props": {"class": "text-caption text-medium-emphasis",
                                                             "text": tracker}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [
                            {"component": "VTextField",
                             "props": {"model": f"up_{suffix}", "value": "" if upload is None else upload,
                                       "placeholder": "0 = 不限速", "type": "number", "suffix": "KB/s",
                                       "density": "compact", "variant": "outlined", "hide-details": True}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 3}, "content": [
                            {"component": "VTextField",
                             "props": {"model": f"pw_{suffix}", "value": "" if peer is None else peer,
                                       "placeholder": "0 = 不限制", "type": "number",
                                       "density": "compact", "variant": "outlined", "hide-details": True}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 1}, "content": [
                            {"component": "VBtn",
                             "props": {"size": "x-small", "color": "error", "variant": "text",
                                       "icon": "mdi-delete"},
                             "events": {"click": {
                                 "api": f"plugin/{self.__class__.__name__}/del_rule?apikey={settings.API_TOKEN}",
                                 "method": "post", "params": {"key": key}}}}]},
                    ]
                })

        rules_card = {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "content": [
                    {"component": "VRow", "props": {"align": "center"}, "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 8}, "content": [
                            {"component": "VText", "props": {"class": "text-subtitle-1",
                                                             "text": "规则表（站点名 → 上传限速 / peer 上限）"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VBtn",
                             "props": {"color": "primary", "variant": "flat", "block": True, "size": "small"},
                             "text": "保存全部",
                             "events": {"click": {
                                 "api": f"plugin/{self.__class__.__name__}/save_rules?apikey={settings.API_TOKEN}",
                                 "method": "post"}}}]},
                    ]}]},
                {"component": "VCardText", "props": {"class": "py-0"}, "content": [
                    {"component": "VText", "props": {
                        "class": "text-caption text-medium-emphasis",
                        "text": "限速填 0 = 不限速，peer 填 0 = 不限制；哪一栏留空就表示这一项不动。改完全表一起生效，存完下一轮巡检会照新值改种。"}}]},
                {"component": "VCardText", "props": {"class": "pt-1"}, "content": rule_blocks or [
                    {"component": "VText",
                     "props": {"class": "text-caption", "text": "（还没有规则）"}}]},
            ]
        }

        add_card = {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "新增一条规则"},
                {"component": "VCardText", "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VSelect",
                             "props": {"model": "rule_downloader", "label": "下载器", "items": names,
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_tracker", "label": "站点 / tracker 关键字",
                                       "placeholder": "例如 hdsky、tracker.xxx.org",
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_upload", "label": "上传限速 KB/s",
                                       "placeholder": "0=不限速", "type": "number",
                                       "density": "compact", "variant": "outlined"}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": "rule_peer", "label": "peer 上限",
                                       "placeholder": "0=不限制", "type": "number",
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
            ]
        }

        page: List[dict] = [status_card, rules_card, add_card]

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
