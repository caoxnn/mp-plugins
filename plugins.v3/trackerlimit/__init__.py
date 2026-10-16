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


import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from pydantic import BaseModel

from app import schemas
from app.schemas.types import NotificationType
from app.plugins import _PluginBase
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.network import SitesHelper
from app.sdk.services import DownloaderHelper

lock = threading.Lock()

# 一次巡检里每种下载器最多记多少条改种明细（详情页/日志用）
MAX_ROWS = 200
# 详情页「最近巡检」保留条数
HISTORY_KEEP = 30


class ScanResultData(BaseModel):
    """一次巡检 / 干跑的结果（业务数据放这里，不放 message）。"""

    scanned: int = 0
    matched: int = 0
    changed: int = 0
    dry: bool = False
    errors: List[str] = []


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
    plugin_version = "0.7.2"
    # 插件作者
    plugin_author = "caoxnn"
    # 作者主页
    author_url = "https://github.com/caoxnn/mp-plugins"
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
    # 两台 TR 上实际有哪些站：{下载器: {站名: {keys:[...], count, upload, peer}}}，存 data/sites
    _sites: Dict[str, Any] = {}
    # 最近一次的配置原文（新增/改规则后回写配置页时用它合并，免得把其它键冲掉）
    _config: Dict[str, Any] = {}
    # 最近巡检记录
    _history: List[dict] = []

    def init_plugin(self, config: dict = None):
        self._rules = self.get_data("rules") or {}
        self._sites = self.get_data("sites") or {}
        self._history = self.get_data("history") or []

        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron") or "0 */6 * * *"
            self._downloaders = list(config.get("downloaders") or [])
            self._config = dict(config)
            self.__apply_box_config(config)
            self.__prune_box_keys(config)
            self.__save_rules()

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
    _pri_cache: Optional[Dict[str, int]] = None
    _pri_at: float = 0.0

    @classmethod
    def __site_pri(cls) -> Dict[str, int]:
        """站点顺序：跟 MP「站点配置」页一致 —— 按 pri，用的就是官方插件那套
        `SiteOper().list_order_by_pri()`（bangumicoll / agentresourceofficer 都这么取）。10 分钟缓存。
        """
        now = time.time()
        if cls._pri_cache is not None and now - cls._pri_at < 600:
            return cls._pri_cache
        table: Dict[str, int] = {}
        try:
            from app.db.oper.site import SiteOper  # noqa: PLC0415
            for idx, site in enumerate(SiteOper().list_order_by_pri() or []):
                name = str(getattr(site, "name", "") or "").strip()
                if name and name not in table:
                    table[name] = idx
        except Exception as err:  # noqa: BLE001
            logger.debug(f"站点限速：读取站点优先级失败 - {err}")
        cls._pri_cache, cls._pri_at = table, now
        return table

    @classmethod
    def __sort_names(cls, names: Any) -> List[str]:
        """站点按 MP 站点配置页的顺序排；MP 里没有的排最后（按名字）。"""
        pri = cls.__site_pri()
        return sorted(names, key=lambda one: (pri.get(str(one), 10 ** 6), str(one)))

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

    @staticmethod
    def __fix_nodes(node: Any) -> Any:
        """VText 不是 Vuetify 组件（MP 页面上根本不渲染）→ 换成官方写法：div + 顶层 text。

        官方插件里文本一律是 `{"component": "div", "props": {"class": ...}, "text": "..."}`
        （文本框类 Vuetify 组件的 props.text 才有效，比如 VAlert/VTextField）。
        """
        if isinstance(node, list):
            return [TrackerLimit.__fix_nodes(one) for one in node]
        if isinstance(node, dict):
            out = {key: TrackerLimit.__fix_nodes(value) for key, value in node.items()}
            if out.get("component") == "VText":
                props = dict(out.get("props") or {})
                text = props.pop("text", out.get("text"))
                out["component"] = "div"
                out["props"] = props
                out["text"] = text
            return out
        return node

    @classmethod
    def __runnable_downloaders(cls) -> List[str]:
        """要管的下载器 = MP 里配的、类型是 Transmission 的那几台（qB 自动跳过）。

        顺序跟 MP 下载器配置一致；勾过下载器就只留勾过的。拿不到实例列表时退回勾选值。
        """
        try:
            trans = list(cls.__transmission_instances() or {})
            if trans:
                checked = [one for one in trans if one in (cls._downloaders or [])]
                return checked or trans
        except Exception as err:  # noqa: BLE001
            logger.debug(f"站点限速：读取下载器实例失败 - {err}")
        return list(cls._downloaders or [])

    @staticmethod
    def __san(value: Any) -> str:
        """配置项用的后缀：只留字母数字"""
        return re.sub(r"[^0-9a-zA-Z]+", "_", str(value or ""))

    @staticmethod
    def __box_token(value: Any) -> str:
        """配置页那两个框的字段名片段。

        必须保留中文 —— MP 的站点名多是中文（馒头/彩虹岛…），只留 ASCII 的话
        所有中文站名都会挤成同一个 key（实测 28 个站只出 4 个 key）。
        """
        text = re.sub(r"[\s/\\:;,.|#+*?<>\[\]()\"'`=]+", "_", str(value or "").strip())
        return text or "site"

    def __box_keys(self) -> Dict[str, str]:
        """当前该有的框字段名：{字段名: 站名}（配置页那两个框）。"""
        out: Dict[str, str] = {}
        for dl in self.__runnable_downloaders() or []:
            for name in (self.__managed_sites(dl) or {}):
                for field in ("up", "pw"):
                    out[f"{field}_{self.__box_token(dl)}__{self.__box_token(name)}"] = name
        return out

    def __prune_box_keys(self, config: Dict[str, Any]) -> None:
        """清掉老方案留在配置里的框字段（配置里越攒越多，页面模型也跟着变脏）。"""
        valid = set(self.__box_keys())
        stale = [str(key) for key in (config or {})
                 if str(key).startswith(("up_", "pw_")) and str(key) not in valid]
        if not stale:
            return
        conf = {key: value for key, value in (config or {}).items() if str(key) not in stale}
        self._config = conf
        self.update_config(conf)
        logger.info(f"站点限速：清掉 {len(stale)} 个过期的框字段")

    def __mirror_box_values(self) -> None:
        """把每个站当前的限速 / peer 镜像进插件数据与配置 —— 配置页那两个框靠它显示当时值。

        只在巡检完 / 保存后调用，**不在渲染页面时写**（渲染里写配置会让宿主重载插件、把这次渲染打断）。
        """
        mirror: Dict[str, Any] = {}
        for dl in self.__runnable_downloaders() or []:
            for name, info in (self.__managed_sites(dl) or {}).items():
                token = self.__box_token(name)
                mirror[f"up_{self.__box_token(dl)}__{token}"] = _as_int(info.get("upload")) or 0
                mirror[f"pw_{self.__box_token(dl)}__{token}"] = _as_int(info.get("peer")) or 0
        if not mirror:
            return
        for key, value in mirror.items():
            self.save_data(key, value)
        if not self._config:
            return
        conf = {k: v for k, v in self._config.items()
                if not str(k).startswith(("up_", "pw_")) or k in mirror}
        if all(str(conf.get(k)) == str(v) for k, v in mirror.items()) and \
                len(conf) == len(self._config):
            return
        conf.update(mirror)
        try:
            self.update_config(conf)
            self._config = conf
        except Exception as err:  # noqa: BLE001
            logger.debug(f"站点限速：回写输入框默认值失败 - {err}")

    def __managed_sites(self, downloader: str) -> Dict[str, Dict[str, Any]]:
        """这个下载器上「已经有规则」的站：{站名: {keys:[...], upload, peer}}"""
        out: Dict[str, Dict[str, Any]] = {}
        for _key, rule in self._rules.items():
            if str(rule.get("downloader") or "") != downloader:
                continue
            name = self.__label(rule.get("tracker"))
            slot = out.setdefault(name, {"keys": [], "upload": None, "peer": None})
            tracker = str(rule.get("tracker") or "")
            if tracker and tracker not in slot["keys"]:
                slot["keys"].append(tracker)
            slot["upload"] = _as_int(rule.get("upload_kbps")) or 0
            slot["peer"] = _as_int(rule.get("peer_limit")) or 0
        return out

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
            inventory = self._sites.setdefault(name, {})
            for torrent in torrents:
                hash_string = str(_attr(torrent, "hash_string", "") or "")
                if not hash_string:
                    continue
                announces = self.__announces(torrent)
                # 顺手记下这台下载器上实际有哪些站（配置页就照这个列出来给人填）
                cur_limited = bool(_attr(torrent, "upload_limited", False))
                cur_upload = int(_attr(torrent, "upload_limit", 0) or 0) if cur_limited else 0
                cur_peer = _as_int(_attr(torrent, "peer_limit", None)) or 0
                for host in self.__hosts(announces):
                    key = SITE_GROUPS.get(host, host)
                    label = self.__label(key)
                    if not label or label == key:
                        continue
                    slot = inventory.setdefault(label, {"keys": [], "count": 0, "upload": 0, "peer": 0})
                    if key not in slot["keys"]:
                        slot["keys"].append(key)
                    slot["count"] = slot.get("count", 0) + 1
                    slot["upload"] = cur_upload
                    slot["peer"] = cur_peer
                rule = self.__match_rule(rules, announces)
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
                self.post_message(mtype=NotificationType.Plugin, title="【站点限速】",
                                  text=f"巡检完成：{detail}（共 {summary['changed']} 个种）")
            except Exception as err:  # noqa: BLE001
                logger.error(f"站点限速：发通知失败 - {err}")
        self.save_data("sites", self._sites)
        self.__mirror_box_values()
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
        """外部自动化用的两个接口（页面上不调它们，所以显式声明 apikey + 具体模型）。"""
        return [{
            "path": "/apply_now",
            "endpoint": self.api_apply_now,
            "methods": ["POST"],
            "auth": "apikey",
            "summary": "立即巡检一次",
            "description": "按规则表巡检并应用限速（同一目标批量改一次）",
            "response_model": schemas.Response[ScanResultData],
        }, {
            "path": "/dry_run",
            "endpoint": self.api_dry_run,
            "methods": ["POST"],
            "auth": "apikey",
            "summary": "干跑一次",
            "description": "只报出会被改的种，不真改",
            "response_model": schemas.Response[ScanResultData],
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
    # 说明（按规范）：页面上的交互走宿主自己的配置保存通道（get_form 的 props.model），
    # 这两个接口是给「外部自动化 / 运维」用的，所以显式声明 auth="apikey"（FAQ03 允许），
    # 并且每个接口都声明具体 response_model、业务数据一律放 data（不放 message）。
    def api_apply_now(self, apikey: str = None) -> schemas.Response[ScanResultData]:
        """立即按规则巡检并应用一次。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response[ScanResultData](success=False, message="API密钥错误")
        try:
            result = self.apply_now() or {}
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：立即巡检失败 - {err} - {traceback.format_exc()}")
            return schemas.Response[ScanResultData](success=False, message=f"巡检失败：{err}")
        return schemas.Response[ScanResultData](
            success=True, message="巡检完成",
            data=ScanResultData(**self.__result_payload(result)))

    def api_dry_run(self, apikey: str = None) -> schemas.Response[ScanResultData]:
        """干跑：只报会被改的种，不动手。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response[ScanResultData](success=False, message="API密钥错误")
        try:
            with lock:
                result = self.scan_and_apply(apply=False) or {}
        except Exception as err:  # noqa: BLE001
            logger.error(f"站点限速：干跑失败 - {err} - {traceback.format_exc()}")
            return schemas.Response[ScanResultData](success=False, message=f"干跑失败：{err}")
        return schemas.Response[ScanResultData](
            success=True, message="干跑完成（没有实际改动）",
            data=ScanResultData(**self.__result_payload(result, dry=True)))

    @staticmethod
    def __result_payload(result: Dict[str, Any], dry: bool = False) -> Dict[str, Any]:
        """把巡检结果整理成 ScanResultData 的字段（业务数据）。"""
        return {
            "scanned": _as_int(result.get("scanned")) or 0,
            "matched": _as_int(result.get("matched")) or 0,
            "changed": _as_int(result.get("changed")) or 0,
            "dry": bool(dry or result.get("dry")),
            "errors": [str(one) for one in (result.get("errors") or [])],
        }

    def __apply_box_config(self, config: Dict[str, Any]) -> None:
        """配置页「站点清单」里那两个框 → 写进规则。

        字段名 `up_<下载器>__<站名>` / `pw_<下载器>__<站名>`（非字母数字换成 _，站名用 MP 站点名）。
        留空 = 这一项不动；填 0 = 不限速 / 不限制。同名站（多个 tracker 域名）一起改。
        """
        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S")
        changed: List[str] = []
        bad: List[str] = []
        for dl in self.__runnable_downloaders() or []:
            sites = self.__managed_sites(dl)
            if not sites:
                continue
            name_of = {self.__box_token(name): name for name in sites}
            for key, raw in (config or {}).items():
                matched = re.fullmatch(r"(up|pw)_(.+?)__(.+)", str(key))
                if not matched or self.__box_token(dl) != matched.group(2):
                    continue
                name = name_of.get(matched.group(3))
                if not name or raw is None or str(raw).strip() == "":
                    continue
                key_name = "upload_kbps" if matched.group(1) == "up" else "peer_limit"
                want = _as_int(raw)
                if want is None or want < 0:
                    bad.append(f"{name} 的{key_name}")
                    continue
                for tracker in sites[name]["keys"]:
                    rule_key = self.rule_key(dl, tracker)
                    rule = self._rules.get(rule_key) or {"downloader": dl, "tracker": tracker,
                                                         "enabled": True}
                    if (_as_int(rule.get(key_name)) or 0) != want:
                        changed.append(f"{name}｜{dl}｜{key_name} "
                                       f"{_as_int(rule.get(key_name)) or 0} → {want}")
                    rule[key_name] = want
                    rule["updated"] = now
                    self._rules[rule_key] = rule
        if changed:
            self.__save_rules()
            self.__mirror_box_values()
            logger.info(f"站点限速：配置页保存 {len(changed)} 项 - {'；'.join(changed[:10])}")
        if bad:
            logger.warning(f"站点限速：配置里填的不是数字 - {'、'.join(bad[:5])}")

    def __state_card(self) -> dict:
        """状态卡 + 立即巡检 / 干跑（配置页就是首页，按钮放这儿）"""
        last = self._history[0] if self._history else None
        bits = [
            f"启用：{'是' if self._enabled else '否'}",
            f"周期：{self._cron or '未设置'}",
            f"管 {len(self._downloaders)} 台",
            f"规则 {len([r for r in self._rules.values() if r.get('enabled', True)])} 条",
        ]
        if last:
            bits.append(f"上次巡检：{last.get('time')} · 扫描 {last.get('scanned', 0)} · 改 {last.get('changed', 0)}"
                        + (f" · 错误 {len(last.get('errors') or [])}" if last.get("errors") else ""))
        # 按钮不自己做了：要马上跑一次就用 MP 标准的「立即运行一次」开关
        return {
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "站点限速（Transmission）"},
                {"component": "VCardText", "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [
                            {"component": "VText", "props": {"text": " ｜ ".join(bits)}}]},
                    ]},
                ]},
            ]
        }

    def __history_card(self) -> dict:
        """最近巡检记录 + 上一轮改了哪些种（页面上不出现下载器的真名，统一叫「下载器 N」）"""
        names = self.__runnable_downloaders() or []
        label_of = {str(dl): f"下载器 {idx}"
                    for idx, dl in enumerate(names, start=1)}
        label_of.update({str(dl).lower(): f"下载器 {idx}"
                         for idx, dl in enumerate(names, start=1)})

        def hide(value: Any) -> str:
            """把下载器真名换成「下载器 N」——只在它是独立词时换，
            免得把 transmission 这类词里的 tr 也换掉。"""
            text = str(value or "")
            for raw, label in sorted(label_of.items(), key=lambda kv: -len(kv[0])):
                if not raw:
                    continue
                text = re.sub(r"(?<![0-9A-Za-z_])" + re.escape(raw) + r"(?![0-9A-Za-z_])",
                              label, text)
            return text

        last = self._history[0] if self._history else None
        rows: List[dict] = []
        if last and last.get("rows"):
            rows.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VText", "props": {
                        "class": "text-subtitle-2",
                        "text": f"上一轮改动（{last.get('time')}，共 {last.get('changed', 0)} 个种）"}}]}]})
            for row in last["rows"][:30]:
                rows.append({"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": hide(row.get("downloader"))}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": str(row.get("name") or "")}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": f"上传 {row.get('upload')}"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption",
                                                         "text": f"peer {row.get('peer')}"}}]}]})
        for item in self._history[:8]:
            rows.append({"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VText", "props": {"class": "text-caption",
                                                     "text": str(item.get("time") or "")}}]},
                {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                    {"component": "VText", "props": {"class": "text-caption",
                                                     "text": f"扫描 {item.get('scanned', 0)}"}}]},
                {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                    {"component": "VText", "props": {"class": "text-caption",
                                                     "text": f"命中 {item.get('matched', 0)}"}}]},
                {"component": "VCol", "props": {"cols": 4, "md": 2}, "content": [
                    {"component": "VText", "props": {"class": "text-caption",
                                                     "text": f"改 {item.get('changed', 0)}"}}]},
                {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                    {"component": "VText", "props": {
                        "class": "text-caption text-medium-emphasis",
                        "text": ("错误：" + hide("；".join(item.get("errors") or []))[:80])
                                if item.get("errors") else "OK"}}]}]})
        return {
            "component": "VCard",
            "props": {"variant": "tonal"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"},
                 "text": "巡检记录"},
                {"component": "VCardText", "content": rows or [
                    {"component": "VText", "props": {"class": "text-caption", "text": "还没跑过。"}}]},
            ]
        }

    # ------------------------------------------------------------------- 配置页
    def __site_box_rows(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置页「站点清单」：一行一个站 —— 站名 + 上传限速 + peer 两个框。

        顺序跟 MP 站点配置页一致（按 pri，见 __site_pri）；字段名
        `up_<下载器>__<站名>` / `pw_<下载器>__<站名>`，直接走宿主自己的配置保存
        （`props.model` + 页面「保存」），插件在 init_plugin 里把它翻成规则。
        """
        rows: List[dict] = []
        model: Dict[str, Any] = {}
        for idx, dl in enumerate(self.__runnable_downloaders() or [], start=1):
            sites = self.__managed_sites(dl)
            if not sites:
                continue
            rows.append({
                "component": "VRow",
                "props": {"dense": True},
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VText", "props": {
                            "class": "text-subtitle-2", "text": f"下载器 {idx}（{len(sites)} 个站）"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {
                            "class": "text-caption", "text": "上传限速 KB/s"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption", "text": "peer 上限"}}]},
                ]
            })
            for name in self.__sort_names(sites):
                upload = _as_int(sites[name].get("upload")) or 0
                peer = _as_int(sites[name].get("peer")) or 0
                up_key = f"up_{self.__box_token(dl)}__{self.__box_token(name)}"
                pw_key = f"pw_{self.__box_token(dl)}__{self.__box_token(name)}"
                model[up_key] = upload
                model[pw_key] = peer
                rows.append({
                    "component": "VRow",
                    "props": {"dense": True, "align": "center"},
                    "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VText", "props": {"text": name}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": up_key, "type": "number",
                                       "label": "限速 KB/s",
                                       "density": "compact", "variant": "outlined",
                                       "hide-details": True}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VTextField",
                             "props": {"model": pw_key, "type": "number",
                                       "label": "peer",
                                       "density": "compact", "variant": "outlined",
                                       "hide-details": True}}]},
                    ]
                })
        return rows, model

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        box_rows, box_model = self.__site_box_rows()
        return self.__fix_nodes([
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
                    *box_rows,
                ]
            }
            , self.__state_card()]), {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "cron": "0 */6 * * *",
            "downloaders": [],
            **box_model,
        }

    # ------------------------------------------------------------------- 详情页
    def get_page(self) -> Optional[List[dict]]:
        """详情页：站点清单（只读）+ 巡检记录。

        按 MP 的规矩，能编辑的东西放在配置页（`props.model`，宿主保存后随配置提交），
        详情页只做展示 —— 所以这里不放输入框。
        """
        rows: List[dict] = []
        for idx, dl in enumerate(self.__runnable_downloaders() or [], start=1):
            sites = self.__managed_sites(dl)
            if not sites:
                continue
            rows.append({
                "component": "VRow",
                "props": {"dense": True},
                "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VText", "props": {
                            "class": "text-subtitle-2", "text": f"下载器 {idx}（{len(sites)} 个站）"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {
                            "class": "text-caption", "text": "上传限速 KB/s"}}]},
                    {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                        {"component": "VText", "props": {"class": "text-caption", "text": "peer 上限"}}]},
                ]
            })
            for name in self.__sort_names(sites):
                rows.append({
                    "component": "VRow",
                    "props": {"dense": True},
                    "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                            {"component": "VText", "props": {"text": name}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VText", "props": {
                                "text": str(_as_int(sites[name].get("upload")) or 0)}}]},
                        {"component": "VCol", "props": {"cols": 6, "md": 2}, "content": [
                            {"component": "VText", "props": {
                                "text": str(_as_int(sites[name].get("peer")) or 0)}}]},
                    ]
                })

        page: List[dict] = [{
            "component": "VCard",
            "props": {"variant": "tonal", "class": "mb-4"},
            "content": [
                {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "站点清单"},
                {"component": "VCardText", "props": {"class": "pt-1"}, "content": rows or [
                    {"component": "VText", "props": {"class": "text-caption", "text": "（还没有规则）"}}]},
            ]
        }, self.__history_card()]
        return self.__fix_nodes(page)

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
