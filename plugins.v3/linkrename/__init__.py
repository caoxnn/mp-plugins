"""硬链接重命名 —— 监控目录文件变化，硬链接到目标目录，再按 MoviePilot 自己的
重命名格式（`settings.RENAME_FORMAT`）给**硬链接**改名。

骨架取自官方插件「实时硬链接」(linkmonitor 1.7.1)，只多三步：

  ① 规则表：把源名换成"能被识别"的名字（认剧 + 补集号；
     集号可写 `{ep:tmdbid:season}`，用名字里的 8 位播出日期去 TMDB 反查）
  ② 识别：`MediaChain().recognize_media(meta=MetaInfoPath(...))`
  ③ 改名：`TransHandler().get_rename_path(template_string=settings.RENAME_FORMAT(type), ...)`

⚠️ 改的是【硬链接的名字】，**原文件一个字节都不动** —— 做种 / 转种 / 其他站辅种全不受影响。
⚠️ 识别不上就【保留原名】（绝不因为认不出就不链），可发通知告警。
"""
import datetime
import re
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchfiles import Change, watch

from app import schemas
from app.chain.media import MediaChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.modules.filemanager.transhandler import TransHandler
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType
from app.sdk.media import MetaInfoPath
from app.sdk.utilities import SystemUtils

lock = threading.Lock()

# 认作"媒体文件"的后缀（只对这些做改名，其它照原样链）
MEDIA_EXTENSIONS = {".mkv", ".mp4", ".ts", ".m2ts", ".avi", ".mov", ".wmv", ".flv", ".rmvb", ".iso"}


def _has_suffix_in(file_path: Path, extensions: List[str]) -> bool:
    """判断文件后缀是否命中给定扩展名列表。"""
    if not file_path.suffix:
        return False
    return file_path.suffix.casefold() in {ext.casefold() for ext in extensions}


def _is_download_tmp_file(file_path: Path) -> bool:
    """判断文件是否为下载器尚未完成的临时文件。"""
    return _has_suffix_in(file_path, settings.DOWNLOAD_TMPEXT)


class WatchfilesEvent:
    """watchfiles 目录监控事件。"""

    def __init__(self, src_path: str, is_directory: bool):
        self.src_path = src_path
        self.dest_path = src_path
        self.is_directory = is_directory


class WatchfilesObserver:
    """基于 watchfiles 的目录监控适配器。"""

    def __init__(self, timeout: int = 10, force_polling: Optional[bool] = None):
        self._timeout = timeout
        self._force_polling = force_polling
        self._stopped = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._handler = None
        self._path = None

    def schedule(self, handler: Any, path: str, recursive: bool = True):
        self._handler = handler
        self._path = path

    def start(self):
        self._stopped.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stopped.set()

    def join(self, timeout: Optional[float] = None):
        if self._thread:
            self._thread.join(timeout=timeout)

    @property
    def daemon(self):
        return True

    @daemon.setter
    def daemon(self, value):
        pass

    def _run(self):
        try:
            self._run_watch(self._force_polling)
        except Exception as err:  # noqa: BLE001
            logger.error("目录监控线程异常：%s" % err)

    def _run_watch(self, force_polling: Optional[bool]):
        for changes in watch(self._path, stop_event=self._stopped, recursive=True,
                             force_polling=force_polling, debounce=2000, step=500):
            if self._stopped.is_set():
                break
            for change_type, event_path in changes:
                try:
                    self._handler.dispatch(change_type, event_path)
                except Exception as err:  # noqa: BLE001
                    logger.error("处理目录事件失败：%s - %s" % (err, traceback.format_exc()))


class FileMonitorHandler:
    """目录监控响应类。"""

    def __init__(self, monpath: str, sync: Any):
        self._watch_path = monpath
        self.sync = sync

    def dispatch(self, change_type: Change, event_path: str):
        if change_type not in {Change.added, Change.modified}:
            return
        path = Path(event_path)
        if not path.exists():
            return
        is_directory = path.is_dir()
        if not is_directory and _is_download_tmp_file(path):
            return
        event = WatchfilesEvent(src_path=event_path, is_directory=is_directory)
        text = "修改" if change_type == Change.modified else "创建"
        self.sync.event_handler(event=event, text=text,
                                mon_path=self._watch_path, event_path=event_path)


class LinkRename(_PluginBase):
    # 插件名称
    plugin_name = "硬链接重命名"
    # 插件描述
    plugin_desc = "监控目录文件变化 → 硬链接到目标目录 → 按 MoviePilot 重命名格式给【链接】改名（原文件不动）。"
    # 插件图标
    plugin_icon = "Linkace_C.png"
    # 插件版本
    plugin_version = "0.2.1"
    # 插件作者
    plugin_author = "nc17"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "linkrename_"
    # 加载顺序
    plugin_order = 5
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _scheduler = None
    _observer = []
    _enabled = False
    _notify = False
    _onlyonce = False
    _cron = None
    _size = 0
    _mode = "fast"
    _monitor_dirs = ""
    _exclude_keywords = ""
    # 是否按 MP 重命名格式改名
    _rename = True
    # 是否保留源目录结构（只改文件名，不套媒体库目录）
    _keep_structure = True
    # 规则表
    _rules = ""
    # 面板「添加规则」用的三个输入框
    _rule_name = ""
    _rule_tmdb = ""
    _rule_season = ""
    # 源目录 → 目的目录
    _dirconf: Dict[str, Optional[Path]] = {}
    # 退出事件
    _event = threading.Event()
    # 改名历史（详情页用）
    _history: List[dict] = []

    def init_plugin(self, config: dict = None):
        self._dirconf = {}
        self._observer = []
        self._history = self.get_data("history") or []

        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._mode = config.get("mode")
            self._monitor_dirs = config.get("monitor_dirs") or ""
            self._exclude_keywords = config.get("exclude_keywords") or ""
            self._cron = config.get("cron")
            self._size = config.get("size") or 0
            self._rename = config.get("rename", True)
            self._keep_structure = config.get("keep_structure", True)
            self._rules = config.get("rules") or ""
            self._rule_name = config.get("rule_name") or ""
            self._rule_tmdb = config.get("rule_tmdb") or ""
            self._rule_season = config.get("rule_season") or ""

        self.stop_service()

        if self._enabled or self._onlyonce:
            monitor_dirs = self._monitor_dirs.split("\n")
            if not monitor_dirs:
                return
            for mon_path in monitor_dirs:
                if not mon_path:
                    continue
                if SystemUtils.is_windows():
                    if mon_path.count(":") > 1:
                        paths = [mon_path.split(":")[0] + ":" + mon_path.split(":")[1],
                                 mon_path.split(":")[2] + ":" + mon_path.split(":")[3]]
                    else:
                        paths = [mon_path]
                else:
                    paths = mon_path.split(":")

                if len(paths) > 1:
                    mon_path = paths[0]
                    target_path = Path(paths[1])
                    self._dirconf[mon_path] = target_path
                else:
                    logger.warn(f"{mon_path} 未配置目的目录，将不会进行硬链接")
                    self.systemmessage.put(f"{mon_path} 未配置目的目录，将不会进行硬链接！",
                                           title="硬链接重命名")
                    continue

                if self._enabled:
                    try:
                        if target_path and target_path.is_relative_to(Path(mon_path)):
                            logger.warn(f"{target_path} 是监控目录 {mon_path} 的子目录，无法监控")
                            self.systemmessage.put(
                                f"{target_path} 是下载目录 {mon_path} 的子目录，无法监控",
                                title="硬链接重命名")
                            continue
                    except Exception as e:  # noqa: BLE001
                        logger.debug(str(e))

                    try:
                        if self._mode == "compatibility":
                            observer = WatchfilesObserver(timeout=10, force_polling=True)
                        else:
                            observer = WatchfilesObserver(timeout=10, force_polling=None)
                        self._observer.append(observer)
                        observer.schedule(FileMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                        observer.daemon = True
                        observer.start()
                        logger.info(f"{mon_path} 的目录监控服务启动（改名：{'开' if self._rename else '关'}）")
                    except Exception as e:  # noqa: BLE001
                        err_msg = str(e)
                        if "inotify" in err_msg and "reached" in err_msg:
                            logger.warn(f"目录监控服务启动出现异常：{err_msg}")
                        else:
                            logger.error(f"{mon_path} 启动目录监控失败：{err_msg}")
                        self.systemmessage.put(f"{mon_path} 启动目录监控失败：{err_msg}",
                                               title="硬链接重命名")

            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info("目录监控服务启动，立即运行一次")
                self._scheduler.add_job(func=self.sync_all, trigger='date',
                                        run_date=datetime.datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3))
                self._onlyonce = False
                self.__update_config()
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def __update_config(self):
        """更新配置"""
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "mode": self._mode,
            "monitor_dirs": self._monitor_dirs,
            "exclude_keywords": self._exclude_keywords,
            "cron": self._cron,
            "size": self._size,
            "rename": self._rename,
            "keep_structure": self._keep_structure,
            "rules": self._rules,
            "rule_name": self._rule_name,
            "rule_tmdb": self._rule_tmdb,
            "rule_season": self._rule_season,
        })

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        """远程全量同步"""
        if not event:
            return
        event_data = event.event_data or {}
        if event_data.get("action") != "linkrename":
            return
        self.post_message(mtype=NotificationType.Plugin,
                          title="开始全量硬链接重命名 ...",
                          text="", userid=event_data.get("user"))
        self.sync_all()
        self.post_message(mtype=NotificationType.Plugin,
                          title="全量硬链接重命名完成！",
                          text="", userid=event_data.get("user"))

    def sync_all(self):
        """全量执行一次"""
        logger.info("开始全量硬链接重命名 ...")
        for mon_path, target_path in self._dirconf.items():
            if not mon_path or not target_path:
                continue
            for root, _, files in Path(mon_path).walk():
                for fname in files:
                    fpath = root / fname
                    if _is_download_tmp_file(fpath):
                        continue
                    try:
                        self.event_handler(event=WatchfilesEvent(src_path=str(fpath), is_directory=False),
                                           text="全量", mon_path=mon_path, event_path=str(fpath))
                    except Exception as e:  # noqa: BLE001
                        logger.error(f"处理 {fpath} 出错：{e}")
        logger.info("全量硬链接重命名完成！")

    def event_handler(self, event, mon_path: str, text: str, event_path: str):
        """处理文件事件"""
        if not event or not mon_path or not event_path:
            return
        if event.is_directory:
            return
        self.__handle_file(event_path=event_path, mon_path=mon_path)

    @staticmethod
    def _link_file(src_path: Path, mon_path: str, target_path: Path,
                   transfer_type: str = "link") -> Tuple[bool, str, Optional[Path]]:
        """对文件做纯链接处理（不做识别重命名），照原文件名+原相对路径。"""
        try:
            rel_path = src_path.relative_to(Path(mon_path))
        except ValueError:
            return False, "文件路径不在监控目录内", None
        new_path = target_path / rel_path
        if new_path.exists():
            return True, "目标路径文件已存在", new_path
        if not new_path.parent.exists():
            new_path.parent.mkdir(parents=True, exist_ok=True)
        if transfer_type == "copy":
            code, errmsg = SystemUtils.copy(src_path, new_path)
        else:
            code, errmsg = SystemUtils.link(src_path, new_path)
        return (True, errmsg, new_path) if code == 0 else (False, errmsg, None)

    # ────────────────────────── 规则表 / 补集号 ──────────────────────────

    def __probe_name(self, link_path: Path) -> str:
        """规则表把源名换成"能被识别"的名字；没配规则或没命中 → 返回原名。"""
        replaced = self.__apply_rules(link_path.name)
        if not replaced:
            return link_path.name
        if replaced.lower().endswith(link_path.suffix.lower()):
            return replaced
        return replaced + link_path.suffix

    def __apply_rules(self, name: str) -> str:
        """规则表：每行 `正则 => 替换`（# 开头跳过）。
        替换里可写 `{ep:tmdbid:season}` —— 用名字里的 8 位播出日期去 TMDB 反查集号。"""
        for raw in (self._rules or "").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=>" not in line:
                continue
            pattern, repl = line.split("=>", 1)
            pattern, repl = pattern.strip(), repl.strip()
            # 傻瓜写法：右边是 TMDB 编号（可带 :季号），左边只要名字里出现就算命中
            simple = re.fullmatch(r"(\d{1,9})(?::(\d{1,3}))?", repl)
            if simple:
                if self.__norm_key(pattern) not in self.__norm_key(name):
                    continue
                new_name = self.__simple_rule_name(
                    int(simple.group(1)),
                    int(simple.group(2)) if simple.group(2) else None,
                    name)
                if new_name:
                    return new_name
                continue
            try:
                if not re.search(pattern, name, flags=re.IGNORECASE):
                    continue
            except re.error as e:  # noqa: BLE001
                logger.warn(f"规则正则无效「{pattern}」：{e}")
                continue
            repl = self.__expand_ep(repl, name)
            try:
                return re.sub(pattern, repl, name, count=1, flags=re.IGNORECASE)
            except re.error as e:  # noqa: BLE001
                logger.warn(f"规则替换失败「{pattern}」：{e}")
                continue
        return ""

    def __expand_ep(self, repl: str, name: str) -> str:
        """展开替换里的 `{ep:tmdbid:season}`。"""
        def _sub(m):
            tmdb_id, season = int(m.group(1)), int(m.group(2))
            dm = re.search(r"(20\d{2})(0[1-9]|1[0-2])([0-2]\d|3[01])", name)
            if not dm:
                logger.warn(f"名字里没有 8 位播出日期，无法补集号：{name}")
                return ""
            ep = self.__episode_by_air_date(tmdb_id, season, dm.group(0))
            if ep is None:
                logger.warn(f"TMDB 查不到该日期的集号（tmdb={tmdb_id} S{season} {dm.group(0)}）")
                return ""
            return str(ep)
        return re.sub(r"\{ep:(\d+):(\d+)\}", _sub, repl)

    @staticmethod
    def __episode_by_air_date(tmdb_id: int, season: int, yyyymmdd: str) -> Optional[int]:
        """按播出日期在 TMDB 该季里反查集号；查不到精确日期就用"已播出集号 + 1"兜底。"""
        try:
            from app.modules.themoviedb.tmdbv3api.objs.season import Season
        except Exception as e:  # noqa: BLE001
            logger.warn(f"TMDB 模块不可用：{e}")
            return None
        want = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
        try:
            detail = Season().details(tmdb_id, season)
        except Exception as e:  # noqa: BLE001
            logger.warn(f"查 TMDB 季信息失败：{e}")
            return None
        if hasattr(detail, "as_dict"):
            detail = detail.as_dict()
        if not isinstance(detail, dict):
            detail = getattr(detail, "__dict__", {}) or {}
        episodes = detail.get("episodes") or []
        for ep in episodes:
            if (ep.get("air_date") or "") == want:
                return int(ep.get("episode_number"))
        aired = [ep for ep in episodes if (ep.get("air_date") or "") and ep["air_date"] < want]
        if aired:
            return max(int(ep.get("episode_number")) for ep in aired) + 1
        return None

    # ────────────────────────── 改名 ──────────────────────────

    def __rename_link(self, link_path: Path, target_root: Path):
        """按规则表 + MP 识别 + MP 重命名格式，给硬链接改名。失败就保留原名。"""
        try:
            if link_path.suffix.lower() not in MEDIA_EXTENSIONS:
                return
            probe_name = self.__probe_name(link_path)
            meta = MetaInfoPath(link_path.parent / probe_name)
            mediainfo = MediaChain().recognize_media(meta=meta)
            if not mediainfo:
                logger.warn(f"{link_path.name} 识别失败，保留原名（用于识别的名字：{probe_name}）")
                self.__record(link_path, "", "识别失败，保留原名", ok=False)
                if self._notify:
                    self.post_message(mtype=NotificationType.Manual,
                                      title=f"硬链接未改名：{link_path.name}",
                                      text=f"识别失败，保留原名\n用于识别：{probe_name}")
                return
            rename_format = settings.RENAME_FORMAT(mediainfo.type)
            if not rename_format:
                logger.warn("重命名格式未配置，跳过改名")
                return
            handler = TransHandler()
            rename_dict = handler.get_naming_dict(meta=meta, mediainfo=mediainfo,
                                                  file_ext=link_path.suffix, episodes_info=None)
            new_file = handler.get_rename_path(template_string=rename_format,
                                               rename_dict=rename_dict,
                                               path=target_root,
                                               source_path=str(link_path))
            # 保留源目录结构 → 只取新文件名；否则套 MP 的媒体库目录结构
            final = link_path.parent / new_file.name if self._keep_structure else new_file
            if final == link_path:
                logger.info(f"{link_path.name} 名字已经是对的，跳过")
                return
            if final.exists():
                logger.warn(f"{final} 已存在，跳过改名")
                self.__record(link_path, final, "目标已存在，跳过", ok=False)
                return
            final.parent.mkdir(parents=True, exist_ok=True)
            # ⚠️ 只改硬链接的名字，源文件（做种那份）不动
            link_path.rename(final)
            logger.info(f"硬链接改名：{link_path.name} → {final}")
            self.__record(link_path, final, "改名成功", ok=True)
            if self._notify:
                self.post_message(mtype=NotificationType.Manual,
                                  title=f"硬链接已改名：{link_path.name}",
                                  text=f"{final}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"改名失败：{e} - {traceback.format_exc()}")

    def __record(self, src: Path, dest: Path, msg: str, ok: bool):
        """记一条改名历史（详情页用）"""
        try:
            self._history.insert(0, {
                "time": datetime.datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d %H:%M:%S"),
                "src": src.name,
                "dest": (dest.name if dest else ""),
                "msg": msg,
                "ok": ok,
            })
            self._history = self._history[:50]
            self.save_data("history", self._history)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"记历史失败：{e}")

    def __handle_file(self, event_path: str, mon_path: str):
        """同步一个文件：先照原样硬链接，再按 MP 格式给链接改名。"""
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            if _is_download_tmp_file(file_path):
                return
            with lock:
                if event_path.find('/@Recycle/') != -1 \
                        or event_path.find('/#recycle/') != -1 \
                        or event_path.find('/.') != -1 \
                        or event_path.find('/@eaDir') != -1:
                    logger.debug(f"{event_path} 是回收站或隐藏的文件")
                    return

                if self._exclude_keywords:
                    for keyword in self._exclude_keywords.split("\n"):
                        if keyword and re.findall(keyword, event_path):
                            logger.info(f"{event_path} 命中过滤关键字 {keyword}，不处理")
                            return

                if self._size and float(self._size) > 0 \
                        and file_path.stat().st_size < float(self._size) * 1024:
                    logger.info(f"{event_path} 文件大小小于最小文件大小，复制...")
                    _transfer_type = "copy"
                else:
                    _transfer_type = "link"

                target: Path = self._dirconf.get(mon_path)
                if not target:
                    logger.warn(f"{mon_path} 未配置目的目录，将不会进行硬链接")
                    return

                state, errmsg, new_path = self._link_file(src_path=file_path, mon_path=mon_path,
                                                          target_path=target,
                                                          transfer_type=_transfer_type)

                if not state:
                    logger.warn(f"{file_path.name} 硬链接失败：{errmsg}")
                    if self._notify:
                        self.post_message(mtype=NotificationType.Manual,
                                          title=f"{file_path.name} 硬链接失败！",
                                          text=f"原因：{errmsg or '未知'}")
                    return

                # 目标已存在说明以前链过，不再重复改名（避免把用户手动改好的名字覆盖掉）
                if errmsg == "目标路径文件已存在":
                    logger.debug(f"{new_path} 已存在，跳过")
                    return

                if self._rename and new_path:
                    self.__rename_link(link_path=new_path, target_root=target)

        except Exception as e:  # noqa: BLE001
            logger.error("目录监控发生错误：%s - %s" % (str(e), traceback.format_exc()))

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/linkrename",
            "event": EventType.PluginAction,
            "desc": "硬链接重命名",
            "category": "",
            "data": {"action": "linkrename"}
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/linkrename",
            "endpoint": self.sync,
            "methods": ["GET"],
            "summary": "硬链接重命名",
            "description": "硬链接重命名",
        }, {
            "path": "/add_rule",
            "endpoint": self.add_rule,
            "methods": ["POST"],
            "summary": "添加改名规则",
            "description": "面板上填 名字 + TMDB编号(:季号)，自动生成一条规则",
        }, {
            "path": "/del_rule",
            "endpoint": self.del_rule,
            "methods": ["POST"],
            "summary": "删除改名规则",
            "description": "删除一条规则",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务"""
        if self._enabled and self._cron:
            try:
                return [{
                    "id": "LinkRenameSyncAll",
                    "name": "全量硬链接重命名定时服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.sync_all,
                    "kwargs": {}
                }]
            except Exception as err:  # noqa: BLE001
                logger.error(f"定时任务配置错误：{err}")
        return []

    def sync(self, apikey: str, action: str = None) -> schemas.Response:
        """API 触发全量同步"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        if action == "linkrename":
            return schemas.Response(success=False, message="请使用 /linkrename 命令")
        self.sync_all()
        return schemas.Response(success=True, message="已开始全量硬链接重命名")

    # ---------------- 面板「添加规则」与傻瓜规则 ----------------
    @staticmethod
    def __norm_key(text: str) -> str:
        """去掉标点空格后小写，用来宽松比对（CCTV1.Voice 和 CCTV1 Voice 视作相同）。"""
        return re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", (text or "").lower())

    @staticmethod
    def __tv_details(tmdb_id: int) -> dict:
        """取 TMDB 剧集详情（剧名 / 首播日期）。"""
        try:
            from app.modules.themoviedb.tmdbv3api.objs.tv import TV
        except Exception as err:  # noqa: BLE001
            logger.warn(f"TMDB tv 模块不可用：{err}")
            return {}
        try:
            detail = TV().details(tmdb_id)
        except Exception as err:  # noqa: BLE001
            logger.warn(f"取剧集信息失败（tmdb={tmdb_id}）：{err}")
            return {}
        if hasattr(detail, "as_dict"):
            detail = detail.as_dict()
        if not isinstance(detail, dict):
            detail = getattr(detail, "__dict__", {}) or {}
        return detail

    @classmethod
    def __tv_title(cls, tmdb_id: int) -> Optional[str]:
        """剧名（规则右边没写剧名时用它自动填）。"""
        return cls.__tv_details(tmdb_id).get("name") or None

    @classmethod
    def __season_by_year(cls, tmdb_id: int, yyyymmdd: str) -> Optional[int]:
        """季号不填时：按「播出年份 - 首播年份 + 1」算（开讲啦 2012 首播 → 2026 是第 15 季）。"""
        first = str(cls.__tv_details(tmdb_id).get("first_air_date") or "")[:4]
        if not first.isdigit():
            return None
        season = int(yyyymmdd[:4]) - int(first) + 1
        return season if season >= 1 else None

    def __simple_rule_name(self, tmdb_id: int, season: Optional[int], name: str) -> Optional[str]:
        """傻瓜规则：名字里抓播出日期 → 查集号 → 拼成「剧名 S季E集 + 原后缀」。"""
        dm = re.search(r"(20\d{2})(0[1-9]|1[0-2])([0-2]\d|3[01])", name)
        if not dm:
            logger.warn(f"名字里没有 8 位播出日期，傻瓜规则不生效：{name}")
            return None
        date_str = dm.group(0)
        if season is None:
            season = self.__season_by_year(tmdb_id, date_str)
            if season is None:
                logger.warn(f"算不出季号（tmdb={tmdb_id} {date_str}）")
                return None
        episode = self.__episode_by_air_date(tmdb_id, season, date_str)
        if episode is None:
            logger.warn(f"TMDB 查不到集号（tmdb={tmdb_id} S{season} {date_str}）")
            return None
        title = self.__tv_title(tmdb_id)
        if not title:
            logger.warn(f"取不到剧名（tmdb={tmdb_id}），傻瓜规则不生效")
            return None
        return f"{title} S{season:02d}E{episode:02d}{name[dm.end():]}"

    def add_rule(self, apikey: str = None, rule_name: str = None, rule_tmdb: str = None,
                 rule_season: str = None) -> schemas.Response:
        """面板「添加规则」：名字 + TMDB编号(:季号) → 自动拼一条规则写进规则表。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        name = str(rule_name or self._rule_name or "").strip()
        tmdb = str(rule_tmdb or self._rule_tmdb or "").strip()
        season = str(rule_season or self._rule_season or "").strip()
        if not name or not re.fullmatch(r"\d{1,9}", tmdb):
            return schemas.Response(success=False,
                                    message="请先填「名字片段」和「TMDB 编号」（TMDB 编号是纯数字）")
        if season and not re.fullmatch(r"\d{1,3}", season):
            return schemas.Response(success=False, message="季号只能是数字，或者留空（留空=按年份算）")
        line = f"{name} => {tmdb}" + (f":{season}" if season else "")
        lines = [one.strip() for one in (self._rules or "").splitlines() if one.strip()]
        if line in lines:
            return schemas.Response(success=True, message=f"这条规则已经在了：{line}")
        lines.append(line)
        self._rules = "\n".join(lines)
        self._rule_name = name
        self._rule_tmdb = tmdb
        self._rule_season = season
        self.__update_config()
        title = self.__tv_title(int(tmdb)) or name
        logger.info(f"面板添加规则：{line}（剧名：{title}）")
        tail = f" 第{season}季" if season else "（季号按年份自动算）"
        return schemas.Response(success=True, message=f"已添加：{title} → TMDB {tmdb}{tail}")

    def del_rule(self, apikey: str = None, line: str = None) -> schemas.Response:
        """面板「删除规则」。"""
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        target = str(line or "").strip()
        lines = [one.strip() for one in (self._rules or "").splitlines() if one.strip()]
        if target not in lines:
            return schemas.Response(success=False, message=f"没找到这条规则：{target}")
        lines.remove(target)
        self._rules = "\n".join(lines)
        self.__update_config()
        logger.info(f"面板删除规则：{target}")
        return schemas.Response(success=True, message=f"已删除：{target}")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'rename', 'label': '按 MP 格式改名'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {'model': 'keep_structure', 'label': '保留源目录结构'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'mode',
                                            'label': '监控模式',
                                            'items': [
                                                {'title': '兼容模式', 'value': 'compatibility'},
                                                {'title': '性能模式', 'value': 'fast'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'cron', 'label': '定时全量同步周期',
                                                  'placeholder': '5位cron表达式，留空关闭'}
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 3},
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {'model': 'size', 'label': '最小文件大小（KB）',
                                                  'placeholder': '小于此大小直接复制'}
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'monitor_dirs',
                                            'label': '监控目录',
                                            'rows': 3,
                                            'placeholder': '源目录:目的目录，一行一个，目的目录不配置则不处理',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'rules',
                                            'label': '重命名规则（改名时用来"认剧"）',
                                            'rows': 4,
                                            'placeholder': '# 一行一条：正则 => 替换\n'
                                                           '# {ep:tmdbid:season} 用名字里的 8 位播出日期去 TMDB 反查集号\n'
                                                           '^CCTV1\\s+Voice\\s+(\\d{8}) => 开讲啦 S15E{ep:223348:15}',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 5},
                                'content': [
                                    {'component': 'VTextField',
                                     'props': {'model': 'rule_name', 'label': '① 名字片段（照抄）',
                                               'placeholder': '例如 CCTV1 Voice', 'density': 'compact'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 4},
                                'content': [
                                    {'component': 'VTextField',
                                     'props': {'model': 'rule_tmdb', 'label': '② TMDB 编号',
                                               'placeholder': '例如 223348', 'density': 'compact'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 6, 'md': 3},
                                'content': [
                                    {'component': 'VTextField',
                                     'props': {'model': 'rule_season', 'label': '③ 季号（可空）',
                                               'placeholder': '留空=按年份算', 'density': 'compact'}}
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {'component': 'VText',
                                     'props': {'class': 'text-caption text-medium-emphasis',
                                               'text': '填好这三个 → 保存 → 到「详情」页点「添加规则」，自动拼一条规则进下面的规则表（不用手写正则）。'}}
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_keywords',
                                            'label': '过滤关键字',
                                            'rows': 2,
                                            'placeholder': '一行一个正则，命中则不处理'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '先硬链接（原文件名原样）到目的目录，再按规则表 + TMDB 识别 + '
                                                    'MP 的重命名格式给【链接】改名 —— 原文件一个字节不动，'
                                                    '做种/转种/其他站辅种都不受影响。识别不上就保留原名。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12},
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'warning',
                                            'variant': 'tonal',
                                            'text': '⚠️ 改名后源文件若再次触发事件，会在目的目录里按原名再链一份出来；'
                                                    '不想出现重复就把对应路径加进「过滤关键字」。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "rename": True,
            "keep_structure": True,
            "mode": "fast",
            "cron": "",
            "size": 0,
            "monitor_dirs": "",
            "rules": "",
            "exclude_keywords": "",
            "rule_name": "",
            "rule_tmdb": "",
            "rule_season": "",
        }

    def get_page(self) -> List[dict]:
        """详情页：快速添加规则 + 现有规则 + 最近改名记录"""
        # 现有傻瓜规则（列出来可删）
        rule_rows = []
        for one in (self._rules or "").splitlines():
            one = one.strip()
            if not one or one.startswith("#") or "=>" not in one:
                continue
            key, _, right = one.partition("=>")
            if not re.fullmatch(r"\d{1,9}(?::\d{1,3})?", right.strip()):
                continue
            rule_rows.append({
                'component': 'VRow',
                'props': {'align': 'center'},
                'content': [
                    {'component': 'VCol', 'props': {'cols': 7}, 'content': [
                        {'component': 'VText', 'props': {'text': key.strip()}}]},
                    {'component': 'VCol', 'props': {'cols': 3}, 'content': [
                        {'component': 'VChip',
                         'props': {'color': 'primary', 'size': 'small', 'variant': 'tonal'},
                         'content': [{'component': 'VText',
                                      'props': {'text': f'TMDB {right.strip()}'}}]}]},
                    {'component': 'VCol', 'props': {'cols': 2}, 'content': [
                        {'component': 'VBtn',
                         'props': {'size': 'small', 'color': 'error', 'variant': 'tonal'},
                         'text': '删除',
                         'events': {'click': {
                             'api': f"plugin/{self.__class__.__name__}/del_rule?apikey={settings.API_TOKEN}",
                             'method': 'post',
                             'params': {'line': one}}}}]},
                ]
            })

        add_card = {
            'component': 'VCard',
            'props': {'variant': 'tonal', 'class': 'mb-4'},
            'content': [
                {'component': 'VCardTitle',
                 'props': {'class': 'text-subtitle-1'},
                 'text': '添加规则：左边照抄名字里的一段，右边填 TMDB 编号，剧名它自己取'},
                {'component': 'VCardText', 'content': [
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 5}, 'content': [
                            {'component': 'VTextField',
                             'props': {'model': 'rule_name', 'label': '名字片段',
                                       'placeholder': '例如 CCTV1 Voice',
                                       'density': 'compact', 'variant': 'outlined'}}]},
                        {'component': 'VCol', 'props': {'cols': 6, 'md': 3}, 'content': [
                            {'component': 'VTextField',
                             'props': {'model': 'rule_tmdb', 'label': 'TMDB 编号',
                                       'placeholder': '例如 223348',
                                       'density': 'compact', 'variant': 'outlined'}}]},
                        {'component': 'VCol', 'props': {'cols': 6, 'md': 2}, 'content': [
                            {'component': 'VTextField',
                             'props': {'model': 'rule_season', 'label': '季号（可空）',
                                       'placeholder': '留空=按年份',
                                       'density': 'compact', 'variant': 'outlined'}}]},
                        {'component': 'VCol', 'props': {'cols': 12, 'md': 2}, 'content': [
                            {'component': 'VBtn',
                             'props': {'color': 'primary', 'variant': 'flat', 'block': True},
                             'text': '添加规则',
                             'events': {'click': {
                                 'api': f"plugin/{self.__class__.__name__}/add_rule?apikey={settings.API_TOKEN}",
                                 'method': 'post'}}}]},
                    ]},
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 12}, 'content': [
                            {'component': 'VText',
                             'props': {'class': 'text-caption text-medium-emphasis',
                                       'text': '填完点「添加规则」立即生效；季号留空就按「播出年份 − 首播年 + 1」算，跨年不用改。'}}]},
                    ]},
                ]},
                {'component': 'VCardText', 'props': {'class': 'pt-0'}, 'content': rule_rows or [
                    {'component': 'VText',
                     'props': {'class': 'text-caption', 'text': '（还没有傻瓜规则）'}}]},
            ]
        }

        if not self._history:
            return [{
                'component': 'VAlert',
                'props': {'type': 'info', 'variant': 'tonal', 'text': '暂无改名记录。'}
            }]
        rows = []
        for item in self._history:
            rows.append({
                'component': 'VRow',
                'content': [
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VText', 'props': {'text': item.get('time', '')}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 2}, 'content': [
                        {'component': 'VChip',
                         'props': {'color': 'success' if item.get('ok') else 'warning',
                                   'size': 'small', 'variant': 'tonal'},
                         'content': [{'component': 'VText', 'props': {'text': item.get('msg', '')}}]}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                        {'component': 'VText', 'props': {'text': item.get('src', '')}}]},
                    {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                        {'component': 'VText', 'props': {'text': item.get('dest', '')}}]},
                ]
            })
        return [
            add_card,
            {'component': 'VRow', 'content': [
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VText', 'props': {'text': '时间'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 2}, 'content': [
                    {'component': 'VText', 'props': {'text': '结果'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 3}, 'content': [
                    {'component': 'VText', 'props': {'text': '原名'}}]},
                {'component': 'VCol', 'props': {'cols': 12, 'md': 4}, 'content': [
                    {'component': 'VText', 'props': {'text': '改后'}}]},
            ]},
            *rows
        ]

    def stop_service(self):
        """退出插件"""
        try:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join(timeout=2)
                except Exception as e:  # noqa: BLE001
                    logger.debug(f"停止监控失败：{e}")
            self._observer = []
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:  # noqa: BLE001
            logger.error(f"stop_service 出错：{e}")
