"""TrackerLimit 纯逻辑单测。

宿主进程外不能实例化插件类（`_PluginBase.__init__` 会构造 Chain，报
「Chain 运行上下文尚未由启动组合根配置」），所以这里只测模块级函数和
类方法里不依赖实例状态的部分（导入路径与生产一致：`app.plugins.trackerlimit`）。
"""

from app.plugins.trackerlimit import SITE_GROUPS, TrackerLimit, _as_int

# 名字修饰：测试里按宿主实际注册的那个类方法来调
MATCH_RULE = TrackerLimit._TrackerLimit__match_rule  # noqa: SLF001


def test_as_int():
    """字符串 / 小数 / 空值 都要归一成 int 或 None。"""
    assert _as_int("10") == 10
    assert _as_int(" 10 ") == 10
    assert _as_int(10) == 10
    assert _as_int("10.6") == 10
    assert _as_int(0) == 0
    assert _as_int("") is None
    assert _as_int(None) is None
    assert _as_int("abc") is None


def test_rule_key():
    """规则键只按 tracker（一套规则管全部下载器）；老调用多传一个下载器也不影响。"""
    assert TrackerLimit.rule_key("MTeam") == "mteam"
    assert TrackerLimit.rule_key(" hdsky ") == "hdsky"
    assert TrackerLimit.rule_key("mteam", "TR") == "mteam"


def test_match_rule_by_alias():
    """先按站群别名精确匹配：馒头 tra1.m-team.cc → mteam。"""
    rules = [{"tracker": "mteam", "upload_kbps": 15}]
    announce = ["http://tra1.m-team.cc/announce.php?passkey=x"]
    assert MATCH_RULE(rules, announce) == rules[0]
    assert SITE_GROUPS.get("tra1.m-team.cc") == "mteam"


def test_match_rule_by_domain():
    """域名本身写在规则里也能命中。"""
    rules = [{"tracker": "tracker.hdsky.me", "upload_kbps": 99999}]
    assert MATCH_RULE(rules, ["https://tracker.hdsky.me/announce"]) == rules[0]


def test_match_rule_fallback_substring():
    """表里没收录的域名，退回子串兜底。"""
    rules = [{"tracker": "cspt.top", "upload_kbps": 10}]
    assert MATCH_RULE(rules, ["https://tracker.cspt.top/announce"]) == rules[0]


def test_box_token_keeps_chinese():
    """配置页字段名必须保留中文 —— 只留 ASCII 会让所有中文站名挤成一个 key。"""
    token = TrackerLimit._TrackerLimit__box_token  # noqa: SLF001
    assert token("馒头") == "馒头"
    assert token("高清杜比") == "高清杜比"
    assert token("UBits") == "UBits"
    assert token("a b/c") == "a_b_c"
    assert token("") == "site"
    assert len({token(one) for one in ["馒头", "天空", "彩虹岛", "我堡"]}) == 4


def test_match_rule_no_hit():
    """没命中就返回 None（巡检时不动这个种）。"""
    rules = [{"tracker": "mteam", "upload_kbps": 15}]
    assert MATCH_RULE(rules, ["https://tracker.unknown-site.example/announce"]) is None
    assert MATCH_RULE(rules, []) is None
