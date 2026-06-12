#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DC·HOT 数据合规与争议解决热点 —— 资讯聚合脚本
从多个国内外信源抓取 RSS/Atom，按关键词过滤、分类、计算热度、聚类去重，
输出 news.json 供前端渲染。仅依赖 Python 标准库。
"""
import concurrent.futures
import email.utils
import gzip
import html
import http.cookiejar
import json
from collections import Counter
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    TZ = timezone(timedelta(hours=8))

OUT_FILE = Path(__file__).parent / "news.json"
ARCHIVE_FILE = Path(__file__).parent / "archive.json"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MAX_AGE_DAYS = 7        # 实时/热点主列表窗口
HIGHLIGHT_DAYS = 60     # 「要闻」回顾窗口（立法与案例），档案滚动保留期
HIGHLIGHT_TOP_N = 15    # 要闻每个栏目最多条数
TIMEOUT = 15

def gnews(query, zh=True):
    q = urllib.parse.quote(query)
    if zh:
        return f"https://news.google.com/rss/search?q={q}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    return f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

def bnews(query):
    return f"https://www.bing.com/news/search?q={urllib.parse.quote(query)}&format=rss"

def sogou(query):
    """搜狗微信搜索（type=2 为公众号文章）"""
    return f"https://weixin.sogou.com/weixin?type=2&query={urllib.parse.quote(query)}"

# 信源配置：
#   name     展示名称
#   url      RSS/Atom 地址
#   region   国内 / 国际
#   weight   信源权重（专业源高，综合源低）
#   implies  该信源天然满足的相关性维度（见下方"相关性闸门"）：
#            data=数据维度  legal=法律维度  dispute=争议解决（单独命中即收录）
SOURCES = [
    # —— 国际专业源（本身即数据保护/争议解决领域）——
    {"name": "EDPB 欧盟数据保护委员会", "url": "https://www.edpb.europa.eu/feed/news_en", "region": "国际", "weight": 3.0, "implies": ("data", "legal")},
    {"name": "noyb 欧洲数字权利中心", "url": "https://noyb.eu/en/rss.xml", "region": "国际", "weight": 2.5, "implies": ("data", "legal")},
    {"name": "Hunton 隐私与信息安全博客", "url": "https://www.huntonprivacyblog.com/feed/", "region": "国际", "weight": 3.0, "implies": ("data", "legal")},
    {"name": "Global Arbitration News", "url": "https://www.globalarbitrationnews.com/feed/", "region": "国际", "weight": 3.0, "implies": ("dispute",)},
    # FTC 是监管机构（法律维度天然满足），但须命中数据维度，排除反垄断等无关执法
    {"name": "FTC 美国联邦贸易委员会", "url": "https://www.ftc.gov/feeds/press-release.xml", "region": "国际", "weight": 2.5, "implies": ("legal",)},
    # 安全媒体（数据维度天然满足），但须命中法律维度，排除纯技术漏洞文章
    {"name": "The Hacker News", "url": "https://feeds.feedburner.com/TheHackersNews", "region": "国际", "weight": 1.5, "implies": ("data",)},
    # —— 微信公众号（搜狗微信搜索，type=sogou：解析结果页并还原真实文章链接）——
    {"name": "微信公众号", "type": "sogou", "url": sogou("数据合规"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "微信公众号", "type": "sogou", "url": sogou("个人信息保护"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "微信公众号", "type": "sogou", "url": sogou("数据出境"), "region": "国内", "weight": 2.0, "implies": ()},
    # —— 国内综合源（须同时命中数据+法律两个维度）——
    {"name": "36氪", "url": "https://36kr.com/feed", "region": "国内", "weight": 1.5, "implies": ()},
    {"name": "Solidot", "url": "https://www.solidot.org/index.rss", "region": "国内", "weight": 1.5, "implies": ()},
    {"name": "cnBeta", "url": "https://www.cnbeta.com.tw/backend.php", "region": "国内", "weight": 1.5, "implies": ()},
    # —— Google News 关键词检索（结果混杂，同样要过双维度闸门）——
    {"name": "Google News", "url": gnews("个人信息保护法 OR 数据安全法 OR 数据合规"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("数据出境 OR 跨境数据流动 OR 出境安全评估"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("网信办 OR 国家数据局 OR 个人信息保护委员会"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("个人信息 处罚 OR 隐私 罚款 OR 数据 执法"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("个人信息 判决 OR 数据 诉讼 OR 隐私 案件"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("商事仲裁 OR 国际仲裁 OR 涉外争议解决"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("GDPR enforcement OR GDPR fine", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("data protection law OR privacy regulation", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("cross-border data transfer", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("data breach class action OR privacy lawsuit", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("international arbitration award", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    # —— 要闻回顾专用：when:60d 拉取近两个月的立法与案例 ——
    {"name": "Google News", "url": gnews("数据 立法 OR 个人信息 条例 when:60d"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("个人信息 判决 OR 数据 案例 when:60d"), "region": "国内", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("data protection law passed when:60d", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
    {"name": "Google News", "url": gnews("GDPR ruling OR privacy lawsuit verdict when:60d", zh=False), "region": "国际", "weight": 2.0, "implies": ()},
]

# —— 相关性闸门 ——
# 一条资讯必须：同时命中「数据维度」×「法律维度」，或命中「争议解决」专门词，才会收录。
# 专业信源通过 implies 预先满足相应维度（如 EDPB 天然 data+legal）。
DATA_RE = re.compile(
    r"数据|个人信息|隐私|个保法|信息安全|网络安全|信息保护|人脸识别|生物识别|泄露|"
    r"data|privacy|personal information|personally identifiable|\bPII\b|GDPR|PIPL|CCPA|CPRA|biometric|cyber|breach", re.I)
LEGAL_RE = re.compile(
    r"合规|监管|执法|处罚|罚款|约谈|立案|诉讼|起诉|应诉|判决|裁决|裁定|法院|法庭|检察|立法|法案|法律|"
    r"条例|办法|规定|草案|征求意见|指南|审计|(?:保护|安全|合规)认证|网信办|数据局|保护委员会|"
    r"compliance|regulat|enforc|\bfine[sd]?\b|penalt|lawsuit|litigat|\bsue[sd]?\b|court|tribunal|ruling|"
    r"judgment|settlement|class action|consent order|legislat|\bbill\b|\bact\b|directive|statute|"
    r"guidance|consultation|audit|\bDPA\b|attorney general|injunction|sanction", re.I)
DISPUTE_RE = re.compile(r"仲裁|争议解决|商事调解|arbitrat|dispute resolution|\bADR\b|mediation", re.I)
# 噪音剔除：律所榜单/评级类公关稿、综合早晚报合集（标题主体与合规无关）、
# 投资者互动问答（股民问询类 IR 内容）、企业营销软文
EXCLUDE_RE = re.compile(
    r"Legal 500|Chambers (USA|Global|Asia)|ranking|recogni[sz]e[sd]?\b|律所.{0,6}(荣誉|上榜|榜单)|"
    r"8点1氪|【早知道】|播早报|氪星晚报|早报丨|晚报丨|"
    r"是否.{0,20}业务往来|投资者互动|互动平台|破局者|新标杆|领跑者", re.I)


def relevant(text, implies):
    if EXCLUDE_RE.search(text):
        return False
    t = text.replace("数据中心", " ")  # "数据中心"不算数据维度（电力/地产新闻常见）
    if "dispute" in implies or DISPUTE_RE.search(t):
        return True
    has_data = "data" in implies or DATA_RE.search(t)
    has_legal = "legal" in implies or LEGAL_RE.search(t)
    return bool(has_data and has_legal)

# 关键词词库（命中加热度；综合源须至少命中一个强关键词才收录）
KEYWORDS = {
    3.0: ["数据合规", "数据安全法", "个人信息保护法", "个保法", "数据出境", "跨境数据", "数据跨境",
          "出境安全评估", "标准合同", "数据分类分级", "重要数据", "合规审计", "网信办", "争议解决",
          "GDPR", "PIPL", "CCPA", "CPRA", "EDPB", "Schrems", "data compliance", "cross-border data",
          "data transfer", "data protection", "dispute resolution"],
    2.0: ["个人信息", "隐私政策", "隐私保护", "数据泄露", "网络安全法", "数据处理者", "算法备案",
          "人脸识别", "仲裁", "诉讼", "判决", "裁决", "罚款", "处罚", "约谈", "执法", "和解",
          "数据要素", "安全评估", "合规", "privacy", "data breach", "arbitration", "litigation",
          "class action", "lawsuit", "enforcement", "fine", "penalty", "settlement", "consent",
          "adequacy", "SCCs", "biometric", "DPA", "ICO", "FTC"],
    1.0: ["隐私", "数据安全", "网络安全", "泄露", "勒索", "法院", "起诉", "立法", "监管", "新规",
          "征求意见", "指南", "条例", "法案", "cookie", "cybersecurity", "regulation", "court",
          "ruling", "tribunal", "guidance", "directive", "investigation", "sanction"],
}

# 分类规则（按顺序匹配，先命中者归类）
CATEGORIES = [
    ("跨境数据", r"数据出境|跨境数据|数据跨境|出境安全评估|标准合同|cross-border|data transfer|adequacy|SCCs?|Schrems|onward transfer"),
    ("诉讼仲裁", r"诉讼|仲裁|判决|裁决|法院|起诉|应诉|和解|集体诉讼|class action|lawsuit|litigation|arbitration|arbitral|tribunal|court|ruling|settlement|\bsue[sd]?\b"),
    ("执法处罚", r"罚款|处罚|约谈|执法|通报|下架|整改|立案|fine[sd]?\b|penalt|enforcement|sanction|investigat"),
    ("立法监管", r"法案|条例|征求意见|立法|新规|办法|指南|标准|草案|模板|发布|发文|出台|regulation|bill\b|directive|\bact\b|guidance|guideline|consultation|draft|framework|rules|template"),
    ("个人信息保护", r"个人信息|隐私|人脸识别|生物识别|privacy|personal data|consent|cookie|biometric|facial recognition"),
    ("安全事件", r"泄露|攻击|勒索|窃取|breach|hack|ransom|leak|exposed|stolen"),
]
DEFAULT_CATEGORY = "行业实践"

# 「立法监管」的官方主体闸门：必须出现官方机构或法律文件名，
# 排除企业内部政策、产品合规等非官方内容（acronym 部分区分大小写，防止误命中普通单词）
OFFICIAL_RE = re.compile(
    r"网信办|国家数据局|工信部|工业和信息化部|公安部|市场监管总局|司法部|国务院|人大|人民银行|央行|证监会|"
    r"金融监管总局|最高人民法院|最高人民检察院|最高法|最高检|检察|法院|保护委员会|数据保护局|监管机构|监管部门|"
    r"政府|部委|主管部门|当局|欧盟|欧委会|欧洲(?:委员会|议会|理事会)|议会|立法机关|国家标准|征求意见|"
    r"(?i:European (?:Commission|Parliament|Council)|Parliament|Congress|Senate|White House|"
    r"regulator|authorit|ministry|government|federal|attorney general|lawmaker|legislature|"
    r"statute|directive|ordinance|executive order|department of|data protection (?:board|authority))|"
    r"\bFTC\b|\bSEC\b|\bICO\b|\bCNIL\b|\bEDPB\b|\bEDPS\b|\bDPA\b|\bCAC\b|\bAct\b")


# 观点/倡议类文章（智库分析、呼吁立法等）不算官方动作
OPINION_RE = re.compile(r"the case for|op-ed|呼吁|观点|倡议", re.I)

# 里程碑事件词：命中则要闻重要度加分（首例判决、法律生效、创纪录罚款等）
LANDMARK_RE = re.compile(
    r"首例|首部|首个|首张|正式(?:施行|生效|实施|通过)|表决通过|审议通过|创纪录|史上最|"
    r"最高.{0,4}罚|亿元|亿欧元|亿美元|亿韩元|landmark|first[- ]ever|record (?:fine|penalty)|"
    r"billion|historic|supreme court|milestone", re.I)


def load_archive():
    if ARCHIVE_FILE.exists():
        try:
            return json.loads(ARCHIVE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# 实体词限额用的英文常用词表（这些词不算"实体"）
COMMON_EN = {
    "data", "breach", "class", "action", "settlement", "privacy", "lawsuit", "gdpr",
    "fine", "fines", "court", "case", "cases", "against", "after", "with", "over",
    "into", "first", "record", "report", "annual", "federal", "state", "protection",
    "personal", "information", "security", "cyber", "compliance", "regulation",
    "enforcement", "authority", "issues", "guidance", "update", "updates", "draft",
    "rules", "bill", "should", "their", "about", "million", "billion",
}


def pick_highlights(archive_items):
    """从 60 天档案中选出立法与案例两栏要闻：按重要度排序，
    近似重复标题只保留最高分一条，同一实体（公司/机构名）最多 2 条。"""
    groups = {"legislation": ("立法监管", "跨境数据"), "cases": ("诉讼仲裁", "执法处罚")}
    out = {}
    for key, cats in groups.items():
        pool = sorted((i for i in archive_items if i["category"] in cats),
                      key=lambda x: -x.get("importance", 0))
        picked, used_entities = [], Counter()
        for it in pool:
            toks = norm_tokens(it["title"])
            if any(jaccard(toks, norm_tokens(p["title"])) >= 0.35 for p in picked):
                continue
            entities = {t for t in toks
                        if t.isascii() and t.isalpha() and len(t) >= 4 and t not in COMMON_EN}
            if any(used_entities[e] >= 2 for e in entities):
                continue  # 同一事件主体已占 2 个名额
            used_entities.update(entities)
            picked.append(it)
            if len(picked) >= HIGHLIGHT_TOP_N:
                break
        out[key] = picked
    return out


def is_official(text):
    return bool(OFFICIAL_RE.search(text))

REASONS = {
    "跨境数据": "涉及数据跨境流动规则，可能影响出境安全评估、标准合同备案等合规路径，建议出海/跨国业务团队关注。",
    "诉讼仲裁": "典型争议解决案例，裁判口径与程序策略对类案处理具有参考价值。",
    "执法处罚": "监管执法动态，反映当前执法重点与尺度，可对照排查自身合规风险。",
    "立法监管": "立法/监管新动向，可能调整合规义务边界，建议跟进后续落地细则。",
    "个人信息保护": "涉及个人信息处理规则与隐私保护实践，与日常数据合规运营直接相关。",
    "安全事件": "数据安全事件通常伴随监管问询与索赔风险，可作为应急响应与合规复盘素材。",
    "行业实践": "行业合规实践动态，可作为制度建设与对标参考。",
}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        raw = resp.read()
    if raw[:2] == b"\x1f\x8b":  # 部分源无视请求头强制 gzip
        raw = gzip.decompress(raw)
    return raw


def text_of(elem, *names):
    for n in names:
        found = elem.find(n)
        if found is not None and (found.text or "").strip():
            return found.text.strip()
    return ""


def strip_html(s):
    return WS_RE.sub(" ", TAG_RE.sub(" ", html.unescape(s or ""))).strip()


def parse_time(s):
    if not s:
        return None
    s = WS_RE.sub(" ", s).strip()  # 36氪等源的日期含多余空格
    try:
        return email.utils.parsedate_to_datetime(s)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def real_url(link):
    """Bing News 的链接是跳转链，提取真实 URL。"""
    if "bing.com" in link and "url=" in link:
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(link).query)
        if qs.get("url"):
            return qs["url"][0]
    return link


def parse_feed(raw, source):
    """解析 RSS 2.0 或 Atom，返回 item 字典列表。"""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # 部分源带非法字符，做一次清洗重试
        cleaned = re.sub(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]", b"", raw)
        root = ET.fromstring(cleaned)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    items = []
    # RSS 2.0
    for it in root.iter("item"):
        title = strip_html(text_of(it, "title"))
        link = real_url(text_of(it, "link"))
        desc = strip_html(text_of(it, "description"))
        pub = parse_time(text_of(it, "pubDate") or text_of(it, "{http://purl.org/dc/elements/1.1/}date"))
        src_el = it.find("source")
        src_name = (src_el.text or "").strip() if src_el is not None and src_el.text else source["name"]
        # Google News 标题尾部带 " - 来源名"，去掉
        if title.endswith(" - " + src_name):
            title = title[: -len(" - " + src_name)].strip()
        items.append({"title": title, "link": link, "summary": desc, "time": pub, "src": src_name})
    # Atom
    for it in root.findall("atom:entry", ns):
        title = strip_html(text_of(it, "atom:title"))
        link_el = it.find("atom:link", ns)
        link = link_el.get("href") if link_el is not None else ""
        desc = strip_html(text_of(it, "atom:summary") or text_of(it, "atom:content"))
        pub = parse_time(text_of(it, "atom:published") or text_of(it, "atom:updated"))
        items.append({"title": title, "link": link, "summary": desc, "time": pub, "src": source["name"]})
    return items


SOGOU_LOCK = threading.Lock()  # 搜狗请求全局串行+限速，避免触发反爬验证


def fetch_sogou(source):
    """搜狗微信搜索：抓取公众号文章。跳转链接带会话 token 会过期，
    需带搜索会话 Cookie 请求跳转页，从 JS 片段拼出真实 mp.weixin.qq.com 地址。"""
    with SOGOU_LOCK:
        return _fetch_sogou(source)


def _fetch_sogou(source):
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [("User-Agent", UA), ("Referer", "https://weixin.sogou.com/"),
                         ("Accept-Language", "zh-CN,zh;q=0.9")]

    def get(url):
        time.sleep(1.2)
        raw = opener.open(url, timeout=TIMEOUT).read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", "ignore")

    page = get(source["url"])
    if 'id="sogou_vr_' not in page:
        if "antispider" in page.lower() or "验证" in page:
            raise RuntimeError("搜狗反爬限流，本轮跳过（通常数十分钟后自动恢复）")
        return []
    cutoff = datetime.now(TZ) - timedelta(days=HIGHLIGHT_DAYS)
    items = []
    for li in re.findall(r'<li[^>]*id="sogou_vr_[^"]*".*?</li>', page, re.S):
        m_title = re.search(r'<h3>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', li, re.S)
        m_ts = re.search(r"timeConvert\('(\d+)'\)", li)
        if not m_title or not m_ts:
            continue
        pub = datetime.fromtimestamp(int(m_ts.group(1)), TZ)
        if pub < cutoff:
            continue  # 旧文先过滤，避免为其解析真实链接（减少请求量防限流）
        link = urllib.parse.urljoin("https://weixin.sogou.com/", html.unescape(m_title.group(1)))
        try:
            frags = re.findall(r"url \+= '([^']*)'", get(link))
            real = "".join(frags).replace("@", "")
            if real.startswith("http"):
                link = real
        except Exception:
            pass  # 还原失败则保留搜狗跳转链接
        m_acc = re.search(r'class="account"[^>]*>(.*?)</a>', li, re.S)
        m_sum = re.search(r'class="txt-info"[^>]*>(.*?)</p>', li, re.S)
        items.append({"title": strip_html(m_title.group(2)),
                      "link": link,
                      "summary": strip_html(m_sum.group(1)) if m_sum else "",
                      "time": pub,
                      "src": strip_html(m_acc.group(1)) if m_acc else source["name"]})
    return items


def keyword_score(text):
    score, hits = 0.0, []
    low = text.lower()
    for w, words in KEYWORDS.items():
        for kw in words:
            if kw.lower() in low:
                score += w
                hits.append(kw)
    return min(score, 12.0), hits


def categorize(title, summary):
    # 标题优先：标题命中的类别比摘要里顺带提到的更能代表主题
    full = title + " " + summary
    for text in (title, summary):
        for name, pattern in CATEGORIES:
            if re.search(pattern, text, re.I):
                if name == "立法监管" and (not is_official(full) or OPINION_RE.search(title)):
                    continue  # 立法监管须有官方主体且非观点文章，否则继续匹配后续类别
                return name
    return DEFAULT_CATEGORY


def norm_tokens(title):
    """标题归一化为 token 集合：英文按词，中文按双字组，用于 Jaccard 去重。
    注意 \\w 含汉字，中英混排片段须再按文字系统拆分（如"事故的coupang的制裁"）。"""
    t = re.sub(r"[^\w一-鿿]+", " ", title.lower())
    tokens = set()
    for part in t.split():
        for run in re.findall(r"[0-9a-z_]+|[一-鿿]+", part):
            if run[0] >= "一":  # 汉字串 → 双字组
                if len(run) > 1:
                    tokens.update(run[i : i + 2] for i in range(len(run) - 1))
                else:
                    tokens.add(run)
            else:
                tokens.add(run)
    return tokens


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def fetch_source(source):
    try:
        if source.get("type") == "sogou":
            items = fetch_sogou(source)
        else:
            items = parse_feed(fetch(source["url"]), source)
        return source, items, None
    except Exception as e:
        return source, [], str(e)


def fetch_all():
    now = datetime.now(TZ)
    cutoff = now - timedelta(days=HIGHLIGHT_DAYS)  # 收集 60 天，主列表稍后再截 7 天
    collected, errors = [], []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for source, items, err in pool.map(fetch_source, SOURCES):
            if err:
                errors.append(f"{source['name']}: {err}")
                continue
            for it in items:
                if not it["title"] or not it["link"]:
                    continue
                t = it["time"]
                if t is None:
                    continue
                if t.tzinfo is None:
                    t = t.replace(tzinfo=timezone.utc)
                t = t.astimezone(TZ)
                if t < cutoff or t > now + timedelta(hours=2):
                    continue
                text = it["title"] + " " + it["summary"]
                if not relevant(text, source["implies"]):
                    continue
                score, hits = keyword_score(text)
                summary = it["summary"]
                if jaccard(norm_tokens(summary[:80]), norm_tokens(it["title"])) > 0.7:
                    summary = ""  # 摘要与标题重复则不展示
                if len(summary) > 180:
                    summary = summary[:178].rstrip() + "…"
                collected.append({
                    "title": it["title"], "url": it["link"], "summary": summary,
                    "time": t, "source": it["src"], "feed": source["name"],
                    "region": source["region"], "weight": source["weight"],
                    "kw_score": score, "keywords": hits,
                })

    # —— 聚类去重：标题相似的合并为一条，来源累加（即"n 个信源"）——
    clusters = []
    for item in sorted(collected, key=lambda x: -x["kw_score"]):
        tokens = norm_tokens(item["title"])
        for c in clusters:
            if jaccard(tokens, c["_tokens"]) >= 0.55:
                if item["source"] not in c["sources"]:
                    c["sources"].append(item["source"])
                c["time"] = max(c["time"], item["time"])
                if len(item["summary"]) > len(c["summary"]):
                    c["summary"] = item["summary"]
                break
        else:
            item["_tokens"] = tokens
            item["sources"] = [item["source"]]
            clusters.append(item)

    # —— 检索类信源限流：同一外部媒体/公众号最多 3 条，避免营销类账号刷屏 ——
    capped, per_src = [], {}
    for c in sorted(clusters, key=lambda x: -(x["kw_score"] + x["weight"])):
        if c["feed"] in ("Google News", "微信公众号"):
            n = per_src.get(c["source"], 0)
            if n >= 3:
                continue
            per_src[c["source"]] = n + 1
        capped.append(c)
    clusters = capped

    # —— 热度、分类与重要度 ——
    results = []
    for c in clusters:
        age_h = (now - c["time"]).total_seconds() / 3600
        recency = max(0.0, 4.0 - age_h / 18.0)  # 连续衰减：新发布 +4，72 小时后归零
        heat = round(c["kw_score"] + c["weight"] + recency + 2.0 * (len(c["sources"]) - 1), 1)
        category = categorize(c["title"], c["summary"])
        top_kw = sorted(set(c["keywords"]), key=lambda k: -len(k))[:3]
        # 重要度（要闻排序用）：不含时效项，立法/案例类别与里程碑事件加分
        importance = c["kw_score"] + c["weight"] + 2.0 * (len(c["sources"]) - 1)
        if category in ("立法监管", "跨境数据"):
            importance += 3.0
        elif category in ("诉讼仲裁", "执法处罚"):
            importance += 2.5
        if LANDMARK_RE.search(c["title"]):
            importance += 3.0
        results.append({
            "title": c["title"], "url": c["url"], "summary": c["summary"],
            "time": c["time"].isoformat(), "date": c["time"].strftime("%Y-%m-%d"),
            "sources": c["sources"], "region": c["region"], "category": category,
            "heat": heat, "hot": False, "tags": top_kw,
            "importance": round(importance, 1),
            "kw_score": round(c["kw_score"], 1), "weight": c["weight"],
            "reason": REASONS[category],
        })

    # —— 档案：60 天滚动合并（跨运行累积，供「要闻」回顾与主列表回填）——
    archive = load_archive()
    for r in results:
        archive[r["url"]] = r
    cut60 = (now - timedelta(days=HIGHLIGHT_DAYS)).isoformat()
    archive = {u: it for u, it in archive.items() if it["time"] >= cut60}
    ARCHIVE_FILE.write_text(json.dumps(archive, ensure_ascii=False, indent=1), encoding="utf-8")
    highlights = pick_highlights(list(archive.values()))

    # —— 主列表回填：信源轮换/限流导致本轮未抓到、但仍在 7 天窗口内的档案条目补回。
    #     主列表不再随单轮抓取波动丢条目；公众号等仅本地可抓的信源，在云端（Actions）
    #     重新生成数据时也能从仓库档案中保留下来 ——
    cut7 = (now - timedelta(days=MAX_AGE_DAYS)).isoformat()
    seen_urls = {r["url"] for r in results}
    for a in archive.values():
        if a["url"] in seen_urls or a["time"] < cut7:
            continue
        item = dict(a)
        age_h = (now - datetime.fromisoformat(item["time"])).total_seconds() / 3600
        recency = max(0.0, 4.0 - age_h / 18.0)
        if "kw_score" in item:
            base = item["kw_score"] + item["weight"]
        else:  # 旧档案条目无组件字段，用重要度近似（扣除类别加成）
            base = max(0.0, item.get("importance", 6.0) - 2.5)
        item["heat"] = round(base + recency + 2.0 * (len(item["sources"]) - 1), 1)
        item["hot"] = False
        results.append(item)

    # —— 主列表（实时/精选/全部动态）只取近 7 天 ——
    items_main = [r for r in results if r["time"] >= cut7]
    for r in sorted(items_main, key=lambda x: -x["heat"])[:10]:
        r["hot"] = True  # 全局热度 Top 10 标记为热点
    items_main.sort(key=lambda x: (x["date"], x["heat"]), reverse=True)

    data = {
        "generated_at": now.isoformat(),
        "count": len(items_main),
        "highlight_count": sum(len(v) for v in highlights.values()),
        "errors": errors,
        "items": items_main,
        "highlights": highlights,
    }
    OUT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    return data


if __name__ == "__main__":
    d = fetch_all()
    print(f"抓取完成：{d['count']} 条资讯 → {OUT_FILE}")
    if d["errors"]:
        print("以下信源抓取失败（已跳过）：", file=sys.stderr)
        for e in d["errors"]:
            print("  -", e, file=sys.stderr)
