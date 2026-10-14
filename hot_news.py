#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hot_news.py —— 财经「时段热点」TOP-N 提取器（零依赖，仅标准库）  v2

核心思路
--------
「热度」用 **跨源共振** 做主信号 —— 同一件事被 5 家里的几家报道、报了几条，
比任何单一平台的热搜榜都稳（热搜可以被买，共振很难）。
「影响」用 硬核关键词权重 + 关联板块 + 关联个股 + 关联标的实际涨跌幅 合成。

数据源（全部免费、免签名）
    东财 7x24  ← 带关联个股代码，是"影响"的主要来源
    同花顺 7x24 ← 带板块标签
    新浪 7x24
    华尔街见闻 7x24 ← 带评论数（讨论热度）
    金十快讯 ← 带官方 important 重要性标记

用法
----
    python hot_news.py                                  # 最近 8 小时 Top5
    python hot_news.py --window 6 --top 5
    python hot_news.py --start 15:00 --end 08:30        # 隔夜时段（自动跨天）
    python hot_news.py --start 09:30 --end 11:30 --date 2026-10-15   # 上午盘中
    python hot_news.py --out md,html,json --outdir ./out
"""

import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

# ----------------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------------

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

TIMEOUT = 20
MAX_PAGES = 4
PAGE_SIZE = 100

W_HEAT, W_IMPACT = 0.55, 0.45
CLUSTER_THRESHOLD = 0.50

DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(DIR, "stock_names.json")

# 关键词影响权重
KEYWORD_WEIGHT = {
    "降准": 6, "降息": 6, "央行": 5, "LPR": 5, "MLF": 4, "逆回购": 4,
    "政治局": 6, "国务院": 5, "国常会": 5, "中央经济工作会议": 6,
    "证监会": 5, "印花税": 5, "注册制": 5, "发改委": 4, "财政部": 4,
    "国资委": 4, "工信部": 4, "能源局": 4, "外汇局": 4,
    "爆雷": 6, "造假": 6, "退市": 5, "立案": 5, "破产": 5, "违约": 5,
    "被查": 5, "减持": 4, "违规": 4, "处罚": 4, "冻结": 4,
    "证监会立案": 6, "警示函": 4, "监管函": 4, "问询": 3,
    "借壳": 6, "重组": 5, "并购": 5, "资产注入": 5, "易主": 5,
    "实控人变更": 5, "收购": 4, "要约": 4, "股权转让": 3,
    "业绩预告": 4, "预增": 4, "预减": 4, "扭亏": 4, "超预期": 4,
    "净利润": 3, "亏损": 3,
    "涨停": 5, "跌停": 5, "连板": 5, "北向资金": 4, "龙虎榜": 4,
    "游资": 4, "IPO": 4, "两融": 3, "主力资金": 3,
    "关税": 6, "制裁": 6, "出口管制": 6, "实体清单": 6, "贸易战": 6,
    "战争": 6, "冲突": 5, "袭击": 5, "停火": 4, "美联储": 5, "加息": 5,
    "非农": 4, "CPI": 4, "PPI": 3, "GDP": 4,
    "稀土": 4, "固态电池": 4, "可控核聚变": 4, "房地产": 4, "地方债": 4,
    "人工智能": 3, "算力": 3, "芯片": 3, "半导体": 3, "锂": 3,
    "光伏": 3, "储能": 3, "机器人": 3, "创新药": 3, "商业航天": 3,
    "军工": 3, "黄金": 3, "原油": 3, "消费券": 3,
}

BULLISH = ["批准", "获批", "中标", "增持", "回购", "预增", "超预期", "签约",
           "涨价", "突破", "合作", "上调", "利好", "放量", "扭亏", "新高",
           "支持", "补贴", "减税", "提速", "投产", "成功", "获批上市"]
BEARISH = ["减持", "立案", "处罚", "亏损", "预减", "下调", "终止", "失败",
           "被查", "退市", "违规", "爆雷", "跌停", "利空", "违约", "冻结",
           "警示", "问询", "禁止", "限制", "暂停", "取消", "下滑", "终止上市"]

GENERIC_TAGS = {"美股", "港股", "A股", "全球", "基金", "期货", "外汇", "债券",
                "公告", "wscn-platform"}


def _is_generic_tag(t):
    return (not t) or t in GENERIC_TAGS or t.endswith("-channel")

DIR_MARKET_CN = {0: "深", 1: "沪", 105: "美", 106: "美", 107: "美",
                 116: "港", 90: "板块", 100: "全球"}


# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------

def http_get_json(url, headers=None, retries=2, backoff=0.8):
    hdrs = {"User-Agent": UA, "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "identity", "Accept-Language": "zh-CN,zh;q=0.9"}
    if headers:
        hdrs.update(headers)
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt == retries:
                warn(f"[HTTP] 失败 {url[:80]} -> {e}")
                return None
            time.sleep(backoff * (attempt + 1))
    return None


def warn(msg):
    print(msg, file=sys.stderr)


# ----------------------------------------------------------------------------
# 文本工具
# ----------------------------------------------------------------------------

PUNCT_RE = re.compile(r"[\s\u3000【】\[\]（）()《》〈〉「」『』、，,。.；;：:！!？?~—\-_/\\|'\"“”‘’·…*#$%^&+=<>@`]+")
NUM_RE = re.compile(r"\d+(?:\.\d+)?")
ENTITY_RE = re.compile(r"^[【\[]\s*([^：:】\]，,。]{2,24})\s*[：:]")
ENTITY_RE2 = re.compile(r"^([\u4e00-\u9fa5A-Za-z0-9]{3,20})\s*[：:]")


def clean(text):
    text = re.sub(r"<[^>]+>", "", text or "")
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    return PUNCT_RE.sub("", text)


def tokens(text):
    text = clean(text)
    if not text:
        return set()
    out = {text[i:i + 2] for i in range(len(text) - 1)}
    out |= {w.lower() for w in re.findall(r"[A-Za-z0-9]{2,}", text)}
    return out


def digits(text):
    return {d for d in NUM_RE.findall(clean(text)) if len(d) >= 2}


def entity(text):
    """抽取新闻主体（公司名/机构名），用于防止模板化标题误聚类。"""
    t = (text or "").strip()
    m = ENTITY_RE.match(t) or ENTITY_RE2.match(t)
    if not m:
        return None
    e = m.group(1).strip()
    for bad in ("消息", "据悉", "记者", "公告", "报道", "快讯"):
        if e.startswith(bad) or e.endswith(bad):
            return None
    return e if 2 <= len(e) <= 24 else None


def jaccard(a, b):
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / min(len(a), len(b)) if inter else 0.0


# ----------------------------------------------------------------------------
# 抓取
# ----------------------------------------------------------------------------

def fetch_eastmoney(pages=MAX_PAGES):
    """东财 7x24 —— 带关联个股代码（stockList）。"""
    items, sort_end = [], ""
    for _ in range(pages):
        url = ("https://np-listapi.eastmoney.com/comm/web/getFastNewsList"
               f"?client=web&biz=web_724&fastColumn=102&sortEnd={sort_end}"
               f"&pageSize={PAGE_SIZE}&req_trace=1")
        d = http_get_json(url)
        if not d or d.get("code") != "1":
            break
        lst = (d.get("data") or {}).get("fastNewsList") or []
        if not lst:
            break
        for it in lst:
            items.append({
                "src": "东财", "time": it.get("showTime") or "",
                "title": (it.get("title") or "").strip(),
                "text": (it.get("summary") or "").strip(),
                "secs": it.get("stockList") or [],
                "tags": [], "buzz": int(it.get("pinglun_Num") or 0), "flag": 0,
                "url": f"https://finance.eastmoney.com/a/{it.get('code','')}.html",
            })
        sort_end = (d.get("data") or {}).get("sortEnd") or ""
        if not sort_end:
            break
        time.sleep(0.15)
    return items


def fetch_ths(pages=MAX_PAGES):
    """同花顺 7x24 —— 带板块 tags。"""
    items = []
    for p in range(1, pages + 1):
        d = http_get_json("https://news.10jqka.com.cn/tapp/news/push/stock/"
                          f"?page={p}&tag=&track=website&pagesize={PAGE_SIZE}")
        if not d or str(d.get("code")) != "200":
            break
        lst = (d.get("data") or {}).get("list") or []
        if not lst:
            break
        for it in lst:
            tstr = ""
            if it.get("ctime"):
                try:
                    tstr = datetime.fromtimestamp(int(it["ctime"])).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
            items.append({
                "src": "同花顺", "time": tstr,
                "title": (it.get("title") or "").strip(),
                "text": (it.get("digest") or "").strip(),
                "secs": [], "tags": [t.get("name", "") for t in (it.get("tags") or []) if t.get("name")],
                "buzz": 0, "flag": 0, "url": it.get("url") or "",
            })
        time.sleep(0.15)
    return items


def fetch_sina(pages=MAX_PAGES):
    """新浪财经 7x24。"""
    items = []
    for p in range(1, pages + 1):
        d = http_get_json("https://zhibo.sina.com.cn/api/zhibo/feed"
                          f"?page={p}&page_size={PAGE_SIZE}&zhibo_id=152&tag_id=0&dire=f&dpc=1")
        if not d or (d.get("result", {}).get("status", {}).get("code") != 0):
            break
        lst = d["result"]["data"].get("feed", {}).get("list") or []
        if not lst:
            break
        for it in lst:
            txt = (it.get("rich_text") or "").strip()
            if not txt:
                continue
            items.append({
                "src": "新浪", "time": it.get("create_time") or "",
                "title": txt[:60], "text": txt,
                "secs": [], "tags": [], "buzz": 0, "flag": 0, "url": "",
            })
        time.sleep(0.15)
    return items


def fetch_wallstreetcn(max_items=150):
    """华尔街见闻 7x24 —— 带评论数。"""
    items, cursor = [], None
    while len(items) < max_items:
        url = ("https://api-one.wallstcn.com/apiv1/content/lives"
               f"?channel=global-channel&client=pc&limit=30")
        if cursor:
            url += f"&cursor={cursor}"
        d = http_get_json(url)
        if not d or d.get("code") != 20000:
            break
        data = d.get("data") or {}
        lst = data.get("items") or []
        if not lst:
            break
        for it in lst:
            tstr = ""
            if it.get("display_time"):
                try:
                    tstr = datetime.fromtimestamp(int(it["display_time"])).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
            art = it.get("article") or {}
            ct = (it.get("content_text") or "").strip()
            items.append({
                "src": "见闻", "time": tstr,
                "title": (art.get("title") or ct[:60]).strip(),
                "text": ct, "secs": [],
                "tags": [c for c in (it.get("channels") or []) if not _is_generic_tag(c)],
                "buzz": int(it.get("comment_count") or 0), "flag": 0,
                "url": art.get("uri") or "",
            })
        cursor = data.get("next_cursor")
        if not cursor:
            break
        time.sleep(0.15)
    return items


def fetch_jin10():
    """金十快讯 —— 带 important 官方重要性标记。"""
    d = http_get_json("https://flash-api.jin10.com/get_flash_list?channel=-8200&vip=1",
                      headers={"x-app-id": "bVBF4FyRTn5NJF5n", "x-version": "1.0.0",
                               "Referer": "https://www.jin10.com/"})
    items = []
    if not d or d.get("status") != 200:
        return items
    for it in (d.get("data") or []):
        dd = it.get("data") or {}
        content = (dd.get("content") or "").strip()
        if not content:
            continue
        items.append({
            "src": "金十", "time": it.get("time") or "",
            "title": (dd.get("title") or content[:60]).strip(),
            "text": content, "secs": [], "tags": [],
            "buzz": 0, "flag": int(it.get("important") or 0), "url": "",
        })
    return items


FETCHERS = [fetch_eastmoney, fetch_ths, fetch_sina, fetch_wallstreetcn, fetch_jin10]


# ----------------------------------------------------------------------------
# 行业关键词搜索（主动触发，不参与定时采集）
# ----------------------------------------------------------------------------

# 常见题材的别名表：用户只打一个词，自动扩展到同义词，避免漏召回。
# 想完全自己控制就传多个词（逗号分隔），此时以你给的为准。
SECTOR_ALIAS = {
    "PCB": ["PCB", "覆铜板", "印制电路板", "线路板", "HDI", "IC载板", "CCL", "铜箔"],
    "半导体": ["半导体", "芯片", "晶圆", "光刻", "封测", "集成电路", "存储芯片", "先进封装"],
    "光伏": ["光伏", "硅片", "电池片", "组件", "逆变器", "多晶硅", "TOPCon", "HJT"],
    "锂电": ["锂电", "动力电池", "正极材料", "负极材料", "隔膜", "电解液", "固态电池"],
    "机器人": ["机器人", "人形机器人", "减速器", "伺服电机", "灵巧手", "丝杠"],
    "算力": ["算力", "AI服务器", "GPU", "液冷", "光模块", "CPO", "交换机", "数据中心"],
    "军工": ["军工", "国防", "航空发动机", "导弹", "卫星", "商业航天"],
    "医药": ["医药", "创新药", "生物医药", "CXO", "医疗器械", "疫苗", "集采"],
    "白酒": ["白酒", "酱酒", "次高端", "高端酒"],
    "房地产": ["房地产", "地产", "楼市", "保交楼", "保障房"],
    "券商": ["券商", "证券", "投行", "两融", "并购重组"],
    "稀土": ["稀土", "永磁", "磁材", "镨钕", "稀有金属"],
    "消费电子": ["消费电子", "苹果链", "果链", "折叠屏", "AR", "VR", "智能穿戴"],
    "汽车": ["汽车", "新能源车", "整车", "智能驾驶", "零部件", "出海"],
}


def expand_keywords(spec):
    """
    'PCB'            -> ['PCB','覆铜板','印制电路板',...]（命中别名表则自动扩展）
    'PCB,覆铜板,CCL' -> ['PCB','覆铜板','CCL']（用户显式指定，不再扩展）
    """
    parts = [p.strip() for p in re.split(r"[,，、|/]", spec) if p.strip()]
    if not parts:
        return []
    if len(parts) > 1:
        return list(dict.fromkeys(parts))
    k = parts[0]
    for name, alias in SECTOR_ALIAS.items():
        if k.lower() == name.lower() or k in alias:
            return list(dict.fromkeys([k] + alias))
    return [k]


def parse_jsonp(text, cb="cb"):
    """东财搜索返回 cb({...})，剥壳成 dict。"""
    if not text:
        return None
    t = text.strip()
    i, j = t.find("("), t.rfind(")")
    if i < 0 or j < 0:
        return None
    try:
        return json.loads(t[i + 1:j])
    except Exception:
        return None


def fetch_eastmoney_search(keyword, pages=3, page_size=30, since=None):
    """东财站内搜索：覆盖历史新闻，带媒体名与正文摘要。按时间倒序，可早停。"""
    items = []
    for p in range(1, pages + 1):
        param = {
            "uid": "", "keyword": keyword, "type": ["cmsArticleWebOld"],
            "client": "web", "clientType": "web", "clientVersion": "curr",
            "param": {"cmsArticleWebOld": {
                "searchScope": "default", "sort": "time", "pageIndex": p,
                "pageSize": page_size, "preTag": "", "postTag": ""}},
        }
        url = ("https://search-api-web.eastmoney.com/search/jsonp?cb=cb&param="
               + urllib.parse.quote(json.dumps(param, ensure_ascii=False)))
        txt = http_get_text(url, headers={"Referer": "https://so.eastmoney.com/"},
                            retries=2)
        d = parse_jsonp(txt)
        arr = ((d or {}).get("result") or {}).get("cmsArticleWebOld") or []
        if not arr:
            break
        oldest = None
        for it in arr:
            title = re.sub(r"</?em>", "", it.get("title") or "").strip()
            content = re.sub(r"</?em>", "", it.get("content") or "").strip()
            ts = (it.get("date") or "").strip()
            t = parse_time(ts)
            if t and (oldest is None or t < oldest):
                oldest = t
            items.append({
                "src": (it.get("mediaName") or "东财搜索").strip(),
                "time": ts, "title": title, "text": content,
                "secs": [], "tags": [], "buzz": 0, "flag": 0,
                "url": it.get("url") or "",
            })
        if since and oldest and oldest < since:      # 已翻到窗口之外，停止
            break
        time.sleep(0.25)
    return items


def match_keywords(item, keywords):
    """标题或摘要命中任一关键词才算相关。"""
    blob = (item.get("title") or "") + " " + (item.get("text") or "")
    hits = [k for k in keywords if k and k.lower() in blob.lower()]
    item["kw_hits"] = hits
    return bool(hits)


# 综述/集锦类标题：会把所有板块或几十家公司罗列一遍，容易被误当成行业新闻。
# 这类文章只有在「标题里就有行业词」时才算相关，否则一律排除。
RECAP_PAT = re.compile(
    r"(收评|午评|晚报|早报|开盘|收盘|复盘|盘面|异动|龙虎榜|两市|全市场|"
    r"三大指数|双创指数|涨停|跌停|午盘|个股下跌|个股上涨|涨幅榜|跌幅榜|"
    r"公告集锦|公告汇总|公告精选|盘后公告|早间公告|晚间公告|公司公告|"
    r"业绩汇总|要闻|一览|速览|盘点|集锦|汇总|快讯汇总|涨幅居前|跌幅居前)")


def is_relevant(item, keywords):
    """
    行业相关性判定 —— 这一步决定搜索质量。

    规则：
      1. 关键词出现在标题里 → 相关（最硬信号）
      2. 标题没命中且属于「综述/集锦」类 → 排除
         （这类文章罗列几十家公司，提一句某行业毫无信息量）
      3. 标题没命中且非综述 → 要求关键词在正文累计出现 ≥3 次（兼顾深度报道）
    """
    title = item.get("title") or ""
    text = item.get("text") or ""
    tl, bl = title.lower(), text.lower()
    hits_title = [k for k in keywords if k and k.lower() in tl]
    hits_text = [k for k in keywords if k and k.lower() in bl]
    item["rel_title"] = bool(hits_title)       # 标题命中=高相关，时间排序时用它分档
    if hits_title:
        return list(dict.fromkeys(hits_title + hits_text))
    if RECAP_PAT.search(title):
        return []
    if sum(bl.count(k.lower()) for k in keywords if k) >= 3:
        return list(dict.fromkeys(hits_text))
    return []


def freshness_bonus(t, now=None, half_life_h=36.0, cap=2.0):
    """越新越加分，指数衰减，上限 cap。"""
    if not t:
        return 0.0
    now = now or datetime.now()
    age_h = max((now - t).total_seconds() / 3600.0, 0.0)
    return round(cap * (0.5 ** (age_h / half_life_h)), 3)


# ----------------------------------------------------------------------------
# 证券代码 -> 名称 / 涨跌幅
# ----------------------------------------------------------------------------

_name_cache = {}
_quote_host = None          # 探测到的可用行情主机
_quote_gave_up = False      # 熔断：行情接口整体不可用时置位，避免反复重试卡死
_quote_calls = 0
QUOTE_MAX_CALLS = 40        # 行情请求总次数上限
QUOTE_MAX_FAILS = 4         # 连续失败上限


def load_cache():
    global _name_cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                _name_cache = json.load(f)
        except Exception:
            _name_cache = {}


def save_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(_name_cache, f, ensure_ascii=False, indent=0)
    except Exception:
        pass


QUOTE_HOSTS = ["https://push2.eastmoney.com", "https://push2delay.eastmoney.com",
               "https://push2delay1.eastmoney.com"]


def http_get_text(url, encoding="utf-8", headers=None, retries=1):
    hdrs = {"User-Agent": UA, "Accept-Encoding": "identity"}
    if headers:
        hdrs.update(headers)
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                txt = raw.decode(encoding, errors="replace")
                return txt.lstrip("\ufeff")             # 部分 CDN 会给 UTF-8 BOM
        except Exception:
            if attempt == retries:
                return None
            time.sleep(0.8)
    return None


def _to_tencent(secid):
    """东财 secid → 腾讯行情代码。腾讯不认板块(BK)，返回 None。"""
    if "." not in secid:
        return None
    mkt, code = secid.split(".", 1)
    if mkt == "90":                                   # 板块指数，腾讯无
        return None
    if mkt in ("105", "106", "107"):                  # 美股
        return "us" + code.replace("_", ".")
    if mkt == "116":                                  # 港股
        return "hk" + code
    if mkt in ("0", "1", "2"):
        if code[:1] in ("4", "8") or code.startswith("92"):
            return "bj" + code                        # 北交所
        return ("sh" if code[:1] == "6" else "sz") + code
    return None


def fetch_quotes_tencent(secids):
    """
    腾讯行情：https://qt.gtimg.cn/q=sz002560,sh600382
    返回形如 v_sz002560="51~通达股份~002560~6.07~6.36~...";
    字段 [1]=名称 [2]=代码 [3]=现价 [4]=昨收 [32]=涨跌幅%
    实测对数据中心 IP 友好，作为主用源。
    """
    pairs = [(s, _to_tencent(s)) for s in secids]
    pairs = [(s, t) for s, t in pairs if t]
    got = {}
    for i in range(0, len(pairs), 60):
        chunk = pairs[i:i + 60]
        txt = http_get_text("https://qt.gtimg.cn/q=" + ",".join(t for _, t in chunk),
                            encoding="gbk", retries=2)
        if not txt:
            continue
        for line in txt.split(";"):
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip().replace("v_", "")
            f = val.strip().strip('"').split("~")
            if len(f) < 6 or not f[1]:
                continue
            got[key] = f
        time.sleep(0.2)
    out = {}
    for s, t in pairs:
        f = got.get(t)
        if not f:
            continue
        try:
            price, prev = float(f[3]), float(f[4])
            chg = round((price - prev) / prev * 100, 2) if prev else None
        except Exception:
            chg = None
        out[s] = {"name": f[1], "chg": chg, "mkt": int(s.split(".")[0])}
    return out


def fetch_quotes_eastmoney(secids):
    """东财行情，主要用来补板块(BK)名称。被拒连时静默失败。"""
    global _quote_host
    order = ([_quote_host] if _quote_host else []) + \
            [h for h in QUOTE_HOSTS if h != _quote_host]
    for h in order:
        d = http_get_json(f"{h}/api/qt/ulist.np/get"
                          f"?secids={','.join(secids)}&fields=f12,f13,f14,f3&fltt=2",
                          headers={"Referer": "https://quote.eastmoney.com/"},
                          retries=0)
        if d is not None:
            _quote_host = h
            out = {}
            for row in ((d.get("data") or {}).get("diff") or []):
                out[f"{row.get('f13')}.{row.get('f12')}"] = {
                    "name": row.get("f14") or row.get("f12"),
                    "chg": None if row.get("f3") in ("-", None) else row.get("f3"),
                    "mkt": row.get("f13")}
            return out
    return {}


def _cached_secs(codes):
    out = []
    for c in codes:
        v = _name_cache.get(c)
        if v:
            out.append({"code": c, "name": v["name"], "chg": v["chg"], "mkt": v["mkt"]})
    return out


def resolve_secs(codes):
    """
    批量解析：腾讯为主（A/港/美股全覆盖），东财补板块名。
    全程有界：请求次数上限 + 熔断，绝不阻塞调用方。
    """
    global _quote_gave_up, _quote_calls
    if not codes or _quote_gave_up:
        return _cached_secs(codes)

    todo = [c for c in dict.fromkeys(codes) if c and c not in _name_cache]
    if not todo:
        return _cached_secs(codes)

    try:
        # —— 第一优先：腾讯（批量，一次 60 个）——
        tencent_codes = [c for c in todo if _to_tencent(c)]
        if tencent_codes:
            res = fetch_quotes_tencent(tencent_codes)
            _quote_calls += 1
            for c in tencent_codes:
                _name_cache[c] = res.get(c)          # 查不到就记 None，不再重复问
        # —— 第二优先：东财，补板块之类腾讯没有的 ——
        rest = [c for c in todo if not _to_tencent(c)]
        if rest:
            for i in range(0, len(rest), 30):
                if _quote_calls >= QUOTE_MAX_CALLS:
                    warn(f"[行情] 达到请求上限 {QUOTE_MAX_CALLS}，停止解析")
                    break
                chunk = rest[i:i + 30]
                _quote_calls += 1
                res = fetch_quotes_eastmoney(chunk)
                if not res:
                    warn("[行情] 东财不可用，板块名称降级为代码显示")
                    for c in rest[i:]:
                        _name_cache.setdefault(c, None)
                    break
                for c in chunk:
                    _name_cache[c] = res.get(c)
                time.sleep(0.3)
    except Exception as e:
        warn(f"[行情] 异常，已降级：{e}")

    return _cached_secs(codes)


def preselect_codes(rows, max_clusters=120, per_cluster=6):
    """从粗排结果里挑出「值得花请求去解析证券」的那部分代码。"""
    codes = []
    for r in rows[:max_clusters]:
        for c in r["secs_raw"][:per_cluster]:
            if c not in codes:
                codes.append(c)
    return codes


def fmt_sec(s):
    if s["mkt"] == 90:
        tag = s["name"]                                   # 板块
    else:
        tag = f"{s['name']}({s['code'].split('.')[-1]})"
    if s["chg"] is not None:
        try:
            tag += f" {float(s['chg']):+.2f}%"
        except Exception:
            pass
    return tag


# ----------------------------------------------------------------------------
# 时间
# ----------------------------------------------------------------------------

def parse_time(s):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime((s or "").strip(), fmt)
        except Exception:
            continue
    return None


def resolve_window(a):
    """
    返回 (start, end)。
    - 未指定 --start/--end 时：取「最近 N 小时」滚动窗口。
    - 指定时：按 --date（缺省为今天）构造；若 end < start 视为跨天（如 15:00→08:30）；
      若整个窗口还在未来（凌晨问隔夜时段就会这样），整体回退一天。
    """
    now = datetime.now()
    if a.start and a.end:
        base = datetime.strptime(a.date, "%Y-%m-%d") if a.date else now
        sh, sm = map(int, a.start.split(":"))
        eh, em = map(int, a.end.split(":"))
        start = base.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end = base.replace(hour=eh, minute=em, second=59, microsecond=0)
        if end < start:                      # 跨天
            end += timedelta(days=1)
        if not a.date and start > now:       # 窗口尚未开始 → 落到最近一次
            start -= timedelta(days=1)
            end -= timedelta(days=1)
        return start, end
    return now - timedelta(hours=a.window), now


# ----------------------------------------------------------------------------
# 聚类 + 打分
# ----------------------------------------------------------------------------

def cluster(items):
    clusters = []
    for it in items:
        blob = it["title"] + it["text"][:80]
        tk, dg, ent = tokens(blob), digits(blob), entity(it["title"])
        best, best_sim = None, 0.0
        for c in clusters:
            if ent and c["entity"] and ent != c["entity"]:
                continue                      # 主体冲突，绝不同簇
            sim = jaccard(tk, c["tokens"])
            if dg and c["digits"]:
                sim *= (0.35 + 0.65 * jaccard(dg, c["digits"]))
            if sim > best_sim:
                best, best_sim = c, sim
        if best is not None and best_sim >= CLUSTER_THRESHOLD:
            best["items"].append(it)
            best["tokens"] |= tk
            best["digits"] |= dg
        else:
            clusters.append({"items": [it], "tokens": tk, "digits": dg, "entity": ent})
    return clusters


def keyword_score(text):
    sc, hits = 0.0, []
    for kw, w in KEYWORD_WEIGHT.items():
        if kw in text:
            sc += w
            hits.append(kw)
    return sc, hits


def polarity(text):
    b = sum(1 for w in BULLISH if w in text)
    s = sum(1 for w in BEARISH if w in text)
    return "利好" if b > s else ("利空" if s > b else "中性")


def build_rows(clusters):
    """
    打分主函数。**只读证券名称缓存，绝不发网络请求** ——
    否则 500+ 个簇会各自重试一次，直接把进程拖死。
    需要行情时，调用方先在外部 resolve_secs() 预热缓存，再调用本函数。
    """
    rows = []
    for c in clusters:
        its = c["items"]
        srcs = {i["src"] for i in its}
        buzz = sum(i["buzz"] for i in its)
        flag = max((i["flag"] for i in its), default=0)
        rep = max(its, key=lambda x: len(x["title"]) + len(x["text"]))
        full = " ".join(i["title"] + " " + i["text"] for i in its[:3])

        codes = sorted({s for i in its for s in i["secs"]})[:6]
        secs = _cached_secs(codes)
        tags = sorted({t for i in its for t in i["tags"] if not _is_generic_tag(t)})

        kw_sc, kw_hits = keyword_score(full)

        # 市场实证项：关联标的的实际涨跌幅
        mkt_term = 0.0
        chgs = [abs(float(s["chg"])) for s in secs if s["chg"] is not None]
        if chgs:
            mkt_term = min(sum(chgs) / len(chgs) / 5.0, 1.5)
            if max(chgs) >= 9.8:
                mkt_term += 1.0
        mkt_term = min(mkt_term, 2.5)

        heat_raw = (3.0 * (len(srcs) - 1) + 1.0 * min(len(its), 8)
                    + 0.12 * min(buzz, 50) + 1.5 * flag
                    + max((i.get("fresh_bonus", 0.0) for i in its), default=0.0))
        impact_raw = (2.0 * kw_sc + 0.8 * min(len(tags), 4)
                      + 0.6 * min(len(secs), 3) + mkt_term)

        rows.append({
            "heat_raw": heat_raw, "impact_raw": impact_raw,
            "n_src": len(srcs), "srcs": sorted(srcs), "n_items": len(its),
            "buzz": buzz, "flag": flag,
            "title": rep["title"], "text": rep["text"], "time": rep["time"],
            "secs": [fmt_sec(s) for s in secs], "secs_raw": codes,
            "tags": tags, "kw": kw_hits,
            "dir": polarity(full), "url": rep["url"],
            "recap": bool(RECAP_PAT.search(rep["title"] or "")),
            "rel_title": any(i.get("rel_title") for i in its),
            "is_search": any("kw_hits" in i for i in its),
        })

    if not rows:
        return rows
    hm = max(r["heat_raw"] for r in rows) or 1.0
    im = max(r["impact_raw"] for r in rows) or 1.0
    for r in rows:
        r["heat"] = round(100 * r["heat_raw"] / hm, 1)
        r["impact"] = round(100 * r["impact_raw"] / im, 1)
        r["score"] = round(W_HEAT * r["heat"] + W_IMPACT * r["impact"], 1)
        # 盘面综述类：罗列一堆板块、信息密度低，降权且方向不好判 → 中性
        if r["recap"]:
            r["score"] = round(r["score"] * 0.75, 1)
            r["dir"] = "中性"
        # 检索模式下，只靠正文顺带提及（标题没提该行业）的同样大幅降权
        if r["is_search"] and not r["rel_title"]:
            r["score"] = round(r["score"] * 0.6, 1)
    rows.sort(key=lambda r: -r["score"])
    return rows


# ----------------------------------------------------------------------------
# 输出
# ----------------------------------------------------------------------------

def md_cell(s, limit=None):
    """表格单元格安全化：竖线会把 markdown 表格撑破，换行同理。"""
    s = (s or "").replace("|", "丨").replace("\n", " ").replace("\r", " ")
    if limit:
        s = s[:limit]
    return s.strip()


def to_markdown(rows, start, end, top, heading=None, scope_label="时段", kw_line=None):
    heading = heading or f"财经时段热点 TOP{top}"
    L = [f"# {heading}", "",
         f"**{scope_label}**：{start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}  ",
         f"**样本**：{sum(r['n_items'] for r in rows)} 条 → "
         f"{len(rows)} 个事件簇"]
    if kw_line:
        L.append(f"**检索词**：{kw_line}")
    L += ["",
          "| # | 得分 | 事件 | 方向 | 共振 | 来源/关联 |", "|---|---|---|---|---|---|"]
    for i, r in enumerate(rows[:top], 1):
        rel = " ".join(r["tags"][:2] + r["secs"][:3]) or "、".join(r["srcs"][:3]) or "—"
        L.append(f"| {i} | {r['score']} | {md_cell(r['title'], 40)} | {r['dir']} | "
                 f"{r['n_src']}源/{r['n_items']}条 | {md_cell(rel)} |")
    L.append("")
    for i, r in enumerate(rows[:top], 1):
        L += [f"## {i}. {r['title']}", "",
              f"- **时间**：{r['time']}　**打分**：{r['score']}"
              f"（热度 {r['heat']} / 影响 {r['impact']}）",
              f"- **共振**：{r['n_src']} 家媒体（{'、'.join(r['srcs'][:6])}），共 {r['n_items']} 条"]
        if r["tags"]:
            L.append(f"- **关联板块**：{'、'.join(r['tags'])}")
        if r["secs"]:
            L.append(f"- **关联标的**：{'、'.join(r['secs'])}")
        if r["kw"]:
            L.append(f"- **命中关键词**：{'、'.join(r['kw'][:6])}")
        if r.get("url"):
            L.append(f"- **原文**：{r['url']}")
        L += [f"- **摘要**：{r['text'][:200]}", ""]
    return "\n".join(L)


def to_html(rows, start, end, top, heading=None, scope_label="时段", kw_line=None):
    def esc(s):
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    heading = heading or f"财经时段热点 TOP{top}"
    cls = {"利好": "up", "利空": "down"}
    cards = []
    for i, r in enumerate(rows[:top], 1):
        chips = "".join(f'<span class="tag">{esc(t)}</span>' for t in r["tags"][:4])
        chips += "".join(f'<span class="stock">{esc(s)}</span>' for s in r["secs"][:5])
        chips += "".join(f'<span class="kw">{esc(k)}</span>' for k in r["kw"][:5])
        link = (f'<a class="src" href="{esc(r["url"])}" target="_blank" rel="noopener">原文 ↗</a>'
                if r.get("url") else "")
        cards.append(f"""
    <div class="card">
      <div class="rank">{i}</div>
      <div class="body">
        <div class="head">
          <span class="score">{r['score']}</span>
          <span class="dir {cls.get(r['dir'],'flat')}">{esc(r['dir'])}</span>
          <span class="meta">{esc(r['time'])} · 热度 {r['heat']} / 影响 {r['impact']}</span>
        </div>
        <div class="title">{esc(r['title'])}</div>
        <div class="res">共振 <b>{r['n_src']}</b> 家媒体（{'、'.join(esc(s) for s in r['srcs'][:6])}）· 共 {r['n_items']} 条 {link}</div>
        <div class="summary">{esc(r['text'][:240])}</div>
        <div class="chips">{chips}</div>
      </div>
    </div>""")
    kw_html = f'<div class="period">检索词：{esc(kw_line)}</div>' if kw_line else ""
    return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(heading)}</title>
<style>
  :root{{--bg:#f7f7f5;--card:#fff;--line:#e6e4de;--txt:#22221f;--muted:#77776f;
        --up:#c0392b;--down:#1e8449;--accent:#185FA5}}
  *{{box-sizing:border-box}}
  body{{margin:0;padding:24px 16px 48px;background:var(--bg);color:var(--txt);
       font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
       line-height:1.6}}
  .wrap{{max-width:820px;margin:0 auto}}
  h1{{font-size:20px;font-weight:600;margin:0 0 6px}}
  .period{{color:var(--muted);font-size:13px;margin-bottom:6px}}
  .period.last{{margin-bottom:20px}}
  .card{{display:flex;gap:14px;background:var(--card);border:1px solid var(--line);
        border-radius:12px;padding:16px;margin-bottom:12px}}
  .rank{{flex:0 0 28px;height:28px;border-radius:50%;background:var(--accent);color:#fff;
        font-size:14px;font-weight:600;display:flex;align-items:center;justify-content:center}}
  .body{{flex:1;min-width:0}}
  .head{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px}}
  .score{{font-size:15px;font-weight:600;color:var(--accent)}}
  .dir{{font-size:12px;padding:1px 7px;border-radius:4px;font-weight:500}}
  .dir.up{{background:#fdeaea;color:var(--up)}}
  .dir.down{{background:#e8f6ee;color:var(--down)}}
  .dir.flat{{background:#f0efeb;color:var(--muted)}}
  .meta{{font-size:12px;color:var(--muted)}}
  .title{{font-size:15px;font-weight:600;margin:2px 0 6px}}
  .res{{font-size:12px;color:var(--muted);margin-bottom:8px}}
  .res b{{color:var(--accent)}}
  .src{{color:var(--accent);text-decoration:none;margin-left:6px}}
  .summary{{font-size:13px;color:#44443f;margin-bottom:10px}}
  .chips{{display:flex;flex-wrap:wrap;gap:6px}}
  .chips span{{font-size:12px;padding:2px 8px;border-radius:4px}}
  .tag{{background:#eaf1fa;color:#185FA5}}
  .stock{{background:#fdf0e8;color:#b5530f}}
  .kw{{background:#f0efeb;color:#66665e}}
</style></head><body><div class="wrap">
  <h1>{esc(heading)}</h1>
  <div class="period">{start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} ·
    样本 {sum(r['n_items'] for r in rows)} 条 / {len(rows)} 个事件簇</div>
  {kw_html}
{''.join(cards)}
</div></body></html>"""


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

INDEX_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root{--bg:#f7f7f5;--card:#fff;--line:#e6e4de;--txt:#22221f;--muted:#77776f;
        --up:#c0392b;--down:#1e8449;--accent:#185FA5}
  *{box-sizing:border-box}
  body{margin:0;padding:24px 16px 56px;background:var(--bg);color:var(--txt);
       font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
       line-height:1.6}
  .wrap{max-width:760px;margin:0 auto}
  h1{font-size:21px;font-weight:600;margin:0 0 4px}
  h2{font-size:14px;font-weight:600;margin:28px 0 10px;color:var(--muted)}
  .sub{color:var(--muted);font-size:13px;margin-bottom:18px}
  .searchbar{display:flex;gap:8px}
  .searchbar input{flex:1;min-width:0;font-size:15px;padding:11px 13px;border:1px solid var(--line);
       border-radius:10px;background:var(--card);color:var(--txt);outline:none;font-family:inherit}
  .searchbar input:focus{border-color:var(--accent)}
  .searchbar button{flex:0 0 auto;font-size:15px;font-weight:500;padding:11px 22px;border:none;
       border-radius:10px;background:var(--accent);color:#fff;cursor:pointer;font-family:inherit}
  .searchbar button:disabled{opacity:.55;cursor:default}
  .chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
  .chip{font-size:12px;padding:4px 11px;border-radius:20px;background:#eef0f3;color:#4a5568;
        cursor:pointer;user-select:none}
  .chip:hover{background:var(--accent);color:#fff}
  .opts{display:flex;flex-wrap:wrap;gap:16px;margin-top:12px;font-size:12px;color:var(--muted)}
  .opts label{display:flex;align-items:center;gap:5px}
  .opts select{font-size:12px;padding:3px 6px;border:1px solid var(--line);border-radius:6px;
        background:var(--card);color:var(--txt);font-family:inherit}
  #status{font-size:12px;color:var(--muted);margin-top:16px;min-height:18px}
  .card{display:flex;gap:14px;background:var(--card);border:1px solid var(--line);
        border-radius:12px;padding:15px;margin-bottom:10px}
  .rank{flex:0 0 26px;height:26px;border-radius:50%;background:var(--accent);color:#fff;
        font-size:13px;font-weight:600;display:flex;align-items:center;justify-content:center}
  .body{flex:1;min-width:0}
  .head{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:5px}
  .score{font-size:15px;font-weight:600;color:var(--accent)}
  .dir{font-size:12px;padding:1px 7px;border-radius:4px;font-weight:500}
  .dir.up{background:#fdeaea;color:var(--up)}
  .dir.down{background:#e8f6ee;color:var(--down)}
  .dir.flat{background:#f0efeb;color:var(--muted)}
  .meta{font-size:12px;color:var(--muted)}
  .title{font-size:15px;font-weight:600;margin:2px 0 6px}
  .title a{color:inherit;text-decoration:none}
  .title a:hover{color:var(--accent)}
  .res{font-size:12px;color:var(--muted);margin-bottom:7px}
  .res b{color:var(--accent)}
  .summary{font-size:13px;color:#44443f;margin-bottom:9px}
  .chips2{display:flex;flex-wrap:wrap;gap:6px}
  .chips2 span{font-size:12px;padding:2px 8px;border-radius:4px}
  .kw{background:#f0efeb;color:#66665e}
  .src{background:#eaf1fa;color:#185FA5}
  mark{background:#fff3c4;color:inherit;padding:0 1px;border-radius:2px}
  .row{display:flex;align-items:center;gap:10px;background:var(--card);
       border:1px solid var(--line);border-radius:10px;padding:11px 14px;
       margin-bottom:8px;text-decoration:none;color:inherit}
  .row:hover{border-color:var(--accent)}
  .tag{flex:0 0 auto;font-size:11px;padding:2px 7px;border-radius:4px}
  .tag.hot{background:#eaf1fa;color:#185FA5}
  .tag.search{background:#fdf0e8;color:#b5530f}
  .name{flex:1;min-width:0;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .time{flex:0 0 auto;font-size:12px;color:var(--muted)}
  .empty{color:var(--muted);font-size:13px;padding:16px 0}
  .note{font-size:12px;color:var(--muted);margin-top:14px;padding-top:12px;
        border-top:1px solid var(--line)}
</style></head><body><div class="wrap">
  <h1>财经热点 · 行业检索</h1>
  <div class="sub">输入行业关键词，实时检索全网财经新闻（结果在浏览器本地排序，无后端）</div>

  <div class="searchbar">
    <input id="q" placeholder="PCB / 半导体 / 固态电池 / 覆铜板…" autocomplete="off">
    <button id="go">搜索</button>
  </div>
  <div class="chips" id="chips"></div>
  <div class="opts">
    <label>时间
      <select id="days">
        <option value="3">近 3 天</option>
        <option value="7" selected>近 7 天</option>
        <option value="14">近 14 天</option>
        <option value="30">近 30 天</option>
        <option value="90">近 90 天</option>
      </select>
    </label>
    <label>条数
      <select id="top">
        <option value="5">5</option>
        <option value="10" selected>10</option>
        <option value="20">20</option>
        <option value="30">30</option>
      </select>
    </label>
    <label>排序
      <select id="sort">
        <option value="hot" selected>按重要性</option>
        <option value="time">按时间</option>
      </select>
    </label>
  </div>

  <div id="status"></div>
  <div id="results"></div>

  <div id="reports">
    <h2>最近生成的报告</h2>
    __ROWS__
    <div class="note">检索由东方财富站内搜索提供数据；打分规则与仓库里的 <code>hot_news.py</code> 一致。</div>
  </div>
</div>
<script>
(function(){
  "use strict";
  var PRESETS = ["PCB","半导体","固态电池","光伏","锂电","机器人","算力","军工","医药","白酒","稀土","消费电子"];

  // 与 hot_news.py 的 SECTOR_ALIAS 保持一致
  var ALIAS = {
    "PCB": ["PCB","覆铜板","印制电路板","线路板","HDI","IC载板","CCL","铜箔"],
    "半导体": ["半导体","芯片","晶圆","光刻","封测","集成电路","存储芯片","先进封装"],
    "光伏": ["光伏","硅片","电池片","组件","逆变器","多晶硅","TOPCon","HJT"],
    "锂电": ["锂电","动力电池","正极材料","负极材料","隔膜","电解液","固态电池"],
    "机器人": ["机器人","人形机器人","减速器","伺服电机","灵巧手","丝杠"],
    "算力": ["算力","AI服务器","GPU","液冷","光模块","CPO","交换机","数据中心"],
    "军工": ["军工","国防","航空发动机","导弹","卫星","商业航天"],
    "医药": ["医药","创新药","生物医药","CXO","医疗器械","疫苗","集采"],
    "白酒": ["白酒","酱酒","次高端","高端酒"],
    "房地产": ["房地产","地产","楼市","保交楼","保障房"],
    "券商": ["券商","证券","投行","两融","并购重组"],
    "稀土": ["稀土","永磁","磁材","镨钕","稀有金属"],
    "消费电子": ["消费电子","苹果链","果链","折叠屏","AR","VR","智能穿戴"],
    "汽车": ["汽车","新能源车","整车","智能驾驶","零部件","出海"]
  };

  // 与 hot_news.py 的 KEYWORD_WEIGHT 保持一致
  var KW = {"降准":6,"降息":6,"央行":5,"LPR":5,"MLF":4,"逆回购":4,"政治局":6,"国务院":5,
    "国常会":5,"中央经济工作会议":6,"证监会":5,"印花税":5,"注册制":5,"发改委":4,"财政部":4,
    "国资委":4,"工信部":4,"能源局":4,"外汇局":4,"爆雷":6,"造假":6,"退市":5,"立案":5,
    "破产":5,"违约":5,"被查":5,"减持":4,"违规":4,"处罚":4,"冻结":4,"警示函":4,"监管函":4,
    "问询":3,"借壳":6,"重组":5,"并购":5,"资产注入":5,"易主":5,"实控人变更":5,"收购":4,
    "要约":4,"股权转让":3,"业绩预告":4,"预增":4,"预减":4,"扭亏":4,"超预期":4,"净利润":3,
    "亏损":3,"涨停":5,"跌停":5,"连板":5,"北向资金":4,"龙虎榜":4,"游资":4,"IPO":4,"两融":3,
    "主力资金":3,"关税":6,"制裁":6,"出口管制":6,"实体清单":6,"贸易战":6,"战争":6,"冲突":5,
    "袭击":5,"停火":4,"美联储":5,"加息":5,"非农":4,"CPI":4,"PPI":3,"GDP":4,"稀土":4,
    "固态电池":4,"可控核聚变":4,"房地产":4,"地方债":4,"人工智能":3,"算力":3,"芯片":3,
    "半导体":3,"锂":3,"光伏":3,"储能":3,"机器人":3,"创新药":3,"商业航天":3,"军工":3,
    "黄金":3,"原油":3,"消费券":3};

  var BULL = ["批准","获批","中标","增持","回购","预增","超预期","签约","涨价","突破","合作",
              "上调","利好","放量","扭亏","新高","支持","补贴","减税","提速","投产","成功"];
  var BEAR = ["减持","立案","处罚","亏损","预减","下调","终止","失败","被查","退市","违规",
              "爆雷","跌停","利空","违约","冻结","警示","问询","禁止","限制","暂停","取消","下滑"];

  var RECAP = /(收评|午评|晚报|早报|开盘|收盘|复盘|盘面|异动|龙虎榜|两市|全市场|三大指数|双创指数|涨停|跌停|午盘|个股下跌|个股上涨|涨幅榜|跌幅榜|公告集锦|公告汇总|公告精选|盘后公告|早间公告|晚间公告|公司公告|业绩汇总|要闻|一览|速览|盘点|集锦|汇总|快讯汇总|涨幅居前|跌幅居前|看点|前瞻|综述|导读|简报|盘前|盘后|隔夜)/;

  var $q = document.getElementById("q"),
      $go = document.getElementById("go"),
      $st = document.getElementById("status"),
      $rs = document.getElementById("results"),
      $rp = document.getElementById("reports"),
      $days = document.getElementById("days"),
      $top = document.getElementById("top"),
      $sort = document.getElementById("sort");

  function esc(s){
    return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;")
      .replace(/>/g,"&gt;").replace(/"/g,"&quot;");
  }
  function expandKeywords(spec){
    var parts = spec.split(/[,，、|/]/).map(function(s){return s.trim();})
                    .filter(function(s){return s.length;});
    if(!parts.length) return [];
    if(parts.length>1) return uniq(parts);
    var k = parts[0];
    for(var name in ALIAS){
      if(k.toLowerCase()===name.toLowerCase() || ALIAS[name].indexOf(k)>=0)
        return uniq([k].concat(ALIAS[name]));
    }
    return [k];
  }
  function uniq(a){ var o=[],seen={}; a.forEach(function(x){ if(!seen[x]){seen[x]=1;o.push(x);} }); return o; }

  function parseTime(s){
    if(!s) return null;
    var t = new Date(String(s).replace(/-/g,"/"));
    return isNaN(t.getTime()) ? null : t;
  }

  function relevance(item, kws){
    var title = item.title||"", text = item.content||"";
    var tl = title.toLowerCase(), bl = text.toLowerCase();
    var hitT = kws.filter(function(k){ return tl.indexOf(k.toLowerCase())>=0; });
    var hitB = kws.filter(function(k){ return bl.indexOf(k.toLowerCase())>=0; });
    if(hitT.length) return {ok:true, hits:uniq(hitT.concat(hitB)), relTitle:true};
    if(RECAP.test(title)) return {ok:false};
    var cnt = 0;
    kws.forEach(function(k){
      var kk = k.toLowerCase(), i = 0;
      while((i = bl.indexOf(kk, i)) >= 0){ cnt++; i += kk.length; }
    });
    if(cnt>=3) return {ok:true, hits:uniq(hitB), relTitle:false};
    return {ok:false};
  }

  function bigrams(s){
    s = String(s||"").replace(/[^\u4e00-\u9fa5A-Za-z0-9]/g,"");
    var o = {}; for(var i=0;i<s.length-1;i++) o[s.substr(i,2)] = 1;
    return o;
  }
  function jac(a,b){
    var ka = Object.keys(a), n = 0;
    if(!ka.length) return 0;
    for(var i=0;i<ka.length;i++) if(b[ka[i]]) n++;
    return n / Math.min(ka.length, Object.keys(b).length || 1);
  }
  function entity(t){
    t = String(t||"").trim();
    var m = t.match(/^[【\[]\s*([^：:】\]，,。]{2,24})\s*[：:]/) || t.match(/^([\u4e00-\u9fa5A-Za-z0-9]{3,20})\s*[：:]/);
    if(!m) return null;
    var e = m[1].trim();
    if(/^(消息|据悉|记者|公告|报道|快讯)/.test(e)) return null;
    return (e.length>=2 && e.length<=24) ? e : null;
  }
  function digits(s){
    var m = String(s||"").match(/\d+(?:\.\d+)?/g) || [], o = {};
    m.forEach(function(d){ if(d.length>=2) o[d]=1; });
    return o;
  }

  function cluster(items){
    var cs = [];
    items.forEach(function(it){
      var blob = (it.title||"") + " " + String(it.content||"").slice(0,80);
      var tk = bigrams(blob), dg = digits(blob), ent = entity(it.title);
      var best = null, bs = 0;
      cs.forEach(function(c){
        if(ent && c.ent && ent !== c.ent) return;
        var sim = jac(tk, c.tk);
        if(Object.keys(dg).length && Object.keys(c.dg).length)
          sim *= (0.35 + 0.65*jac(dg, c.dg));
        if(sim > bs){ bs = sim; best = c; }
      });
      if(best && bs >= 0.50){
        best.items.push(it);
        Object.keys(tk).forEach(function(k){ best.tk[k]=1; });
        Object.keys(dg).forEach(function(k){ best.dg[k]=1; });
      } else {
        cs.push({items:[it], tk:tk, dg:dg, ent:ent});
      }
    });
    return cs;
  }

  function polarity(s){
    var b=0, e=0;
    BULL.forEach(function(w){ if(s.indexOf(w)>=0) b++; });
    BEAR.forEach(function(w){ if(s.indexOf(w)>=0) e++; });
    return b>e ? "利好" : (e>b ? "利空" : "中性");
  }

  function buildRows(cs, now){
    var rows = cs.map(function(c){
      var its = c.items;
      var srcs = uniq(its.map(function(i){ return i.media || "东方财富"; }));
      var rep = its.reduce(function(a,b){
        return ((b.title||"").length+(b.content||"").length) > ((a.title||"").length+(a.content||"").length) ? b : a;
      });
      var full = its.slice(0,3).map(function(i){ return (i.title||"")+" "+(i.content||""); }).join(" ");
      var hits = uniq([].concat.apply([], its.map(function(i){ return i.kwHits||[]; })));
      var kwScore = 0, kws = [];
      for(var k in KW){ if(full.indexOf(k)>=0){ kwScore += KW[k]; kws.push(k); } }
      var t = parseTime(rep.date);
      var ageH = t ? Math.max((now - t)/3600000, 0) : 999;
      var fresh = 2.0 * Math.pow(0.5, ageH/36);
      var relTitle = its.some(function(i){ return i.relTitle; });

      var heat = 3.0*(srcs.length-1) + Math.min(its.length,8) + fresh;
      var impact = 2.0*kwScore + 0.8*Math.min(hits.length,4) + (relTitle?1.5:0);
      return {
        heatRaw: heat, impactRaw: impact,
        nSrc: srcs.length, srcs: srcs, nItems: its.length,
        title: rep.title, text: rep.content, date: rep.date, url: rep.url,
        hits: hits, kws: kws, relTitle: relTitle,
        recap: RECAP.test(rep.title||""),
        dir: polarity(full)
      };
    });
    if(!rows.length) return rows;
    var mh = Math.max.apply(null, rows.map(function(r){return r.heatRaw;})) || 1;
    var mi = Math.max.apply(null, rows.map(function(r){return r.impactRaw;})) || 1;
    rows.forEach(function(r){
      r.heat = Math.round(1000*r.heatRaw/mh)/10;
      r.impact = Math.round(1000*r.impactRaw/mi)/10;
      r.score = Math.round(10*(0.55*r.heat + 0.45*r.impact))/10;
      // 综述/集锦类：罗列一堆板块，信息密度低
      if(r.recap){ r.score = Math.round(r.score*7.5)/10; r.dir = "中性"; }
      // 只靠正文顺带提及（标题完全没提该行业）：大幅降权，
      // 否则「美股全线下跌」这种快讯会挤掉真正的行业新闻
      if(!r.relTitle) r.score = Math.round(r.score*6)/10;
    });
    rows.sort(function(a,b){ return b.score - a.score; });
    return rows;
  }

  function hi(text, kws){
    var s = esc(text);
    uniq(kws).sort(function(a,b){ return b.length-a.length; }).forEach(function(k){
      if(!k) return;
      var re = new RegExp(k.replace(/[.*+?^${}()|[\]\\]/g,"\\$&"), "gi");
      s = s.replace(re, function(m){ return "<mark>"+m+"</mark>"; });
    });
    return s;
  }

  function jsonp(url, cb){
    return new Promise(function(resolve, reject){
      var s = document.createElement("script");
      var done = false;
      var timer = setTimeout(function(){ if(!done){ done=true; clean(); reject(new Error("请求超时")); } }, 15000);
      function clean(){
        clearTimeout(timer);
        try{ delete window[cb]; }catch(e){ window[cb]=undefined; }
        if(s.parentNode) s.parentNode.removeChild(s);
      }
      window[cb] = function(d){ if(done) return; done=true; clean(); resolve(d); };
      s.onerror = function(){ if(done) return; done=true; clean(); reject(new Error("网络请求失败")); };
      s.src = url;
      document.head.appendChild(s);
    });
  }

  function searchPage(kw, page, size){
    var cb = "emcb_" + Date.now() + "_" + Math.floor(Math.random()*1e6);
    var param = {
      uid:"", keyword:kw, type:["cmsArticleWebOld"],
      client:"web", clientType:"web", clientVersion:"curr",
      param:{ cmsArticleWebOld:{
        searchScope:"default", sort:"time", pageIndex:page,
        pageSize:size, preTag:"", postTag:"" }}
    };
    var url = "https://search-api-web.eastmoney.com/search/jsonp?cb=" + cb +
              "&param=" + encodeURIComponent(JSON.stringify(param));
    return jsonp(url, cb).then(function(d){
      d = d || {};
      return ((d.result||{}).cmsArticleWebOld) || [];
    });
  }

  function render(rows, kws, meta){
    if(!rows.length){
      $rs.innerHTML = '<div class="empty">没有找到相关新闻。换个词，或把时间范围放宽试试。</div>';
      return;
    }
    var html = rows.map(function(r,i){
      var chips = r.srcs.slice(0,4).map(function(s){
        return '<span class="src">'+esc(s)+'</span>'; }).join("");
      chips += r.kws.slice(0,5).map(function(k){ return '<span class="kw">'+esc(k)+'</span>'; }).join("");
      chips += r.hits.slice(0,4).map(function(k){ return '<span class="kw">命中 '+esc(k)+'</span>'; }).join("");
      var t = r.url ? '<a href="'+esc(r.url)+'" target="_blank" rel="noopener">'+hi(r.title,kws)+'</a>' : hi(r.title,kws);
      var d = {"利好":"up","利空":"down"}[r.dir] || "flat";
      return '<div class="card"><div class="rank">'+(i+1)+'</div><div class="body">'
        + '<div class="head"><span class="score">'+r.score+'</span>'
        + '<span class="dir '+d+'">'+esc(r.dir)+'</span>'
        + '<span class="meta">'+esc(r.date)+' · 热度 '+r.heat+' / 影响 '+r.impact+'</span></div>'
        + '<div class="title">'+t+'</div>'
        + '<div class="res">共振 <b>'+r.nSrc+'</b> 家媒体（'+esc(r.srcs.slice(0,5).join("、"))+'）· 共 '+r.nItems+' 条</div>'
        + '<div class="summary">'+hi(String(r.text||"").slice(0,240), kws)+'</div>'
        + '<div class="chips2">'+chips+'</div></div></div>';
    }).join("");
    $rs.innerHTML = html;
    $st.textContent = "「" + meta.spec + "」扩展 " + kws.length + " 词（检索 " +
      (meta.terms||[]).join("、") + "）· 抓取 " + meta.fetched + " 条 → 相关 " + meta.kept +
      " 条 → 归并 " + meta.clusters + " 个事件 · 展示 " +
      Math.min(rows.length, meta.top) + " 条 · 耗时 " + meta.ms + "ms";
      if(kws.length>1) $st.textContent += " ｜ " + kws.join("、");
  }

  var running = false;
  function run(){
    var spec = $q.value.trim();
    if(!spec){ $st.textContent = "请输入行业关键词"; return; }
    if(running) return;
    running = true; $go.disabled = true;
    var kws = expandKeywords(spec);
    var days = parseInt($days.value,10), top = parseInt($top.value,10),
        sort = $sort.value;
    var terms = kws.slice(0,2);          // 主词 + 第一个同义词，明显提高召回
    var pages = Math.min(3, Math.max(1, Math.ceil(top/10) + 1));
    var t0 = Date.now(), got = [], failed = 0;

    $rp.style.display = "none";
    $rs.innerHTML = "";
    $st.textContent = "检索中…";

    var tasks = [];
    terms.forEach(function(term){
      for(var p=1; p<=pages; p++){
        tasks.push(searchPage(term, p, 30).catch(function(){ failed++; return []; }));
      }
    });
    Promise.all(tasks).then(function(res){
      res.forEach(function(arr){
        arr.forEach(function(it){ got.push(it); });
      });
      var now = Date.now();
      var since = now - days*86400000;
      var kept = [], seen = {};
      got.forEach(function(it){
        var t = parseTime(it.date);
        if(t && t.getTime() < since) return;
        var key = (it.title||"") + "|" + (it.date||"");
        if(seen[key]) return;
        seen[key] = 1;
        var r = relevance(it, kws);
        if(!r.ok) return;
        it.kwHits = r.hits; it.relTitle = r.relTitle;
        kept.push(it);
      });
      var cs = cluster(kept);
      var rows = buildRows(cs, now);
      if(sort === "time"){
        rows.sort(function(a,b){
          var ta = parseTime(a.date) || new Date(0), tb = parseTime(b.date) || new Date(0);
          return tb - ta;
        });
        rows.sort(function(a,b){ return (a.relTitle?0:1) - (b.relTitle?0:1); });
      }
      render(rows, kws, {spec:spec, terms:terms, fetched:got.length, kept:kept.length,
                         clusters:cs.length, top:top, ms:Date.now()-t0});
      if(failed) $st.textContent += " · " + failed + " 页请求失败";
      running = false; $go.disabled = false;
      location.hash = "q=" + encodeURIComponent(spec) +
                       "&days=" + days + "&top=" + top + "&sort=" + sort;
    }).catch(function(e){
      $st.textContent = "检索失败：" + e.message;
      running = false; $go.disabled = false;
    });
  }

  // 快捷标签
  document.getElementById("chips").innerHTML = PRESETS.map(function(k){
    return '<span class="chip" data-k="'+esc(k)+'">'+esc(k)+'</span>'; }).join("");
  document.getElementById("chips").addEventListener("click", function(e){
    var k = e.target.getAttribute && e.target.getAttribute("data-k");
    if(k){ $q.value = k; run(); }
  });

  $go.addEventListener("click", run);
  $q.addEventListener("keydown", function(e){ if(e.key === "Enter") run(); });
  [$days,$top,$sort].forEach(function(el){ el.addEventListener("change", function(){ if($q.value.trim()) run(); }); });

  // 从 URL 恢复
  (function(){
    var h = location.hash.replace(/^#/,""), o = {};
    h.split("&").forEach(function(p){ var kv = p.split("="); if(kv[0]) o[kv[0]] = decodeURIComponent(kv[1]||""); });
    if(o.days) $days.value = o.days;
    if(o.top)  $top.value  = o.top;
    if(o.sort) $sort.value = o.sort;
    if(o.q){ $q.value = o.q; run(); }
  })();
})();
</script></body></html>"""


def build_index(outdir, title="财经热点 · 行业检索"):
    """扫描 outdir 里已生成的 html，重建首页：上部是实时检索，下部是历史报告。"""
    try:
        items = []
        for fn in os.listdir(outdir):
            if not fn.endswith(".html") or fn == "index.html":
                continue
            p = os.path.join(outdir, fn)
            mt = os.path.getmtime(p)
            kind = "行业检索" if fn.startswith("search_") else "时段热点"
            items.append((mt, fn, kind))
        items.sort(reverse=True)
    except Exception as e:
        warn(f"[索引] 跳过：{e}")
        return None

    def esc(s):
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows = []
    for mt, fn, kind in items:
        t = datetime.fromtimestamp(mt).strftime("%m-%d %H:%M")
        label = esc(fn[:-5])
        cls = "tag search" if kind == "行业检索" else "tag hot"
        rows.append(
            f'<a class="row" href="{esc(fn)}">'
            f'<span class="{cls}">{kind}</span>'
            f'<span class="name">{label}</span>'
            f'<span class="time">{t}</span></a>')

    if not rows:
        rows = ['<div class="empty">还没有生成任何报告。到 Actions 里跑一次吧。</div>']

    html = (INDEX_TEMPLATE
            .replace("__TITLE__", esc(title))
            .replace("__ROWS__", "\n    ".join(rows)))
    p = os.path.join(outdir, "index.html")
    try:
        open(p, "w", encoding="utf-8").write(html)
        return p
    except Exception as e:
        warn(f"[索引] 写入失败：{e}")
        return None


def write_outputs(rows, start, end, args, prefix, heading,
                  scope_label="时段", kw_line=None):
    """统一落盘 md/html/json，返回 (written, md_text)。"""
    os.makedirs(args.outdir, exist_ok=True)
    fmts = {f.strip() for f in args.out.split(",") if f.strip()}
    md = to_markdown(rows, start, end, args.top, heading, scope_label, kw_line)
    written = []
    if "md" in fmts:
        p = os.path.join(args.outdir, f"{prefix}.md")
        open(p, "w", encoding="utf-8").write(md)
        written.append(p)
    if "html" in fmts:
        p = os.path.join(args.outdir, f"{prefix}.html")
        open(p, "w", encoding="utf-8").write(
            to_html(rows, start, end, args.top, heading, scope_label, kw_line))
        written.append(p)
    if "json" in fmts:
        p = os.path.join(args.outdir, f"{prefix}.json")
        json.dump({"window": [str(start), str(end)],
                   "heading": heading, "keywords": kw_line,
                   "rows": rows[:args.top]},
                  open(p, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
        written.append(p)
    if "html" in fmts:
        build_index(args.outdir)
    return written, md


def _collect_live(pages, label="抓取"):
    """抓 5 个实时快讯源。搜索模式下用来补充「最近几小时」的新消息。"""
    out = []
    for fn in FETCHERS:
        try:
            if fn in (fetch_jin10, fetch_wallstreetcn):
                got = fn()
            else:
                got = fn(pages)
        except Exception as e:
            warn(f"[{label}] {fn.__name__} 异常：{e}")
            continue
        warn(f"[{label}] {fn.__name__}: {len(got)} 条")
        out.extend(got)
        time.sleep(0.15)
    return out


def run_search(args):
    """行业关键词搜索模式：主动触发，不参与定时采集。"""
    now = datetime.now()
    kws = expand_keywords(args.keyword)
    if not kws:
        warn("[错误] 关键词为空")
        return 1
    warn(f"[检索] {args.keyword} → 扩展为 {len(kws)} 个词：{'、'.join(kws[:10])}")

    start = now - timedelta(days=args.days)
    end = now

    # 1) 东财站内搜索：覆盖历史新闻
    hist = fetch_eastmoney_search(kws[0], pages=args.search_pages, page_size=30,
                                  since=start)
    warn(f"[搜索] 站内 {len(hist)} 条")

    # 2) 实时快讯：补充最近几小时（同时带来关联个股/板块字段）
    live = _collect_live(args.pages, label="搜补") if args.live else []
    warn(f"[搜索] 实时快讯 {len(live)} 条")

    # 3) 关键词相关性过滤 + 时间过滤
    pool = []
    for it in hist + live:
        hits = is_relevant(it, kws)
        if not hits:
            continue
        it["kw_hits"] = hits
        t = parse_time(it["time"])
        it["time_dt"] = t
        if t and start <= t <= end:
            it["fresh_bonus"] = freshness_bonus(t, now)
            pool.append(it)
        elif t is None:
            pool.append(it)
    warn(f"[搜索] 相关且落在最近 {args.days} 天内：{len(pool)} 条")

    if not pool:
        warn("[错误] 没有命中任何新闻，换个词或加大 --days 试试")
        return 1

    if args.sort == "time":
        pool.sort(key=lambda x: x["time_dt"] or datetime.min, reverse=True)

    cls = cluster(pool)
    warn(f"[聚类] {len(cls)} 个事件簇")
    rows = build_rows(cls)
    codes = preselect_codes(rows, max_clusters=40)
    if codes:
        resolve_secs(codes)
        rows = build_rows(cls)
    save_cache()

    if args.sort == "time":
        # 先按时间倒序，再按「标题是否命中关键词」分档（稳定排序保留档内时间序）
        rows.sort(key=lambda r: parse_time(r["time"]) or datetime.min, reverse=True)
        rows.sort(key=lambda r: 0 if r.get("rel_title") else 1)

    heading = f"{args.keyword} 行业新闻 TOP{args.top}"
    prefix = f"search_{re.sub(r'[^0-9A-Za-z一-龥]+', '', args.keyword)[:12]}"
    written, md = write_outputs(rows, start, end, args, prefix, heading,
                                scope_label="检索范围", kw_line="、".join(kws))
    if not args.quiet:
        print(md)
    for p in written:
        warn(f"[输出] {p}")
    return 0


def run_hot(args):
    """时段热点模式：全市场自动采集。"""
    start, end = resolve_window(args)
    warn(f"[窗口] {start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M}")

    all_items = _collect_live(args.pages)

    kept = []
    for it in all_items:
        t = parse_time(it["time"])
        it["time_dt"] = t
        if t and start <= t <= end:
            kept.append(it)
    warn(f"[过滤] 窗口内 {len(kept)} 条 / 共 {len(all_items)} 条")
    if not kept:
        warn("[警告] 窗口内无数据，回退到全部抓取结果")
        kept = [it for it in all_items if it["time_dt"]]

    cls = cluster(kept)
    warn(f"[聚类] {len(cls)} 个事件簇")

    # 两段式打分：先粗排 → 只解析头部簇的证券 → 用热缓存重排
    rows = build_rows(cls)
    codes = preselect_codes(rows, max_clusters=120)
    if codes:
        warn(f"[解析] 头部簇关联证券 {len(codes)} 个")
        resolve_secs(codes)
        rows = build_rows(cls)
    save_cache()

    if not rows:
        warn("[错误] 无可用数据")
        return 1

    tag = f"{start:%m%d_%H%M}-{end:%m%d_%H%M}"
    written, md = write_outputs(rows, start, end, args, f"hot_{tag}",
                                f"财经时段热点 TOP{args.top}")
    if not args.quiet:
        print(md)
    for p in written:
        warn(f"[输出] {p}")
    return 0


def main():
    ap = argparse.ArgumentParser(
        description="财经热点 / 行业新闻检索（零依赖）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例：
  时段热点（自动采集全市场）
    python hot_news.py --start 15:00 --end 08:30 --top 5
    python hot_news.py --window 8

  行业新闻检索（只在主动搜索时跑，不参与定时采集）
    python hot_news.py --keyword PCB
    python hot_news.py --keyword PCB --days 14 --top 10
    python hot_news.py --keyword "覆铜板,CCL,IC载板" --sort time
    python hot_news.py --keyword 机器人 --outdir ./out --out md,html
""")
    ap.add_argument("--keyword", "-k", help="行业关键词；单关键词会自动扩展同义词，"
                                            "逗号分隔多个词则以你给的为准")
    ap.add_argument("--days", type=float, default=7.0, help="检索回溯天数（默认7）")
    ap.add_argument("--sort", choices=["hot", "time"], default="hot",
                    help="检索结果排序：hot=按重要性（默认），time=按时间倒序")
    ap.add_argument("--no-live", dest="live", action="store_false", default=True,
                    help="检索时不抓实时快讯，只查站内历史")
    ap.add_argument("--search-pages", type=int, default=3, help="站内搜索翻页数（默认3）")
    ap.add_argument("--window", type=float, default=8.0, help="回溯小时数（默认8）")
    ap.add_argument("--start", help="起始 HH:MM")
    ap.add_argument("--end", help="结束 HH:MM")
    ap.add_argument("--date", help="基准日期 YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument("--out", default="md,html,json")
    ap.add_argument("--outdir", default=DIR)
    ap.add_argument("--pages", type=int, default=MAX_PAGES)
    ap.add_argument("--quiet", action="store_true", help="不打印正文，只写文件")
    ap.add_argument("--build-index", action="store_true",
                    help="只重建首页 docs/index.html（含搜索入口）后退出")
    args = ap.parse_args()

    load_cache()
    if args.build_index:
        p = build_index(args.outdir)
        warn(f"[索引] {p}" if p else "[索引] 失败")
        return 0 if p else 1
    if args.keyword:
        return run_search(args)
    return run_hot(args)


if __name__ == "__main__":
    sys.exit(main())
