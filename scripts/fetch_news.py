"""
fetch_news.py
每日抓取財經新聞，輸出為 Markdown 報告。
- 鉅亨網：使用官方 JSON API（v3 為主，media API 為備援）
- 其他來源：RSS feedparser
"""

import csv
import feedparser
import io
import re
import sys
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# 台灣時區 UTC+8
TW_TZ = timezone(timedelta(hours=8))

# ── 鉅亨網 API ──────────────────────────────────────────────────────────────
# 主要：news.cnyes.com v3 API
CNYES_API_V3 = "https://news.cnyes.com/api/v3/news/category/{category}"
# 備援：api.cnyes.com media API v1
CNYES_MEDIA_API = "https://api.cnyes.com/media/api/v1/newslist/category/{category}"

CNYES_CATEGORIES = {
    "鉅亨網－台股": "tw_stock",
    "鉅亨網－國際股": "wd_stock",
    "鉅亨網－港股": "cn_stock",
    "鉅亨網－美股": "us_stock",
}

# ── RSS 來源（鉅亨網已改用 API，不在此列）─────────────────────────────────
RSS_SOURCES = [
    {
        "name": "經濟日報－財經",
        "url": "https://money.udn.com/rssfeed/news/1001/5591",
        "max_items": 5,
    },
    {
        "name": "Yahoo Finance",
        "url": "https://finance.yahoo.com/rss/topstories",
        "max_items": 5,
    },
    {
        "name": "MarketWatch Top Stories",
        "url": "https://feeds.marketwatch.com/marketwatch/topstories/",
        "max_items": 5,
    },
    {
        "name": "WSJ Markets",
        "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "max_items": 10,
    },
    {
        "name": "WSJ US Business",
        "url": "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
        "max_items": 10,
    },
    # ── 中央通訊社 ──────────────────────────────────────────────────────────
    {
        "name": "中央社－財經",
        "url": "https://feeds.feedburner.com/rsscna/finance",
        "max_items": 10,
    },
    {
        "name": "中央社－科技",
        "url": "https://feeds.feedburner.com/rsscna/technology",
        "max_items": 10,
    },
    # ── 經濟日報（股市）────────────────────────────────────────────────────
    {
        "name": "經濟日報－股市",
        "url": "https://money.udn.com/rssfeed/news/1001/5590",
        "max_items": 10,
    },
    # ── NHK ─────────────────────────────────────────────────────────────────
    {
        "name": "NHK World",
        "url": "https://news.web.nhk/n-data/conf/na/rss/cat5.xml",
        "max_items": 10,
    },
    # ── 日經新聞 ─────────────────────────────────────────────────────────────
    {
        "name": "日經－市場",
        "url": "https://assets.wor.jp/rss/rdf/nikkei/markets.rdf",
        "max_items": 10,
    },
    {
        "name": "日經－商業",
        "url": "https://assets.wor.jp/rss/rdf/nikkei/business.rdf",
        "max_items": 10,
    },
    # ── 路透社 ───────────────────────────────────────────────────────────────
    {
        "name": "路透社－市場",
        "url": "https://assets.wor.jp/rss/rdf/reuters/markets.rdf",
        "max_items": 10,
    },
    {
        "name": "路透社－經濟",
        "url": "https://assets.wor.jp/rss/rdf/reuters/economy.rdf",
        "max_items": 10,
    },
    # ── 彭博社 ───────────────────────────────────────────────────────────────
    {
        "name": "彭博社－市場",
        "url": "https://assets.wor.jp/rss/rdf/bloomberg/markets.rdf",
        "max_items": 10,
    },
    {
        "name": "彭博社－財經",
        "url": "https://assets.wor.jp/rss/rdf/bloomberg/finance.rdf",
        "max_items": 10,
    },
    # ── 日本報紙 ─────────────────────────────────────────────────────────────
    {
        "name": "產經新聞－經濟",
        "url": "https://assets.wor.jp/rss/rdf/sankei/economy.rdf",
        "max_items": 10,
    },
    {
        "name": "讀賣新聞－經濟",
        "url": "https://assets.wor.jp/rss/rdf/yomiuri/economy.rdf",
        "max_items": 10,
    },
]

HEADERS = {
    "User-Agent": "MorningFinanceBrief/1.0 (https://github.com/wwwchen/Morning-Finance-Brief)"
}

# ── 大盤指數 ─────────────────────────────────────────────────────────────────
# yfinance 為主要來源，Stooq 為備援
INDICES = [
    {"name": "台灣加權", "yf": "^TWII", "stooq": "^twig"},
    {"name": "S&P 500",  "yf": "^GSPC", "stooq": "^spx"},
    {"name": "那斯達克", "yf": "^IXIC", "stooq": "^ndq"},
    {"name": "道　　瓊", "yf": "^DJI",  "stooq": "^dji"},
    {"name": "日經 225", "yf": "^N225", "stooq": "^nkx"},
    {"name": "恆生指數", "yf": "^HSI",  "stooq": "^hsi"},
]


def _stooq_last_close(symbol: str) -> tuple[float, float, str] | None:
    """從 Stooq 抓最近兩個交易日收盤價。回傳 (close, prev_close, date_str) 或 None。"""
    try:
        resp = requests.get(
            "https://stooq.com/q/d/l/",
            params={"s": symbol, "i": "d"},
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(resp.text)))
        if len(rows) < 2:
            return None
        last, prev = rows[-1], rows[-2]
        return float(last["Close"]), float(prev["Close"]), last["Date"]
    except Exception as e:
        print(f"  [WARN] Stooq {symbol}: {e}", file=sys.stderr)
        return None


def fetch_indices() -> list[dict]:
    """抓取各大盤指數最後收盤價，yfinance 為主，Stooq 為備援。"""
    print("\n--- 大盤指數 ---")
    try:
        import yfinance as yf  # noqa: PLC0415
        yf_ok = True
    except ImportError:
        print("  [WARN] yfinance 未安裝，切換 Stooq 備援", file=sys.stderr)
        yf_ok = False

    results = []
    for idx in INDICES:
        entry: dict = {
            "name": idx["name"],
            "close": None,
            "prev_close": None,
            "change": None,
            "change_pct": None,
            "date": "",
        }
        fetched = False

        if yf_ok:
            try:
                hist = yf.Ticker(idx["yf"]).history(period="5d")["Close"].dropna()
                if len(hist) >= 2:
                    close = float(hist.iloc[-1])
                    prev_close = float(hist.iloc[-2])
                    entry.update(
                        close=close,
                        prev_close=prev_close,
                        change=close - prev_close,
                        change_pct=(close - prev_close) / prev_close * 100,
                        date=hist.index[-1].strftime("%Y-%m-%d"),
                    )
                    fetched = True
                    print(f"  OK (yfinance): {idx['name']} {close:,.2f}")
            except Exception as e:
                print(f"  [WARN] yfinance {idx['yf']}: {e}", file=sys.stderr)

        if not fetched:
            r = _stooq_last_close(idx["stooq"])
            if r:
                close, prev_close, date = r
                entry.update(
                    close=close,
                    prev_close=prev_close,
                    change=close - prev_close,
                    change_pct=(close - prev_close) / prev_close * 100,
                    date=date,
                )
                print(f"  OK (Stooq):    {idx['name']} {close:,.2f}")
            else:
                print(f"  [ERROR] {idx['name']} 無法取得", file=sys.stderr)

        results.append(entry)
    return results


def fetch_cnyes_api(name: str, category: str, max_items: int = 30, pages: int = 1) -> list[dict]:
    """呼叫鉅亨網 JSON API，v3 為主，media API 為備援。每個 category 抓 pages 頁。
    API 每頁最少回 10 篇，最多 30 篇（limit < 10 會被忽略）。
    """
    print(f"  Fetching: {name} (API) ...")

    def _parse_entries(entries: list) -> list[dict]:
        results = []
        for item in entries:
            news_id = item.get("newsId", "")
            url = f"https://news.cnyes.com/news/id/{news_id}" if news_id else item.get("url", "")
            publish_ts = item.get("publishAt")
            published = (
                datetime.fromtimestamp(publish_ts, tz=TW_TZ).strftime("%Y-%m-%d %H:%M")
                if publish_ts else ""
            )
            raw = item.get("summary") or item.get("content") or ""
            summary = re.sub(r"<[^>]+>", "", raw).strip()
            summary = summary[:200] + "\u2026" if len(summary) > 200 else summary
            results.append({
                "source": name,
                "title": (item.get("title") or "(無標題)").strip(),
                "link": url,
                "summary": summary,
                "published": published,
            })
        return results

    # 嘗試 v3 API（多頁）
    try:
        all_results = []
        for page in range(1, pages + 1):
            resp = requests.get(
                CNYES_API_V3.format(category=category),
                params={"limit": max_items, "page": page},
                headers=HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
            all_results.extend(_parse_entries(resp.json().get("items", {}).get("data", [])))
        if all_results:
            print(f"  OK (v3 API): {len(all_results)} 篇 ({pages} 頁)")
            return all_results
    except Exception as e:
        print(f"  [WARN] v3 API 失敗，切換備援: {e}", file=sys.stderr)

    # Fallback：media API（多頁）
    try:
        all_results = []
        for page in range(1, pages + 1):
            resp = requests.get(
                CNYES_MEDIA_API.format(category=category),
                params={"limit": max_items, "page": page},
                headers=HEADERS,
                timeout=15,
            )
            resp.raise_for_status()
            all_results.extend(_parse_entries(resp.json().get("items", {}).get("data", [])))
        print(f"  OK (media API): {len(all_results)} 篇 ({pages} 頁)")
        return all_results
    except Exception as e:
        print(f"  [ERROR] {name} 全部 API 失敗: {e}", file=sys.stderr)
        return []


def fetch_feed(source: dict) -> list[dict]:
    """解析單一 RSS 來源，回傳新聞列表。"""
    name = source["name"]
    url = source["url"]
    max_items = source["max_items"]

    print(f"  Fetching: {name} ...")
    try:
        feed = feedparser.parse(url, request_headers=HEADERS)
        if feed.bozo and not feed.entries:
            print(f"  [WARN] {name}: 解析失敗或無文章", file=sys.stderr)
            return []

        items = []
        for entry in feed.entries[:max_items]:
            summary = entry.get("summary", "")
            # 清掉 HTML tag
            summary = re.sub(r"<[^>]+>", "", summary).strip()
            summary = summary[:200] + "…" if len(summary) > 200 else summary

            items.append(
                {
                    "source": name,
                    "title": entry.get("title", "(無標題)").strip(),
                    "link": entry.get("link", ""),
                    "summary": summary,
                    "published": entry.get("published", ""),
                }
            )
        print(f"  OK: {len(items)} 篇")
        return items

    except Exception as e:
        print(f"  [ERROR] {name}: {e}", file=sys.stderr)
        return []


def build_markdown(
    all_news: list[dict],
    report_date: str,
    indices: list[dict] | None = None,
) -> str:
    """將新聞列表組合成 Markdown 報告。"""
    lines = [
        f"# 📰 晨間財經報告 {report_date}",
        "",
        f"> 自動產生時間：{datetime.now(TW_TZ).strftime('%Y-%m-%d %H:%M')} (台灣時間)",
        "",
    ]

    if indices:
        lines.append("## 📊 大盤指數")
        lines.append("")
        lines.append("| 指數 | 收盤價 | 漲跌 | 漲跌幅 | 日期 |")
        lines.append("|------|-------:|-----:|-------:|------|")  
        for idx in indices:
            if idx["close"] is None:
                lines.append(f"| {idx['name']} | — | — | — | — |")
                continue
            arrow = "▲" if idx["change"] >= 0 else "▼"
            sign  = "+" if idx["change"] >= 0 else ""
            lines.append(
                f"| {idx['name']} "
                f"| {idx['close']:>12,.2f} "
                f"| {arrow} {sign}{idx['change']:,.2f} "
                f"| {sign}{idx['change_pct']:.2f}% "
                f"| {idx['date']} |"
            )
        lines.append("")
        lines.append("---")
        lines.append("")

    # 依來源分組
    grouped: dict[str, list[dict]] = {}
    for item in all_news:
        grouped.setdefault(item["source"], []).append(item)

    for source_name, items in grouped.items():
        lines.append(f"## {source_name}")
        lines.append("")
        for item in items:
            lines.append(f"### [{item['title']}]({item['link']})")
            if item["published"]:
                lines.append(f"*{item['published']}*")
            if item["summary"]:
                lines.append("")
                lines.append(item["summary"])
            lines.append("")
        lines.append("---")
        lines.append("")

    lines.append(f"*共 {len(all_news)} 則新聞*")
    return "\n".join(lines)


def main():
    today_tw = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    print(f"=== Morning Finance Brief {today_tw} ===")

    all_news = []

    # 鉅亨網 JSON API
    for name, category in CNYES_CATEGORIES.items():
        all_news.extend(fetch_cnyes_api(name, category))

    # RSS 來源
    for source in RSS_SOURCES:
        all_news.extend(fetch_feed(source))

    if not all_news:
        print("[ERROR] 無法取得任何新聞，終止執行。", file=sys.stderr)
        sys.exit(1)

    # 抓取大盤指數
    indices = fetch_indices()

    # 輸出 Markdown
    output_dir = Path("output/briefs")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_stem = datetime.now(TW_TZ).strftime("%Y%m%d")
    md_path = output_dir / f"{file_stem}.md"
    md_content = build_markdown(all_news, today_tw, indices)
    md_path.write_text(md_content, encoding="utf-8")
    print(f"\nMarkdown 報告已儲存：{md_path}  ({len(all_news)} 則)")




if __name__ == "__main__":
    main()
