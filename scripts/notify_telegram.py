"""
notify_telegram.py
讀取當日新聞 JSON，整理後推播到 Telegram Bot。
需要環境變數：TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

TW_TZ = timezone(timedelta(hours=8))
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4000  # Telegram 上限 4096，留些餘裕


def get_env(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        print(f"[ERROR] 環境變數 {key} 未設定", file=sys.stderr)
        sys.exit(1)
    return val


def send_message(token: str, chat_id: str, text: str) -> bool:
    """傳送訊息到 Telegram，失敗時回傳 False。"""
    url = TELEGRAM_API.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"[ERROR] Telegram 推播失敗: {e}", file=sys.stderr)
        return False


def _format_indices_block(indices: list[dict]) -> str:
    """將大盤指數格式化為 Telegram HTML 區塊。"""
    if not indices:
        return ""
    lines = ["📊 <b>大盤指數</b>"]
    for idx in indices:
        if idx["close"] is None:
            lines.append(f"{idx['name']}: —")
            continue
        arrow = "▲" if idx["change"] >= 0 else "▼"
        sign  = "+" if idx["change"] >= 0 else ""
        lines.append(
            f"{idx['name']}: {idx['close']:,.2f} "
            f"{arrow} {sign}{idx['change']:,.2f} ({sign}{idx['change_pct']:.2f}%) "
            f"[{idx['date']}]"
        )
    return "\n".join(lines) + "\n"


def build_telegram_messages(
    all_news: list[dict], report_date: str, indices: list[dict] | None = None
) -> list[str]:
    """將新聞拆成不超過 MAX_MESSAGE_LENGTH 的訊息串列。"""
    indices_block = _format_indices_block(indices or [])
    header = (
        f"📰 <b>晨間財經報告 {report_date}</b>\n"
        f"🕖 {datetime.now(TW_TZ).strftime('%H:%M')} 台灣時間\n"
        f"共 {len(all_news)} 則新聞\n"
        "─────────────────\n\n"
    )
    if indices_block:
        header += indices_block + "─────────────────\n\n"

    messages = []
    current = header

    grouped: dict[str, list[dict]] = {}
    for item in all_news:
        grouped.setdefault(item["source"], []).append(item)

    for source_name, items in grouped.items():
        section = f"<b>【{source_name}】</b>\n"
        for item in items:
            line = f'• <a href="{item["link"]}">{item["title"]}</a>\n'
            section += line
        section += "\n"

        # 若加入這段會超過長度限制，先把目前的存起來，開新訊息
        if len(current) + len(section) > MAX_MESSAGE_LENGTH:
            messages.append(current.rstrip())
            current = section
        else:
            current += section

    if current.strip():
        messages.append(current.rstrip())

    return messages


def main():
    today_tw = datetime.now(TW_TZ).strftime("%Y-%m-%d")
    token = get_env("TELEGRAM_BOT_TOKEN")
    chat_id = get_env("TELEGRAM_CHAT_ID")

    file_stem = datetime.now(TW_TZ).strftime("%Y%m%d")
    json_path = Path("output/briefs") / f"{file_stem}.json"
    if not json_path.exists():
        print(f"[ERROR] 找不到新聞資料：{json_path}", file=sys.stderr)
        sys.exit(1)

    all_news = json.loads(json_path.read_text(encoding="utf-8"))
    # 相容新版本 JSON 格式 {"indices": [...], "news": [...]}
    if isinstance(all_news, dict):
        indices = all_news.get("indices", [])
        all_news = all_news.get("news", [])
    else:
        indices = []
    print(f"讀取 {len(all_news)} 則新聞，準備推播...")

    messages = build_telegram_messages(all_news, today_tw, indices)
    print(f"拆分為 {len(messages)} 則 Telegram 訊息")

    success = True
    for i, msg in enumerate(messages, 1):
        print(f"  傳送第 {i}/{len(messages)} 則...")
        if not send_message(token, chat_id, msg):
            success = False

    if not success:
        sys.exit(1)
    print("推播完成！")


if __name__ == "__main__":
    main()
