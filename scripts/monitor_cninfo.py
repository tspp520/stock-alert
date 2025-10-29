import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# ====== 配置 ======
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl?url=data/person-stock-data-tables',
    'Origin': 'http://www.cninfo.com.cn',
    'Accept': 'application/json, text/plain, */*',
}

BASE_URL = "http://www.cninfo.com.cn/data20/shareholeder/"
DETAIL_URL = BASE_URL + "detail"
STAT_URL = BASE_URL + "stat"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

WEBHOOK = os.getenv("WECHAT_WEBHOOK")
if not WEBHOOK:
    raise EnvironmentError("未设置 WECHAT_WEBHOOK 环境变量")

# ====== 工具函数 ======
def build_template_card_msg(title: str, df: pd.DataFrame) -> dict:
    """构建 template_card 消息体（text_notice 类型）"""
    # 主标题
    main_title = {"title": f"🔔 {title}", "desc": f"新增 {len(df)} 条记录"}

    # 水平键值对列表（最多6项）
    horizontal_content_list = []
    for _, row in df.head(5).iterrows():
        sec = str(row.get("SECNAME", row.get("SECCODE", "—"))).strip()
        holder = str(row.get("F002V", "—")).strip().replace("\n", " ").replace("|", "/")[:20]
        amount = str(row.get("F004N", "—")).strip()
        date = str(row.get("VARYDATE", "—")).strip()
        # 每条记录用一个字段展示，keyname 为序号，value 为简要信息
        horizontal_content_list.append({
            "keyname": f"{len(horizontal_content_list)+1}.",
            "value": f"{sec} | {holder} | {amount} | {date}"
        })

    if len(df) > 5:
        horizontal_content_list.append({
            "keyname": "…",
            "value": f"共 {len(df)} 条，仅展示前5条"
        })

    # 整体卡片点击跳转（可选，比如跳转到你的网页或 GitHub）
    card_action = {
        "type": 1,
        "url": "https://github.com/tspp520/stock-alert"  # 替换为你自己的链接
    }

    return {
        "msgtype": "template_card",
        "template_card": {
            "card_type": "text_notice",
            "main_title": main_title,
            "horizontal_content_list": horizontal_content_list,
            "card_action": card_action
        }
    }

def send_wechat_template_card(card_msg: dict):
    try:
        res = requests.post(WEBHOOK, json=card_msg, timeout=10)
        if res.status_code == 200:
            print("✅ 模板卡片消息发送成功")
        else:
            print(f"⚠️ 消息发送失败: {res.text}")
    except Exception as e:
        print(f"❌ 发送异常: {e}")


def is_within_recent_days(date_str: str, days=5) -> bool:
    """判断变动日期是否在最近 N 个自然日内（用于近似交易日）"""
    if not date_str or date_str == "N/A" or not isinstance(date_str, str):
        return False
    try:
        vary_date = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        today = datetime.now().date()
        return 0 <= (today - vary_date).days <= days
    except Exception:
        return False

def send_wechat_markdown(content: str):
    payload = {"msgtype": "markdown", "markdown": {"content": content}}
    try:
        res = requests.post(WEBHOOK, json=payload, timeout=10)
        if res.status_code == 200:
            print("✅ Markdown 消息发送成功")
        else:
            print(f"⚠️ 消息发送失败: {res.text}")
    except Exception as e:
        print(f"❌ 发送异常: {e}")

def build_markdown_msg(title: str, df: pd.DataFrame) -> str:
    lines = [f"### 🔔 {title}（新增 {len(df)} 条）"]
    lines.append("| 股票 | 股东 | 数量(股) | 日期 |")
    lines.append("|---|---|---|---|")
    for _, row in df.head(8).iterrows():
        sec = str(row.get("SECNAME", row.get("SECCODE", "—"))).strip()
        holder = str(row.get("F002V", "—")).strip().replace("\n", " ").replace("|", "/")
        amount = str(row.get("F004N", "—")).strip()
        date = str(row.get("VARYDATE", "—")).strip()
        lines.append(f"| {sec} | {holder} | {amount} | {date} |")
    if len(df) > 8:
        lines.append(f"\n> 共 {len(df)} 条，仅展示前8条")
    return "\n".join(lines)

def fetch_data(url, data_type="inc", time_mark="oneMonth"):
    params = {'type': data_type, 'timeMark': time_mark}
    try:
        resp = requests.post(url, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") == 200:
            inner = data.get("data", {})
            records = inner.get("records") or inner.get("list") or []
            print(f"✅ 获取 {url.split('/')[-1]} ({data_type}) 共 {len(records)} 条")
            return records
        else:
            print("⚠️ 接口返回非200:", data.get("msg", data))
            return []
    except Exception as e:
        print(f"❌ 请求失败: {e}")
        return []

def load_history(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path, dtype=str)
    return pd.DataFrame()

def save_data(df, file_path):
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

def compare_and_notify(new_df, old_df, title):
    if new_df.empty:
        return False

    # 过滤：仅保留最近 5 个自然日内的变动
    new_df = new_df.copy()
    new_df["VARYDATE"] = new_df["VARYDATE"].astype(str)
    new_df = new_df[new_df["VARYDATE"].apply(lambda x: is_within_recent_days(x, days=5))]
    
    if new_df.empty:
        print(f"🕒 {title}：无近期变动，跳过")
        return False

    if old_df.empty:
        diff = new_df
    else:
        key_cols = ["SECCODE", "DECLAREDATE", "VARYDATE", "F002V"]
        for col in key_cols:
            if col not in new_df.columns:
                new_df[col] = ""
            if col not in old_df.columns:
                old_df[col] = ""
        merged = new_df.merge(old_df[key_cols], on=key_cols, how='left', indicator=True)
        diff = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not diff.empty:
        card_msg = build_template_card_msg(title, diff)
        send_wechat_template_card(card_msg)

        return True
    return False

# ====== 主程序 ======
def main():
    print(f"🕒 开始运行监控脚本 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

    # 增持明细
    inc_detail_raw = fetch_data(DETAIL_URL, "inc")
    inc_detail_df = pd.DataFrame(inc_detail_raw)
    old_inc_detail = load_history(f"{DATA_DIR}/last_inc_detail.csv")
    compare_and_notify(inc_detail_df, old_inc_detail, "增持明细")
    save_data(inc_detail_df, f"{DATA_DIR}/last_inc_detail.csv")

    # 增持汇总
    inc_summary_raw = fetch_data(STAT_URL, "inc")
    inc_summary_df = pd.DataFrame(inc_summary_raw)
    old_inc_summary = load_history(f"{DATA_DIR}/last_inc_summary.csv")
    compare_and_notify(inc_summary_df, old_inc_summary, "增持汇总")
    save_data(inc_summary_df, f"{DATA_DIR}/last_inc_summary.csv")

    # 减持明细
    desc_detail_raw = fetch_data(DETAIL_URL, "desc")
    desc_detail_df = pd.DataFrame(desc_detail_raw)
    old_desc_detail = load_history(f"{DATA_DIR}/last_desc_detail.csv")
    compare_and_notify(desc_detail_df, old_desc_detail, "减持明细")
    save_data(desc_detail_df, f"{DATA_DIR}/last_desc_detail.csv")

    # 减持汇总
    desc_summary_raw = fetch_data(STAT_URL, "desc")
    desc_summary_df = pd.DataFrame(desc_summary_raw)
    old_desc_summary = load_history(f"{DATA_DIR}/last_desc_summary.csv")
    compare_and_notify(desc_summary_df, old_desc_summary, "减持汇总")
    save_data(desc_summary_df, f"{DATA_DIR}/last_desc_summary.csv")

    print("✅ 本轮监控结束")

if __name__ == "__main__":
    main()
