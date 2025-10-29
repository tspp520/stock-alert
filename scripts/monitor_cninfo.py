import os
import requests
import pandas as pd
from datetime import datetime
import json

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

def send_wechat_msg(content: str):
    payload = {"msgtype": "text", "text": {"content": content}}
    try:
        res = requests.post(WEBHOOK, json=payload, timeout=10)
        if res.status_code == 200:
            print("✅ 企业微信消息发送成功")
        else:
            print(f"⚠️ 消息发送失败: {res.text}")
    except Exception as e:
        print(f"❌ 发送异常: {e}")

def load_history(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path, dtype=str)
    return pd.DataFrame()

def save_data(df, file_path):
    df.to_csv(file_path, index=False, encoding="utf-8-sig")

def compare_and_notify(new_df, old_df, title):
    if new_df.empty:
        return False
    if old_df.empty:
        diff = new_df
    else:
        # 合并去重：以关键字段组合为唯一标识
        key_cols = ["SECCODE", "DECLAREDATE", "VARYDATE", "F002V"]
        # 确保列存在
        for col in key_cols:
            if col not in new_df.columns:
                new_df[col] = ""
            if col not in old_df.columns:
                old_df[col] = ""
        merged = new_df.merge(old_df[key_cols], on=key_cols, how='left', indicator=True)
        diff = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

    if not diff.empty:
        msg = f"【{title}】发现 {len(diff)} 条新记录\n"
        for _, row in diff.head(5).iterrows():
            sec = row.get("SECNAME", row.get("SECCODE", "未知"))
            holder = row.get("F002V", "未知股东")
            change = row.get("F004N", "N/A")
            date = row.get("VARYDATE", "N/A")
            msg += f"• {sec} | {holder} | {change}股 | {date}\n"
        if len(diff) > 5:
            msg += f"... 共 {len(diff)} 条"
        send_wechat_msg(msg)
        return True
    return False

# ====== 主逻辑 ======
def main():
    all_new_records = 0

    # 1. 增持明细
    inc_detail_raw = fetch_data(DETAIL_URL, "inc")
    inc_detail_df = pd.DataFrame(inc_detail_raw)
    old_inc_detail = load_history(f"{DATA_DIR}/last_inc_detail.csv")
    if compare_and_notify(inc_detail_df, old_inc_detail, "增持明细"):
        all_new_records += len(inc_detail_df)
    save_data(inc_detail_df, f"{DATA_DIR}/last_inc_detail.csv")

    # 2. 增持汇总
    inc_summary_raw = fetch_data(STAT_URL, "inc")
    inc_summary_df = pd.DataFrame(inc_summary_raw)
    old_inc_summary = load_history(f"{DATA_DIR}/last_inc_summary.csv")
    if compare_and_notify(inc_summary_df, old_inc_summary, "增持汇总"):
        all_new_records += len(inc_summary_df)
    save_data(inc_summary_df, f"{DATA_DIR}/last_inc_summary.csv")

    # 3. 减持明细
    desc_detail_raw = fetch_data(DETAIL_URL, "desc")
    desc_detail_df = pd.DataFrame(desc_detail_raw)
    old_desc_detail = load_history(f"{DATA_DIR}/last_desc_detail.csv")
    if compare_and_notify(desc_detail_df, old_desc_detail, "减持明细"):
        all_new_records += len(desc_detail_df)
    save_data(desc_detail_df, f"{DATA_DIR}/last_desc_detail.csv")

    # 4. 减持汇总
    desc_summary_raw = fetch_data(STAT_URL, "desc")
    desc_summary_df = pd.DataFrame(desc_summary_raw)
    old_desc_summary = load_history(f"{DATA_DIR}/last_desc_summary.csv")
    if compare_and_notify(desc_summary_df, old_desc_summary, "减持汇总"):
        all_new_records += len(desc_summary_df)
    save_data(desc_summary_df, f"{DATA_DIR}/last_desc_summary.csv")

    print(f"📌 本轮运行结束，共发现 {all_new_records} 条新记录（含汇总）")

if __name__ == "__main__":
    main()
