import time
import datetime
import pytz
import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

# ==========================================
# 1. 港股通觀察名單（可隨時自行增減 5 位數代號）
# ==========================================
WATCHLIST = [
    "00700", "03690", "01810", "09988", "00941",
    "00883", "01024", "02015", "09868", "00981",
    "02382", "09660", "01686", "02858", "09880",
    "02276", "06082", "06166", "00388", "01211"
]

# CCASS 席位分類字典（港交所參與者編號前綴或代碼）
NORTHBOUND_IDS = {"A00003", "A00004"}  # 滬港通 / 深港通（中登香港 / 創盈）
LONG_ONLY_IDS = {
    "C00010",  # 花旗 Citibank
    "C00019",  # 匯豐 HSBC
    "C00100",  # 摩根大通 JPMorgan
    "C00039",  # 渣打 Standard Chartered
    "C00018",  # 法巴 BNP
}
RETAIL_IDS = {
    "B01955",  # 富途證券
    "B01491",  # 耀才證券
    "B01590",  # 盈透 IB
    "B02153",  # 老虎證券
    "B01274",  # 中銀國際/零售常見席位
}

CCASS_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def get_ccass_snapshot(session, stock_code, date_str):
    """抓取指定日期 (YYYY/MM/DD) 的 HKEX CCASS 持倉快照"""
    try:
        r_get = session.get(CCASS_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r_get.text, "lxml")

        viewstate = soup.find("input", {"id": "__VIEWSTATE"})
        generator = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
        if not viewstate or not generator:
            return None

        payload = {
            "__EVENTTARGET": "btnSearch",
            "__VIEWSTATE": viewstate["value"],
            "__VIEWSTATEGENERATOR": generator["value"],
            "today": datetime.datetime.now().strftime("%Y%m%d"),
            "sortBy": "shareholding",
            "sortDirection": "desc",
            "txtShareholdingDate": date_str,
            "txtStockCode": stock_code.zfill(5),
        }

        r_post = session.post(CCASS_URL, data=payload, headers=HEADERS, timeout=20)
        soup_res = BeautifulSoup(r_post.text, "lxml")

        rows = soup_res.select("table.table-scroll tbody tr")
        if not rows:
            return None

        records = []
        for row in rows:
            pid = row.select_one("td.col-participant-id div.mobile-list-body")
            name = row.select_one("td.col-participant-name div.mobile-list-body")
            shares = row.select_one("td.col-shareholding div.mobile-list-body")
            pct = row.select_one("td.col-shareholding-percent div.mobile-list-body")
            if not (pid and shares and pct):
                continue

            pid_txt = pid.get_text(strip=True)
            shares_val = int(shares.get_text(strip=True).replace(",", ""))
            pct_val = float(pct.get_text(strip=True).replace("%", "") or 0)
            records.append({
                "id": pid_txt,
                "name": name.get_text(strip=True) if name else "",
                "shares": shares_val,
                "pct": pct_val,
            })

        if not records:
            return None

        df = pd.DataFrame(records)
        top10_pct = df.head(10)["pct"].sum()
        nb_pct = df[df["id"].isin(NORTHBOUND_IDS)]["pct"].sum()
        long_pct = df[df["id"].isin(LONG_ONLY_IDS)]["pct"].sum()
        retail_pct = df[df["id"].isin(RETAIL_IDS)]["pct"].sum()

        return {
            "df": df.set_index("id"),
            "participants": len(df),
            "top10_pct": round(top10_pct, 2),
            "nb_pct": round(nb_pct, 2),
            "long_pct": round(long_pct, 2),
            "retail_pct": round(retail_pct, 2),
        }
    except Exception as e:
        print(f"[{stock_code}] CCASS 讀取失敗 ({date_str}): {e}")
        return None


def get_trading_dates(hist):
    """從股價歷史中取得最新、前一日、以及 20 個交易日前的日期"""
    dates = hist.index.strftime("%Y/%m/%d").tolist()
    latest = dates[-1]
    prev_1d = dates[-2] if len(dates) >= 2 else latest
    prev_20d = dates[-21] if len(dates) >= 21 else dates[0]
    return latest, prev_1d, prev_20d


def analyze_stock(session, code):
    yf_ticker = f"{int(code):04d}.HK"
    tk = yf.Ticker(yf_ticker)
    hist = tk.history(period="9mo")
    if hist.empty or len(hist) < 30:
        return None

    close = round(float(hist["Close"].iloc[-1]), 2)
    prev_close = float(hist["Close"].iloc[-2])
    chg_pct = round((close - prev_close) / prev_close * 100, 2)

    ma50 = hist["Close"].rolling(50).mean().iloc[-1]
    ma150 = hist["Close"].rolling(150).mean().iloc[-1] if len(hist) >= 150 else ma50
    stage2 = (close > ma50) and (ma50 > ma150)

    # T-2 日全市場成交量（用於排雷：存倉/射倉比對）
    t2_vol = int(hist["Volume"].iloc[-3]) if len(hist) >= 3 else int(hist["Volume"].iloc[-1])

    latest_d, prev1_d, prev20_d = get_trading_dates(hist)

    # 抓取最新、前一日（檢查存倉）、20日前（檢查斜率與貨歸邊）
    snap_now = get_ccass_snapshot(session, code, latest_d)
    if not snap_now:
        snap_now = get_ccass_snapshot(session, code, prev1_d)
        if not snap_now:
            return None

    time.sleep(0.5)
    snap_prev1 = get_ccass_snapshot(session, code, prev1_d)
    time.sleep(0.5)
    snap_20d = get_ccass_snapshot(session, code, prev20_d)
    time.sleep(0.5)

    # 計算 20 日籌碼變化
    nb_chg_20d = round(snap_now["nb_pct"] - snap_20d["nb_pct"], 2) if snap_20d else 0.0
    long_chg_20d = round(snap_now["long_pct"] - snap_20d["long_pct"], 2) if snap_20d else 0.0
    retail_chg_20d = round(snap_now["retail_pct"] - snap_20d["retail_pct"], 2) if snap_20d else 0.0
    top10_chg_20d = round(snap_now["top10_pct"] - snap_20d["top10_pct"], 2) if snap_20d else 0.0
    part_chg_20d = (snap_now["participants"] - snap_20d["participants"]) if snap_20d else 0

    # 排雷：檢查單一席位單日持股變動是否大於 T-2 成交量
    trap_flag = False
    if snap_prev1 and t2_vol > 0:
        merged = snap_now["df"][["shares"]].join(
            snap_prev1["df"][["shares"]], lsuffix="_now", rsuffix="_prev", how="outer"
        ).fillna(0)
        max_single_jump = (merged["shares_now"] - merged["shares_prev"]).abs().max()
        if max_single_jump > t2_vol * 1.05:
            trap_flag = True

    # CANSLIM "I" 綜合評分 (滿分 5 分)
    score = 0
    if stage2:
        score += 1
    if 5.0 <= snap_now["nb_pct"] <= 25.0 and nb_chg_20d > 0.2:
        score += 1  # 處於 5%-25% 增倉黃金期
    if long_chg_20d > 0.1 or nb_chg_20d > 0.3:
        score += 1  # 外資大行或北水持續增倉
    if retail_chg_20d < -0.05:
        score += 1  # 散戶席位同步減倉（貨歸邊）
    if top10_chg_20d > 0 and part_chg_20d <= 0:
        score += 1  # 集中度上升且參與者減少

    try:
        name = tk.info.get("shortName") or yf_ticker
    except Exception:
        name = yf_ticker

    return {
        "code": f"{code}.HK",
        "name": name,
        "price": close,
        "chg_pct": chg_pct,
        "stage2": "✅ Stage 2" if stage2 else "⏸️ 整理/弱勢",
        "nb_pct": snap_now["nb_pct"],
        "nb_chg_20d": nb_chg_20d,
        "long_chg_20d": long_chg_20d,
        "retail_chg_20d": retail_chg_20d,
        "top10_pct": snap_now["top10_pct"],
        "top10_chg_20d": top10_chg_20d,
        "part_chg_20d": part_chg_20d,
        "trap": "⚠️ 疑似存倉/射倉" if trap_flag else "正常",
        "score": score,
    }


def build_html(results):
    hkt = pytz.timezone("Asia/Hong_Kong")
    now_str = datetime.datetime.now(hkt).strftime("%Y-%m-%d %H:%M HKT")

    rows_html = ""
    for r in sorted(results, key=lambda x: (x["score"], x["nb_chg_20d"]), reverse=True):
        # 綠升紅跌配色
        price_cls = "up" if r["chg_pct"] >= 0 else "down"
        nb_cls = "up" if r["nb_chg_20d"] > 0 else ("down" if r["nb_chg_20d"] < 0 else "")
        long_cls = "up" if r["long_chg_20d"] > 0 else ("down" if r["long_chg_20d"] < 0 else "")
        # 散戶減倉對機構吸籌屬正面訊號，以綠色標記
        retail_cls = "up" if r["retail_chg_20d"] < 0 else ("down" if r["retail_chg_20d"] > 0 else "")
        top10_cls = "up" if r["top10_chg_20d"] > 0 else ("down" if r["top10_chg_20d"] < 0 else "")

        golden_tag = '<span class="badge gold">黃金期</span>' if (5 <= r["nb_pct"] <= 25 and r["nb_chg_20d"] > 0) else ""
        trap_html = f'<span class="badge warn">{r["trap"]}</span>' if "疑似" in r["trap"] else '<span class="muted">正常</span>'

        rows_html += f"""
        <tr>
            <td><strong>{r['code']}</strong><br><small class="muted">{r['name']}</small></td>
            <td>{r['price']:.2f} <span class="{price_cls}">({r['chg_pct']:+.2f}%)</span></td>
            <td>{r['stage2']}</td>
            <td>{r['nb_pct']:.2f}% {golden_tag}</td>
            <td class="{nb_cls}">{r['nb_chg_20d']:+.2f}%</td>
            <td class="{long_cls}">{r['long_chg_20d']:+.2f}%</td>
            <td class="{retail_cls}">{r['retail_chg_20d']:+.2f}%</td>
            <td>{r['top10_pct']:.1f}% <span class="{top10_cls}">({r['top10_chg_20d']:+.2f}%)</span></td>
            <td>{r['part_chg_20d']:+d}</td>
            <td>{trap_html}</td>
            <td><strong>{r['score']} / 5</strong></td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html lang="zh-HK">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>港股 CANSLIM 機構籌碼 Dashboard（北水 + CCASS）</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
  h1 {{ margin: 0 0 6px 0; font-size: 24px; color: #f8fafc; }}
  .meta {{ color: #94a3b8; font-size: 13px; margin-bottom: 20px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.3); overflow-x: auto; }}
  table.dataTable {{ color: #e2e8f0; border-collapse: collapse; width: 100% !important; }}
  table.dataTable thead th {{ background: #334155; color: #f8fafc; padding: 12px 10px; text-align: left; border-bottom: 2px solid #475569; }}
  table.dataTable tbody td {{ padding: 12px 10px; border-bottom: 1px solid #334155; }}
  table.dataTable tbody tr:hover {{ background: #26354d; }}
  .up {{ color: #22c55e; font-weight: 600; }}
  .down {{ color: #ef4444; font-weight: 600; }}
  .muted {{ color: #94a3b8; font-size: 12px; }}
  .badge {{ padding: 2px 7px; border-radius: 999px; font-size: 11px; font-weight: 600; }}
  .badge.gold {{ background: #ca8a04; color: #fefce8; margin-left: 4px; }}
  .badge.warn {{ background: #b91c1c; color: #fee2e2; }}
  .dataTables_wrapper .dataTables_filter input, .dataTables_wrapper .dataTables_length select {{ background: #0f172a; color: #f8fafc; border: 1px solid #475569; border-radius: 6px; padding: 4px 8px; }}
  .dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_paginate {{ color: #94a3b8 !important; margin-top: 12px; }}
</style>
</head>
<body>
  <h1>港股 CANSLIM 機構籌碼 Dashboard（北水 + CCASS）</h1>
  <div class="meta">最後更新：{now_str} ｜ 每日 09:00（開市前）及 16:30（收市後）自動更新 ｜ 配色：綠升紅跌</div>
  <div class="card">
    <table id="dashboard" class="display">
      <thead>
        <tr>
          <th>股票</th>
          <th>現價 / 漲跌</th>
          <th>趨勢狀態</th>
          <th>北水持股 %</th>
          <th>北水 20日變動</th>
          <th>外資大行 20日</th>
          <th>散戶席位 20日</th>
          <th>Top 10 集中度</th>
          <th>參與者數目變動</th>
          <th>存倉/射倉排雷</th>
          <th>I 評分</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </div>
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script>
  $(document).ready(function() {{
    $('#dashboard').DataTable({{
      order: [[10, 'desc']],
      pageLength: 25,
      language: {{ search: "搜尋股票：", lengthMenu: "顯示 _MENU_ 筆", info: "第 _START_ 至 _END_ 筆，共 _TOTAL_ 筆" }}
    }});
  }});
</script>
</body>
</html>"""

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("index.html 已成功生成！")


if __name__ == "__main__":
    session = requests.Session()
    results = []
    for code in WATCHLIST:
        print(f"正在掃描 {code}.HK ...")
        res = analyze_stock(session, code)
        if res:
            results.append(res)
    build_html(results)
