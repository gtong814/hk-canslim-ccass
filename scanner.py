import re
import datetime
import pytz
import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 核心自選股（無論是否處於 Stage 2，都會強制納入 CCASS 深度掃描）
# ==========================================
PINNED_NAMES = {
    "00700": "騰訊控股", "03690": "美團-W", "01810": "小米集團-W",
    "09988": "阿里巴巴-W", "00941": "中國移動", "00883": "中國海洋石油",
    "01024": "快手-W", "02015": "理想汽車-W", "09868": "小鵬汽車-W",
    "00981": "中芯國際", "02382": "舜宇光學", "09660": "地平線機械人",
    "01686": "新意網集團", "02858": "易鑫集團", "09880": "優必選",
    "02276": "康耐特光學", "06082": "壁仞科技", "06166": "劍橋科技",
    "00388": "香港交易所", "01211": "比亞迪股份"
}
PINNED_CODES = set(PINNED_NAMES.keys())

MAX_CCASS_STOCKS = 95  # 每次最多深度掃描 CCASS 的股票上限（兼顧覆蓋率與執行速度）

# CCASS 席位分類代碼
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

MUTUAL_URL = "https://www3.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t=hk"
CCASS_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": CCASS_URL,
    "Origin": "https://www3.hkexnews.hk",
}


def extract_cell(row, css_class):
    td = row.select_one(f"td.{css_class}")
    if not td:
        return ""
    body = td.select_one("div.mobile-list-body")
    return body.get_text(strip=True) if body else td.get_text(strip=True)


def parse_mutual_table(html):
    """解析港交所滬深港通持股總表，回傳 {5位代號: {'name': 股名, 'nb_pct': 北水比例}}"""
    soup = BeautifulSoup(html, "lxml")
    result = {}
    for row in soup.select("table tbody tr"):
        tds = row.find_all("td")
        if len(tds) < 4:
            continue

        vals = []
        for td in tds[:4]:
            body = td.select_one("div.mobile-list-body")
            vals.append(body.get_text(strip=True) if body else td.get_text(" ", strip=True))

        m_code = re.search(r"\b(\d{5})\b", vals[0])
        if not m_code:
            continue
        code = m_code.group(1)
        if code.startswith("8"):  # 排除 8 字頭人民幣雙櫃台重複股份
            continue

        name = PINNED_NAMES.get(code) or re.sub(r"^股份名稱[:：]\s*", "", vals[1]).strip()
        m_pct = re.search(r"([\d\.]+)\s*%", vals[3])
        pct = float(m_pct.group(1)) if m_pct else 0.0
        result[code] = {"name": name, "nb_pct": pct}
    return result


def fetch_eastmoney_southbound():
    """備援接口：分頁讀取全部約 550 隻港股通名單"""
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    result = {}
    for page in range(1, 9):  # 每頁 100 隻，抓第 1 至 8 頁完整覆蓋全部港股通
        params = {
            "pn": page,
            "pz": 100,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "b:MK0144,b:MK0146",
            "fields": "f12,f14",
        }
        try:
            r = requests.get(url, params=params, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=12)
            diff = r.json().get("data", {}).get("diff", [])
            if isinstance(diff, dict):
                diff = list(diff.values())
            if not diff:
                break
            for item in diff:
                code = str(item.get("f12", "")).zfill(5)
                if len(code) == 5 and code.isdigit() and not code.startswith("8"):
                    name = PINNED_NAMES.get(code) or str(item.get("f14", code)).strip()
                    result[code] = {"name": name, "nb_pct": 0.0}
        except Exception as e:
            print(f"備援港股通第 {page} 頁讀取失敗: {e}", flush=True)
            break
    return result


def get_southbound_universe(recent_dates, date_20d_str=None):
    """取得全港股通約 550 隻名單、中文股名及北水比例"""
    session = requests.Session()
    headers = {**HEADERS, "Referer": MUTUAL_URL}
    r_get = session.get(MUTUAL_URL, headers=headers, timeout=15)
    latest_map = parse_mutual_table(r_get.text)

    soup = BeautifulSoup(r_get.text, "lxml")
    vs = soup.find("input", {"id": "__VIEWSTATE"})
    gen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    val = soup.find("input", {"id": "__EVENTVALIDATION"})

    # 若初始 GET 為空表，主動帶入最新交易日進行 POST 查詢
    if not latest_map and vs and gen:
        for d_try in reversed(recent_dates[-3:]):
            try:
                payload = {
                    "__EVENTTARGET": "btnSearch",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": vs["value"],
                    "__VIEWSTATEGENERATOR": gen["value"],
                    "today": datetime.datetime.now().strftime("%Y%m%d"),
                    "txtShareholdingDate": d_try,
                }
                if val:
                    payload["__EVENTVALIDATION"] = val["value"]
                r_post = session.post(MUTUAL_URL, data=payload, headers=headers, timeout=15)
                latest_map = parse_mutual_table(r_post.text)
                if latest_map:
                    print(f"透過 POST ({d_try}) 成功讀取港股通總表", flush=True)
                    break
            except Exception:
                continue

    # 若港交所總表暫無回傳，自動切換至分頁備援接口讀取全 550+ 隻名單
    if not latest_map:
        print("港交所北水總表暫無回傳，啟動分頁備援接口讀取全港股通名單...", flush=True)
        latest_map = fetch_eastmoney_southbound()

    print(f"成功讀取全港股通名單：共 {len(latest_map)} 隻", flush=True)

    prev20_map = {}
    if date_20d_str and vs and gen:
        try:
            payload = {
                "__EVENTTARGET": "btnSearch",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": vs["value"],
                "__VIEWSTATEGENERATOR": gen["value"],
                "today": datetime.datetime.now().strftime("%Y%m%d"),
                "txtShareholdingDate": date_20d_str,
            }
            if val:
                payload["__EVENTVALIDATION"] = val["value"]
            r_post = session.post(MUTUAL_URL, data=payload, headers=headers, timeout=15)
            prev20_map = parse_mutual_table(r_post.text)
            print(f"成功讀取 20 日前 ({date_20d_str}) 北水總表：共 {len(prev20_map)} 隻", flush=True)
        except Exception as e:
            print(f"20 日前北水總表略過: {e}", flush=True)

    return latest_map, prev20_map


def batch_download_prices(codes):
    """分批極速下載全部 ~550 隻港股通的 9 個月量價數據"""
    price_stats = {}
    code_list = list(codes)
    chunk_size = 150

    for i in range(0, len(code_list), chunk_size):
        chunk = code_list[i:i + chunk_size]
        yf_map = {f"{int(c):04d}.HK": c for c in chunk}
        tickers = list(yf_map.keys())
        print(f"正在批量下載股價 ({i + 1} - {min(i + chunk_size, len(code_list))})...", flush=True)

        try:
            raw = yf.download(
                tickers, period="9mo", group_by="ticker",
                threads=True, progress=False, auto_adjust=True
            )
        except Exception as e:
            print(f"批量下載部分失敗: {e}", flush=True)
            continue

        for yf_tk, code in yf_map.items():
            try:
                df = raw[yf_tk] if len(tickers) > 1 else raw
                df = df.dropna(subset=["Close"])
                if len(df) < 30:
                    continue

                close = float(df["Close"].iloc[-1])
                prev_close = float(df["Close"].iloc[-2])
                chg_pct = round((close - prev_close) / prev_close * 100, 2)

                ma50 = float(df["Close"].rolling(50).mean().iloc[-1])
                ma150 = float(df["Close"].rolling(150).mean().iloc[-1]) if len(df) >= 150 else ma50
                high_9m = float(df["High"].max())

                if pd.isna(close) or pd.isna(ma50) or pd.isna(ma150):
                    continue

                stage2 = (close > ma50) and (ma50 > ma150)
                near_high = close >= (high_9m * 0.82)
                t2_vol = int(df["Volume"].iloc[-3]) if len(df) >= 3 and not pd.isna(df["Volume"].iloc[-3]) else 0

                price_stats[code] = {
                    "price": round(close, 2),
                    "chg_pct": chg_pct,
                    "stage2": stage2,
                    "near_high": near_high,
                    "t2_vol": t2_vol,
                    "dates": df.index.strftime("%Y/%m/%d").tolist(),
                }
            except Exception:
                continue

    return price_stats


def get_ccass_snapshot(session, stock_code, date_str):
    """抓取指定日期 (YYYY/MM/DD) 的 HKEX CCASS 持倉快照"""
    try:
        r_get = session.get(CCASS_URL, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r_get.text, "lxml")

        vs = soup.find("input", {"id": "__VIEWSTATE"})
        gen = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
        val = soup.find("input", {"id": "__EVENTVALIDATION"})
        if not vs or not gen:
            return None

        payload = {
            "__EVENTTARGET": "btnSearch",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": vs["value"],
            "__VIEWSTATEGENERATOR": gen["value"],
            "today": datetime.datetime.now().strftime("%Y%m%d"),
            "sortBy": "shareholding",
            "sortDirection": "desc",
            "txtShareholdingDate": date_str,
            "txtStockCode": stock_code.zfill(5),
        }
        if val:
            payload["__EVENTVALIDATION"] = val["value"]

        r_post = session.post(CCASS_URL, data=payload, headers=HEADERS, timeout=12)
        soup_res = BeautifulSoup(r_post.text, "lxml")

        rows = soup_res.select("table.table-scroll tbody tr")
        if not rows:
            return None

        records = []
        for row in rows:
            pid_txt = extract_cell(row, "col-participant-id")
            shares_txt = extract_cell(row, "col-shareholding").replace(",", "")
            pct_txt = extract_cell(row, "col-shareholding-percent").replace("%", "")
            if not (pid_txt and shares_txt.isdigit()):
                continue
            records.append({
                "id": pid_txt,
                "shares": int(shares_txt),
                "pct": float(pct_txt or 0),
            })

        if not records:
            return None

        df = pd.DataFrame(records)
        return {
            "df": df.set_index("id"),
            "participants": len(df),
            "top10_pct": round(df.head(10)["pct"].sum(), 2),
            "nb_pct": round(df[df["id"].isin(NORTHBOUND_IDS)]["pct"].sum(), 2),
            "long_pct": round(df[df["id"].isin(LONG_ONLY_IDS)]["pct"].sum(), 2),
            "retail_pct": round(df[df["id"].isin(RETAIL_IDS)]["pct"].sum(), 2),
        }
    except Exception:
        return None


def analyze_ccass_detail(item):
    code = item["code_raw"]
    name = item["name"]
    p = item["price_info"]
    dates = p["dates"]
    session = requests.Session()

    snap_now = None
    now_idx = len(dates) - 1
    for offset in [1, 2, 3]:
        if len(dates) >= offset:
            snap_now = get_ccass_snapshot(session, code, dates[-offset])
            if snap_now:
                now_idx = len(dates) - offset
                break

    if not snap_now:
        return {
            "code": f"{code}.HK", "name": name, "price": p["price"], "chg_pct": p["chg_pct"],
            "is_stage2": p["stage2"],
            "stage2": "✅ Stage 2" if p["stage2"] else "⏸️ 整理/弱勢",
            "nb_pct": item["nb_pct_summary"], "nb_chg_20d": item["nb_chg_summary"],
            "long_chg_20d": 0.0, "retail_chg_20d": 0.0,
            "top10_pct": None, "top10_chg_20d": 0.0,
            "part_chg_20d": 0, "trap": "暫無CCASS", "score": 1 if p["stage2"] else 0,
        }

    prev1_d = dates[max(0, now_idx - 1)]
    prev20_d = dates[max(0, now_idx - 20)]
    snap_prev1 = get_ccass_snapshot(session, code, prev1_d)
    snap_20d = get_ccass_snapshot(session, code, prev20_d)

    nb_pct = snap_now["nb_pct"]
    nb_chg_20d = round(nb_pct - snap_20d["nb_pct"], 2) if snap_20d else item["nb_chg_summary"]
    long_chg_20d = round(snap_now["long_pct"] - snap_20d["long_pct"], 2) if snap_20d else 0.0
    retail_chg_20d = round(snap_now["retail_pct"] - snap_20d["retail_pct"], 2) if snap_20d else 0.0
    top10_chg_20d = round(snap_now["top10_pct"] - snap_20d["top10_pct"], 2) if snap_20d else 0.0
    part_chg_20d = (snap_now["participants"] - snap_20d["participants"]) if snap_20d else 0

    # 排雷：檢查單一席位單日持股變動是否大於 T-2 成交量
    trap_flag = False
    if snap_prev1 and p["t2_vol"] > 0:
        merged = snap_now["df"][["shares"]].join(
            snap_prev1["df"][["shares"]], lsuffix="_now", rsuffix="_prev", how="outer"
        ).fillna(0)
        max_single_jump = (merged["shares_now"] - merged["shares_prev"]).abs().max()
        if max_single_jump > p["t2_vol"] * 1.05:
            trap_flag = True

    # CANSLIM "I" 綜合評分 (滿分 5 分)
    score = 0
    if p["stage2"]:
        score += 1
    if 5.0 <= nb_pct <= 25.0 and nb_chg_20d > 0.2:
        score += 1  # 處於 5%-25% 增倉黃金期
    if long_chg_20d > 0.1 or nb_chg_20d > 0.3:
        score += 1  # 外資大行或北水持續增倉
    if retail_chg_20d < -0.05:
        score += 1  # 散戶席位同步減倉（貨歸邊）
    if top10_chg_20d > 0 and part_chg_20d <= 0:
        score += 1  # 集中度上升且參與者減少

    print(f"完成 {code}.HK ({name}) | 北水: {nb_pct}% ({nb_chg_20d:+.2f}%) | 評分: {score}/5", flush=True)
    return {
        "code": f"{code}.HK",
        "name": name,
        "price": p["price"],
        "chg_pct": p["chg_pct"],
        "is_stage2": p["stage2"],
        "stage2": "✅ Stage 2" if p["stage2"] else "⏸️ 整理/弱勢",
        "nb_pct": nb_pct,
        "nb_chg_20d": nb_chg_20d,
        "long_chg_20d": long_chg_20d,
        "retail_chg_20d": retail_chg_20d,
        "top10_pct": snap_now["top10_pct"],
        "top10_chg_20d": top10_chg_20d,
        "part_chg_20d": part_chg_20d,
        "trap": "⚠️ 疑似存倉/射倉" if trap_flag else "正常",
        "score": score,
    }


def build_html(results, total_universe, total_stage2):
    hkt = pytz.timezone("Asia/Hong_Kong")
    now_str = datetime.datetime.now(hkt).strftime("%Y-%m-%d %H:%M HKT")

    rows_html = ""
    for r in sorted(results, key=lambda x: (x["score"], x["is_stage2"], x["nb_chg_20d"]), reverse=True):
        price_cls = "up" if r["chg_pct"] >= 0 else "down"
        nb_cls = "up" if r["nb_chg_20d"] > 0 else ("down" if r["nb_chg_20d"] < 0 else "")
        long_cls = "up" if r["long_chg_20d"] > 0 else ("down" if r["long_chg_20d"] < 0 else "")
        retail_cls = "up" if r["retail_chg_20d"] < 0 else ("down" if r["retail_chg_20d"] > 0 else "")
        top10_cls = "up" if r["top10_chg_20d"] > 0 else ("down" if r["top10_chg_20d"] < 0 else "")

        is_golden = (r["nb_pct"] is not None) and (5.0 <= r["nb_pct"] <= 25.0) and (r["nb_chg_20d"] > 0)
        golden_tag = '<span class="badge gold">黃金期</span>' if is_golden else ""

        if r["nb_pct"] is None:
            nb_cell = '<span class="muted">N/A</span>'
        else:
            nb_cell = f"{r['nb_pct']:.2f}% {golden_tag}"

        if r["top10_pct"] is None:
            top10_cell = '<span class="muted">N/A</span>'
        else:
            top10_cell = f"{r['top10_pct']:.1f}% <span class=\"{top10_cls}\">({r['top10_chg_20d']:+.2f}%)</span>"

        trap_html = f'<span class="badge warn">{r["trap"]}</span>' if "疑似" in r["trap"] else f'<span class="muted">{r["trap"]}</span>'

        row_tags = []
        if r["is_stage2"]:
            row_tags.append("stage2")
        if is_golden:
            row_tags.append("golden")
        if r["score"] >= 4:
            row_tags.append("high")

        rows_html += f"""
        <tr data-tags="{' '.join(row_tags)}">
            <td><strong>{r['code']}</strong><br><small class="muted">{r['name']}</small></td>
            <td>{r['price']:.2f} <span class="{price_cls}">({r['chg_pct']:+.2f}%)</span></td>
            <td>{r['stage2']}</td>
            <td>{nb_cell}</td>
            <td class="{nb_cls}">{r['nb_chg_20d']:+.2f}%</td>
            <td class="{long_cls}">{r['long_chg_20d']:+.2f}%</td>
            <td class="{retail_cls}">{r['retail_chg_20d']:+.2f}%</td>
            <td>{top10_cell}</td>
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
<title>全港股通 CANSLIM 機構籌碼 Dashboard（北水 + CCASS）</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
  h1 {{ margin: 0 0 6px 0; font-size: 24px; color: #f8fafc; }}
  .meta {{ color: #94a3b8; font-size: 13px; margin-bottom: 14px; }}
  .stats-bar {{ display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }}
  .stat-pill {{ background: #1e293b; border: 1px solid #334155; padding: 8px 14px; border-radius: 8px; font-size: 13px; }}
  .stat-pill strong {{ color: #38bdf8; font-size: 15px; }}
  .filter-bar {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }}
  .filter-btn {{ background: #334155; color: #e2e8f0; border: 1px solid #475569; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; }}
  .filter-btn:hover, .filter-btn.active {{ background: #2563eb; border-color: #3b82f6; color: #fff; font-weight: 600; }}
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
  <h1>全港股通 CANSLIM 機構籌碼 Dashboard（北水 + CCASS）</h1>
  <div class="meta">最後更新：{now_str} ｜ 每日 09:00（開市前）及 16:30（收市後）自動更新 ｜ 配色：綠升紅跌</div>
  <div class="stats-bar">
    <div class="stat-pill">全港股通掃描：<strong>{total_universe}</strong> 隻</div>
    <div class="stat-pill">符合 Stage 2 上升趨勢：<strong>{total_stage2}</strong> 隻</div>
    <div class="stat-pill">CCASS 深度席位拆解：<strong>{len(results)}</strong> 隻</div>
  </div>
  <div class="filter-bar">
    <button class="filter-btn active" onclick="setFilter('all', this)">全部顯示 ({len(results)})</button>
    <button class="filter-btn" onclick="setFilter('stage2', this)">只看 ✅ Stage 2</button>
    <button class="filter-btn" onclick="setFilter('golden', this)">北水 5%-25% 黃金期</button>
    <button class="filter-btn" onclick="setFilter('high', this)">高分股（I 評分 ≥ 4）</button>
  </div>
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
  var currentTag = 'all';
  $.fn.dataTable.ext.search.push(function(settings, data, dataIndex) {{
    if (currentTag === 'all') return true;
    var row = settings.aoData[dataIndex].nTr;
    var tags = (row.getAttribute('data-tags') || '').split(' ');
    return tags.indexOf(currentTag) !== -1;
  }});

  var table;
  $(document).ready(function() {{
    table = $('#dashboard').DataTable({{
      order: [[10, 'desc']],
      pageLength: 50,
      language: {{ search: "搜尋股票：", lengthMenu: "顯示 _MENU_ 筆", info: "第 _START_ 至 _END_ 筆，共 _TOTAL_ 筆" }}
    }});
  }});

  function setFilter(tag, btn) {{
    currentTag = tag;
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    table.draw();
  }}
</script>
</body>
</html>"""

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"index.html 已成功生成！共輸出 {len(results)} 隻深度掃描標的。", flush=True)


if __name__ == "__main__":
    ref_hist = yf.Ticker("0700.HK").history(period="3mo")
    ref_dates = ref_hist.index.strftime("%Y/%m/%d").tolist()
    date_20d = ref_dates[-21] if len(ref_dates) >= 21 else (ref_dates[0] if ref_dates else None)

    # 1. 取得全港股通約 550 隻名單及北水比例
    latest_map, prev20_map = get_southbound_universe(ref_dates, date_20d)
    all_codes = sorted(set(latest_map.keys()) | PINNED_CODES)

    # 2. 批量下載全部 ~550 隻股價，篩選 Stage 2 與強勢股
    price_stats = batch_download_prices(all_codes)
    total_stage2 = sum(1 for v in price_stats.values() if v["stage2"])
    print(f"股價初篩完成：全港股通 {len(latest_map)} 隻中，共有 {total_stage2} 隻處於 Stage 2。", flush=True)

    # 3. 組合需要深度抓 CCASS 的名單（核心自選股 + 所有 Stage 2 上升趨勢股 + 北水 20日急增股）
    candidates = []
    for code in all_codes:
        if code not in price_stats:
            continue
        p = price_stats[code]
        default_name = PINNED_NAMES.get(code, f"{code}.HK")
        info_now = latest_map.get(code, {"name": default_name, "nb_pct": 0.0})
        info_20d = prev20_map.get(code, {})
        nb_now = info_now["nb_pct"]
        nb_chg = round(nb_now - info_20d["nb_pct"], 2) if "nb_pct" in info_20d else 0.0

        is_pinned = code in PINNED_CODES
        is_stage2 = p["stage2"]
        is_early_accum = (not is_stage2) and (nb_chg >= 0.8)

        if is_pinned or is_stage2 or is_early_accum:
            priority = (100 if is_pinned else 0) + (50 if is_stage2 else 0) + nb_chg
            candidates.append({
                "code_raw": code,
                "name": info_now["name"],
                "price_info": p,
                "nb_pct_summary": nb_now,
                "nb_chg_summary": nb_chg,
                "priority": priority,
            })

    candidates.sort(key=lambda x: x["priority"], reverse=True)
    selected = candidates[:MAX_CCASS_STOCKS]
    print(f"篩選出 {len(selected)} 隻標的進行 CCASS 席位深度分析...", flush=True)

    # 4. 多線程抓取 CCASS 席位明細
    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(analyze_ccass_detail, item) for item in selected]
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    build_html(results, len(latest_map), total_stage2)
