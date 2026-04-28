#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, datetime, logging, requests, sqlite3, json, traceback
import pandas as pd, numpy as np, yfinance as yf
from fpdf import FPDF

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not (BOT_TOKEN and CHAT_ID):
    logging.error("❌ Thiếu biến môi trường Telegram"); sys.exit(1)

SYMBOLS_VN = ["VNINDEX", "VN30", "VCB", "VIC", "VNM", "TCB", "HPG", "FPT"]
SYMBOLS_GL = ["^GSPC", "^DJI", "USDVND=X", "GC=F", "CL=F"]
DB_PATH = "vn_market.db"
TODAY = datetime.date.today().isoformat()

class DB:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS prices (date TEXT, ticker TEXT, o REAL, h REAL, l REAL, c REAL, vol REAL, src TEXT, PRIMARY KEY(date,ticker));
            CREATE TABLE IF NOT EXISTS scenarios (date TEXT, ticker TEXT, bp REAL, bsp REAL, bup REAL, bt REAL, bst REAL, but REAL, err REAL, PRIMARY KEY(date,ticker));
            CREATE TABLE IF NOT EXISTS bctc (ticker TEXT, period TEXT, roe REAL, pe REAL, de REAL, ni REAL, last TEXT, PRIMARY KEY(ticker,period));
            CREATE TABLE IF NOT EXISTS quality (date TEXT, total INT, miss INT, err REAL, notes TEXT);
        """); self.conn.commit()
    def upsert(self, table, d):
        cols, vals = ", ".join(d.keys()), ", ".join(["?"]*len(d))
        pk = "date,ticker" if table in ["prices","scenarios"] else "ticker,period" if table=="bctc" else "date"
        self.conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT({pk}) DO UPDATE SET {', '.join([f'{k}=excluded.{k}' for k in d])}", list(d.values())); self.conn.commit()
    def query(self, sql, p=()): return self.conn.execute(sql, p).fetchall()
    def close(self): self.conn.close()

def fetch_data(db):
    miss, notes = 0, []
    try:
        from vnstock import Vnstock
        vns = Vnstock()
        df = vns.quote.history(symbol=",".join(SYMBOLS_VN), start=(datetime.date.today()-datetime.timedelta(days=30)).isoformat(), end=TODAY)
        df = df[["ticker","time","open","high","low","close","volume"]].dropna()
        df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")
        for _, r in df.iterrows():
            db.upsert("prices", {"date":r["time"],"ticker":r["ticker"],"o":r["open"],"h":r["high"],"l":r["low"],"c":r["close"],"vol":r["volume"],"src":"vnstock"})
    except Exception as e: miss += len(SYMBOLS_VN); notes.append(f"vnstock:{str(e)[:50]}")
    try:
        yf_data = yf.download(SYMBOLS_GL, period="2d", group_by="ticker", progress=False)
        for t in SYMBOLS_GL:
            try:
                r = yf_data[t].iloc[-1]; db.upsert("prices", {"date":r.name.strftime("%Y-%m-%d"),"ticker":t,"o":0,"h":0,"l":0,"c":r["Close"],"vol":r.get("Volume",0),"src":"yfinance"})
            except: miss += 1
    except Exception as e: miss += len(SYMBOLS_GL); notes.append(f"yfinance:{str(e)[:50]}")
    db.upsert("quality", {"date":TODAY,"total":len(SYMBOLS_VN)+len(SYMBOLS_GL),"miss":miss,"err":round((miss/max(1,len(SYMBOLS_VN)+len(SYMBOLS_GL)))*100,2),"notes":" | ".join(notes)})

def analyze(db):
    sc_list = []
    prices = db.query("SELECT * FROM prices WHERE date=?", (TODAY,))
    for row in prices:
        t, c = row[1], row[5]
        if t not in SYMBOLS_VN: continue
        hist = [x[0] for x in db.query("SELECT close FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 20", (t,))]
        atr = np.mean([max(abs(h-l), abs(h-c0), abs(l-c0)) for h,l,c0 in zip(hist[1:],hist[1:],hist[:-1])][-14:]) if len(hist)>14 else c*0.02
        p, r1, s1 = (hist[-1]+max(hist[-1]-atr,min(hist))+min(hist))/3, 0, 0 # Fallback pivot đơn giản
        p, r1, s1 = (row[3]+row[4]+c)/3, 2*((row[3]+row[4]+c)/3)-row[4], 2*((row[3]+row[4]+c)/3)-row[3]
        bt, bst, bull_t = s1-atr*0.5, p, r1+atr*0.5
        bp, bsp, bup = 0.30, 0.40, 0.30
        db.upsert("scenarios", {"date":TODAY,"ticker":t,"bp":bp*100,"bsp":bsp*100,"bup":bup*100,"bt":round(bt,2),"bst":round(bst,2),"but":round(bull_t,2),"err":round((atr/c)*100,1)})
        sc_list.append((t, bp*100, bsp*100, bup*100, bt, bst, bull_t, round((atr/c)*100,1)))
    return sc_list

def get_bctc(db):
    lines = []
    for sym in ["VCB","VNM","TCB","HPG"]:
        cached = db.query("SELECT * FROM bctc WHERE ticker=? ORDER BY last DESC LIMIT 1", (sym,))
        if cached and (datetime.datetime.now()-datetime.datetime.strptime(cached[0][6],"%Y-%m-%d")).days < 60:
            lines.append(f"{sym}: {cached[0][1]} | ROE {cached[0][2]:.1f}% | P/E {cached[0][3]:.1f}"); continue
        try:
            from vnstock import Vnstock
            df = Vnstock().company.finance(symbol=sym, report_type="quarterly")
            if not df.empty:
                r = df.iloc[-1]; db.upsert("bctc", {"ticker":sym,"period":f"{r.get('year','')}Q{r.get('quarter','')}","roe":r.get("roe",0),"pe":r.get("pe",0),"de":r.get("debt_to_equity",0),"ni":r.get("net_income",0),"last":TODAY})
                lines.append(f"{sym}: {r.get('year','')}Q{r.get('quarter','')} | ROE {r.get('roe',0):.1f}% | P/E {r.get('pe',0):.1f}")
        except: lines.append(f"{sym}: ⚠️ Không lấy được")
    return lines

class PDFGen(FPDF):
    def __init__(self, fp): super().__init__(); self.add_font("D", "", fp, uni=True); self.add_font("D", "B", fp, uni=True); self.set_auto_page_break(auto=True, margin=15)
    def header(self): self.set_font("D","B",13); self.cell(0,10,"BÁO CÁO THỊ TRƯỜNG CKVN",ln=True,align="C"); self.set_font("D","",9); self.cell(0,5,f"Ngày: {TODAY} | Nguồn: Free | Bot Telegram",ln=True,align="C"); self.line(10,20,200,20); self.ln(4)
    def footer(self): self.set_y(-15); self.set_font("D","I",8); self.cell(0,10,"⚠️ Dữ liệu có sai số kỹ thuật. Không thay thế tư vấn chuyên nghiệp.",align="C")
    def sec(self, t, l): self.set_font("D","B",10); self.set_fill_color(235,235,235); self.cell(0,7,f" {t}",fill=True,ln=True); self.set_font("D","",9); [self.multi_cell(0,5,f"- {x}") for x in l]; self.ln(2)

def send_pdf(pdf_bytes, name="report.pdf"):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    res = requests.post(url, data={"chat_id": CHAT_ID, "caption": f"📊 Báo cáo {TODAY}"}, files={"document": (name, pdf_bytes, "application/pdf")})
    logging.info("✅ Telegram OK" if res.status_code==200 else f"❌ TG Error: {res.text}")

def main():
    try:
        db = DB(); fetch_data(db); sc = analyze(db); bctc = get_bctc(db); q = db.query("SELECT * FROM quality WHERE date=?", (TODAY,))
        vn_data = [(t,c,v) for t,c,v in db.query("SELECT ticker,c,vol FROM prices WHERE date=?", (TODAY,)) if t in SYMBOLS_VN]
        gl_data = [(t,c) for t,c in db.query("SELECT ticker,c FROM prices WHERE date=?", (TODAY,)) if t in SYMBOLS_GL]
        db.close()

        fp = "/tmp/DejaVuSans.ttf"
        if not os.path.exists(fp):
            r = requests.get("https://github.com/dejavu-fonts/dejavu-fonts/raw/master/ttf/DejaVuSans/DejaVuSans.ttf")
            with open(fp, "wb") as f: f.write(r.content)

        pdf = PDFGen(fp); pdf.add_page()
        pdf.sec("1. CHỈ SỐ VN", [f"{t}: {c} (Vol: {int(v):,})" for t,c,v in vn_data])
        pdf.sec("2. LIÊN THỊ TRƯỜNG", [f"{t}: {c}" for t,c in gl_data])
        pdf.sec("3. KỊCH BẢN 1 NGÀY", [f"{t}: Bear {bp:.0f}% ({bt}) | Base {bsp:.0f}% ({bst}±{err}%) | Bull {bup:.0f}% ({but})" for t,bp,bsp,bup,bt,bst,but,err in sc])
        pdf.sec("4. BCTC QUÝ", bctc)
        pdf.sec("5. CHẤT LƯỢNG DỮ LIỆU", [f"Missing: {q[0][2]}/{q[0][1]} | Sai số: ±{q[0][3]}% | {q[0][4]}"])
        send_pdf(pdf.output(dest="S").encode("latin1"), f"VN_Market_{TODAY}.pdf")
    except Exception as e: logging.critical(f"💥 {e}\n{traceback.format_exc()}")

if __name__ == "__main__": main()
