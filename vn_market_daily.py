#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, datetime, logging, requests, sqlite3, json, traceback, time
import pandas as pd
import numpy as np
import yfinance as yf
from fpdf import FPDF

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
VNSTOCK_API_KEY = os.getenv("VNSTOCK_API_KEY")

if not (BOT_TOKEN and CHAT_ID):
    logger.error("❌ Missing Telegram credentials!")
    sys.exit(1)

# Configuration
SYMBOLS_VN = ["VNINDEX", "VN30", "VCB", "VIC", "VNM", "TCB", "HPG", "FPT"]
SYMBOLS_GL = ["^GSPC", "^DJI", "USDVND=X", "GC=F", "CL=F"]
DB_PATH = "vn_market.db"
TODAY = datetime.date.today().isoformat()

# ======================== DATABASE ========================
class DB:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self._create_tables()
    
    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS prices (
                date TEXT, ticker TEXT, o REAL, h REAL, l REAL, c REAL, vol REAL, src TEXT,
                PRIMARY KEY(date, ticker)
            );
            CREATE TABLE IF NOT EXISTS scenarios (
                date TEXT, ticker TEXT, bp REAL, bsp REAL, bup REAL,
                bt REAL, bst REAL, but REAL, err REAL,
                PRIMARY KEY(date, ticker)
            );
            CREATE TABLE IF NOT EXISTS quality (
                date TEXT PRIMARY KEY, total INT, miss INT, err REAL, notes TEXT
            );
        """)
        self.conn.commit()

    def insert_or_replace(self, table, data):
        if table == "prices":
            self.conn.executemany(
                "INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)", data)
        elif table == "scenarios":
            self.conn.executemany(
                "INSERT OR REPLACE INTO scenarios VALUES (?,?,?,?,?,?,?,?,?)", data)
        elif table == "quality":
            self.conn.execute(
                "INSERT OR REPLACE INTO quality VALUES (?,?,?,?,?)", data)
        self.conn.commit()

    def query(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def close(self):
        self.conn.close()

# ======================== FETCH VN DATA (vnstock) ========================
def fetch_vn_data():
    """
    Fetch VN data using vnstock with API key (if available).
    With API key: 60 requests/minute
    Without API key: 20 requests/minute
    """
    logger.info("📥 Fetching VN data via vnstock...")
    
    try:
        from vnstock.api.quote import Quote
        
        # Get API key from environment variable
        api_key = os.getenv("VNSTOCK_API_KEY")
        
        if api_key:
            logger.info("✅ Using VNSTOCK API Key (60 req/min)")
        else:
            logger.warning("⚠️ No API Key found - using Guest mode (20 req/min)")
        
        all_data = []
        for symbol in SYMBOLS_VN:
            try:
                # Initialize Quote with API key if available
                if api_key:
                    quote = Quote(symbol=symbol, source='VCI', api_key=api_key)
                else:
                    quote = Quote(symbol=symbol, source='VCI')
                
                # Fetch 30 days of history
                df = quote.history(period='30d')
                
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    date_str = latest.name.strftime("%Y-%m-%d") if hasattr(latest.name, 'strftime') else TODAY
                    
                    all_data.append((
                        date_str, symbol,
                        float(latest.get('Open', 0)),
                        float(latest.get('High', 0)),
                        float(latest.get('Low', 0)),
                        float(latest.get('Close', 0)),
                        float(latest.get('Volume', 0)),
                        'vnstock'
                    ))
                    logger.info(f"✅ Fetched {symbol}: {latest.get('Close', 0)}")
                    
                    # Rate limiting:
                    # - With API key (60/min): sleep 1.0 second
                    # - Without API key (20/min): sleep 3.5 seconds
                    sleep_time = 1.0 if api_key else 3.5
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"⚠️ No data for {symbol}")
                    
            except Exception as e:
                logger.error(f"❌ Error fetching {symbol}: {str(e)[:60]}")
                # Still wait to respect rate limit
                time.sleep(1.0 if api_key else 3.5)
                continue
        
        logger.info(f"📊 Total fetched: {len(all_data)}/{len(SYMBOLS_VN)} symbols")
        return all_data
        
    except ImportError:
        logger.error("❌ vnstock not installed. Run: pip install vnstock")
        return []
    except Exception as e:
        logger.error(f"❌ vnstock fetch failed: {e}")
        return []

# ======================== FETCH GLOBAL DATA (yfinance) ========================
def fetch_global_data():
    """Fetch global markets using yfinance"""
    logger.info("📥 Fetching Global data via yfinance...")
    
    try:
        df = yf.download(SYMBOLS_GL, period='2d', group_by='ticker', progress=False, threads=False)
        
        all_data = []
        for ticker in SYMBOLS_GL:
            try:
                if isinstance(df.columns, pd.MultiIndex):
                    ticker_df = df[ticker]
                else:
                    ticker_df = df
                
                if ticker_df.empty:
                    continue
                
                latest = ticker_df.iloc[-1]
                date_str = latest.name.strftime("%Y-%m-%d") if hasattr(latest.name, 'strftime') else TODAY
                
                all_data.append((
                    date_str, ticker,
                    float(latest.get('Open', 0)),
                    float(latest.get('High', 0)),
                    float(latest.get('Low', 0)),
                    float(latest.get('Close', 0)),
                    float(latest.get('Volume', 0)),
                    'yfinance'
                ))
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch {ticker}: {e}")
        
        return all_data
        
    except Exception as e:
        logger.error(f"❌ yfinance fetch failed: {e}")
        return []

# ======================== MAIN FETCH ========================
def fetch_all_data(db):
    miss = 0
    notes = []
    
    # Fetch VN data
    vn_records = fetch_vn_data()
    if vn_records:
        db.insert_or_replace("prices", vn_records)
        logger.info(f"✅ Inserted {len(vn_records)} VN records")
    else:
        miss += len(SYMBOLS_VN)
        notes.append("vnstock: No data")
    
    # Fetch Global data
    global_records = fetch_global_data()
    if global_records:
        db.insert_or_replace("prices", global_records)
        logger.info(f"✅ Inserted {len(global_records)} Global records")
    else:
        miss += len(SYMBOLS_GL)
        notes.append("yfinance: No data")
    
    # Log quality
    total = len(SYMBOLS_VN) + len(SYMBOLS_GL)
    err_pct = round((miss / max(1, total)) * 100, 2)
    db.insert_or_replace("quality", (TODAY, total, miss, err_pct, " | ".join(notes)))

# ======================== ANALYZE ========================
def analyze(db):
    logger.info("📈 Analyzing market data...")
    
    # Get today's data or most recent
    prices = db.query("SELECT * FROM prices WHERE date=?", (TODAY,))
    if not prices:
        last_date = db.query("SELECT date FROM prices ORDER BY date DESC LIMIT 1")
        if last_date:
            prices = db.query("SELECT * FROM prices WHERE date=?", (last_date[0][0],))
    
    scenarios = []
    for row in prices:
        date, ticker, o, h, l, c, vol, src = row
        if ticker not in SYMBOLS_VN:
            continue
        
        # Get historical data for ATR
        hist = db.query(
            "SELECT high, low, close FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 20",
            (ticker,)
        )
        
        if len(hist) < 5:
            continue
        
        # Calculate ATR (14-period)
        tr_list = []
        for i in range(1, min(14, len(hist))):
            high_prev, low_prev, close_prev = hist[i][0], hist[i][1], hist[i-1][2]
            tr = max(
                abs(high_prev - low_prev),
                abs(high_prev - close_prev),
                abs(low_prev - close_prev)
            )
            tr_list.append(tr)
        
        atr = np.mean(tr_list) if tr_list else c * 0.02
        
        # Pivot Points
        pivot = (h + l + c) / 3
        r1 = 2 * pivot - l
        s1 = 2 * pivot - h
        
        # Scenario targets
        bear_target = s1 - atr * 0.5
        base_target = pivot
        bull_target = r1 + atr * 0.5
        
        # Probabilities
        bear_prob, base_prob, bull_prob = 30.0, 40.0, 30.0
        
        # Error percentage
        err_pct = round((atr / c) * 100, 1) if c > 0 else 15.0
        
        scenarios.append((
            TODAY, ticker,
            bear_prob, base_prob, bull_prob,
            round(bear_target, 2), round(base_target, 2), round(bull_target, 2),
            err_pct
        ))
    
    if scenarios:
        db.insert_or_replace("scenarios", scenarios)
        logger.info(f"✅ Generated {len(scenarios)} scenarios")
    
    return scenarios

# ======================== PDF GENERATOR ========================
def download_font():
    font_path = "/tmp/NotoSans-Regular.ttf"
    if os.path.exists(font_path):
        return font_path
    
    logger.info("Downloading font...")
    url = "https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200 and len(response.content) > 10000:
            with open(font_path, "wb") as f:
                f.write(response.content)
            logger.info("✅ Font downloaded")
            return font_path
    except Exception as e:
        logger.error(f"❌ Font download failed: {e}")
    return None

class PDFGenerator(FPDF):
    def __init__(self, font_path):
        super().__init__()
        if font_path:
            self.add_font("NotoSans", "", font_path)
            self.add_font("NotoSans", "B", font_path)
            self.font_name = "NotoSans"
        else:
            self.font_name = "Helvetica"
    
    def header(self):
        self.set_font(self.font_name, "B", 14)
        self.cell(0, 10, "BAO CAO THI TRUONG CHUNG KHOAN VN", ln=True, align="C")
        self.set_font(self.font_name, "", 9)
        self.cell(0, 5, f"Ngay: {TODAY} | Nguon: vnstock + yfinance", ln=True, align="C")
        self.line(10, 20, 200, 20)
        self.ln(5)
    
    def footer(self):
        self.set_y(-15)
        self.set_font(self.font_name, "I", 8)
        self.cell(0, 10, "⚠️ Du lieu co sai so ky thuat. Khong thay the tu van chuyen nghiep.", align="C")
    
    def section(self, title, lines):
        self.set_font(self.font_name, "B", 11)
        self.set_fill_color(235, 235, 235)
        self.cell(0, 8, f"  {title}", fill=True, ln=True)
        self.set_font(self.font_name, "", 9)
        for line in lines:
            self.multi_cell(0, 5, f"- {line}")
        self.ln(2)

def generate_pdf(vn_data, global_data, scenarios, quality):
    font_path = download_font()
    pdf = PDFGenerator(font_path)
    pdf.add_page()
    
    # VN data
    vn_lines = [f"{t}: {c} (Vol: {int(v):,})" for t, c, v in vn_data]
    pdf.section("1. CHI SO VIET NAM", vn_lines or ["Khong co du lieu"])
    
    # Global data
    global_lines = [f"{t}: {c}" for t, c in global_data]
    pdf.section("2. LIEN THI TRUONG", global_lines or ["Khong co du lieu"])
    
    # Scenarios
    scenario_lines = [
        f"{t}: Bear {bp:.0f}% ({bt}) | Base {bsp:.0f}% ({bst}+/-{err}%) | Bull {bup:.0f}% ({but})"
        for t, bp, bsp, bup, bt, bst, but, err in scenarios
    ]
    pdf.section("3. KICH BAN 1 NGAY", scenario_lines or ["Khong co du lieu"])
    
    # Quality
    quality_lines = [
        f"Thieu: {quality['miss']}/{quality['total']} | Sai so: +/-{quality['err']}%",
        f"Ghi chu: {quality['notes']}"
    ]
    pdf.section("4. CHAT LUONG DU LIEU", quality_lines)
    
    try:
        return pdf.output(dest="S").encode("latin1")
    except Exception as e:
        logger.error(f"❌ PDF generation failed: {e}")
        return b"PDF generation failed"

# ======================== SEND TELEGRAM ========================
def send_telegram(pdf_bytes, filename="market_report.pdf"):
    logger.info("📤 Sending to Telegram...")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    
    try:
        response = requests.post(
            url,
            data={"chat_id": CHAT_ID, "caption": f"📊 Bao cao thi truong - {TODAY}"},
            files={"document": (filename, pdf_bytes, "application/pdf")},
            timeout=30
        )
        if response.status_code == 200:
            logger.info("✅ Telegram sent!")
            return True
        else:
            logger.error(f"❌ Telegram error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Send failed: {e}")
        return False

# ======================== MAIN ========================
def main():
    try:
        logger.info("🚀 Starting daily report...")
        db = DB()
        
        # Fetch data
        fetch_all_data(db)
        
        # Analyze
        scenarios = analyze(db)
        
        # Get data for PDF
        run_date = db.query("SELECT date FROM prices ORDER BY date DESC LIMIT 1")
        run_date = run_date[0][0] if run_date else TODAY
        
        vn_data = [(t, c, v) for t, c, v in db.query(
            "SELECT ticker, c, vol FROM prices WHERE date=?", (run_date,)
        ) if t in SYMBOLS_VN]
        
        global_data = [(t, c) for t, c in db.query(
            "SELECT ticker, c FROM prices WHERE date=?", (run_date,)
        ) if t in SYMBOLS_GL]
        
        quality_data = db.query("SELECT * FROM quality WHERE date=?", (run_date,))
        quality = {
            "total": quality_data[0][1] if quality_data else 0,
            "miss": quality_data[0][2] if quality_data else 0,
            "err": quality_data[0][3] if quality_data else 0,
            "notes": quality_data[0][4] if quality_data else ""
        }
        
        db.close()
        
        # Generate & send
        pdf_bytes = generate_pdf(vn_data, global_data, scenarios, quality)
        if send_telegram(pdf_bytes, f"VN_Market_{run_date.replace('-', '')}.pdf"):
            logger.info("🎉 Done!")
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"💥 CRITICAL: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
