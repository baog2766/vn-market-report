#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, datetime, logging, requests, sqlite3, json, traceback
import pandas as pd
import numpy as np
import yfinance as yf
from fpdf import FPDF

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Debug environment variables
logger.info("=" * 50)
logger.info("DEBUG: Checking environment variables...")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
logger.info(f"TELEGRAM_BOT_TOKEN exists: {bool(BOT_TOKEN)}")
logger.info(f"TELEGRAM_CHAT_ID exists: {bool(CHAT_ID)}")
logger.info(f"TELEGRAM_CHAT_ID value: {CHAT_ID}")
logger.info("=" * 50)

if not BOT_TOKEN:
    logger.error("❌ ERROR: TELEGRAM_BOT_TOKEN is missing or empty!")
    sys.exit(1)
    
if not CHAT_ID:
    logger.error("❌ ERROR: TELEGRAM_CHAT_ID is missing or empty!")
    sys.exit(1)

logger.info(f"✅ Bot Token: {BOT_TOKEN[:20]}...")
logger.info(f"✅ Chat ID: {CHAT_ID}")

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
            DROP TABLE IF EXISTS prices;
            DROP TABLE IF EXISTS scenarios;
            DROP TABLE IF EXISTS bctc;
            DROP TABLE IF EXISTS quality;
            
            CREATE TABLE prices (
                date TEXT,
                ticker TEXT,
                o REAL,
                h REAL,
                l REAL,
                c REAL,
                vol REAL,
                src TEXT,
                PRIMARY KEY(date, ticker)
            );
            
            CREATE TABLE scenarios (
                date TEXT,
                ticker TEXT,
                bp REAL,
                bsp REAL,
                bup REAL,
                bt REAL,
                bst REAL,
                but REAL,
                err REAL,
                PRIMARY KEY(date, ticker)
            );
            
            CREATE TABLE bctc (
                ticker TEXT,
                period TEXT,
                roe REAL,
                pe REAL,
                de REAL,
                ni REAL,
                last TEXT,
                PRIMARY KEY(ticker, period)
            );
            
            CREATE TABLE quality (
                date TEXT PRIMARY KEY,
                total INT,
                miss INT,
                err REAL,
                notes TEXT
            );
        """)
        self.conn.commit()
        logger.info("✅ Database tables created")

    def insert_prices(self, data):
        self.conn.executemany(
            "INSERT OR REPLACE INTO prices VALUES (?,?,?,?,?,?,?,?)",
            data
        )
        self.conn.commit()

    def insert_scenarios(self, data):
        self.conn.executemany(
            "INSERT OR REPLACE INTO scenarios VALUES (?,?,?,?,?,?,?,?,?)",
            data
        )
        self.conn.commit()

    def insert_bctc(self, data):
        self.conn.executemany(
            "INSERT OR REPLACE INTO bctc VALUES (?,?,?,?,?,?,?)",
            data
        )
        self.conn.commit()

    def insert_quality(self, date, total, miss, err, notes):
        self.conn.execute(
            "INSERT OR REPLACE INTO quality VALUES (?,?,?,?,?)",
            (date, total, miss, err, notes)
        )
        self.conn.commit()

    def query(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def close(self):
        self.conn.close()

# ======================== FETCH DATA ========================
def fetch_data(db):
    logger.info("📥 Fetching market data...")
    miss = 0
    notes = []
    
    # Fetch VN data
    try:
        from vnstock import Vnstock
        vns = Vnstock()
        logger.info("Fetching VN data from vnstock...")
        df = vns.quote.history(
            symbol=",".join(SYMBOLS_VN),
            start=(datetime.date.today() - datetime.timedelta(days=30)).isoformat(),
            end=TODAY
        )
        if not df.empty:
            df = df[["ticker", "time", "open", "high", "low", "close", "volume"]].dropna()
            df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d")
            records = []
            for _, r in df.iterrows():
                records.append((r["time"], r["ticker"], r["open"], r["high"], r["low"], r["close"], r["volume"], "vnstock"))
            db.insert_prices(records)
            logger.info(f"✅ Fetched {len(records)} VN records")
        else:
            miss += len(SYMBOLS_VN)
            notes.append("vnstock: No data returned")
    except Exception as e:
        miss += len(SYMBOLS_VN)
        error_msg = f"vnstock: {str(e)[:80]}"
        notes.append(error_msg)
        logger.error(f"❌ {error_msg}")

    # Fetch Global data
    try:
        logger.info("Fetching Global data from yfinance...")
        yf_data = yf.download(SYMBOLS_GL, period="2d", group_by="ticker", progress=False)
        records = []
        for t in SYMBOLS_GL:
            try:
                if t in yf_data.columns.levels[0] or (isinstance(yf_data.columns, pd.MultiIndex) and t in yf_data.columns.levels[0]):
                    r = yf_data[t].iloc[-1]
                    records.append((r.name.strftime("%Y-%m-%d"), t, 0, 0, 0, r["Close"], r.get("Volume", 0), "yfinance"))
                elif t in yf_data:
                    r = yf_data[t].iloc[-1]
                    records.append((r.name.strftime("%Y-%m-%d"), t, 0, 0, 0, r["Close"], r.get("Volume", 0), "yfinance"))
            except Exception as e:
                miss += 1
                logger.warning(f"⚠️ Could not fetch {t}: {e}")
        
        if records:
            db.insert_prices(records)
            logger.info(f"✅ Fetched {len(records)} Global records")
    except Exception as e:
        miss += len(SYMBOLS_GL)
        error_msg = f"yfinance: {str(e)[:80]}"
        notes.append(error_msg)
        logger.error(f"❌ {error_msg}")

    # Save quality log
    total = len(SYMBOLS_VN) + len(SYMBOLS_GL)
    err_pct = round((miss / max(1, total)) * 100, 2)
    db.insert_quality(TODAY, total, miss, err_pct, " | ".join(notes))
    logger.info(f"📊 Quality: {miss}/{total} missing, error rate: {err_pct}%")

# ======================== ANALYZE ========================
def analyze(db):
    logger.info("📈 Analyzing market data...")
    prices = db.query("SELECT * FROM prices WHERE date=?", (TODAY,))
    scenarios = []
    
    for row in prices:
        date, ticker, o, h, l, c, vol, src = row
        if ticker not in SYMBOLS_VN:
            continue
        
        # Get historical data for ATR calculation
        hist = db.query(
            "SELECT high, low, close FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 20",
            (ticker,)
        )
        
        if len(hist) < 5:
            continue
        
        # Calculate ATR (simplified)
        try:
            tr_list = []
            for i in range(1, min(14, len(hist))):
                high_prev = hist[i][0]
                low_prev = hist[i][1]
                close_prev = hist[i-1][2]
                tr = max(
                    abs(high_prev - low_prev),
                    abs(high_prev - close_prev),
                    abs(low_prev - close_prev)
                )
                tr_list.append(tr)
            
            atr = np.mean(tr_list) if tr_list else c * 0.02
        except:
            atr = c * 0.02
        
        # Calculate Pivot Points
        pivot = (h + l + c) / 3
        r1 = 2 * pivot - l
        s1 = 2 * pivot - h
        
        # Calculate scenario targets
        bear_target = s1 - atr * 0.5
        base_target = pivot
        bull_target = r1 + atr * 0.5
        
        # Probabilities (simple model)
        bear_prob = 30.0
        base_prob = 40.0
        bull_prob = 30.0
        
        # Calculate error percentage
        err_pct = round((atr / c) * 100, 1) if c > 0 else 15.0
        
        scenarios.append((
            TODAY, ticker,
            bear_prob, base_prob, bull_prob,
            round(bear_target, 2), round(base_target, 2), round(bull_target, 2),
            err_pct
        ))
    
    if scenarios:
        db.insert_scenarios(scenarios)
        logger.info(f"✅ Generated {len(scenarios)} scenarios")
    
    return scenarios

# ======================== BCTC ========================
def get_bctc(db):
    logger.info("📑 Fetching financial reports...")
    lines = []
    bctc_records = []
    
    for sym in ["VCB", "VNM", "TCB", "HPG"]:
        # Check cache
        cached = db.query(
            "SELECT * FROM bctc WHERE ticker=? ORDER BY last DESC LIMIT 1",
            (sym,)
        )
        
        if cached:
            last_date = datetime.datetime.strptime(cached[0][6], "%Y-%m-%d")
            if (datetime.datetime.now() - last_date).days < 60:
                lines.append(f"{sym}: {cached[0][1]} | ROE {cached[0][2]:.1f}% | P/E {cached[0][3]:.1f}")
                continue
        
        # Fetch new data
        try:
            from vnstock import Vnstock
            vns = Vnstock()
            df = vns.company.finance(symbol=sym, report_type="quarterly")
            
            if not df.empty:
                r = df.iloc[-1]
                period = f"{r.get('year', '')}Q{r.get('quarter', '')}"
                roe = float(r.get('roe', 0))
                pe = float(r.get('pe', 0))
                de = float(r.get('debt_to_equity', 0))
                ni = float(r.get('net_income', 0))
                
                bctc_records.append((sym, period, roe, pe, de, ni, TODAY))
                lines.append(f"{sym}: {period} | ROE {roe:.1f}% | P/E {pe:.1f}")
            else:
                lines.append(f"{sym}: No data")
        except Exception as e:
            lines.append(f"{sym}: ⚠️ Error - {str(e)[:40]}")
            logger.warning(f"⚠️ Could not fetch BCTC for {sym}: {e}")
    
    if bctc_records:
        db.insert_bctc(bctc_records)
    
    return lines

# ======================== PDF GENERATOR ========================
class PDFGenerator(FPDF):
    def __init__(self, font_path):
        super().__init__()
        self.add_font("DejaVu", "", font_path, uni=True)
        self.add_font("DejaVu", "B", font_path, uni=True)
        self.set_auto_page_break(auto=True, margin=15)
    
    def header(self):
        self.set_font("DejaVu", "B", 14)
        self.cell(0, 10, "BAO CAO THI TRUONG CHUNG KHOAN VN", ln=True, align="C")
        self.set_font("DejaVu", "", 9)
        self.cell(0, 5, f"Ngay: {TODAY} | Nguon: Free API | Telegram Bot", ln=True, align="C")
        self.line(10, 20, 200, 20)
        self.ln(5)
    
    def footer(self):
        self.set_y(-15)
        self.set_font("DejaVu", "I", 8)
        self.cell(0, 10, "⚠️ Du lieu co sai so ky thuat. Khong thay the tu van chuyen nghiep.", align="C")
    
    def section(self, title, lines):
        self.set_font("DejaVu", "B", 11)
        self.set_fill_color(235, 235, 235)
        self.cell(0, 8, f"  {title}", fill=True, ln=True)
        self.set_font("DejaVu", "", 9)
        for line in lines:
            self.multi_cell(0, 5, f"- {line}")
        self.ln(2)

def generate_pdf(vn_data, global_data, scenarios, bctc_lines, quality):
    logger.info("📄 Generating PDF...")
    
    # Download font
    font_path = "/tmp/DejaVuSans.ttf"
    if not os.path.exists(font_path):
        logger.info("Downloading font...")
        try:
            r = requests.get("https://github.com/dejavu-fonts/dejavu-fonts/raw/master/ttf/DejaVuSans/DejaVuSans.ttf")
            with open(font_path, "wb") as f:
                f.write(r.content)
            logger.info("✅ Font downloaded")
        except Exception as e:
            logger.error(f"❌ Could not download font: {e}")
            # Fallback to built-in font
            font_path = None
    
    pdf = PDFGenerator(font_path) if font_path else FPDF()
    pdf.add_page()
    
    # VN Indices
    vn_lines = [f"{t}: {c} (Vol: {int(v):,})" for t, c, v in vn_data]
    pdf.section("1. CHI SO VIET NAM", vn_lines)
    
    # Global Markets
    global_lines = [f"{t}: {c}" for t, c in global_data]
    pdf.section("2. LIEN THI TRUONG", global_lines)
    
    # Scenarios
    scenario_lines = [
        f"{t}: Bear {bp:.0f}% ({bt}) | Base {bsp:.0f}% ({bst}+/-{err}%) | Bull {bup:.0f}% ({but})"
        for t, bp, bsp, bup, bt, bst, but, err in scenarios
    ]
    pdf.section("3. KICH BAN 1 NGAY", scenario_lines)
    
    # BCTC
    pdf.section("4. BAO CAO TAI CHINH", bctc_lines)
    
    # Quality
    quality_lines = [
        f"Thieu: {quality['miss']}/{quality['total']} | Sai so: +/-{quality['err']}%",
        f"Ghi chu: {quality['notes']}"
    ]
    pdf.section("5. CHAT LUONG DU LIEU", quality_lines)
    
    pdf_bytes = pdf.output(dest="S").encode("latin1")
    logger.info("✅ PDF generated")
    return pdf_bytes

# ======================== SEND TELEGRAM ========================
def send_telegram(pdf_bytes, filename="market_report.pdf"):
    logger.info("📤 Sending to Telegram...")
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    
    try:
        response = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "caption": f"📊 Bao cao thi truong CKVN - {TODAY}"
            },
            files={
                "document": (filename, pdf_bytes, "application/pdf")
            },
            timeout=30
        )
        
        if response.status_code == 200:
            logger.info("✅ Telegram sent successfully!")
            return True
        else:
            logger.error(f"❌ Telegram error: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Failed to send Telegram: {e}")
        return False

# ======================== MAIN ========================
def main():
    try:
        logger.info("🚀 Starting daily market report...")
        
        # Initialize DB
        db = DB()
        
        # Fetch data
        fetch_data(db)
        
        # Analyze
        scenarios = analyze(db)
        
        # Get BCTC
        bctc_lines = get_bctc(db)
        
        # Get data for PDF
        vn_data = [
            (t, c, v) for t, c, v in db.query(
                "SELECT ticker, c, vol FROM prices WHERE date=?", (TODAY,)
            ) if t in SYMBOLS_VN
        ]
        
        global_data = [
            (t, c) for t, c in db.query(
                "SELECT ticker, c FROM prices WHERE date=?", (TODAY,)
            ) if t in SYMBOLS_GL
        ]
        
        quality_data = db.query("SELECT * FROM quality WHERE date=?", (TODAY,))
        quality = {
            "total": quality_data[0][1] if quality_data else 0,
            "miss": quality_data[0][2] if quality_data else 0,
            "err": quality_data[0][3] if quality_data else 0,
            "notes": quality_data[0][4] if quality_data else ""
        }
        
        db.close()
        
        # Generate PDF
        pdf_bytes = generate_pdf(vn_data, global_data, scenarios, bctc_lines, quality)
        
        # Send to Telegram
        success = send_telegram(pdf_bytes, f"VN_Market_{TODAY.replace('-', '')}.pdf")
        
        if success:
            logger.info("🎉 Daily report completed successfully!")
        else:
            logger.warning("⚠️ Report generated but failed to send to Telegram")
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"💥 CRITICAL ERROR: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
