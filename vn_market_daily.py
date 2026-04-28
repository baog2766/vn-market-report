#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, datetime, logging, requests, sqlite3, json, traceback, time
import pandas as pd
import numpy as np
import yfinance as yf

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Environment variables
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

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

# ======================== FETCH VN DATA (NEW VNSTOCK API) ========================
def fetch_vn_data():
    """
    Fetch VN data using vnstock NEW API v4.0+
    from vnstock.api.quote import Quote
    """
    logger.info("📥 Fetching VN data via vnstock (NEW API v4.0+)...")
    
    all_data = []
    
    try:
        from vnstock.api.quote import Quote
        
        logger.info("✅ Using vnstock.api.quote.Quote (NEW API)")
        
        for symbol in SYMBOLS_VN:
            try:
                logger.info(f"  Fetching {symbol}...")
                
                # NEW API syntax
                q = Quote(symbol=symbol, source='VCI')
                df = q.history(period='30d')
                
                if df is not None and not df.empty:
                    latest = df.iloc[-1]
                    
                    date_str = latest.get('time', TODAY)
                    if isinstance(date_str, pd.Timestamp):
                        date_str = date_str.strftime("%Y-%m-%d")
                    
                    open_price = float(latest.get('open', 0))
                    high_price = float(latest.get('high', 0))
                    low_price = float(latest.get('low', 0))
                    close_price = float(latest.get('close', 0))
                    volume = float(latest.get('volume', 0))
                    
                    all_data.append((
                        date_str, symbol,
                        open_price, high_price, low_price,
                        close_price, volume,
                        'vnstock'
                    ))
                    logger.info(f"✅ {symbol}: {close_price}")
                    
                    # Rate limiting
                    time.sleep(1.0)
                else:
                    logger.warning(f"⚠️ No data for {symbol}")
                    
            except Exception as e:
                logger.error(f"❌ Error fetching {symbol}: {str(e)[:80]}")
                time.sleep(1.0)
                continue
        
    except ImportError:
        logger.error("❌ vnstock.api.quote not available, using yfinance fallback")
        all_data = fetch_vn_yfinance()
    except Exception as e:
        logger.error(f"❌ vnstock failed: {e}")
        all_data = fetch_vn_yfinance()
    
    logger.info(f"📊 Total fetched: {len(all_data)}/{len(SYMBOLS_VN)} symbols")
    return all_data

def fetch_vn_yfinance():
    """Fallback to yfinance for VN stocks"""
    logger.info("📥 Fetching VN data via yfinance (fallback)...")
    
    yf_symbols = {
        "VNINDEX": "^VNINDEX",
        "VN30": "VN30F1M.HO",
        "VCB": "VCB.HO",
        "VIC": "VIC.HO",
        "VNM": "VNM.HO",
        "TCB": "TCB.HO",
        "HPG": "HPG.HO",
        "FPT": "FPT.HO"
    }
    
    all_data = []
    for symbol, yf_symbol in yf_symbols.items():
        try:
            df = yf.download(yf_symbol, period='30d', progress=False)
            
            if not df.empty:
                latest = df.iloc[-1]
                date_str = latest.name.strftime("%Y-%m-%d") if hasattr(latest.name, 'strftime') else TODAY
                
                all_data.append((
                    date_str, symbol,
                    float(latest.get('Open', 0)),
                    float(latest.get('High', 0)),
                    float(latest.get('Low', 0)),
                    float(latest.get('Close', 0)),
                    float(latest.get('Volume', 0)),
                    'yfinance'
                ))
                logger.info(f"✅ {symbol}: {latest.get('Close', 0)}")
                
                time.sleep(0.5)
        except Exception as e:
            logger.error(f"❌ Error fetching {symbol}: {str(e)[:60]}")
            continue
    
    return all_data

# ======================== FETCH GLOBAL DATA ========================
def fetch_global_data():
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
        
        hist = db.query(
            "SELECT high, low, close FROM prices WHERE ticker=? ORDER BY date DESC LIMIT 20",
            (ticker,)
        )
        
        if len(hist) < 5:
            continue
        
        # Calculate ATR
        tr_list = []
        for i in range(1, min(14, len(hist))):
            high_prev, low_prev, close_prev = hist[i][0], hist[i][1], hist[i-1][2]
            tr = max(abs(high_prev - low_prev), abs(high_prev - close_prev), abs(low_prev - close_prev))
            tr_list.append(tr)
        
        atr = np.mean(tr_list) if tr_list else c * 0.02
        
        # Pivot Points
        pivot = (h + l + c) / 3
        r1 = 2 * pivot - l
        s1 = 2 * pivot - h
        
        # Scenarios
        bear_target = s1 - atr * 0.5
        base_target = pivot
        bull_target = r1 + atr * 0.5
        
        scenarios.append((
            TODAY, ticker,
            30.0, 40.0, 30.0,
            round(bear_target, 2), round(base_target, 2), round(bull_target, 2),
            round((atr / c) * 100, 1) if c > 0 else 15.0
        ))
    
    if scenarios:
        db.insert_or_replace("scenarios", scenarios)
        logger.info(f"✅ Generated {len(scenarios)} scenarios")
    
    return scenarios

# ======================== GENERATE TEXT REPORT ========================
def generate_text_report(vn_data, global_data, scenarios, quality):
    """Generate text message for Telegram"""
    logger.info("📝 Generating text report...")
    
    lines = []
    lines.append(f"📊 *BÁO CÁO THỊ TRƯỜNG CKVN*")
    lines.append(f"📅 Ngày: {TODAY}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    
    # VN Indices
    lines.append("🇻🇳 *CHỈ SỐ VIỆT NAM*")
    if vn_data:
        for t, c, v in vn_data:
            lines.append(f"• {t}: *{c:,.0f}* (Vol: {v:,.0f})")
    else:
        lines.append("• Không có dữ liệu")
    lines.append("")
    
    # Global Markets
    lines.append("🌍 *LIÊN THỊ TRƯỜNG*")
    if global_data:
        for t, c in global_data:
            lines.append(f"• {t}: *{c:,.2f}*")
    else:
        lines.append("• Không có dữ liệu")
    lines.append("")
    
    # Scenarios
    lines.append("📈 *KỊCH BẢN 1 NGÀY*")
    if scenarios:
        for t, bp, bsp, bup, bt, bst, but, err in scenarios[:3]:  # Top 3 only
            lines.append(f"• {t}:")
            lines.append(f"  🐻 Bear {bp:.0f}%: {bt}")
            lines.append(f"  ⚖️ Base {bsp:.0f}%: {bst} ±{err}%")
            lines.append(f"  🐂 Bull {bup:.0f}%: {but}")
    else:
        lines.append("• Không có dữ liệu")
    lines.append("")
    
    # Quality
    lines.append("📋 *CHẤT LƯỢNG DỮ LIỆU*")
    lines.append(f"• Thiếu: {quality['miss']}/{quality['total']}")
    lines.append(f"• Sai số: ±{quality['err']}%")
    if quality['notes']:
        lines.append(f"• Ghi chú: {quality['notes'][:50]}")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    lines.append("⚠️ *Lưu ý:* Dữ liệu mang tính chất tham khảo")
    lines.append("Nguồn: vnstock + yfinance (Free API)")
    
    return "\n".join(lines)

# ======================== SEND TELEGRAM ========================
def send_telegram_message(text):
    """Send text message to Telegram"""
    logger.info("📤 Sending text message to Telegram...")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    try:
        response = requests.post(
            url,
            data={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "Markdown"
            },
            timeout=30
        )
        if response.status_code == 200:
            logger.info("✅ Telegram message sent!")
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
        
        # Get data for report
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
        report_text = generate_text_report(vn_data, global_data, scenarios, quality)
        if send_telegram_message(report_text):
            logger.info("🎉 Done!")
        else:
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"💥 CRITICAL: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
