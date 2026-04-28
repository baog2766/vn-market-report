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
            CREATE TABLE IF NOT EXISTS prices (
                date TEXT, 
                ticker TEXT, 
                o REAL, h REAL, l REAL, c REAL, vol REAL, src TEXT, 
                PRIMARY KEY(date, ticker)
            );
            CREATE TABLE IF NOT EXISTS scenarios (
                date TEXT, 
                ticker TEXT, 
                bp REAL, bsp REAL, bup REAL, 
                bt REAL, bst REAL, but REAL, err REAL, 
                PRIMARY KEY(date, ticker)
            );
            CREATE TABLE IF NOT EXISTS bctc (
                ticker TEXT, 
                period TEXT, 
                roe REAL, pe REAL, de REAL, ni REAL, last TEXT, 
                PRIMARY KEY(ticker, period)
            );
            CREATE TABLE IF NOT EXISTS quality (
                date TEXT PRIMARY KEY, 
                total INT, miss INT, err REAL, notes TEXT
            );
        """)
        self.conn.commit()

    def upsert(self, table, d):
        cols = ", ".join(d.keys())
        vals = ", ".join(["?"] * len(d))
        
        # Xác định conflict target cho từng bảng
        if table == "quality":
            # Bảng quality chỉ có 1 record per date
            sql = f"""
                INSERT INTO {table} ({cols}) VALUES ({vals})
                ON CONFLICT(date) DO UPDATE SET 
                    total=excluded.total,
                    miss=excluded.miss,
                    err=excluded.err,
                    notes=excluded.notes
            """
        elif table in ["prices", "scenarios"]:
            pk = "date, ticker"
            updates = ", ".join([f"{k}=excluded.{k}" for k in d.keys() if k not in ["date", "ticker"]])
            sql = f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT({pk}) DO UPDATE SET {updates}"
        elif table == "bctc":
            pk = "ticker, period"
            updates = ", ".join([f"{k}=excluded.{k}" for k in d.keys() if k not in ["ticker", "period"]])
            sql = f"INSERT INTO {table} ({cols}) VALUES ({vals}) ON CONFLICT({pk}) DO UPDATE SET {updates}"
        else:
            # Fallback: INSERT OR REPLACE
            sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({vals})"
        
        self.conn.execute(sql, list(d.values()))
        self.conn.commit()

    def query(self, sql, p=()): 
        return self.conn.execute(sql, p).fetchall()
    
    def close(self): 
        self.conn.close()
