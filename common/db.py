
import os, pyodbc

try:
    from telegram_config import AI_BOTS_CONN as CFG_CONN
except ImportError:
    CFG_CONN = None

AI_BOTS_CONN = CFG_CONN or os.getenv("AI_BOTS_CONN")
if not AI_BOTS_CONN:
    raise RuntimeError("AI_BOTS_CONN must be defined in telegram_config.py or env var")

def db_conn():
    return pyodbc.connect(AI_BOTS_CONN, autocommit=False)

