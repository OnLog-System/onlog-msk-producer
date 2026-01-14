import sqlite3
import random
from datetime import datetime, timedelta

FACTORY = "F02"
ROWS = 50_000           # 조절 가능
START = datetime.now() - timedelta(days=1)

def gen_ts(i):
    return (START + timedelta(seconds=i * 5)).isoformat()

def gen_env(db):
    c = db.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS sensor_env (
            ts TEXT, factory TEXT, sensor_id TEXT,
            temperature REAL, humidity REAL
        )
    """)
    for i in range(ROWS):
        c.execute(
            "INSERT INTO sensor_env VALUES (?,?,?,?,?)",
            (
                gen_ts(i), FACTORY,
                f"env-{i%10}",
                round(random.uniform(18, 32), 2),
                round(random.uniform(30, 70), 2),
            )
        )
    db.commit()

def gen_scale(db):
    c = db.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS sensor_scale (
            ts TEXT, factory TEXT, scale_id TEXT, weight REAL
        )
    """)
    for i in range(ROWS):
        c.execute(
            "INSERT INTO sensor_scale VALUES (?,?,?,?)",
            (
                gen_ts(i), FACTORY,
                f"scale-{i%3}",
                round(random.uniform(0, 50), 3)
            )
        )
    db.commit()

def gen_machine(db):
    c = db.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS machine (
            ts TEXT, factory TEXT, machine_id TEXT, state TEXT
        )
    """)
    states = ["IDLE", "RUNNING", "ERROR"]
    for i in range(ROWS):
        c.execute(
            "INSERT INTO machine VALUES (?,?,?,?)",
            (
                gen_ts(i), FACTORY,
                f"machine-{i%5}",
                random.choice(states)
            )
        )
    db.commit()

if __name__ == "__main__":
    BASE_DIR = "/mnt/nvme/onlog"
    
    sqlite3.connect(f"{BASE_DIR}/F02_sensor_env.sqlite").execute("PRAGMA journal_mode=WAL;")
    gen_env(sqlite3.connect(f"{BASE_DIR}/F02_sensor_env.sqlite"))

    sqlite3.connect(f"{BASE_DIR}/F02_sensor_scale.sqlite").execute("PRAGMA journal_mode=WAL;")
    gen_scale(sqlite3.connect(f"{BASE_DIR}/F02_sensor_scale.sqlite"))

    sqlite3.connect(f"{BASE_DIR}/F02_machine.sqlite").execute("PRAGMA journal_mode=WAL;")
    gen_machine(sqlite3.connect(f"{BASE_DIR}/F02_machine.sqlite"))

    print("Dummy DBs generated")
