import psycopg2
from pymongo import MongoClient
from datetime import datetime
import json
import os
import time
from dotenv import load_dotenv

load_dotenv()
STATE_FILE = "state.json"

def get_env(name, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise Exception(f"Env variable {name} is required")
    return value

def get_last_sync():
    if not os.path.exists(STATE_FILE):
        return "1970-01-01"
    with open(STATE_FILE) as f:
        return json.load(f)["last_sync"]

def save_last_sync(ts):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_sync": ts}, f)

PG_CONFIG = {
    "host": get_env("POSTGRES_HOST"),
    "port": get_env("POSTGRES_PORT"),
    "dbname": get_env("POSTGRES_DB"),
    "user": get_env("POSTGRES_USER"),
    "password": get_env("POSTGRES_PASSWORD"),
}

MONGO_URI = get_env("MONGO_URI")
MONGO_DB = get_env("MONGO_DB")
SYNC_INTERVAL = int(get_env("SYNC_INTERVAL", 300))

pg = psycopg2.connect(**PG_CONFIG)
mongo = MongoClient(MONGO_URI)
mdb = mongo[MONGO_DB]

def replicate():
    last_sync = get_last_sync()
    cur = pg.cursor()

    cur.execute("""
        SELECT id, name, email
        FROM customers
        WHERE created_at > %s
    """, (last_sync,))
    customers = cur.fetchall()

    cur.execute("""
        SELECT 
            o.id,
            o.customer_id,
            o.status,
            o.created_at,
            o.updated_at,
            o.deleted_at,
            p.id,
            p.name,
            op.amount
        FROM orders o
        JOIN order_products op ON op.order_id = o.id
        JOIN products p ON p.id = op.product_id
        WHERE o.updated_at > %s
           OR o.deleted_at IS NOT NULL
    """, (last_sync,))
    rows = cur.fetchall()

    for c in customers:
        mdb.customers.update_one(
            {"_id": c[0]},
            {
                "$setOnInsert": {
                    "_id": c[0],
                    "name": c[1],
                    "email": c[2],
                    "orders": [],
                    "synced_at": datetime.utcnow()
                }
            },
            upsert=True
        )

    orders_map = {}

    for r in rows:
        order_id = r[0]

        if order_id not in orders_map:
            orders_map[order_id] = {
                "order_id": order_id,
                "customer_id": r[1],
                "status": r[2],
                "placed_at": r[3],
                "updated_at": r[4],
                "deleted_at": r[5],
                "products": []
            }

        orders_map[order_id]["products"].append({
            "product_id": r[6],
            "name": r[7],
            "amount": float(r[8])
        })

    for order in orders_map.values():
        customer_id = order["customer_id"]

        if order["deleted_at"] is not None:
            mdb.customers.update_one(
                {"_id": customer_id},
                {
                    "$pull": {"orders": {"order_id": order["order_id"]}}
                }
            )
        else:
            mdb.customers.update_one(
                {"_id": customer_id},
                {
                    "$pull": {"orders": {"order_id": order["order_id"]}}
                }
            )
            mdb.customers.update_one(
                {"_id": customer_id},
                {
                    "$push": {"orders": order}
                }
            )

    now = datetime.utcnow().isoformat()
    save_last_sync(now)

    print(f"[{now}] synced {len(customers)} customers, {len(orders_map)} orders")


if __name__ == "__main__":
    while True:
        try:
            replicate()
        except Exception as e:
            print("ERROR:", e)

        time.sleep(SYNC_INTERVAL)