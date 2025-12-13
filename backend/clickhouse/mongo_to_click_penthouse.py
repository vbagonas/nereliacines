from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.decimal128 import Decimal128

# Your existing connectors
from backend.mongas.db import MongoDB
from backend.clickhouse.clickhouse import ClickHouseClient


class MongoToClickHouseNormalizedImporter:
    USER_COLS = ["user_id", "vardas", "pavarde", "gimimo_data", "tel_numeris", "miestas", "slaptazodis"]
    HOBBY_COLS = ["user_id", "pomegis"]

    EVENT_COLS = ["event_id", "pavadinimas", "miestas", "adresas", "vieta", "tipas", "data", "renginio_trukme", "amziaus_cenzas"]
    TICKET_TYPE_COLS = ["event_id", "bilieto_tipas_id", "kaina", "likutis"]

    ORDER_ITEM_COLS = ["order_id", "vartotojo_id", "uzsakymo_data", "renginys_id", "bilieto_tipas_id", "kiekis", "kaina"]

    def drop_all_tables(self) -> None:
        tables = [
            "uzsakymai_bilietai",
            "renginio_bilietu_tipai",
            "renginiai",
            "vartotojo_pomegiai",
            "vartotojai",
        ]

        for t in tables:
            self.ch.command(f"DROP TABLE IF EXISTS {self.ch_db}.{t}")

    def __init__(self, ch_db: str = "default", batch_size: int = 5000) -> None:
        self.mongo = MongoDB()
        self.ch = ClickHouseClient().client
        self.ch_db = ch_db
        self.batch_size = batch_size

    # ---------- conversions ----------
    def _dt64_3_utc(self, x: Any) -> Optional[datetime]:
        # handles datetime and {"$date": ...}
        if x is None:
            return None
        if isinstance(x, dict) and "$date" in x:
            x = x["$date"]
        if not isinstance(x, datetime):
            return None
        if x.tzinfo is None:
            x = x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc)

    def _to_decimal_2(self, x: Any) -> Decimal:
        """
        Convert Mongo Decimal128 / extended JSON decimal / numeric -> Decimal with 2 dp.
        Return Decimal for clickhouse_connect insert.
        """
        if x is None:
            return Decimal("0.00")
        if isinstance(x, Decimal128):
            return Decimal(str(x.to_decimal())).quantize(Decimal("0.01"))
        if isinstance(x, dict) and "$numberDecimal" in x:
            return Decimal(str(x["$numberDecimal"])).quantize(Decimal("0.01"))
        return Decimal(str(x)).quantize(Decimal("0.01"))

    def _id_str(self, x: Any) -> str:
        if isinstance(x, ObjectId):
            return str(x)
        if isinstance(x, dict) and "$oid" in x:
            return str(x["$oid"])
        return str(x) if x is not None else ""

    # ---------- DDL ----------
    def create_schema(self) -> None:
        self.ch.command(f"CREATE DATABASE IF NOT EXISTS {self.ch_db}")

        self.ch.command(f"""
        CREATE TABLE IF NOT EXISTS {self.ch_db}.vartotojai (
          user_id String,
          vardas String,
          pavarde String,
          gimimo_data Nullable(DateTime64(3, 'UTC')),
          tel_numeris String,
          miestas String,
          slaptazodis String,
          ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (user_id)
        """)

        # User hobbies (one row per hobby)
        self.ch.command(f"""
        CREATE TABLE IF NOT EXISTS {self.ch_db}.vartotojo_pomegiai (
          user_id String,
          pomegis String,
          ingested_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY (user_id, pomegis)
        """)

        self.ch.command(f"""
        CREATE TABLE IF NOT EXISTS {self.ch_db}.renginiai (
          event_id String,
          pavadinimas String,
          miestas String,
          adresas String,
          vieta String,
          tipas String,
          data DateTime64(3, 'UTC') DEFAULT toDateTime64('1970-01-01 00:00:00', 3, 'UTC'),
          renginio_trukme UInt32,
          amziaus_cenzas String,
          ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (data, event_id)
        """)

        self.ch.command(f"""
        CREATE TABLE IF NOT EXISTS {self.ch_db}.renginio_bilietu_tipai (
          event_id String,
          bilieto_tipas_id String,
          kaina Decimal(12,2),
          likutis Int32,
          ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (event_id, bilieto_tipas_id)
        """)

        self.ch.command(f"""
        CREATE TABLE IF NOT EXISTS {self.ch_db}.uzsakymai_bilietai (
          order_id String,
          vartotojo_id String,
          uzsakymo_data DateTime64(3, 'UTC') DEFAULT toDateTime64('1970-01-01 00:00:00', 3, 'UTC'),
          renginys_id String,
          bilieto_tipas_id String,
          kiekis Int32,
          kaina Decimal(12,2),
          ingested_at DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (uzsakymo_data, order_id, renginys_id, bilieto_tipas_id)
        """)

    def truncate_all(self) -> None:
        for t in ["uzsakymai_bilietai", "renginio_bilietu_tipai", "renginiai", "vartotojai"]:
            self.ch.command(f"TRUNCATE TABLE IF EXISTS {self.ch_db}.{t}")

    # ---------- import helpers ----------
    def _insert(self, table: str, rows: List[Dict[str, Any]], cols: List[str]) -> None:
        if not rows:
            return

        data = [[r.get(c) for c in cols] for r in rows]

        self.ch.insert(
            f"{self.ch_db}.{table}",
            data,
            column_names=cols
        )

    # ---------- imports -------


    def import_users_and_hobbies(self) -> None:
        print("Importing users + hobbies...")

        user_buf: List[Dict[str, Any]] = []
        hobby_buf: List[Dict[str, Any]] = []

        for u in self.mongo.vartotojai.find():
            user_id = self._id_str(u.get("_id"))

            # user row
            user_buf.append({
                "user_id": user_id,
                "vardas": str(u.get("Vardas", "")),
                "pavarde": str(u.get("Pavarde", "")),
                "gimimo_data": self._dt64_3_utc(u.get("Gimimo_data")),
                "tel_numeris": str(u.get("Tel_numeris", "")),
                "miestas": str(u.get("Miestas", "")),
                "slaptazodis": str(u.get("Slaptazodis", "")),
            })

            # hobbies rows
            p = u.get("Pomegiai", {})
            if isinstance(p, dict):
                hobbies = list(p.values())
            elif isinstance(p, list):
                hobbies = p
            else:
                hobbies = []

            for h in hobbies:
                hobby_buf.append({"user_id": user_id, "pomegis": str(h)})

            # flush
            if len(user_buf) >= self.batch_size:
                self._insert("vartotojai", user_buf, self.USER_COLS)
                user_buf.clear()

            if len(hobby_buf) >= self.batch_size:
                self._insert("vartotojo_pomegiai", hobby_buf, self.HOBBY_COLS)
                hobby_buf.clear()

        # final flush
        self._insert("vartotojai", user_buf, self.USER_COLS)
        self._insert("vartotojo_pomegiai", hobby_buf, self.HOBBY_COLS)

    def import_events_and_ticket_types(self) -> None:
        print("Importing events + ticket types...")

        ev_buf: List[Dict[str, Any]] = []
        tt_buf: List[Dict[str, Any]] = []

        for e in self.mongo.renginiai.find():
            event_id = self._id_str(e.get("_id"))

            ev_buf.append({
                "event_id": event_id,
                "pavadinimas": str(e.get("Pavadinimas", "")),
                "miestas": str(e.get("Miestas", "")),
                "adresas": str(e.get("Adresas", "")),
                "vieta": str(e.get("Vieta", "")),
                "tipas": str(e.get("Tipas", "")),
                "data": self._dt64_3_utc(e.get("Data")),
                "renginio_trukme": int(e.get("Renginio_trukme", 0) or 0),
                "amziaus_cenzas": str(e.get("Amziaus_cenzas", "")),
            })

            for b in e.get("Bilieto_tipas", []) or []:
                tt_buf.append({
                    "event_id": event_id,
                    "bilieto_tipas_id": str(b.get("Bilieto_tipas_id", "")),
                    "kaina": self._to_decimal_2(b.get("Kaina")),
                    "likutis": int(b.get("Likutis", 0) or 0),
                })

            if len(ev_buf) >= self.batch_size:
                self._insert("renginiai", ev_buf, self.EVENT_COLS)
                ev_buf.clear()

            if len(tt_buf) >= self.batch_size:
                self._insert("renginio_bilietu_tipai", tt_buf, self.TICKET_TYPE_COLS)
                tt_buf.clear()

        self._insert("renginiai", ev_buf, self.EVENT_COLS)
        self._insert("renginio_bilietu_tipai", tt_buf, self.TICKET_TYPE_COLS)

    def import_order_items_only(self) -> None:
        print("Importing order line items (uzsakymai_bilietai) ...")

        it_buf: List[Dict[str, Any]] = []

        cursor = getattr(self.mongo, "uzsakymai", None)
        if cursor is None:
            raise AttributeError("MongoDB() does not expose .uzsakymai collection. Update MongoDB wrapper mapping.")

        for o in cursor.find():
            order_id = self._id_str(o.get("_id"))
            vartotojo_id = str(o.get("vartotojo_id", ""))
            uzsakymo_data = self._dt64_3_utc(o.get("uzsakymo_data"))

            for b in o.get("Bilietai", []) or []:
                it_buf.append({
                    "order_id": order_id,
                    "vartotojo_id": vartotojo_id,
                    "uzsakymo_data": uzsakymo_data,
                    "renginys_id": str(b.get("renginys_id", "")),
                    "bilieto_tipas_id": str(b.get("Bilieto_tipas_id", "")),
                    "kiekis": int(b.get("Kiekis", 1) or 1),
                    "kaina": self._to_decimal_2(b.get("Kaina")),
                })

                if len(it_buf) >= self.batch_size:
                    self._insert("uzsakymai_bilietai", it_buf, self.ORDER_ITEM_COLS)
                    it_buf.clear()

        self._insert("uzsakymai_bilietai", it_buf, self.ORDER_ITEM_COLS)

    # -------------------------
    # Run
    # -------------------------
    def run(self, rebuild: bool = False) -> None:
        print("ClickHouse import started")
        if rebuild:
            print("Rebuild requested: dropping all tables")
            self.drop_all_tables()
        self.create_schema()
        self.import_users_and_hobbies()
        self.import_events_and_ticket_types()
        self.import_order_items_only()

        print("Import complete!")

# Example usage:
if __name__ == "__main__":
    importer = MongoToClickHouseNormalizedImporter(ch_db="default", batch_size=5000)
    importer.run(rebuild=True)
    