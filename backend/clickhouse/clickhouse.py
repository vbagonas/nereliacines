import clickhouse_connect
from pathlib import Path
import yaml 
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class ClickHouseClient():
    def __init__(self) -> None:
        self.client = self.connect()
        self.ch_db = "default"  # Database name

    def connect(self):
        with open(PROJECT_ROOT / 'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('clickhouse_user', data)

        client = clickhouse_connect.get_client(
            host='ygwf2e1uyd.eu-central-1.aws.clickhouse.cloud',
            user=creds['username'],
            password=creds['password'],
            secure=True
        )

        return client
    
    # ============================================
    # HELPER FUNCTIONS (for data conversion)
    # ============================================
    
    def _dt64_3_utc(self, x: Any) -> Optional[datetime]:
        """Convert various date formats to UTC datetime"""
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
        """Convert to Decimal with 2 decimal places"""
        if x is None:
            return Decimal("0.00")
        return Decimal(str(x)).quantize(Decimal("0.01"))
    
    def _id_str(self, x: Any) -> str:
        """Convert ObjectId to string"""
        if isinstance(x, dict) and "$oid" in x:
            return str(x["$oid"])
        return str(x) if x is not None else ""
    
    # ============================================
    # REAL-TIME SYNC FUNCTIONS (NEW)
    # ============================================
    
    def sync_user(self, user_doc: Dict) -> None:
        """
        Sync a single user to ClickHouse.
        Called after user registration in MongoDB.
        
        Args:
            user_doc: User document from MongoDB
        """
        print(f"📤 Syncing user to ClickHouse: {user_doc.get('_id')}")
        
        user_id = self._id_str(user_doc.get("_id"))
        
        # 1. Insert/update user row
        user_data = [[
            user_id,
            str(user_doc.get("Vardas", "")),
            str(user_doc.get("Pavarde", "")),
            self._dt64_3_utc(user_doc.get("Gimimo_data")),
            str(user_doc.get("Tel_numeris", "")),
            str(user_doc.get("Miestas", "")),
            str(user_doc.get("Slaptazodis", ""))
        ]]
        
        self.client.insert(
            f"{self.ch_db}.vartotojai",
            user_data,
            column_names=["user_id", "vardas", "pavarde", "gimimo_data", 
                         "tel_numeris", "miestas", "slaptazodis"]
        )
        
        # 2. Insert user hobbies
        pomegiai = user_doc.get("Pomegiai", [])
        if isinstance(pomegiai, dict):
            pomegiai = list(pomegiai.values())
        elif not isinstance(pomegiai, list):
            pomegiai = []
        
        if pomegiai:
            hobby_data = [[user_id, str(h)] for h in pomegiai]
            self.client.insert(
                f"{self.ch_db}.vartotojo_pomegiai",
                hobby_data,
                column_names=["user_id", "pomegis"]
            )
        
        print(f"✅ User synced: {user_id} with {len(pomegiai)} hobbies")
    
    def sync_order_item(self, order_doc: Dict) -> None:
        """
        Sync order items (tickets) to ClickHouse.
        Called after successful purchase in MongoDB.
        
        Args:
            order_doc: Order document from MongoDB
        """
        order_id = self._id_str(order_doc.get("_id"))
        vartotojo_id = str(order_doc.get("vartotojo_id", ""))
        uzsakymo_data = self._dt64_3_utc(order_doc.get("uzsakymo_data"))
        
        print(f"📤 Syncing order to ClickHouse: {order_id}")
        
        # Process each ticket in the order
        order_items = []
        for bilietas in order_doc.get("Bilietai", []):
            order_items.append([
                order_id,
                vartotojo_id,
                uzsakymo_data,
                str(bilietas.get("renginys_id", "")),
                str(bilietas.get("Bilieto_tipas_id", "")),
                int(bilietas.get("Kiekis", 1)),
                self._to_decimal_2(bilietas.get("Kaina"))
            ])
        
        if order_items:
            self.client.insert(
                f"{self.ch_db}.uzsakymai_bilietai",
                order_items,
                column_names=["order_id", "vartotojo_id", "uzsakymo_data",
                            "renginys_id", "bilieto_tipas_id", "kiekis", "kaina"]
            )
            print(f"✅ Order synced: {order_id} with {len(order_items)} items")
    
    def update_ticket_inventory(self, event_id: str, bilieto_tipas_id: str, new_likutis: int) -> None:
        """
        Update ticket inventory in ClickHouse.
        Called after purchase updates ticket count in MongoDB.
        
        Args:
            event_id: Event ID
            bilieto_tipas_id: Ticket type ID
            new_likutis: New remaining ticket count
        """
        print(f"📤 Updating ticket inventory in ClickHouse: {event_id}/{bilieto_tipas_id} → {new_likutis}")
        
        # ClickHouse uses ALTER TABLE UPDATE for mutations
        query = f"""
            ALTER TABLE {self.ch_db}.renginio_bilietu_tipai
            UPDATE likutis = {new_likutis}
            WHERE event_id = '{event_id}' AND bilieto_tipas_id = '{bilieto_tipas_id}'
        """
        
        self.client.command(query)
        print(f"✅ Ticket inventory updated")