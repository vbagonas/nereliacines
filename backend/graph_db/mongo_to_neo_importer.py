from bson.decimal128 import Decimal128
from backend.mongas.db import MongoDB
from backend.graph_db.graph import GraphDB

class MongoToNeoImporter:
    def __init__(self):
        self.mongo = MongoDB()
        self.neo = GraphDB()

    def convert_price(self, price):
        if isinstance(price, Decimal128):
            return float(price.to_decimal())
        if isinstance(price, dict) and "$numberDecimal" in price:
            return float(price["$numberDecimal"])
        return float(price)

    def import_users(self):
        print("Importing users...")
        for u in self.mongo.vartotojai.find():
            self.neo.session.run("""
                MERGE (u:User {id: $id})
                SET u.vardas = $vardas, u.pavarde = $pavarde, u.miestas = $miestas
            """, {
                "id": u["_id"],
                "vardas": u.get("Vardas", ""),
                "pavarde": u.get("Pavarde", ""),
                "miestas": u.get("Miestas", "")
            })

    def import_events(self):
        print("Importing events...")
        for e in self.mongo.renginiai.find():
            self.neo.session.run("""
                MERGE (ev:Event {id: $id})
                SET ev.pavadinimas = $pavadinimas, ev.miestas = $miestas, ev.tipas = $tipas
            """, {
                "id": e["_id"],
                "pavadinimas": e.get("Pavadinimas", ""),
                "miestas": e.get("Miestas", ""),
                "tipas": e.get("Tipas", "")
            })

    def import_orders(self):
        print("Importing orders...")
        for o in self.mongo.uzsakymai.find():
            user_id = o.get("vartotojo_id")

            # Užtikrinti, kad vartotojas node egzistuoja
            self.neo.session.run("""
                MERGE (u:User {id: $uid})
            """, {"uid": user_id})

            for b in o.get("Bilietai", []):
                event_id = b["renginys_id"]
                kaina = self.convert_price(b["Kaina"])
                kiekis = b.get("Kiekis", 1)
                tipas = b["Bilieto_tipas_id"]

                # Užtikrinti, kad event node egzistuoja
                self.neo.session.run("""
                    MERGE (e:Event {id: $eid})
                """, {"eid": event_id})

                # Sukurti BOUGHT relationship
                self.neo.session.run("""
                    MATCH (u:User {id: $uid})
                    MATCH (e:Event {id: $eid})
                    MERGE (u)-[r:BOUGHT {
                        kiekis: $kiekis,
                        kaina: $kaina,
                        bilieto_tipas: $tipas
                    }]->(e)
                """, {
                    "uid": user_id,
                    "eid": event_id,
                    "kiekis": kiekis,
                    "kaina": kaina,
                    "tipas": tipas
                })

    def run(self):
        print("Import started")
        self.neo.session.run("MATCH (n) DETACH DELETE n")  # visada išvalome grafą
        self.import_users()
        self.import_events()
        self.import_orders()
        print("Import complete!")
