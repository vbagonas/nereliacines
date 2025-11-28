from bson.decimal128 import Decimal128
from backend.mongas.db import MongoDB
from backend.graph_db.graph import GraphDB
from datetime import datetime

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

            # konvertuojam gimimo datą
            gimimo_data = None
            if isinstance(u.get("Gimimo_data"), dict) and "$date" in u["Gimimo_data"]:
                gimimo_data = u["Gimimo_data"]["$date"]

            # telefono nr.
            tel = None
            if isinstance(u.get("Tel_numeris"), dict) and "$numberLong" in u["Tel_numeris"]:
                tel = u["Tel_numeris"]["$numberLong"]

            p = u.get("Pomegiai", [])

            if isinstance(p, dict):
                pomegiai = list(p.values())
            elif isinstance(p, list):
                pomegiai = p
            else:
                pomegiai = []


            self.neo.session.run("""
                MERGE (u:User {id: $id})
                SET u.vardas = $vardas,
                    u.pavarde = $pavarde,
                    u.miestas = $miestas,
                    u.tel = $tel,
                    u.gimimo_data = $gimimo_data,
                    u.pomegiai = $pomegiai
            """, {
                "id": u["_id"],
                "vardas": u.get("Vardas", ""),
                "pavarde": u.get("Pavarde", ""),
                "miestas": u.get("Miestas", ""),
                "tel": tel,
                "gimimo_data": gimimo_data,
                "pomegiai": pomegiai
            })


    def import_events(self):
        print("Importing events...")

        for e in self.mongo.renginiai.find():

            import json

            # konvertuojam datą
            data = None
            raw_data = e.get("Data")
            if isinstance(raw_data, dict) and "$date" in raw_data:
                data = raw_data["$date"]  # senas JSON style
            elif isinstance(raw_data, datetime):
                data = raw_data  # tikras datetime objektas

            # konvertuojam bilietų tipus -> JSON string
            bilietu_tipai = []
            for b in e.get("Bilieto_tipas", []):
                bilietu_tipai.append({
                    "Bilieto_tipas_id": b.get("Bilieto_tipas_id"),
                    "Kaina": float(b.get("Kaina", 0)),
                    "Likutis": int(b.get("Likutis", 0))
                })
            bilietu_tipai = json.dumps(bilietu_tipai)

            # organizatoriai -> JSON string
            organizatoriai = json.dumps(e.get("Organizatorius", []))

            self.neo.session.run("""
            MERGE (ev:Event {id: $id})
            SET ev.pavadinimas = $pavadinimas,
                ev.miestas = $miestas,
                ev.adresas = $adresas,
                ev.vieta = $vieta,
                ev.tipas = $tipas,
                ev.data = datetime($data),
                ev.renginio_trukme = $renginio_trukme,
                ev.amziaus_cenzas = $amziaus_cenzas,
                ev.bilieto_tipai = $bilieto_tipai,
                ev.organizatoriai = $organizatoriai
            """, {
                "id": e["_id"],
                "pavadinimas": e.get("Pavadinimas", ""),
                "miestas": e.get("Miestas", ""),
                "adresas": e.get("Adresas", ""),
                "vieta": e.get("Vieta", ""),
                "tipas": e.get("Tipas", ""),
                "data": data.isoformat() if data else None,
                "renginio_trukme": e.get("Renginio_trukme"),
                "amziaus_cenzas": e.get("Amziaus_cenzas"),
                "bilieto_tipai": bilietu_tipai,
                "organizatoriai": organizatoriai
            })


    def import_orders(self):
        print("Importing orders...")

        for o in self.mongo.uzsakymai.find():
            user_id = o.get("vartotojo_id")

            # konvertuojam užsakymo datą
            uzsakymo_data = None
            if isinstance(o.get("uzsakymo_data"), dict) and "$date" in o["uzsakymo_data"]:
                uzsakymo_data = o["uzsakymo_data"]["$date"]

            uzsakymo_id = str(o["_id"]["$oid"]) if isinstance(o["_id"], dict) else str(o["_id"])

            # užtikrinam, kad user node egzistuoja
            self.neo.session.run("MERGE (u:User {id: $uid})", {"uid": user_id})

            # einam per bilietus
            for index, b in enumerate(o.get("Bilietai", [])):

                event_id = b["renginys_id"]
                kaina = self.convert_price(b["Kaina"])
                kiekis = b.get("Kiekis", 1)
                tipas = b.get("Bilieto_tipas_id")

                # generate unique relationship ID
                order_item_id = f"{uzsakymo_id}-{index}"

                # užtikrinam event node
                self.neo.session.run("MERGE (e:Event {id: $eid})", {"eid": event_id})

                # sukurti relationship su pilna info
                self.neo.session.run("""
                    MATCH (u:User {id: $uid})
                    MATCH (e:Event {id: $eid})
                    MERGE (u)-[r:BOUGHT {
                        order_item_id: $order_item_id
                    }]->(e)
                    SET r.kiekis = $kiekis,
                        r.kaina = $kaina,
                        r.bilieto_tipas = $tipas,
                        r.uzsakymo_id = $uzsakymo_id,
                        r.uzsakymo_data = $uzsakymo_data
                """, {
                    "uid": user_id,
                    "eid": event_id,
                    "order_item_id": order_item_id,
                    "kiekis": kiekis,
                    "kaina": kaina,
                    "tipas": tipas,
                    "uzsakymo_id": uzsakymo_id,
                    "uzsakymo_data": uzsakymo_data
                })


    def run(self):
        print("Import started")
        self.neo.session.run("MATCH (n) DETACH DELETE n")  # visada išvalome grafą
        self.import_users()
        self.import_events()
        self.import_orders()
        print("Import complete!")
