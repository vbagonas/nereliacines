from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timezone
from bson import Decimal128
from decimal import Decimal, InvalidOperation
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
import bcrypt

from backend.mongas.db import MongoDB
from backend.redysas.ops import RedisClient

class EventApp:
    def __init__(self, port=8080):
        self.db = MongoDB()
        self.redis = RedisClient()
        self.port = port
        self.app = Flask(__name__)
        CORS(self.app)
        self._register_routes()

    # --------------------------
    # Health
    # --------------------------
    def _register_routes(self):
        app = self.app

        @app.get("/_health")
        def health():
            try:
                self.db.client.admin.command("ping")
                return jsonify({
                    "ok": True,
                    "time": datetime.now(timezone.utc).isoformat() + "Z",
                    "db": "connected"
                })
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)}), 500

        # ----------------------
        # Login service
        # ----------------------
        @app.post("/api/v1/login")
        def login():
            body = request.get_json(force=True)
            email = body.get("email")
            password = body.get("password")

            user = self.db.vartotojai.find_one({"_id": email})
            if not user or not self.verify_password(password, user.get("Slaptazodis", "")):
                return app.response_class(
                    dumps({"ok": False, "error": "Invalid credentials"}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json",
                    status=401
                )

            public = {k: v for k, v in user.items() if k != "Slaptazodis"}
            return app.response_class(
                dumps({"ok": True, "user": public}, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )

        # ----------------------
        # Register service
        # ----------------------
        @app.post("/api/v1/register")
        def register_user():
            payload = request.get_json(force=True)
            email = payload.get("email")
            if not email:
                return app.response_class(
                    dumps({"ok": False, "error": "Missing user id (email)."}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=400
                )

            slaptazodis = payload.get("slaptazodis")
            if not slaptazodis:
                return app.response_class(
                    dumps({"ok": False, "error": "Password is required"}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=400
                )

            hashed_password = self.hash_password(slaptazodis)
            if self.db.vartotojai.find_one({"_id": email}):
                return app.response_class(
                    dumps({"ok": False, "error": "User already exists."}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=409
                )

            user_doc = {
                "_id": email,
                "Vardas": payload.get("vardas"),
                "Pavarde": payload.get("pavarde"),
                "Gimimo_data": payload.get("gimimo_data"),
                "Tel_numeris": payload.get("tel_numeris"),
                "Miestas": payload.get("miestas"),
                "Pomegiai": payload.get("pomegiai", []) or [],
                "Slaptazodis": hashed_password,
            }

            self.db.vartotojai.insert_one(user_doc)
            public_user = {k: v for k, v in user_doc.items() if k != "Slaptazodis"}

            return app.response_class(
                dumps({"ok": True, "user": public_user}, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )

        # ----------------------
        # Purchase service
        # ----------------------
        @app.post("/api/v1/purchase")
        def purchase():
            payload = request.get_json(force=True)
            vartotojo_id = payload.get("vartotojo_id")
            renginys_id = payload.get("renginys_id")
            bilieto_tipas_id = payload.get("bilieto_tipas_id")
            kiekis = int(payload.get("kiekis", 1))
            kaina_hint = payload.get("kaina") or payload.get("kaina_hint")
            idx_hint = payload.get("idx")

            if not (vartotojo_id and renginys_id and kiekis > 0):
                return app.response_class(
                    dumps({"ok": False, "error": "Provide vartotojo_id, renginys_id, and kiekis>0."},
                          json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=400
                )

            if not self.db.vartotojai.find_one({"_id": vartotojo_id}):
                return app.response_class(
                    dumps({"ok": False, "error": "User not found. Register first."},
                          json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=404
                )

            with self.db.client.start_session() as s:
                with s.start_transaction():
                    cache_key = f"event:{renginys_id}"
                    ev = self.redis.get_cache(cache_key)
                    if not ev:
                        ev = self.db.renginiai.find_one({"_id": renginys_id}, session=s)
                        if not ev:
                            return app.response_class(
                                dumps({"ok": False, "error": "Event not found."}, json_options=RELAXED_JSON_OPTIONS),
                                mimetype="application/json", status=404
                                )
                        
                    tickets = ev.get("Bilieto_tipas") or []
                    if not tickets:
                        return app.response_class(
                            dumps({"ok": False, "error": "This event has no ticket types."}, json_options=RELAXED_JSON_OPTIONS),
                            mimetype="application/json", status=404
                        )

                    idx = self._resolve_ticket_index(tickets, bilieto_tipas_id, kaina_hint, idx_hint)
                    if idx is None:
                        return app.response_class(
                            dumps({"ok": False, "error": "Ticket type not found."}, json_options=RELAXED_JSON_OPTIONS),
                            mimetype="application/json", status=404
                        )

                    chosen = tickets[idx]
                    bilieto_tipas_id = chosen.get("Bilieto_tipas_id")
                    kaina_dec128 = chosen.get("Kaina")
                    likutis = int(chosen.get("Likutis", 0))

                    if kiekis > likutis:
                        return app.response_class(
                            dumps({"ok": False, "error": f"Not enough tickets. Remainder: {likutis}"},
                                  json_options=RELAXED_JSON_OPTIONS),
                            mimetype="application/json", status=400
                        )

                    res = self.db.renginiai.update_one(
                        {"_id": renginys_id,
                         "Bilieto_tipas": {"$elemMatch": {"Bilieto_tipas_id": bilieto_tipas_id, "Likutis": {"$gte": kiekis}}}},
                        {"$inc": {"Bilieto_tipas.$.Likutis": -kiekis}},
                        session=s
                    )
                    if res.modified_count != 1:
                        return app.response_class(
                            dumps({"ok": False, "error": "Concurrent update issue."}, json_options=RELAXED_JSON_OPTIONS),
                            mimetype="application/json", status=409
                        )

                    order = {
                        "vartotojo_id": vartotojo_id,
                        "uzsakymo_data": datetime.now(timezone.utc),
                        "Bilietai": [{
                            "renginys_id": renginys_id,
                            "Bilieto_tipas_id": bilieto_tipas_id,
                            "Kiekis": kiekis,
                            "Kaina": kaina_dec128
                        }]
                    }
                    ins = self.db.uzsakymai.insert_one(order, session=s)
                    self.redis.invalidate_cache(cache_key)

            return app.response_class(
                dumps({"ok": True, "message": "Purchase successful.", "order_id": str(ins.inserted_id), "order": order},
                      json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )
        
        # ---------------------
        # Reading events
        # ---------------------
        
        @app.get("/api/v1/events/<renginys_id>")
        def read_event(renginys_id):
            cache_key = f"event:{renginys_id}"
    
            # Patikrinam Redis cache
            cached = self.redis.get_cache(cache_key)
            if cached:
                print(f"Loaded event {renginys_id} from Redis")
                return app.response_class(
                    dumps({"cached": True, "event": cached}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json"
                )

            # Jei nėra cache, paimame iš Mongo
            event = self.db.renginiai.find_one({"_id": renginys_id})
            if not event:
                return app.response_class(
                    dumps({"ok": False, "error": "Event not found"}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json",
                    status=404
                )

            # Įdedam į cache 
            self.redis.set_cache(cache_key, event)
            print(f"Loaded event {renginys_id} from Mongo and cached in Redis")

            return app.response_class(
                dumps({"cached": False, "event": event}, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )
        
        # ----------------------
        # Analytics endpoints
        # ----------------------
        @app.get("/api/v1/analytics/vilnius-events")
        def vilnius_events():
            pipeline = [
                {"$match": {"Miestas": {"$regex": r"^\s*Vilnius\s*$", "$options": "i"}}},
                {"$sort": {"Data": 1}},
                {"$project": {"_id": 1, "Pavadinimas": 1, "Data": 1, "Bilieto_tipas": 1}},
                {"$group": {"_id": None, "data": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
                {"$project": {"_id": 0, "count": 1, "data": 1}}
            ]
            doc = next(self.db.renginiai.aggregate(pipeline), {"count": 0, "data": []})
            return app.response_class(
                dumps(doc, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )
        
        @app.get("/api/v1/analytics/top3-by-tickets")
        def top3_by_tickets():
            cache_key = "analytics:top3_events"
    
        # Try Redis cache first
            cached = self.redis.get_cache(cache_key)
            if cached:
                print("Returning top3 events from Redis cache")
                return app.response_class(
                    dumps(cached, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json")

            print("Computing aggregation in MongoDB...")
            pipeline = [
                {"$unwind": {"path": "$Bilietai", "preserveNullAndEmptyArrays": False}},
                {"$group": {
                    "_id": "$Bilietai.renginys_id",
                    "tickets_sold": {"$sum": {"$toInt": {"$ifNull": ["$Bilietai.Kiekis", 0]}}}
                }},

                {"$sort": {"tickets_sold": -1}},
                {"$limit": 3},

                {"$lookup": {
                    "from": "Renginiai",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "event"
                }},
                {"$unwind": "$event"},
                {"$project": {
                    "_id": 0,
                    "renginys_id": "$_id",
                    "tickets_sold": 1,
                    "Pavadinimas": "$event.Pavadinimas",
                    "Data": "$event.Data",
                    "Miestas": "$event.Miestas",
                    "Vieta": "$event.Vieta",
                    "Tipas": "$event.Tipas"
                }},

                {"$group": {
                    "_id": None,
                    "data": {"$push": "$$ROOT"},
                    "count": {"$sum": 1}}},
                {"$project": {"_id": 0, "count": 1, "data": 1}}
            ]


            doc = next(self.db.uzsakymai.aggregate(pipeline), {"count": 0, "data": []})
    
            # Cache result for 10 minutes (600 seconds)
            self.redis.set_cache(cache_key, doc, ttl=600)
    
            return app.response_class(
                dumps(doc, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )


    # ----------------------
    # Utility methods
    # ----------------------
    @staticmethod
    def hash_password(password):
        salt = bcrypt.gensalt(rounds=12)
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed.decode("utf-8")

    @staticmethod
    def verify_password(plain, hashed):
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))

    @staticmethod
    def _resolve_ticket_index(tickets, bilieto_tipas_id, kaina_hint, idx_hint):
        if bilieto_tipas_id:
            for i, t in enumerate(tickets):
                if t.get("Bilieto_tipas_id") == bilieto_tipas_id:
                    return i
        if kaina_hint is not None:
            try:
                hint_dec = Decimal(str(kaina_hint))
                for i, t in enumerate(tickets):
                    k = t.get("Kaina")
                    if isinstance(k, Decimal128) and k.to_decimal() == hint_dec:
                        return i
            except (InvalidOperation, ValueError):
                pass
        if idx_hint is not None:
            try:
                idx_hint = int(idx_hint)
                if 0 <= idx_hint < len(tickets):
                    return idx_hint
            except (TypeError, ValueError):
                pass
        if len(tickets) == 1:
            return 0
        return None

    # ----------------------
    # Run
    # ----------------------
    def run(self):
        self.app.run(host="0.0.0.0", port=self.port, debug=True)

if __name__=="__main__":
    app = EventApp()
    app.run()