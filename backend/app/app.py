from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timezone
from bson import Decimal128
from decimal import Decimal, InvalidOperation
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
import bcrypt
from datetime import datetime, timezone, date
import json
from flask.json.provider import DefaultJSONProvider
from flask.json.provider import DefaultJSONProvider
from datetime import datetime
from cassandra.util import Date
import uuid
from uuid import UUID


from backend.mongas.db import MongoDB
from backend.redysas.ops import RedisClient

# ----------------------
# Cart (Redis) constants
# ----------------------
CART_TTL = 30  # 30 sec

def cart_key(owner_id: str) -> str:
    return f"cart:{owner_id}"

class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, Date):
            return str(obj)  # paprastas ir patikimas sprendimas
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)
    
    def dumps(self, obj, **kwargs):
        # ensure_ascii=False leis rodyti lietuviškas raides tiesiogiai
        kwargs.setdefault("ensure_ascii", False)
        return super().dumps(obj, **kwargs)
        
class EventApp:
    def __init__(self, port=8080):
        self.db = MongoDB()
        self.redis = RedisClient()
        self.kasandre = KasandrManager()
        self.port = port
        self.app = Flask(__name__)
        self.app.json_provider_class = CustomJSONProvider
        self.app.json = self.app.json_provider_class(self.app)
        CORS(self.app)
        self._register_routes()

    # --------------------------
    # Health
    # --------------------------
    def _register_routes(self):
        app = self.app
        kasandre = self.kasandre

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
                    print(cache_key)
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

                    print(tickets)
                    for ticket_type in tickets:
                        print(ticket_type.get("Bilieto_tipas_id"), bilieto_tipas_id)
                        if ticket_type.get("Bilieto_tipas_id") == bilieto_tipas_id:
                            chosen = ticket_type
                            break

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
                            "Kaina": chosen.get("Kaina")
                        }]
                    }
                    ins = self.db.uzsakymai.insert_one(order, session=s)
                    
                    # Aktyvi invalidacija
                    self.redis.invalidate_cache(cache_key)

                    # Patikrinam ar liko bilietų → jei ne, pašalinam iš valid_events
                    updated_event = self.db.renginiai.find_one({"_id": renginys_id})
                    tickets = updated_event.get("Bilieto_tipas", [])
                    has_available = any(int(t.get("Likutis", 0)) > 0 for t in tickets)

                    valid_ids = self.redis.get_cache("valid_events") or []
                    if not has_available:
                        valid_ids = [eid for eid in valid_ids if eid != str(renginys_id)]
                        self.redis.set_cache("valid_events", valid_ids)


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
        
        @app.get("/api/v1/events")
        def read_all_events():
        # Patikrinam globalų valid_events raktą
            valid_ids = self.redis.get_cache("valid_events")
            
            if valid_ids:
                print("Loaded valid event IDs from Redis")
                return app.response_class(
                    dumps({"cached": True, "event_ids": valid_ids}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json"
                )

        # Jei nėra cache, paimam visus renginius iš Mongo
            print("Loading valid events from MongoDB...")
            events = list(self.db.renginiai.find({}))

            valid_ids = []
            now = datetime.now()

            for ev in events:
            # Tikrinam ar renginys turi bilietų likutį > 0
                tickets = ev.get("Bilieto_tipas", [])
                has_available = any(int(t.get("Likutis", 0)) > 0 for t in tickets)

                event_date = ev.get("Data")
                if isinstance(event_date, str):
                # jei data Mongo kaip string, konvertuojam į datetime
                    event_date = datetime.fromisoformat(event_date.replace("Z", "+00:00"))

                print(event_date, now)
                not_past = event_date > now
                
                if has_available and not_past:
                    event_id_str = str(ev["_id"])
                    valid_ids.append(event_id_str)

                    # Kuriam atskirą raktą kiekvienam renginiui
                    self.redis.set_cache(f"event:{event_id_str}", ev)
                    print(f"Cached event {event_id_str}")

            # Įrašom globalų sąrašą į cache
            self.redis.set_cache("valid_events", valid_ids)
            print("✅ Cached list of valid event IDs")

            # Grąžinam rezultatą
            return app.response_class(
                dumps({"cached": False, "event_ids": valid_ids}, json_options=RELAXED_JSON_OPTIONS),
                mimetype="application/json"
            )

        # ----------------------
        # Cart API (Redis Hash)
        # ----------------------
        @app.get("/api/v1/cart")
        def cart_get():
            owner_id = (request.args.get("owner_id") or "").strip()
            if not owner_id:
                return jsonify({"ok": False, "error": "owner_id is required"}), 400

            items = self.redis.client.hgetall(cart_key(owner_id)) or {}
            return jsonify({"ok": True, "items": items})

        @app.post("/api/v1/cart")
        def cart_add():
            """
            Body: { "owner_id": "...", "product_id": "...", "qty": 1 }
            Increments qty (creates if absent). Removes if result <= 0.
            """
            data = request.get_json(force=True)
            owner_id = (data.get("owner_id") or "").strip()
            product_id = str(data.get("product_id") or "").strip()
            qty = int(data.get("qty", 1))

            if not owner_id or not product_id or qty == 0:
                return jsonify({"ok": False, "error": "owner_id, product_id and non-zero qty are required"}), 400

            key = cart_key(owner_id)
            current = int(self.redis.client.hget(key, product_id) or 0) + qty
            if current <= 0:
                self.redis.client.hdel(key, product_id)
            else:
                self.redis.client.hset(key, product_id, current)
            self.redis.client.expire(key, CART_TTL)

            return jsonify({"ok": True, "items": self.redis.client.hgetall(key) or {}})

        @app.put("/api/v1/cart")
        def cart_set():
            """
            Body: { "owner_id": "...", "product_id": "...", "qty": 3 }
            Sets absolute quantity (deletes if qty <= 0).
            """
            data = request.get_json(force=True)
            owner_id = (data.get("owner_id") or "").strip()
            product_id = str(data.get("product_id") or "").strip()
            qty = int(data.get("qty", 0))

            if not owner_id or not product_id:
                return jsonify({"ok": False, "error": "owner_id and product_id are required"}), 400

            key = cart_key(owner_id)
            if qty <= 0:
                self.redis.client.hdel(key, product_id)
            else:
                self.redis.client.hset(key, product_id, qty)
            self.redis.client.expire(key, CART_TTL)

            return jsonify({"ok": True, "items": self.redis.client.hgetall(key) or {}})

        @app.delete("/api/v1/cart")
        def cart_clear():
            """
            Query: ?owner_id=...
            """
            owner_id = (request.args.get("owner_id") or "").strip()
            if not owner_id:
                return jsonify({"ok": False, "error": "owner_id is required"}), 400

            self.redis.client.delete(cart_key(owner_id))
            return jsonify({"ok": True})

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
        # INSERT klausimas
        # ----------------------
        @app.post("/api/v1/questions")
        def create_question():
            body = request.get_json(force=True)
            event_id = body.get("event_id")
            user_id = body.get("user_id")
            text = body.get("text")
            result = kasandre.insert_question(event_id, user_id, text)
            return jsonify(result), 201

        # ----------------------
        # INSERT atsakymas
        # ----------------------
        @app.post("/api/v1/answers")
        def create_answer():
            body = request.get_json(force=True)
            question_id = body.get("question_id")
            user_id = body.get("user_id")
            text = body.get("text")
            result = kasandre.insert_answer(question_id, user_id, text)
            return jsonify(result), 201

        # ----------------------
        # GET klausimai su atsakymais (helper)
        # ----------------------
        @app.get("/api/v1/questions_with_answers")
        def get_questions_with_answers():
            limit = int(request.args.get("limit", 50))
            questions = kasandre.get_questions_with_answers(limit)
            return jsonify(questions)
  

    @app.get("/api/v1/get_questions")
    def get_questions():
        questions = self.kasandre.get_questions()
        return jsonify({"ok": True, "questions": questions})

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


    # ----------------------
    # Run
    # ----------------------
    def run(self):
        self.app.run(host="0.0.0.0", port=self.port, debug=True)

if __name__=="__main__":
    app = EventApp()
    app.run()