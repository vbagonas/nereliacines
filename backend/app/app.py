from flask import Flask, jsonify, request
from datetime import datetime, timezone
from bson import Decimal128
from decimal import Decimal, InvalidOperation
from bson.json_util import dumps, RELAXED_JSON_OPTIONS
import bcrypt
from mongas.db import MongoDB

class EventApp:
    def __init__(self, port=5000):
        self.db = MongoDB()
        self.port = port
        self.app = Flask(__name__)
        self._register_routes()

    # --------------------------
    # Health
    # --------------------------
    def _register_routes(self):
        app = self.app

        @app.get("/_health")
        def health():
            try:
                self.client.admin.command("ping")
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
            email = payload.get("_id") or payload.get("vartotojo_id")
            if not email:
                return app.response_class(
                    dumps({"ok": False, "error": "Missing user id (email)."}, json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=400
                )

            slaptazodis = payload.get("Slaptazodis")
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
                "Vardas": payload.get("Vardas"),
                "Pavarde": payload.get("Pavarde"),
                "Gimimo_data": payload.get("Gimimo_data"),
                "Tel_numeris": payload.get("Tel_numeris"),
                "Miestas": payload.get("Miestas"),
                "Pomegiai": payload.get("Pomegiai", []) or [],
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

            with self.client.start_session() as s:
                with s.start_transaction():
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

            return app.response_class(
                dumps({"ok": True, "message": "Purchase successful.", "order_id": str(ins.inserted_id), "order": order},
                      json_options=RELAXED_JSON_OPTIONS),
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

