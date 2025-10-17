import os
from datetime import datetime
from typing import Optional
from datetime import datetime
from decimal import Decimal, InvalidOperation
from bson.decimal128 import Decimal128
from bson.json_util import dumps, RELAXED_JSON_OPTIONS

from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://vaiciutemigle_db_user:RSfeISmEhRRhRma9@manomb.gi8bjhg.mongodb.net/")
DB_NAME = os.getenv("DB_NAME", "eVent")
PORT = int(os.getenv("PORT", "8000"))
ORGANIZER_NAME = os.getenv("ORGANIZER_NAME", "Vilniaus universitetas")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

Vartotojai = db["Vartotojai"]
Renginiai = db["Renginiai"]
Uzsakymai = db["Užsakymai"]

app = Flask(__name__)

# ----------------------
# Health
# ----------------------
@app.get("/_health")
def health():
    try:
        client.admin.command('ping')
        return jsonify({"ok": True, "time": datetime.utcnow().isoformat() + "Z", "db": "connected"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ----------------------
# Register service
# ----------------------
@app.post("/api/v1/register")
def register_user():
    payload = request.get_json(force=True)
    user = {
        "vartotojo_id": payload.get("vartotojo_id"),
        "Vardas": payload.get("Vardas"),
        "Pavarde": payload.get("Pavarde"),
        "Gimimo_data": payload.get("Gimimo_data"),
        "Tel_numeris": payload.get("Tel_numeris"),
        "Miestas": payload.get("Miestas"),
        "Pomegiai": payload.get("Pomegiai", []) or [],
    }

    if not user["vartotojo_id"]:
        return app.response_class(
            dumps({"ok": False, "error": "Missing vartotojo_id (email)."}, json_options=RELAXED_JSON_OPTIONS),
            mimetype="application/json",
            status=400
        )

    existing = Vartotojai.find_one({"vartotojo_id": user["vartotojo_id"]})
    if existing:
        return app.response_class(
            dumps({"ok": False, "error": "User already exists."}, json_options=RELAXED_JSON_OPTIONS),
            mimetype="application/json",
            status=409
        )

    Vartotojai.insert_one(user)

    return app.response_class(
        dumps({"ok": True, "user": user}, json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json",
    )

# ----------------------
# Purchase service
# ----------------------
@app.post("/api/v1/purchase")
def purchase():
    payload = request.get_json(force=True)
    vartotojo_id     = payload.get("vartotojo_id")
    renginys_id      = payload.get("renginys_id")
    bilieto_tipas_id = payload.get("bilieto_tipas_id")
    kiekis           = int(payload.get("kiekis", 1))
    kaina_hint       = payload.get("kaina")
    if kaina_hint is None:
        kaina_hint = payload.get("kaina_hint")
    idx_hint         = payload.get("idx")
 
    if not (vartotojo_id and renginys_id and kiekis > 0):
        return app.response_class(
            dumps({"ok": False, "error": "Provide vartotojo_id, renginys_id, and kiekis>0. Also provide one of: bilieto_tipas_id | kaina | idx (unless event has exactly one ticket type)."},
                  json_options=RELAXED_JSON_OPTIONS),
            mimetype="application/json", status=400
        )
 
    if not Vartotojai.find_one({"vartotojo_id": vartotojo_id}):
        return app.response_class(
            dumps({"ok": False, "error": "User not found. Register first."},
                  json_options=RELAXED_JSON_OPTIONS),
            mimetype="application/json", status=404
        )
 
    with client.start_session() as s:
        with s.start_transaction():
            ev = Renginiai.find_one({"Renginio_id": renginys_id}, session=s)
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
 
            idx = None
 
            if bilieto_tipas_id:
                for i, t in enumerate(tickets):
                    if t.get("Bilieto_tipas_id") == bilieto_tipas_id:
                        idx = i
                        break
 
            if idx is None and kaina_hint is not None:
                try:
                    hint_dec = Decimal(str(kaina_hint))
 
                    for i, t in enumerate(tickets):
                        k = t.get("Kaina")  
                        if isinstance(k, Decimal128):
                            t_dec = k.to_decimal()
                            if t_dec == hint_dec:
                                idx = i
                                break
                except (InvalidOperation, ValueError):
                    pass
 
            if idx is None and idx_hint is not None:
                try:
                    i2 = int(idx_hint)
                    if 0 <= i2 < len(tickets):
                        idx = i2
                except (TypeError, ValueError):
                    pass
 
            if idx is None and len(tickets) == 1:
                idx = 0
 
            if idx is None:
                return app.response_class(
                    dumps({"ok": False, "error": "Ticket type not found. Provide correct bilieto_tipas_id OR kaina OR idx."},
                          json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=404
                )
 
            chosen = tickets[idx]
            bilieto_tipas_id = chosen.get("Bilieto_tipas_id")
            kaina_dec128     = chosen.get("Kaina") 
            likutis          = int(chosen.get("Likutis", 0))
 
            if kiekis > likutis:
                return app.response_class(
                    dumps({"ok": False, "error": f"Not enough tickets. Remainder: {likutis}"},
                          json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=400
                )
 
            res = Renginiai.update_one(
                {
                    "Renginio_id": renginys_id,
                    "Bilieto_tipas": {
                        "$elemMatch": {"Bilieto_tipas_id": bilieto_tipas_id, "Likutis": {"$gte": kiekis}}
                    }
                },
                {"$inc": {"Bilieto_tipas.$.Likutis": -kiekis}},
                session=s
            )
            if res.modified_count != 1:
                return app.response_class(
                    dumps({"ok": False, "error": "Not enough tickets or ticket type not found (concurrent update)."},
                          json_options=RELAXED_JSON_OPTIONS),
                    mimetype="application/json", status=409
                )
 
            order = {
                "vartotojo_id": vartotojo_id,
                "uzsakymo_data": datetime.utcnow(),
                "Bilietai": [{
                    "renginys_id": renginys_id,
                    "Bilieto_tipas_id": bilieto_tipas_id,
                    "Kiekis": kiekis,
                    "Kaina": kaina_dec128
                }]
            }
            ins = Uzsakymai.insert_one(order, session=s)
 
    return app.response_class(
        dumps({"ok": True, "message": "Purchase successful.", "order_id": str(ins.inserted_id), "order": order},
              json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json"
    )

# ----------------------
# Events in Vilnius
# ----------------------
@app.get("/api/v1/analytics/vilnius-events")
def vilnius_events():
    pipeline = [
        {"$match": {"Miestas": {"$regex": r"^\s*Vilnius\s*$", "$options": "i"}}},
        {"$sort": {"Data": 1}},
        {"$project": {
            "_id": 0,
            "Renginio_id": 1,
            "Pavadinimas": 1,
            "Data": 1,
            "Tickets": {
                "$map": {
                    "input": {"$ifNull": ["$Bilieto_tipas", []]},
                    "as": "t",
                    "in": {
                        "Bilieto_tipas_id": {"$ifNull": ["$$t.Bilieto_tipas_id", ""]},
                        "Kaina": {"$ifNull": ["$$t.Kaina", None]},
                        "Likutis": {"$ifNull": ["$$t.Likutis", 0]}
                    }
                }
            }
        }},
        {"$group": {"_id": None, "data": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "count": 1, "data": 1}}
    ]
 
    doc = next(Renginiai.aggregate(pipeline), {"count": 0, "data": []})
 
    payload = [
        {"count": doc.get("count", 0)},
        {"data": doc.get("data", [])}
    ]
    return app.response_class(
        dumps(payload, json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json"
    )

# ----------------------
# Top-3 events by sold tickets
# ----------------------
@app.get("/api/v1/analytics/top3-by-tickets")
def top3_by_tickets():
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
            "foreignField": "Renginio_id",
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
            "count": {"$sum": 1}
        }},
        {"$project": {"_id": 0, "count": 1, "data": 1}}
    ]

    doc = next(Uzsakymai.aggregate(pipeline), {"count": 0, "data": []})
    return app.response_class(
        dumps(doc, json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json"
    )

# ----------------------
# Average duration by organizer
# ----------------------
@app.get("/api/v1/analytics/avg-duration-by-organizer")
def avg_duration_by_organizer():
    pipeline = [
        {"$unwind": {"path": "$Organizatorius", "preserveNullAndEmptyArrays": False}},

        {"$group": {
            "_id": {"$ifNull": ["$Organizatorius.Pavadinimas", "(be pavadinimo)"]},
            "avg_duration": {"$avg": {"$toDouble": {"$ifNull": ["$Renginio_trukme", 0]}}},
            "events_count": {"$sum": 1}
        }},

        {"$sort": {"avg_duration": -1}},

        {"$project": {
            "_id": 0,
            "Organizatorius": "$_id",
            "avg_trukme_min": {"$round": ["$avg_duration", 2]},
            "events_count": 1
        }},

        {"$group": {
            "_id": None,
            "data": {"$push": "$$ROOT"},
            "count": {"$sum": 1}
        }},
        {"$project": {"_id": 0, "count": 1, "data": 1}}
    ]

    doc = next(Renginiai.aggregate(pipeline), {"count": 0, "data": []})
    return app.response_class(
        dumps(doc, json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)