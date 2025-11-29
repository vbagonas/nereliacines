from flask import Blueprint, jsonify
from backend.app.extensions import db, redis

analytics_bp = Blueprint("analytics", __name__, url_prefix="/api/v1/analytics")


@analytics_bp.get("vilnius-events")
def vilnius_events():
    pipeline = [
        {"$match": {"Miestas": {"$regex": r"^\s*Vilnius\s*$", "$options": "i"}}},
        {"$sort": {"Data": 1}},
        {"$project": {"_id": 1, "Pavadinimas": 1, "Data": 1, "Bilieto_tipas": 1}},
        {"$group": {"_id": None, "data": {"$push": "$$ROOT"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "count": 1, "data": 1}}
    ]
    doc = next(db.renginiai.aggregate(pipeline), {"count": 0, "data": []})
    return app.response_class(
        dumps(doc, json_options=RELAXED_JSON_OPTIONS),
        mimetype="application/json"
    )


@analytics_bp.get("/top3-by-tickets")
def top3_by_tickets():
    cache_key = "analytics:top3_events"

    # Try Redis cache first
    cached = redis.get_cache(cache_key)
    if cached:
        print("Returning top3 events from Redis cache")
        return cached

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


    doc = next(db.uzsakymai.aggregate(pipeline), {"count": 0, "data": []})

    # Cache result for 10 minutes (600 seconds)
    redis.set_cache(cache_key, doc, ttl=600)

    return doc