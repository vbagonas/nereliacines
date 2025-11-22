from flask import Blueprint, jsonify
from datetime import datetime
from backend.app.extensions import db, redis

events_bp = Blueprint('events', __name__, url_prefix='/api/v1')


@events_bp.get("/events/<renginys_id>")
def read_event(renginys_id):
    cache_key = f"event:{renginys_id}"

    # Patikrinam Redis cache
    cached = redis.get_cache(cache_key)
    if cached:
        print(f"Loaded event {renginys_id} from Redis")
        return jsonify({"cached": True, "event": cached})

    # Jei nėra cache, paimame iš Mongo
    event = db.renginiai.find_one({"_id": renginys_id})
    if not event:
        return jsonify({"ok": False, "error": "Event not found"})

    # Įdedam į cache 
    redis.set_cache(cache_key, event)
    print(f"Loaded event {renginys_id} from Mongo and cached in Redis")

    return jsonify({"cached": False, "event": event})

@events_bp.get("/events")
def read_all_events():
    # Patikrinam globalų valid_events raktą
    valid_ids = redis.get_cache("valid_events")
    
    if valid_ids:
        print("Loaded valid event IDs from Redis")
        return jsonify({"cached": True, "event_ids": valid_ids})
    
    # Jei nėra cache, paimam visus renginius iš Mongo
    print("Loading valid events from MongoDB...")
    events = list(db.renginiai.find({}))

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
            redis.set_cache(f"event:{event_id_str}", ev)
            print(f"Cached event {event_id_str}")

    # Įrašom globalų sąrašą į cache
    redis.set_cache("valid_events", valid_ids)
    print("✅ Cached list of valid event IDs")

    # Grąžinam rezultatą
    return jsonify({"cached": False, "event_ids": valid_ids})
