from flask import Blueprint, jsonify, request
from backend.app.extensions import db, redis
from datetime import datetime, timezone
purchase_bp = Blueprint('purchase', __name__, url_prefix='/api/v1')

@purchase_bp.post("/api/v1/purchase")
def purchase():
    payload = request.get_json(force=True)
    vartotojo_id = payload.get("vartotojo_id")
    renginys_id = payload.get("renginys_id")
    bilieto_tipas_id = payload.get("bilieto_tipas_id")
    kiekis = int(payload.get("kiekis", 1))
    

    if not (vartotojo_id and renginys_id and kiekis > 0):
        return jsonify({"ok": False, "error": "Provide vartotojo_id, renginys_id, and kiekis>0."})

    if not db.vartotojai.find_one({"_id": vartotojo_id}):
        return jsonify({"ok": False, "error": "User not found. Register first."})

    with db.client.start_session() as s:
        with s.start_transaction():
            cache_key = f"event:{renginys_id}"
            print(cache_key)
            ev = redis.get_cache(cache_key)

            if not ev:
                ev = db.renginiai.find_one({"_id": renginys_id}, session=s)
                if not ev:
                    return jsonify({"ok": False, "error": "Event not found."})
                
            tickets = ev.get("Bilieto_tipas") or []
            if not tickets:
                return jsonify({"ok": False, "error": "This event has no ticket types."})

            print(tickets)
            for ticket_type in tickets:
                print(ticket_type.get("Bilieto_tipas_id"), bilieto_tipas_id)
                if ticket_type.get("Bilieto_tipas_id") == bilieto_tipas_id:
                    chosen = ticket_type
                    break

            likutis = int(chosen.get("Likutis", 0))
            if kiekis > likutis:
                return jsonify({"ok": False, "error": f"Not enough tickets. Remainder: {likutis}"})

            res = db.renginiai.update_one(
                {"_id": renginys_id,
                    "Bilieto_tipas": {"$elemMatch": {"Bilieto_tipas_id": bilieto_tipas_id, "Likutis": {"$gte": kiekis}}}},
                {"$inc": {"Bilieto_tipas.$.Likutis": -kiekis}},
                session=s
            )
            if res.modified_count != 1:
                return jsonify({"ok": False, "error": "Concurrent update issue."})

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
            ins = db.uzsakymai.insert_one(order, session=s)
            
            # Aktyvi invalidacija
            redis.invalidate_cache(cache_key)

            # Patikrinam ar liko bilietų → jei ne, pašalinam iš valid_events
            updated_event = db.renginiai.find_one({"_id": renginys_id})
            tickets = updated_event.get("Bilieto_tipas", [])
            has_available = any(int(t.get("Likutis", 0)) > 0 for t in tickets)

            valid_ids = redis.get_cache("valid_events") or []
            if not has_available:
                valid_ids = [eid for eid in valid_ids if eid != str(renginys_id)]
                redis.set_cache("valid_events", valid_ids)


    return jsonify({"ok": True, "message": "Purchase successful.", "order_id": str(ins.inserted_id), "order": order})