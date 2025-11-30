from flask import Blueprint, jsonify, request
from backend.app.extensions import redis
from backend.app.config import CART_TTL, cart_key


cart_bp = Blueprint('cart', __name__, url_prefix='/api/v1')

@cart_bp.get("/cart")
def cart_get():
    owner_id = (request.args.get("owner_id") or "").strip()
    if not owner_id:
        return jsonify({"ok": False, "error": "owner_id is required"}), 400

    items = redis.client.hgetall(cart_key(owner_id)) or {}
    return jsonify({"ok": True, "items": items})

@cart_bp.post("/cart")  # ✅ FIXED: Removed duplicate /api/v1
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
    current = int(redis.client.hget(key, product_id) or 0) + qty
    if current <= 0:
        redis.client.hdel(key, product_id)
    else:
        redis.client.hset(key, product_id, current)
    redis.client.expire(key, CART_TTL)

    return jsonify({"ok": True, "items": redis.client.hgetall(key) or {}})

@cart_bp.put("/cart")  # ✅ FIXED: Removed duplicate /api/v1
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
        redis.client.hdel(key, product_id)
    else:
        redis.client.hset(key, product_id, str(qty))
    redis.client.expire(key, CART_TTL)

    return jsonify({"ok": True, "items": redis.client.hgetall(key) or {}})

@cart_bp.delete("/cart")  # ✅ FIXED: Removed duplicate /api/v1
def cart_clear():
    """
    Query: ?owner_id=...
    """
    owner_id = (request.args.get("owner_id") or "").strip()
    if not owner_id:
        return jsonify({"ok": False, "error": "owner_id is required"}), 400

    redis.client.delete(cart_key(owner_id))
    return jsonify({"ok": True})