from flask import Blueprint, jsonify, request
from backend.app.extensions import db, neo4, clickhouse
from backend.app.utils.auth import verify_password, hash_password

auth_bp = Blueprint('auth', __name__, url_prefix='/api/v1')


@auth_bp.post("/login")
def login():
    body = request.get_json(force=True)
    email = body.get("email")
    password = body.get("password")

    user = db.vartotojai.find_one({"_id": email})
    if not user or not verify_password(password, user.get("Slaptazodis", "")):
        return jsonify({"ok": False, "error": "Invalid credentials"}), 401

    public = {k: v for k, v in user.items() if k != "Slaptazodis"}
    return jsonify({"ok": True, "user": public})


@auth_bp.post("/register")
def register_user():
    payload = request.get_json(force=True)
    email = payload.get("email")
    
    if not email:
        return jsonify({"ok": False, "error": "Missing user id (email)."}), 400

    slaptazodis = payload.get("slaptazodis")
    if not slaptazodis:
        return jsonify({"ok": False, "error": "Password is required"}), 400

    hashed_password = hash_password(slaptazodis)
    
    if db.vartotojai.find_one({"_id": email}):
        return jsonify({"ok": False, "error": "User already exists."}), 409

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

    # STEP 1: Save to MongoDB
    db.vartotojai.insert_one(user_doc)
    
    # STEP 2: Sync to Neo4j
    neo4.add_user(
        user_id=email,
        vardas=user_doc.get("Vardas"),
        pavarde=user_doc.get("Pavarde"),
        miestas=user_doc.get("Miestas"),
        pomegiai=user_doc.get("Pomegiai", [])
    )
    
    # STEP 3: 📤 Sync to ClickHouse
    try:
        clickhouse.sync_user(user_doc)
    except Exception as e:
        print(f"⚠️ ClickHouse sync failed (non-critical): {e}")

    public_user = {k: v for k, v in user_doc.items() if k != "Slaptazodis"}
    return jsonify({"ok": True, "user": public_user})