from flask import Blueprint, jsonify
from backend.app.extensions import db
from datetime import datetime, timezone

health_bp = Blueprint('health', __name__)

@health_bp.get("/_health")
def health():
    try:
        db.client.admin.command("ping")
        return jsonify({
            "ok": True,
            "time": datetime.now(timezone.utc).isoformat() + "Z",
            "db": "connected"
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500