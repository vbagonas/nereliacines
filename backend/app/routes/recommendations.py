from flask import Blueprint, jsonify, request
from backend.app.extensions import neo4
from backend.app.routes.analytics import top3_by_tickets

recommendations_bp = Blueprint('recommendations', __name__, url_prefix='/api/v1')

@recommendations_bp.get("/recommendations/<user_id>")
def get_recommendations(user_id):
    if neo4.has_purchase_history(user_id):
        events = neo4.recommend_collaborative(user_id)
        
    else:
        events = top3_by_tickets()

    print(events)

    return jsonify(events), 200



@recommendations_bp.get("/recommendations/upcoming/<user_id>")
def get_upcoming_recommendations():
    body = request.get_json(force=True)
    user_id = body.get('user_id')

    events = neo4.recommend_collaborative_upcoming(user_id)    

    return jsonify(events), 200