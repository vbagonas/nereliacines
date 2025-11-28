from flask import Blueprint, jsonify, request
from backend.app.extensions import neo4

recommendations_bp = Blueprint('recommendations', __name__, url_prefix='/api/v1')

@recommendations_bp.get("/recommendations/<user_id>")
def get_recommendations():
    body = request.get_json(force=True)
    user_id = body.get('user_id')

    events = neo4.recommend_collaborative(user_id, limit=5)    
    print(events)



@recommendations_bp.get("/recommendations/upcoming/<user_id>")
def get_upcoming_recommendations():
    body = request.get_json(force=True)
    user_id = body.get('user_id')

    events = neo4.recommend_collaborative_upcoming(user_id, limit=5, months_ahead=2)    
    print(events)
