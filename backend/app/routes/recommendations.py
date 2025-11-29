from flask import Blueprint, jsonify
from backend.app.extensions import neo4

recommendations_bp = Blueprint('recommendations', __name__, url_prefix='/api/v1')

@recommendations_bp.get("/recommendations/<user_id>")
def get_recommendations(user_id):
    """
    Simple recommendations - naudoja Neo4j collaborative filtering
    """
    try:
        # Naudojam graph.py metodą
        events = neo4.recommend_collaborative(user_id)
        
        print(f"✅ Neo4j returned {len(events)} recommendations for {user_id}")
        
        # Konvertuojam DateTime → string
        for event in events:
            for key, value in event.items():
                if hasattr(value, 'iso_format'):
                    event[key] = value.iso_format()
                elif 'DateTime' in str(type(value)):
                    event[key] = str(value)
        
        print(f"📤 Sending {len(events)} events to frontend")
        return jsonify(events), 200
        
    except Exception as e:
        print(f"❌ Error in recommendations: {e}")
        import traceback
        traceback.print_exc()
        return jsonify([]), 200