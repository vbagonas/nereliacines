from flask import Blueprint, jsonify
from backend.app.extensions import neo4
from backend.app.routes.analytics import top3_by_tickets as top3
recommendations_bp = Blueprint('recommendations', __name__, url_prefix='/api/v1')


@recommendations_bp.get("/recommendations/<user_id>")
def get_recommendations(user_id):
    """Get all recommended events for user (riboto gylio)"""
    print(f"Getting recommendations for user {user_id}")
    if neo4.has_purchase_history(user_id):
        print("User has purchase history, getting collaborative recommendations")
        events = neo4.recommend_collaborative(user_id)
    else:
        print("User has no purchase history, returning top3 events by tickets sold")
        events = top3()

    return jsonify(events), 200


@recommendations_bp.get("/recommendations/upcoming/<user_id>")
def get_upcoming_recommendations(user_id):
    """
    Artimiausi rekomenduojami renginiai (24 mėn į priekį).

    Jei vartotojas turi istoriją – pirmiausia bandom collaborative,
    jeigu nieko nėra – imam paprastus artimiausius.
    Jei istorijos nėra – irgi imam artimiausius.
    """
    print(f"Getting upcoming recommendations for user {user_id}")
    if neo4.has_purchase_history(user_id):
        print("User has purchase history, getting collaborative recommendations")
        events = neo4.recommend_collaborative_upcoming(user_id)
        if not events:
            events = neo4.get_upcoming_events(months=24, limit=5)
    else:
        print("User has no purchase history, returning top3 events by tickets sold")
        events = top3()
    return jsonify(events), 200


@recommendations_bp.get("/recommendations/organizers/<user_id>")
def get_recommended_organizers(user_id):
    """
    Rekomenduojami organizatoriai, naudojant
    neo4.recommend_organizers_unlimited (neribotas gylis / laikas).
    """
    # jei vartotojas nieko nepirko – grąžinam tuščią, frontend gali slėpti bloką
    if not neo4.has_purchase_history(user_id):
        return jsonify([]), 200

    organizers = neo4.recommend_organizers_unlimited(user_id, limit=3)
    return jsonify(organizers), 200