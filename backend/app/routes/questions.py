from flask import Blueprint, jsonify, request
from backend.app.extensions import cassandra
from datetime import datetime

questions_bp = Blueprint('questions', __name__, url_prefix='/api/v1')

# ----------------------
# INSERT klausimas
# ----------------------
@questions_bp.post("/questions")
def create_question():
    body = request.get_json(force=True)
    event_id = body.get("event_id")
    user_id = body.get("user_id")
    text = body.get("text")
    result = cassandra.insert_question(event_id, user_id, text)
    return jsonify(result), 201

# ----------------------
# INSERT atsakymas
# ----------------------
@questions_bp.post("/answers")
def create_answer():
    body = request.get_json(force=True)
    question_id = body.get("question_id")
    user_id = body.get("user_id")
    text = body.get("text")
    result = cassandra.insert_answer(question_id, user_id, text)
    return jsonify(result), 201

# ----------------------
# GET klausimai su atsakymais (helper)
# ----------------------
@questions_bp.get("/questions_with_answers")
def get_questions_with_answers():
    limit = int(request.args.get("limit", 50))
    questions = cassandra.get_questions_with_answers(limit)
    return jsonify(questions)


@questions_bp.get("/get_questions")
def get_questions():
    questions = cassandra.get_questions_all()
    return jsonify({"ok": True, "questions": questions})


@questions_bp.get("/get_questions_by_event/<event_id>")
def get_questions_by_event(event_id):
    questions = cassandra.get_questions_by_event(event_id)
    return jsonify({"ok": True, "questions": questions})

@questions_bp.get("/get_questions_by_date")
def get_questions_by_date():
    # expects ?date=YYYY-MM-DD
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "Missing query param ?date=YYYY-MM-DD"}), 400
    try:
        qdate = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid date format. Use YYYY-MM-DD."}), 400

    questions = cassandra.get_questions_by_date(qdate)
    return jsonify({"ok": True, "questions": questions})


# ---- Answers (simple) ----

@questions_bp.get("/get_answers_by_question/<question_id>")
def get_answers_by_question(question_id):
    answers = cassandra.get_answers_by_question(question_id)
    return jsonify({"ok": True, "answers": answers})