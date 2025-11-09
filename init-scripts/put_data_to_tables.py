import pandas as pd
from cassandra.cluster import Cluster
from uuid import uuid4
from datetime import date, datetime  # ✅ Added datetime import

KEYSPACE = "event_app"
EXCEL_FILE = "Cassandrai.xlsx"

def main():
    # Connect to Cassandra
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    # Read Excel sheets
    df_questions = pd.read_excel(EXCEL_FILE, sheet_name="Questions")
    df_answers   = pd.read_excel(EXCEL_FILE, sheet_name="Answers")

    # ------------------------------
    #  INSERT QUESTIONS
    # ------------------------------
    question_uuid_map = {}  # maps EVxxxx → generated UUID

    for index, row in df_questions.iterrows():
        q_raw_id = row["question_id"]
        event_id = row["rengiinio_id"]
        user_id  = row["user_id"]
        q_text   = row["question"]
        q_uuid   = uuid4()

        question_uuid_map[q_raw_id] = q_uuid

        today = date.today()
        now = datetime.utcnow()  # ✅ Added this line

        # Insert into questions_all
        # ✅ Added created_at field and reordered columns
        session.execute("""
            INSERT INTO questions_all (question_date, created_at, question_id, event_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (today, now, q_uuid, event_id, user_id, q_text))

        # Insert into questions_by_event
        # ✅ Added created_at field and reordered columns
        session.execute("""
            INSERT INTO questions_by_event (event_id, created_at, question_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, now, q_uuid, user_id, q_text))

        # Insert into questions_by_date
        # ✅ Added created_at field and reordered columns
        session.execute("""
            INSERT INTO questions_by_date (question_date, created_at, question_id, event_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (today, now, q_uuid, event_id, user_id, q_text))

    print("✅ Questions inserted successfully.")

    # ------------------------------
    #  INSERT ANSWERS
    # ------------------------------
    for index, row in df_answers.iterrows():
        q_raw_id = row["klausimo_id"]
        user_id  = row["user_id"]
        a_text   = row["answer"]

        # Convert klausimo_id into the real UUID stored in Cassandra
        q_uuid = question_uuid_map.get(q_raw_id)

        if not q_uuid:
            print(f"⚠️ Warning: no UUID found for klausimo_id={q_raw_id}, skipping")
            continue

        a_uuid = uuid4()
        now = datetime.utcnow()  # ✅ Changed from date.today() to datetime.utcnow()

        # ✅ Changed answer_date to created_at
        session.execute("""
            INSERT INTO answers_by_question (question_id, created_at, answer_id, user_id, answer_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (q_uuid, now, a_uuid, user_id, a_text))

    print("✅ Answers inserted successfully.")

    cluster.shutdown()

if __name__ == "__main__":
    main()