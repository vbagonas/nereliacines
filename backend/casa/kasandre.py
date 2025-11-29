# from uuid import UUID, uuid4
# from datetime import datetime, date
# from cassandra.cluster import Cluster

# class CassandraRepository:
#     def __init__(self, hosts=None, port=9042, keyspace="event_app"):
#         self.hosts = hosts or ["localhost"]
#         self.port = port
#         self.keyspace = keyspace

#         # Prisijungimas prie Cassandra
#         self.cluster = Cluster(self.hosts, port=self.port)
#         self.session = self.cluster.connect()
#         self.session.set_keyspace(self.keyspace)
#         print("✅ Connected to Cassandra keyspace:", self.keyspace)

#     # =========================
#     # INSERT funkcijos
#     # =========================

#     def insert_question(self, event_id, user_id, text):
#         question_id = uuid4()
#         created_at = datetime.utcnow()
#         question_date = date.today()

#         self.session.execute(
#             """
#             INSERT INTO questions_by_event (event_id, created_at, question_id, user_id, question_text)
#             VALUES (%s, %s, %s, %s, %s)
#             """,
#             (event_id, created_at, question_id, user_id, text),
#         )

        
#         self.session.execute(
#             """
#             INSERT INTO questions_by_date (question_date, created_at, question_id, event_id, user_id, question_text)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             """,
#             (question_date, created_at, question_id, event_id, user_id, text),
#         )

        
#         self.session.execute(
#             """
#             INSERT INTO questions_all (question_date, created_at, question_id, event_id, user_id, question_text)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             """,
#             (question_date, created_at, question_id, event_id, user_id, text),
#         )

#         return {"question_id": str(question_id)}

#     def insert_answer(self, question_id, user_id, text):
#         question_uuid = UUID(question_id)
#         answer_id = uuid4()
#         created_at = datetime.utcnow()

#         self.session.execute(
#            """
#             INSERT INTO answers_by_question (question_id, created_at, answer_id, user_id, answer_text)
#             VALUES (%s, %s, %s, %s, %s)
#             """,
#             (question_uuid, created_at, answer_id, user_id, text),
#         )

#         return {"answer_id": str(answer_id)}

#     # =========================
#     # GET funkcijos
#     # =========================

#     def get_questions_all(self, limit=100):
#         query = f"SELECT * FROM questions_all LIMIT {limit}"
#         rows = self.session.execute(query)
#         return [dict(row._asdict()) for row in rows]

#     def get_questions_by_event(self, event_id):
#         query = "SELECT * FROM questions_by_event WHERE event_id = %s"
#         rows = self.session.execute(query, (event_id,))
#         return [dict(row._asdict()) for row in rows]

#     def get_questions_by_date(self, question_date: date):
#         query = "SELECT * FROM questions_by_date WHERE question_date = %s"
#         rows = self.session.execute(query, (question_date,))
#         return [dict(row._asdict()) for row in rows]

#     def get_answers_by_question(self, question_id):
#         query = "SELECT * FROM answers_by_question WHERE question_id = %s"
#         rows = self.session.execute(query, (question_id,))
#         #return [dict(row._asdict()) for row in rows]
#         return [
#         {
#             "answer_id": str(row.answer_id),
#             "user_id": row.user_id,
#             "answer_text": row.answer_text,
#             "created_at": row.created_at.isoformat()
#         }
#         for row in rows
#     ]
    
#     # =========================
#     # Helper: klausimai su atsakymais
#     # =========================

#     def get_questions_with_answers(self, limit=50):
#         questions = self.get_questions_all(limit)
#         for q in questions:
#             q["answers"] = self.get_answers_by_question(q["question_id"])
#         return questions

#     def close(self):
#         self.cluster.shutdown()
#         print("✅ Cassandra connection closed")


from uuid import UUID, uuid4
from datetime import datetime, date
from cassandra.cluster import Cluster

class CassandraRepository:
    def __init__(self, hosts=None, port=9042, keyspace="event_app"):
        self.hosts = hosts or ["localhost"]
        self.port = port
        self.keyspace = keyspace

        # Prisijungimas prie Cassandra
        self.cluster = Cluster(self.hosts, port=self.port)
        self.session = self.cluster.connect()
        self.session.set_keyspace(self.keyspace)
        print("✅ Connected to Cassandra keyspace:", self.keyspace)

    # =========================
    # INSERT funkcijos
    # =========================

    def insert_question(self, event_id, user_id, text):
        question_id = uuid4()
        created_at = datetime.utcnow()
        question_date = date.today()

        self.session.execute(
            """
            INSERT INTO questions_by_event (event_id, question_date, question_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (event_id, question_date, question_id, user_id, text),
        )

        self.session.execute(
            """
            INSERT INTO questions_by_date (question_date, question_id, event_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (question_date, question_id, event_id, user_id, text),
        )

        self.session.execute("""
            INSERT INTO questions_by_event_and_date (event_id, question_date, question_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, question_date, question_id, user_id, text))

        self.session.execute(
            """
            INSERT INTO questions_all (question_id, question_date, event_id, user_id, question_text)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (question_id, question_date, event_id, user_id, text),
        )

        return {"question_id": str(question_id)}

    def insert_answer(self, question_id, user_id, text):
        question_uuid = UUID(question_id)
        answer_id = uuid4()
        answer_date = date.today()

        self.session.execute(
           """
            INSERT INTO answers_by_question (question_id, answer_date, answer_id, user_id, answer_text)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (question_uuid, answer_date, answer_id, user_id, text),
        )

        self.session.execute("""
            INSERT INTO answers_by_question_and_date (question_id, answer_date, answer_id, user_id, answer_text)
            VALUES (%s, %s, %s, %s, %s)
        """, (question_uuid, answer_date, answer_id, user_id, text))

        return {"answer_id": str(answer_id)}

    # =========================
    # GET funkcijos - FIXED
    # =========================

    def get_questions_all(self, limit=100):
        """Fetch all questions with proper JSON serialization."""
        query = f"SELECT * FROM questions_all LIMIT {limit}"
        rows = self.session.execute(query)
        
        # ✅ Convert UUID and date to strings
        return [
            {
                "question_id": str(row.question_id),
                "event_id": row.event_id,
                "user_id": row.user_id,
                "text": row.question_text,
                "question_date": str(row.question_date)
            }
            for row in rows
        ]

    def get_questions_by_event(self, event_id):
        query = "SELECT * FROM questions_by_event WHERE event_id = %s"
        rows = self.session.execute(query, (event_id,))
        return [
            {
                "question_id": str(row.question_id),
                "event_id": row.event_id,
                "user_id": row.user_id,
                "text": row.question_text,
                "question_date": str(row.question_date)
            }
            for row in rows
        ]

    def get_questions_by_event_and_date(self, event_id, question_date: date):
        query = """
            SELECT * FROM questions_by_event_and_date
            WHERE event_id = %s AND question_date = %s
        """
        rows = self.session.execute(query, (event_id, question_date))
        return [
            {
                "question_id": str(row.question_id),
                "event_id": row.event_id,
                "user_id": row.user_id,
                "text": row.question_text,
                "question_date": str(row.question_date),
            }
            for row in rows
        ]

    def get_questions_by_date(self, question_date: date):
        query = "SELECT * FROM questions_by_date WHERE question_date = %s"
        rows = self.session.execute(query, (question_date,))
        return [
            {
                "question_id": str(row.question_id),
                "event_id": row.event_id,
                "user_id": row.user_id,
                "text": row.question_text,
                "question_date": str(row.question_date)
            }
            for row in rows
        ]

    def get_answers_by_question(self, question_id):
        if isinstance(question_id, str):
            question_id = UUID(question_id)
            
        query = "SELECT * FROM answers_by_question WHERE question_id = %s"
        rows = self.session.execute(query, (question_id,))
        
        return [
            {
                "answer_id": str(row.answer_id),
                "user_id": row.user_id,
                "text": row.answer_text,
                "answer_date": str(row.answer_date)
            }
            for row in rows
        ]
    
    def get_answers_by_question_and_date(self, question_id, answer_date: date):
        if isinstance(question_id, str):
            question_id = UUID(question_id)
            
        query = "SELECT * FROM answers_by_question WHERE question_id = %s AND answer_date = %s"
        rows = self.session.execute(query, (question_id, answer_date))
        
        return [
            {
                "answer_id": str(row.answer_id),
                "user_id": row.user_id,
                "text": row.answer_text,
                "answer_date": str(row.answer_date)
            }
            for row in rows
        ]

    # =========================
    # Helper: klausimai su atsakymais
    # =========================

    def get_questions_with_answers(self, limit=50):
        """Get questions with their answers."""
        questions = self.get_questions_all(limit)
        
        for q in questions:
            q["answers"] = self.get_answers_by_question(q["question_id"])
        
        return questions

    def close(self):
        self.cluster.shutdown()
        print("✅ Cassandra connection closed")