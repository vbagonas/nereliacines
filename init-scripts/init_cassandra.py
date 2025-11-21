from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "event_app"

def create_keyspace_and_tables():
    
    # Prisijungiam prie Cassandra
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()

    # Sukuriam keyspace (jei dar nera)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)

    # Naudojam savo keyspace
    session.set_keyspace(KEYSPACE)

    # 1 Lentelė: visi klausimai
    session.execute("""
        CREATE TABLE IF NOT EXISTS questions_all (
            question_id UUID,
            question_date DATE,
            event_id TEXT,
            user_id TEXT,
            question_text TEXT,
            PRIMARY KEY ((question_id))
        );
        """)

    # 2 Lentelė: klausimai pagal renginį
    session.execute("""
        CREATE TABLE IF NOT EXISTS questions_by_event (
            event_id TEXT,
            question_date DATE,
            question_id UUID,
            user_id TEXT,
            question_text TEXT,
            PRIMARY KEY ((event_id), question_date, question_id)
        ) WITH CLUSTERING ORDER BY (question_date DESC);
    """)

    # 3 Lentelė: klausimai pagal datą
    session.execute("""
        CREATE TABLE IF NOT EXISTS questions_by_date (
            question_date DATE,
            question_id UUID,
            event_id TEXT,
            user_id TEXT,
            question_text TEXT,
            PRIMARY KEY ((question_date), question_id)
        ) WITH CLUSTERING ORDER BY (question_id ASC);
    """)

    # 4 Lentelė: atsakymai pagal klausimo ID
    session.execute("""
        CREATE TABLE IF NOT EXISTS answers_by_question (
            question_id UUID,
            answer_date DATE,
            answer_id UUID,
            user_id TEXT,
            answer_text TEXT,
            PRIMARY KEY ((question_id), answer_date, answer_id)
        ) WITH CLUSTERING ORDER BY (answer_date DESC);
    """)

    print("Keyspace ir lentelės sėkmingai sukurtos Cassandra duomenų bazėje.")
    cluster.shutdown()


if __name__ == "__main__":
    create_keyspace_and_tables()