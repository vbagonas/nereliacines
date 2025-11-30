from neo4j import GraphDatabase
from pathlib import Path
import yaml
from neo4j import GraphDatabase
from pathlib import Path
import yaml
import json
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class GraphDB():
    def __init__(self):
        self.driver = self.connect()
        # You can keep this, but we won't rely on it anymore for queries
        self.session = self.driver.session(database="neo4j")
        print("✅ Connected to Neo4j database")

    def connect(self):
        with open(PROJECT_ROOT / 'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('neo4j_user', data)

        driver = GraphDatabase.driver(
            creds['uri'],
            auth=(creds['username'], creds['password'])
        )
        return driver

    def close(self):
        """Close connection"""
        self.session.close()
        self.driver.close()

    def _run_query(self, query, params=None):
        """
        Execute query and return results.

        IMPORTANT: we open a NEW session per call to avoid 'defunct connection'
        errors when a long-lived session goes stale.
        """
        with self.driver.session(database="neo4j") as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]

    # -------------------------
    # Basic helpers
    # -------------------------
    def add_event(self, event_id, event_date):
        """Create/update event with date"""
        query = """
            MERGE (e:Event {id: $event_id})
            SET e.date = datetime($event_date)
        """
        return self._run_query(query, {
            'event_id': event_id,
            'event_date': event_date
        })

    def has_purchase_history(self, user_id):
        """Check if user has any purchase history"""
        query = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(:Event)
            RETURN count(*) as purchase_count
        """
        result = self._run_query(query, {'user_id': user_id})
        return result[0]['purchase_count'] > 0 if result else False

    def add_purchase(self, user_id, event_id, event_date=None):
        """Record purchase relationship between user and event"""
        query = """
            MERGE (u:User {id: $user_id})
            MERGE (e:Event {id: $event_id})
            MERGE (u)-[p:BOUGHT]->(e)
            ON CREATE SET p.timestamp = timestamp()
        """
        if event_date:
            query += " SET e.data = datetime($event_date)"

        return self._run_query(query, {
            'user_id': user_id,
            'event_id': event_id,
            'event_date': event_date
        })

    # -------------------------
    # Event recommendations
    # -------------------------
    def recommend_collaborative(self, user_id):
        """All recommended events based on similar users"""
        query = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
            MATCH (other:User)-[:BOUGHT]->(e)
            MATCH (other)-[:BOUGHT]->(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
            WITH rec, COUNT(DISTINCT other) as similarity_score
            ORDER BY similarity_score DESC
            LIMIT 10
            RETURN rec.id as event_id,
                   rec.data as event_date,
                   rec.pavadinimas as title,
                   rec.tipas as type,
                   rec.adresas as address,
                   rec.miestas as city,
                   rec.vieta as venue,
                   rec.amziaus_cenzas as age_restriction,
                   rec.renginio_trukme as duration,
                   rec.bilieto_tipai as ticket_types,
                   rec.organizatoriai as organizers
        """
        return self._run_query(query, {'user_id': user_id})

    def recommend_collaborative_upcoming(self, user_id):
        """Upcoming recommended events only (24 months)"""
        query = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
            MATCH (other:User)-[:BOUGHT]->(e)
            MATCH (other)-[:BOUGHT]->(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
              AND datetime(rec.data) >= datetime()
              AND datetime(rec.data) <= datetime() + duration({months: 24})
            WITH rec, COUNT(DISTINCT other) as similarity_score
            ORDER BY similarity_score DESC
            LIMIT 5
            RETURN rec.id as event_id,
                   rec.data as event_date,
                   rec.pavadinimas as title,
                   rec.tipas as type,
                   rec.adresas as address,
                   rec.miestas as city,
                   rec.vieta as venue,
                   rec.amziaus_cenzas as age_restriction,
                   rec.renginio_trukme as duration,
                   rec.bilieto_tipai as ticket_types,
                   rec.organizatoriai as organizers
        """
        return self._run_query(query, {'user_id': user_id})

    # -------------------------
    # User management
    # -------------------------
    def add_user(self, user_id, miestas=None, pavarde=None,
                 pomegiai=None, vardas=None):
        """Add a new user with properties"""
        query = """
            MERGE (u:User {id: $user_id})
            ON CREATE SET u.created_at = timestamp()
        """

        params = {'user_id': user_id}

        if miestas:
            query += ", u.miestas = $miestas"
            params['miestas'] = miestas

        if pavarde:
            query += ", u.pavarde = $pavarde"
            params['pavarde'] = pavarde

        if pomegiai:
            query += ", u.pomegiai = $pomegiai"
            params['pomegiai'] = pomegiai

        if vardas:
            query += ", u.vardas = $vardas"
            params['vardas'] = vardas

        query += " RETURN u"

        return self._run_query(query, params)

    # -------------------------
    # Upcoming events (simple)
    # -------------------------
    def get_upcoming_events(self, months=2, limit=5):
        """Get upcoming events within specified time range"""
        query = """
            MATCH (e:Event)
            WHERE datetime(e.data) >= datetime()
              AND datetime(e.data) <= datetime() + duration({months: $months})
            RETURN e.id as event_id,
                   e.data as event_date,
                   e.pavadinimas as title,
                   e.tipas as type,
                   e.adresas as address,
                   e.miestas as city,
                   e.vieta as venue,
                   e.amziaus_cenzas as age_restriction,
                   e.renginio_trukme as duration,
                   e.bilieto_tipai as ticket_types,
                   e.organizatoriai as organizers
            ORDER BY e.data
            LIMIT $limit
        """

        return self._run_query(query, {
            'months': months,
            'limit': limit
        })

        # -------------------------
    # Organizer recommendations (UNLIMITED, SIMPLE)
    # -------------------------
    def recommend_organizers_unlimited(self, user_id, limit=3):
        """
        Recommend organizers based on the collaborative graph of purchases.

        Logic (simple, but works and ignores event dates so past events COUNT):

        1) Take events this user has bought.
        2) Find *other* users who bought those same events.
        3) Collect ALL events those other users have bought that the original
           user has NOT bought yet.
        4) From those recommended events, read ev.organizatoriai (JSON string)
           and count how often each (name, email) pair appears.
        5) Return TOP N organizers as:
           [{ "name": ..., "email": ..., "score": <int> }, ...]
        """
        cypher = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
            MATCH (other:User)-[:BOUGHT]->(e)
            MATCH (other)-[:BOUGHT]->(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
            RETURN rec.organizatoriai AS organizers_json
        """

        rows = self._run_query(cypher, {"user_id": user_id})

        counter = Counter()

        for row in rows:
            raw = row.get("organizers_json")
            if not raw:
                continue

            try:
                org_list = json.loads(raw)
            except Exception:
                # if parsing fails, skip this event
                continue

            # Sometimes it could be a single object instead of list
            if isinstance(org_list, dict):
                org_list = [org_list]

            if not isinstance(org_list, list):
                continue

            for org in org_list:
                if not isinstance(org, dict):
                    continue
                name = (org.get("Pavadinimas") or org.get("pavadinimas") or "").strip()
                email = (org.get("El_pastas") or org.get("el_pastas") or "").strip()

                # We need at least a name or an email to be useful
                if not (name or email):
                    continue

                key = (name, email)
                counter[key] += 1

        # Build sorted result
        result = []
        for (name, email), score in counter.most_common(limit):
            result.append({
                "name": name or "Organizatorius",
                "email": email,
                "score": int(score)
            })
        return result


