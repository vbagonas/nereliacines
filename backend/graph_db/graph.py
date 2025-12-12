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


    def _run_query(self, query, params=None):
        """
        Execute query and return results.

        IMPORTANT: we open a NEW session per call to avoid 'defunct connection'
        errors when a long-lived session goes stale.
        """
        with self.driver.session(database="neo4j") as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]


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
    # Organizer recommendations (MULTI-HOP)
    # -------------------------
    def recommend_organizers_unlimited(self, user_id, limit=3, max_depth=6):
        """
        Recommend organizers based on a multi-hop collaborative graph of purchases.

        We walk the User–Event graph up to `max_depth` hops:

            (u:User {id: user_id})-[:BOUGHT*1..max_depth]-(rec:Event)

        That means:
        - 1 hop  -> events this user bought
        - 2 hops -> other users
        - 3+ hops -> events/users further away, etc.

        We then:
        - Exclude events this user already bought.
        - Collect organizers from the remaining events.
        - Count how often each (name, email) pair appears.
        - Return top N organizers.
        """
        # NOTE: variable-length upper bound can't be parameterized, so we
        # inject max_depth directly into the Cypher string.
        cypher = f"""
            MATCH (u:User {{id: $user_id}})
            MATCH p = (u)-[:BOUGHT*1..{max_depth}]-(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
            WITH DISTINCT rec
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

                # Count organizers (unique by name+email)
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

