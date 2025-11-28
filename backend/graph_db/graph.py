from neo4j import GraphDatabase
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

class GraphDB():
    def __init__(self):
        self.driver = self.connect()
        self.session = self.driver.session(database="neo4j")
        print("✅ Connected to Neo4j database")

    def connect(self):
        with open(PROJECT_ROOT/'creds.yml', 'r') as f:
            data = yaml.safe_load(f) or {}
        creds = data.get('neo4j_user', data)

        driver = GraphDatabase.driver(creds['uri'], auth=(creds['username'], creds['password']))
        return driver
    
    def close(self):
        """Close connection"""
        self.session.close()
        self.driver.close()
    
    def _run_query(self, query, params=None):
        """Execute query and return results"""
        result = self.session.run(query, params or {})
        return [record.data() for record in result]

    def add_event(self, event_id, event_date):
        """Create/update event with date"""
        query = """
            MERGE (e:Event {id: $event_id})
            SET e.date = datetime($event_date)
        """
        return self._run_query(query, {'event_id': event_id, 'event_date': event_date})


    def add_purchase(self, user_id, event_id, event_date=None):
        """Record purchase and optionally set event date"""
        query = """
            MERGE (u:User {id: $user_id})
            MERGE (e:Event {id: $event_id})
            MERGE (u)-[p:PURCHASED]->(e)
            ON CREATE SET p.timestamp = timestamp()
        """
        if event_date:
            query += "SET e.date = datetime($event_date)"
        
        return self._run_query(query, {
            'user_id': user_id, 
            'event_id': event_id,
            'event_date': event_date
        })


    def recommend_collaborative(self, user_id):
        """All recommended events based on similar users"""
        print(user_id)

        query = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
            MATCH (other:User)-[:BOUGHT]->(e)
            MATCH (other)-[:BOUGHT]->(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
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
                rec.organizatoriai as organizers,
                similarity_score
        """
        return self._run_query(query, {'user_id': user_id})
    

    def recommend_collaborative_upcoming(self, user_id):
        """Upcoming recommended events only"""
        query = """
            MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
            MATCH (other:User)-[:BOUGHT]->(e)
            MATCH (other)-[:BOUGHT]->(rec:Event)
            WHERE NOT (u)-[:BOUGHT]->(rec)
            AND datetime(rec.data) >= datetime()
            AND datetime(rec.data) <= datetime() + duration({months: 2})
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
                rec.organizatoriai as organizers,
                similarity_score
        """
        return self._run_query(query, {
            'user_id': user_id, 
        })