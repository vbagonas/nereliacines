#!/usr/bin/env python3
"""
Debug script to check why recommendations aren't showing
Run: python debug_neo4j.py
"""

from backend.graph_db.graph import GraphDB
from backend.mongas.db import MongoDB

def main():
    neo = GraphDB()
    mongo = MongoDB()

    user_id = "vycka.b@yahoo.com"  # ✅ Change this to your email
    
    print("="*60)
    print("NEO4J RECOMMENDATIONS DEBUG")
    print("="*60)
    print(f"Testing user: {user_id}\n")

    # 1. Check MongoDB purchases
    print("1️⃣  MONGODB PURCHASES:")
    orders = list(mongo.uzsakymai.find({"vartotojo_id": user_id}))
    print(f"   Total orders: {len(orders)}")
    if orders:
        for i, o in enumerate(orders, 1):
            print(f"   Order {i}:")
            for b in o.get("Bilietai", []):
                print(f"     - Event: {b['renginys_id']}, Qty: {b.get('Kiekis', 1)}")
    else:
        print("   ❌ No orders found in MongoDB!")
        return

    # 2. Check Neo4j user exists
    print("\n2️⃣  NEO4J USER:")
    user = neo._run_query("MATCH (u:User {id: $id}) RETURN u", {"id": user_id})
    if user:
        print(f"   ✅ User exists in Neo4j")
    else:
        print(f"   ❌ User NOT found in Neo4j!")
        print("   Run: MongoToNeoImporter().run()")
        return

    # 3. Check Neo4j BOUGHT relationships
    print("\n3️⃣  NEO4J PURCHASES (BOUGHT relationships):")
    purchases = neo._run_query("""
        MATCH (u:User {id: $id})-[:BOUGHT]->(e:Event)
        RETURN e.id as event_id, e.pavadinimas as title
    """, {"id": user_id})
    print(f"   Total purchases: {len(purchases)}")
    if purchases:
        for p in purchases:
            print(f"     - {p['event_id']}: {p.get('title', 'N/A')}")
    else:
        print("   ❌ No BOUGHT relationships in Neo4j!")
        print("   Check if purchase.py calls neo4.add_purchase()")
        return

    # 4. Test has_purchase_history()
    print("\n4️⃣  has_purchase_history() FUNCTION:")
    has_history = neo.has_purchase_history(user_id)
    print(f"   Result: {has_history}")
    if not has_history:
        print("   ❌ Function returns False even though purchases exist!")
        return

    # 5. Test collaborative filtering (no date filter)
    print("\n5️⃣  COLLABORATIVE FILTERING (all events):")
    all_recs = neo._run_query("""
        MATCH (u:User {id: $user_id})-[:BOUGHT]->(e:Event)
        MATCH (other:User)-[:BOUGHT]->(e)
        MATCH (other)-[:BOUGHT]->(rec:Event)
        WHERE NOT (u)-[:BOUGHT]->(rec)
        WITH rec, COUNT(DISTINCT other) as score
        ORDER BY score DESC
        LIMIT 10
        RETURN rec.id as event_id, rec.pavadinimas as title, score
    """, {"user_id": user_id})
    print(f"   Found {len(all_recs)} recommendations (without date filter)")
    if all_recs:
        for r in all_recs:
            print(f"     - {r['event_id']}: {r.get('title', 'N/A')} (score: {r['score']})")
    else:
        print("   ❌ No recommendations found!")
        print("   Possible reasons:")
        print("     - User bought ALL events in system")
        print("     - No other users bought same events")
        return

    # 6. Test with date filter (12 months)
    print("\n6️⃣  UPCOMING RECOMMENDATIONS (12 months):")
    upcoming_recs = neo.recommend_collaborative_upcoming(user_id)
    print(f"   Found {len(upcoming_recs)} upcoming recommendations")
    if upcoming_recs:
        for r in upcoming_recs:
            print(f"     - {r.get('event_id')}: {r.get('title')} | {r.get('event_date')}")
    else:
        print("   ❌ No upcoming recommendations!")
        if all_recs:
            print("   → All recommended events are >12 months away or have NULL dates")
            print("\n   Event dates from step 5:")
            for r in all_recs[:3]:
                event_info = neo._run_query(
                    "MATCH (e:Event {id: $id}) RETURN e.data as date",
                    {"id": r['event_id']}
                )
                if event_info:
                    print(f"     {r['event_id']}: {event_info[0].get('date', 'NULL')}")

    # 7. Summary
    print("\n" + "="*60)
    print("📊 SUMMARY:")
    print(f"   MongoDB orders: {len(orders)}")
    print(f"   Neo4j user exists: {len(user) > 0}")
    print(f"   Neo4j BOUGHT relationships: {len(purchases)}")
    print(f"   has_purchase_history(): {has_history}")
    print(f"   Recommendations (all): {len(all_recs)}")
    print(f"   Recommendations (upcoming): {len(upcoming_recs)}")
    
    if len(upcoming_recs) == 0:
        print("\n🔧 RECOMMENDED FIX:")
        if len(all_recs) > 0:
            print("   → Change 12 months to 24 months in graph.py")
            print("   → Or remove date filter entirely")
        elif len(purchases) == 0:
            print("   → Check purchase.py calls neo4.add_purchase()")
        else:
            print("   → Need more users/events in database")
    print("="*60)

if __name__ == "__main__":
    main()

    