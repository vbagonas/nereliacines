from flask import Flask
from flask_cors import CORS
from datetime import datetime, date
from flask.json.provider import DefaultJSONProvider
from cassandra.util import Date
from neo4j.time import DateTime as Neo4jDateTime   
import uuid
from neo4j import GraphDatabase
from pathlib import Path
import yaml
import json
from collections import Counter


from backend.app.routes.health import health_bp
from backend.app.routes.auth import auth_bp
from backend.app.routes.events import events_bp
from backend.app.routes.purchase import purchase_bp
from backend.app.routes.cart import cart_bp
from backend.app.routes.analytics import analytics_bp
from backend.app.routes.questions import questions_bp
from backend.app.routes.recommendations import recommendations_bp  
from backend.app.routes.analytics_clickhouse import analytics_ch_bp




from backend.graph_db.mongo_to_neo_importer import MongoToNeoImporter


class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, Date):
            return str(obj)  # paprastas ir patikimas sprendimas

        # 👇 NEW: handle Neo4j DateTime objects
        if isinstance(obj, Neo4jDateTime):
            # Neo4j DateTime usually has iso_format(); fall back to str if not
            return obj.iso_format() if hasattr(obj, "iso_format") else str(obj)

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)

        return super().default(obj)

    
    def dumps(self, obj, **kwargs):
        # ensure_ascii=False leis rodyti lietuviÅ¡kas raides tiesiogiai
        kwargs.setdefault("ensure_ascii", False)
        return super().dumps(obj, **kwargs)
    

def create_app():
    app = Flask(__name__)
    app.json_provider_class = CustomJSONProvider
    app.json = app.json_provider_class(app)
    
    CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=False)
    
    # Register blueprints
    app.register_blueprint(health_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(events_bp)
    app.register_blueprint(purchase_bp)
    app.register_blueprint(cart_bp)
    app.register_blueprint(analytics_bp)
    app.register_blueprint(questions_bp)
    app.register_blueprint(recommendations_bp)  # ✅ ADDED
    app.register_blueprint(analytics_ch_bp) 

    
    # Importuojam iš Mongo → Neo4j serveriui startuojant
    mongo_importer = MongoToNeoImporter()
    mongo_importer.run()
    
    return app


if __name__ == "__main__":
    app = create_app()

    app.run(host="0.0.0.0", port=8080, debug=True)