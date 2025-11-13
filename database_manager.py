import os
import json
import hashlib
from typing import Dict, Optional, Any
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client

class MultiFirestoreManager:
    def __init__(self):
        self.auth_app = None
        self.firestore_clients: Dict[str, Client] = {}
        self.database_count = 0
        self._initialize_databases()
    
    def _initialize_databases(self):
        """Initialize all Firestore databases from service account files"""
        # Initialize main auth project using FIREBASE_SERVICE_ACCOUNT_JSON
        firebase_creds_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        auth_cred = None
        
        if firebase_creds_json:
            try:
                auth_cred = credentials.Certificate(json.loads(firebase_creds_json))
                print("Using Firebase credentials from environment variable")
            except Exception as e:
                print(f"Failed to parse Firebase credentials from env var: {e}")
        
        # Fallback to file if env var failed or doesn't exist
        if not auth_cred:
            auth_cred_path = 'firebase-service-account.json'
            if os.path.exists(auth_cred_path):
                try:
                    auth_cred = credentials.Certificate(auth_cred_path)
                    print("Using Firebase credentials from local file")
                except Exception as e:
                    print(f"Failed to load Firebase credentials from file: {e}")
        
        if auth_cred:
            try:
                # Initialize default app for auth
                self.auth_app = firebase_admin.initialize_app(auth_cred)
                
                # Use same credentials for main database
                main_app = firebase_admin.initialize_app(auth_cred, name='main_db')
                self.firestore_clients['main'] = firestore.client(main_app)
                self.database_count += 1
                print(f"✓ Firebase Auth and Firestore initialized successfully")
            except Exception as e:
                print(f"Failed to initialize Firebase: {e}")
        else:
            print("✗ No Firebase credentials found")
        
        # Initialize additional databases from environment variables
        if auth_cred:  # Only try additional DBs if main auth worked
            for i in range(1, 10):
                db_cred_json = os.getenv(f'DB{i}_CREDENTIALS')
                if db_cred_json:
                    try:
                        db_name = f'db{i}'
                        cred = credentials.Certificate(json.loads(db_cred_json))
                        app = firebase_admin.initialize_app(cred, name=f'db_{db_name}')
                        self.firestore_clients[db_name] = firestore.client(app)
                        self.database_count += 1
                        print(f"Initialized additional Firestore database: {db_name}")
                    except Exception as e:
                        print(f"Failed to initialize DB{i}: {e}")
            
            # Auto-duplicate main database if no additional databases found
            if self.database_count == 1:
                print("Only 1 database found, creating additional connections for load balancing")
                try:
                    # Create 2 more connections to the same database for load distribution
                    for i in range(2, 4):
                        app_name = f'main_db_{i}'
                        dup_app = firebase_admin.initialize_app(auth_cred, name=app_name)
                        self.firestore_clients[f'main_{i}'] = firestore.client(dup_app)
                        self.database_count += 1
                        print(f"Created duplicate connection: main_{i}")
                except Exception as e:
                    print(f"Failed to create duplicate connections: {e}")
            
            # Fallback: Initialize from local files if no additional env vars
            if self.database_count <= 3:  # If we don't have enough databases
                db_dir = 'firestore_credentials'
                if os.path.exists(db_dir):
                    for filename in os.listdir(db_dir):
                        if filename.endswith('.json'):
                            db_name = filename.replace('.json', '')
                            cred_path = os.path.join(db_dir, filename)
                            try:
                                cred = credentials.Certificate(cred_path)
                                app = firebase_admin.initialize_app(cred, name=f'db_{db_name}')
                                self.firestore_clients[db_name] = firestore.client(app)
                                self.database_count += 1
                                print(f"Initialized local Firestore database: {db_name}")
                            except Exception as e:
                                print(f"Failed to initialize {db_name}: {e}")
    
    def get_database_for_user(self, user_id: str) -> Optional[Client]:
        """Select database based on user ID hash for load balancing with failover"""
        if not self.firestore_clients:
            return None
        
        # Use hash of user_id to consistently assign users to databases
        hash_value = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        db_names = list(self.firestore_clients.keys())
        
        # Try primary database first
        primary_index = hash_value % len(db_names)
        primary_db = db_names[primary_index]
        
        # Return primary database (failover handled in operations)
        return self.firestore_clients[primary_db]
    
    def _execute_with_failover(self, operation, user_email: str, *args, **kwargs):
        """Execute database operation with automatic failover"""
        if not self.firestore_clients:
            return None
        
        # Get all available databases
        db_names = list(self.firestore_clients.keys())
        hash_value = int(hashlib.md5(user_email.encode()).hexdigest(), 16)
        primary_index = hash_value % len(db_names)
        
        # Try databases in order: primary first, then others
        ordered_dbs = [db_names[primary_index]] + [db for i, db in enumerate(db_names) if i != primary_index]
        
        for db_name in ordered_dbs:
            try:
                db = self.firestore_clients[db_name]
                result = operation(db, *args, **kwargs)
                return result
            except Exception as e:
                print(f"Database {db_name} failed for {user_email}: {e}")
                if "429" in str(e) or "quota" in str(e).lower():
                    print(f"Quota exceeded on {db_name}, trying next database")
                    continue
                elif db_name == ordered_dbs[-1]:  # Last database
                    print(f"All databases failed for {user_email}")
                    return None
                continue
        return None
    
    def get_auth_app(self):
        """Get the main Firebase Auth app"""
        return self.auth_app
    
    def save_user_data(self, user_email: str, data: dict) -> bool:
        """Save user data to assigned Firestore database with failover"""
        def _save_operation(db, email, user_data):
            doc_ref = db.collection('users').document(email)
            doc_ref.set(user_data, merge=True)
            return True
        
        result = self._execute_with_failover(_save_operation, user_email, user_email, data)
        return result is not None
    
    def get_user_data(self, user_email: str) -> Optional[dict]:
        """Get user data from assigned Firestore database with failover"""
        def _get_operation(db, email):
            doc_ref = db.collection('users').document(email)
            doc = doc_ref.get()
            return doc.to_dict() if doc.exists else None
        
        return self._execute_with_failover(_get_operation, user_email, user_email)
    
    def update_user_data(self, user_email: str, updates: dict) -> bool:
        """Update user data in assigned Firestore database with failover"""
        def _update_operation(db, email, update_data):
            doc_ref = db.collection('users').document(email)
            doc_ref.update(update_data)
            return True
        
        result = self._execute_with_failover(_update_operation, user_email, user_email, updates)
        return result is not None
    
    def delete_user_data(self, user_email: str) -> bool:
        """Delete user data from assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return False
        try:
            doc_ref = db.collection('users').document(user_email)
            doc_ref.delete()
            return True
        except Exception as e:
            print(f"Error deleting user data: {e}")
            return False
    
    def save_user_tokens(self, user_email: str, tokens: dict) -> bool:
        """Save user drive tokens to assigned Firestore database with failover"""
        def _save_tokens_operation(db, email, token_data):
            doc_ref = db.collection('user_drive_tokens').document(email)
            doc_ref.set(token_data)
            return True
        
        result = self._execute_with_failover(_save_tokens_operation, user_email, user_email, tokens)
        return result is not None
    
    def get_user_tokens(self, user_email: str) -> dict:
        """Get user drive tokens from assigned Firestore database with failover"""
        def _get_tokens_operation(db, email):
            doc_ref = db.collection('user_drive_tokens').document(email)
            doc = doc_ref.get()
            return doc.to_dict() if doc.exists else {}
        
        result = self._execute_with_failover(_get_tokens_operation, user_email, user_email)
        return result if result is not None else {}

# Global instance
db_manager = MultiFirestoreManager()

# For backward compatibility
db = db_manager.get_database_for_user('default') if db_manager.database_count > 0 else None
