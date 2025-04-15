#!/usr/bin/env python3
import sys
import traceback
import logging
from cassandra.cluster import Cluster
from collections import defaultdict
import socket
import time
import os

# Configure logging
logging.basicConfig(
    filename="index_builder.log",
    filemode="w",
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger("index_builder")

class IndexBuilder:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.doc_lengths = defaultdict(int)
        self.current_term = None
        self.current_doc = None
        self.current_count = 0
        
    def connect_to_cassandra(self):
        log.info("Establishing Cassandra connection...")
        
        # Get Cassandra host from environment or use default
        cassandra_host = os.environ.get('CASSANDRA_HOST', 'cassandra-server')
        log.info(f"Using Cassandra host: {cassandra_host}")
        
        # Try to connect with a timeout
        try:
            # First check if the host is reachable
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((cassandra_host, 9042))
            sock.close()
            
            if result != 0:
                log.error(f"Cannot connect to {cassandra_host}:9042")
                # Try localhost as fallback
                log.info("Trying localhost as fallback...")
                cassandra_host = 'localhost'
            
            # Connect to Cassandra
            log.info(f"Connecting to Cassandra at {cassandra_host}...")
            self.cluster = Cluster([cassandra_host], connect_timeout=30)
            self.session = self.cluster.connect()
            log.info("Successfully connected to Cassandra")
            
        except Exception as e:
            log.error(f"Failed to connect to Cassandra: {e}")
            # Try one more time with localhost
            try:
                log.info("Trying localhost as last resort...")
                self.cluster = Cluster(['localhost'], connect_timeout=30)
                self.session = self.cluster.connect()
                log.info("Successfully connected to Cassandra via localhost")
            except Exception as e2:
                log.error(f"Failed to connect to Cassandra via localhost: {e2}")
                raise Exception("Could not connect to Cassandra after multiple attempts")
        
    def setup_keyspace(self):
        keyspace = "search_keyspace"
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        self.session.set_keyspace(keyspace)
        
    def create_tables(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS inverted_index (
                term text,
                doc_id text,
                freq int,
                PRIMARY KEY (term, doc_id)
            )
        """)
        
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS doc_length (
                doc_id text PRIMARY KEY,
                length int
            )
        """)
        
    def process_term(self, term, doc_id, count):
        if term == self.current_term and doc_id == self.current_doc:
            self.current_count += count
        else:
            self.save_current_term()
            self.current_term = term
            self.current_doc = doc_id
            self.current_count = count
            
    def save_current_term(self):
        if self.current_term and self.current_doc:
            try:
                self.session.execute("""
                    INSERT INTO inverted_index (term, doc_id, freq)
                    VALUES (%s, %s, %s)
                """, (self.current_term, self.current_doc, self.current_count))
                self.doc_lengths[self.current_doc] += self.current_count
            except Exception as e:
                log.error(f"Failed to save term: {e}")
                traceback.print_exc(file=sys.stderr)
                
    def save_doc_lengths(self):
        log.info("Saving document lengths...")
        for doc_id, length in self.doc_lengths.items():
            try:
                self.session.execute("""
                    INSERT INTO doc_length (doc_id, length)
                    VALUES (%s, %s)
                """, (doc_id, length))
            except Exception as e:
                log.error(f"Failed to save doc length: {e}")
                traceback.print_exc(file=sys.stderr)

def main():
    builder = IndexBuilder()
    try:
        builder.connect_to_cassandra()
        builder.setup_keyspace()
        builder.create_tables()
        
        log.info("Processing input data...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            try:
                term, doc_id, count = line.split('\t')
                builder.process_term(term, doc_id, int(count))
            except Exception as e:
                log.error(f"Error processing line: {e}")
                traceback.print_exc(file=sys.stderr)
                
        builder.save_current_term()
        builder.save_doc_lengths()
        log.info("Index building completed successfully")
        
    except Exception as e:
        log.error(f"Fatal error: {e}")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
