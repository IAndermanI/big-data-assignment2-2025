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
    filename="stats_processor.log",
    filemode="w",
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger("stats_processor")

class StatsProcessor:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.term_documents = defaultdict(set)
        self.total_docs = set()
        
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
        
    def create_stats_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS statistics (
                term text PRIMARY KEY,
                df int,
                n int,
                avg_doc_length float
            )
        """)
        
    def process_term_doc_pair(self, term, doc_id):
        self.term_documents[term].add(doc_id)
        self.total_docs.add(doc_id)
        
    def calculate_statistics(self):
        log.info("Calculating document statistics...")
        rows = self.session.execute("SELECT doc_id, length FROM doc_length")
        doc_lengths = {row.doc_id: row.length for row in rows}
        
        total_length = sum(doc_lengths.values())
        doc_count = len(doc_lengths)
        avg_length = total_length / doc_count if doc_count else 0
        
        log.info(f"Processed {doc_count} documents with average length {avg_length:.2f}")
        return doc_count, avg_length
        
    def save_statistics(self, total_docs, avg_length):
        log.info("Saving term statistics...")
        for term, docs in self.term_documents.items():
            try:
                df = len(docs)
                self.session.execute("""
                    INSERT INTO statistics (term, df, n, avg_doc_length)
                    VALUES (%s, %s, %s, %s)
                """, (term, df, total_docs, avg_length))
            except Exception as e:
                log.error(f"Failed to save statistics for term {term}: {e}")
                traceback.print_exc(file=sys.stderr)

def main():
    processor = StatsProcessor()
    try:
        processor.connect_to_cassandra()
        processor.setup_keyspace()
        processor.create_stats_table()
        
        log.info("Processing term-document pairs...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            try:
                term, doc_id = line.split('\t')
                processor.process_term_doc_pair(term, doc_id)
            except Exception as e:
                log.error(f"Error processing line: {e}")
                traceback.print_exc(file=sys.stderr)
                
        total_docs, avg_length = processor.calculate_statistics()
        processor.save_statistics(total_docs, avg_length)
        log.info("Statistics processing completed successfully")
        
    except Exception as e:
        log.error(f"Fatal error: {e}")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
