#!/usr/bin/env python3
import sys
import os
import re
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    filename="stats_collector.log",
    filemode="w",
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger("stats_collector")

class StatsCollector:
    def __init__(self):
        self.word_pattern = re.compile(r'\b\w+\b')
        self.doc_id = self._get_document_id()
        self.processed_words = set()
        
    def _get_document_id(self):
        env_vars = ['mapreduce_map_input_file', 'map_input_file']
        for var in env_vars:
            if var in os.environ:
                return os.path.basename(os.environ[var]).split('.')[0]
        return 'unknown_doc'
    
    def extract_unique_words(self, text):
        return set(self.word_pattern.findall(text.lower()))
    
    def process_line(self, line):
        words = self.extract_unique_words(line)
        new_words = words - self.processed_words
        self.processed_words.update(words)
        return [(word, self.doc_id) for word in new_words]

def main():
    collector = StatsCollector()
    log.info(f"Collecting statistics for document: {collector.doc_id}")
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        for word, doc_id in collector.process_line(line):
            print(f"{word}\t{doc_id}")

if __name__ == "__main__":
    main()
