#!/usr/bin/env python3
import sys
import os
import re
from collections import defaultdict
import logging

# Configure logging with different format
logging.basicConfig(
    filename="doc_processor.log",
    filemode="w",
    level=logging.INFO,
    format="[%(levelname)s] %(message)s"
)
log = logging.getLogger("doc_processor")

class DocumentProcessor:
    def __init__(self):
        self.word_pattern = re.compile(r'\b\w+\b')
        self.doc_id = self._get_document_id()
        
    def _get_document_id(self):
        env_vars = ['mapreduce_map_input_file', 'map_input_file']
        for var in env_vars:
            if var in os.environ:
                return os.path.basename(os.environ[var]).split('.')[0]
        return 'unknown_doc'
    
    def extract_words(self, text):
        return self.word_pattern.findall(text.lower())
    
    def process_line(self, line):
        words = self.extract_words(line)
        return [(word, self.doc_id, 1) for word in words]

def main():
    processor = DocumentProcessor()
    log.info(f"Processing document: {processor.doc_id}")
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        for word, doc_id, count in processor.process_line(line):
            print(f"{word}\t{doc_id}\t{count}")

if __name__ == "__main__":
    main()
