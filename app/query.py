#!/usr/bin/env python3

import sys
import math
import re

from pyspark import SparkConf, SparkContext
from cassandra.cluster import Cluster

K1 = 1.2
B = 0.75

def tokenize(text):
    return re.findall(r'[a-zA-Z0-9]+', text.lower())

def compute_bm25(freq, doc_len, df, n, avg_dl):
    """
    freq: term frequency in doc
    doc_len: length of doc
    df: doc frequency for this term
    n: total number of docs
    avg_dl: average doc length
    """
    idf = math.log((n - df + 0.5) / (df + 0.5))

    # TF-based factor
    numerator = freq * (K1 + 1)
    denominator = freq + K1*(1 - B + B*(doc_len/avg_dl))
    return idf * (numerator / denominator)


def main():
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py <query terms...>")
        sys.exit(0)

    query_str = " ".join(sys.argv[1:])
    print(f"Query = {query_str}")

    conf = SparkConf().setAppName("bm25Query")
    sc = SparkContext(conf=conf)

    cluster = Cluster(['cassandra-server'])
    session = cluster.connect("search_keyspace")

    rows_doclen = session.execute("SELECT doc_id, length FROM doc_length")
    doclen_map = {}
    for row in rows_doclen:
        doclen_map[row.doc_id] = row.length

    rows_stats = session.execute("SELECT term, df, n, avg_doc_length FROM statistics")
    df_map = {}
    N = 0
    avg_dl = 0.0

    for row in rows_stats:
        df_map[row.term] = (row.df or 0)
        N = row.n
        avg_dl = row.avg_doc_length

    rows_index = session.execute("SELECT term, doc_id, freq FROM inverted_index")
    index_data = []
    for row in rows_index:
        index_data.append((row.term, row.doc_id, row.freq))

    cluster.shutdown()

    rdd_index = sc.parallelize(index_data)

    query_tokens = tokenize(query_str)
    query_token_set = set(query_tokens)

    bc_doclen = sc.broadcast(doclen_map)
    bc_df_map = sc.broadcast(df_map)
    bc_N = sc.broadcast(N)
    bc_avgdl = sc.broadcast(avg_dl)

    rdd_filtered = rdd_index.filter(lambda x: x[0] in query_token_set)

    def calc_bm25_for_term_doc(row):
        term, doc_id, freq = row
        doclen = bc_doclen.value.get(doc_id, 0)
        df_val = bc_df_map.value.get(term, 1)
        score = compute_bm25(freq, doclen, df_val, bc_N.value, bc_avgdl.value)
        return (doc_id, score)

    rdd_scores = rdd_filtered.map(calc_bm25_for_term_doc)

    rdd_doc_scores = rdd_scores.reduceByKey(lambda a, b: a + b)

    top10 = rdd_doc_scores.takeOrdered(10, key=lambda x: -x[1])

    print("Top 10 relevant documents (doc_id, BM25):")
    for (doc_id, score) in top10:
        print(f"{doc_id}\t{score}")

    sc.stop()


if __name__ == "__main__":
    main()
