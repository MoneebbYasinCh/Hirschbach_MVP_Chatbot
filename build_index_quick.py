#!/usr/bin/env python3
"""
Quick script to build the streaming BM25 index
"""

import os
import sys

# Add the project root to the path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from Search.streaming_bm25_search import StreamingBM25Search

def main():
    """Build the streaming BM25 index quickly"""
    print("🔄 Building Streaming BM25 Index...")
    
    # Initialize streaming BM25 search
    streaming_bm25 = StreamingBM25Search()
    
    # Path to your CSV file with unique values
    csv_path = os.path.join(project_root, 'Data', 'unique_values_bm25.csv')
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return
    
    print(f"📊 Building index from: {csv_path}")
    
    # Build the index (this will create a SQLite database)
    streaming_bm25.build_index_from_csv(csv_path)
    
    # Get and display index statistics
    stats = streaming_bm25.get_index_stats()
    print(f"\n📈 Index Statistics:")
    print(f"  - Total documents: {stats.get('total_documents', 0)}")
    print(f"  - Unique terms: {stats.get('unique_terms', 0)}")
    print(f"  - Average document length: {stats.get('avg_document_length', 0):.2f}")
    
    # Test the index
    print("\n🧪 Testing the index...")
    
    # Test search for common terms
    test_queries = ["CA", "California", "OPEN", "CLOSED", "TX"]
    
    for query in test_queries:
        print(f"\nSearching for '{query}':")
        results = streaming_bm25.search(query, top_k=3)
        for result in results:
            print(f"  {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    print(f"\n✅ Streaming BM25 index built successfully!")
    print(f"📁 Index database: {streaming_bm25.index_db_path}")
    
    # Close the connection
    streaming_bm25.close()

if __name__ == "__main__":
    main()
