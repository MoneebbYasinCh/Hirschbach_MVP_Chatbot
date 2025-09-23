#!/usr/bin/env python3
"""
Test New BM25 Search with Updated File
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'Search'))

from Search.bm25_search import BM25UniqueValuesSearch

def test_new_bm25():
    """Test the new BM25 search with the updated file"""
    print("🧪 Testing New BM25 Search with Updated File")
    print("=" * 50)
    
    # Initialize BM25 search
    bm25_search = BM25UniqueValuesSearch()
    
    # Load from new CSV file
    csv_path = os.path.join(os.path.dirname(__file__), 'Data', 'unique_values_bm25.csv')
    print(f"Loading from: {csv_path}")
    
    try:
        bm25_search.load_csv_and_build_index(csv_path)
        print(f"✅ Successfully loaded {len(bm25_search.documents)} documents")
        print(f"✅ Indexed {len(bm25_search.term_frequencies)} unique terms")
    except Exception as e:
        print(f"❌ Error loading CSV: {str(e)}")
        return False
    
    # Test searches
    print("\n🔍 Testing Searches:")
    print("-" * 30)
    
    # Test 1: Search for state codes
    print("\n1. Searching for 'CA' (California):")
    results = bm25_search.search("CA", top_k=5)
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    # Test 2: Search with column filter
    print("\n2. Searching for 'TX' in Claim State column:")
    results = bm25_search.search("TX", top_k=5, column_filter="Claim State")
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    # Test 3: Search for accident types
    print("\n3. Searching for 'Crash' in Accident Type:")
    results = bm25_search.search("Crash", top_k=5, column_filter="Accident Type")
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    # Test 4: Search for cargo flags
    print("\n4. Searching for 'Y' in Cargo Claim Flag:")
    results = bm25_search.search("Y", top_k=5, column_filter="Cargo Claim Flag")
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    # Test 5: Fuzzy search
    print("\n5. Fuzzy search for 'california' (should find CA):")
    results = bm25_search.search("california", top_k=5)
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.column_name} = '{result.value}' (score: {result.score:.3f})")
    
    # Test 6: Get all values for a column
    print("\n6. All values for 'Claim State' column:")
    values = bm25_search.get_column_values("UNKNOWN", "Claim State")
    print(f"   Found {len(values)} values: {', '.join(values[:10])}{'...' if len(values) > 10 else ''}")
    
    print("\n✅ All tests completed successfully!")
    return True

if __name__ == "__main__":
    test_new_bm25()
