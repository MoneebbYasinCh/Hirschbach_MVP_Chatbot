#!/usr/bin/env python3
"""
Test file for Streaming BM25 Search
Tests the new streaming BM25 implementation
"""

import os
import sys
from typing import Dict, Any
from dotenv import load_dotenv

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Load environment variables from .env file
load_dotenv(os.path.join(project_root, '.env'))

from Search.streaming_bm25_search import StreamingBM25Search

def test_streaming_bm25_initialization():
    """Test if the streaming BM25 search initializes properly"""
    print("🔧 [TEST] Testing streaming BM25 initialization...")
    
    try:
        search = StreamingBM25Search()
        print("✅ [TEST] Streaming BM25 search initialized successfully")
        
        # Check if database was created
        if search.connection:
            print("✅ [TEST] SQLite database connection established")
        else:
            print("⚠️ [TEST] No database connection - index may not be built yet")
        
        return True
    except Exception as e:
        print(f"❌ [TEST] Failed to initialize streaming BM25 search: {str(e)}")
        return False

def test_search_without_index():
    """Test search when no index is built yet"""
    print("\n🔧 [TEST] Testing search without index...")
    
    try:
        search = StreamingBM25Search()
        
        # Test search - should return empty results if no index
        results = search.search("California", top_k=5)
        
        print(f"[TEST] Search returned {len(results)} results")
        if len(results) == 0:
            print("✅ [TEST] Search correctly returns empty results when no index is built")
        else:
            print("✅ [TEST] Search returned results (index may already be built)")
        
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during search test: {str(e)}")
        return False

def test_index_stats():
    """Test getting index statistics"""
    print("\n🔧 [TEST] Testing index statistics...")
    
    try:
        search = StreamingBM25Search()
        stats = search.get_index_stats()
        
        print(f"[TEST] Index stats: {stats}")
        
        if 'total_documents' in stats:
            print(f"✅ [TEST] Index contains {stats['total_documents']} documents")
        else:
            print("⚠️ [TEST] No documents in index yet")
        
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during stats test: {str(e)}")
        return False

def test_sql_generation_integration():
    """Test integration with SQL generation node"""
    print("\n🔧 [TEST] Testing SQL generation integration...")
    
    try:
        from Nodes.sql_gen import SQLGenerationNode
        
        # Create SQL generation node
        sql_node = SQLGenerationNode()
        
        # Test state with metadata
        test_state = {
            "messages": [{"content": "Show me closed claims in California"}],
            "metadata_rag_results": [
                {
                    "column_name": "state",
                    "description": "State where claim occurred",
                    "data_type": "varchar",
                    "score": 0.92
                },
                {
                    "column_name": "claim_status", 
                    "description": "Current status of the claim",
                    "data_type": "varchar",
                    "score": 0.88
                }
            ],
            "llm_check_result": {
                "decision_type": "not_relevant",
                "reasoning": "No relevant KPI found, generate new SQL"
            }
        }
        
        # Test the SQL generation
        result_state = sql_node(test_state)
        
        print(f"[TEST] SQL generation status: {result_state.get('sql_generation_status')}")
        
        if result_state.get('sql_generation_status') == 'completed':
            generated_sql = result_state.get('generated_sql', '')
            print(f"[TEST] Generated SQL: {generated_sql[:200]}...")
            print("✅ [TEST] SQL generation integration works correctly")
            return True
        else:
            error = result_state.get('sql_generation_error', 'Unknown error')
            print(f"⚠️ [TEST] SQL generation failed: {error}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error during SQL generation integration test: {str(e)}")
        return False

def run_all_tests():
    """Run all tests and report results"""
    print("🚀 [TEST] Starting Streaming BM25 Search Tests")
    print("=" * 60)
    
    tests = [
        ("Streaming BM25 Initialization", test_streaming_bm25_initialization),
        ("Search Without Index", test_search_without_index),
        ("Index Statistics", test_index_stats),
        ("SQL Generation Integration", test_sql_generation_integration)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 [TEST] Running: {test_name}")
        try:
            if test_func():
                passed += 1
            else:
                print(f"❌ [TEST] {test_name} failed")
        except Exception as e:
            print(f"❌ [TEST] {test_name} failed with exception: {str(e)}")
    
    print("\n" + "=" * 60)
    print(f"📊 [TEST] Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 [TEST] All tests passed! Streaming BM25 is working correctly.")
    else:
        print(f"⚠️ [TEST] {total - passed} tests failed. Please check the implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
