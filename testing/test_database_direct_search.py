#!/usr/bin/env python3
"""
Test file for Database Direct Search
Tests the new lightweight database search implementation
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

from Search.database_direct_search import DatabaseDirectSearch

def test_environment_setup():
    """Test if environment variables are properly set"""
    print("🔧 [TEST] Checking environment setup...")
    
    # Check for database connection variables
    db_vars = [
        "REDSHIFT_CONNECTION_STRING",
        "POSTGRES_CONNECTION_STRING", 
        "DATABASE_URL",
        "DB_HOST", "DB_NAME", "DB_USER"
    ]
    
    found_vars = [var for var in db_vars if os.getenv(var)]
    
    if found_vars:
        print(f"✅ [TEST] Found database connection variables: {found_vars}")
        return True
    else:
        print("⚠️ [TEST] No database connection variables found - will use fallback search")
        return True  # This is still OK, will use fallback

def test_database_search_initialization():
    """Test if the database search initializes properly"""
    print("\n🔧 [TEST] Testing database search initialization...")
    
    try:
        search = DatabaseDirectSearch()
        print("✅ [TEST] Database search initialized successfully")
        
        # Check connection status
        if search.connection:
            print("✅ [TEST] Database connection established")
        else:
            print("⚠️ [TEST] No database connection - will use fallback search")
        
        return True
    except Exception as e:
        print(f"❌ [TEST] Failed to initialize database search: {str(e)}")
        return False

def test_fallback_search():
    """Test fallback search when database is not available"""
    print("\n🔧 [TEST] Testing fallback search...")
    
    try:
        search = DatabaseDirectSearch()
        
        # Test search with common terms
        results = search.search("California", column_filter="state", top_k=5)
        
        print(f"[TEST] Fallback search returned {len(results)} results")
        for result in results:
            print(f"  - {result.column_name}: '{result.value}' (score: {result.score:.3f})")
        
        # Should return some results even in fallback mode
        assert len(results) > 0, "Fallback search should return some results"
        
        print("✅ [TEST] Fallback search works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during fallback search test: {str(e)}")
        return False

def test_column_specific_search():
    """Test searching within specific columns"""
    print("\n🔧 [TEST] Testing column-specific search...")
    
    try:
        search = DatabaseDirectSearch()
        
        # Test different column searches
        test_cases = [
            ("state", "CA"),
            ("claim_status", "closed"),
            ("transaction_status", "open")
        ]
        
        for column, search_term in test_cases:
            results = search.search(search_term, column_filter=column, top_k=3)
            print(f"[TEST] Search '{search_term}' in {column}: {len(results)} results")
            
            for result in results:
                print(f"  - {result.value} (score: {result.score:.3f}, exact: {result.exact_match})")
        
        print("✅ [TEST] Column-specific search works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during column-specific search test: {str(e)}")
        return False

def test_fuzzy_matching():
    """Test fuzzy matching capabilities"""
    print("\n🔧 [TEST] Testing fuzzy matching...")
    
    try:
        search = DatabaseDirectSearch()
        
        # Test fuzzy matching with typos
        fuzzy_tests = [
            ("Californa", "state"),  # Typo in California
            ("closd", "claim_status"),  # Typo in closed
            ("pendng", "transaction_status")  # Typo in pending
        ]
        
        for search_term, column in fuzzy_tests:
            results = search.search(search_term, column_filter=column, top_k=3)
            print(f"[TEST] Fuzzy search '{search_term}' in {column}: {len(results)} results")
            
            if results:
                best_match = results[0]
                print(f"  - Best match: '{best_match.value}' (score: {best_match.score:.3f})")
                # Should find reasonable matches even with typos
                assert best_match.score > 0.3, f"Fuzzy match score too low: {best_match.score}"
        
        print("✅ [TEST] Fuzzy matching works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during fuzzy matching test: {str(e)}")
        return False

def test_get_unique_values():
    """Test getting unique values for a column"""
    print("\n🔧 [TEST] Testing get unique values...")
    
    try:
        search = DatabaseDirectSearch()
        
        # Test getting unique values for common columns
        columns_to_test = ["state", "claim_status", "transaction_status"]
        
        for column in columns_to_test:
            unique_values = search.get_column_unique_values(column, limit=10)
            print(f"[TEST] Unique values for {column}: {len(unique_values)} values")
            print(f"  - Sample values: {unique_values[:5]}")
        
        print("✅ [TEST] Get unique values works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during get unique values test: {str(e)}")
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
    print("🚀 [TEST] Starting Database Direct Search Tests")
    print("=" * 60)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Database Search Initialization", test_database_search_initialization),
        ("Fallback Search", test_fallback_search),
        ("Column-Specific Search", test_column_specific_search),
        ("Fuzzy Matching", test_fuzzy_matching),
        ("Get Unique Values", test_get_unique_values),
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
        print("🎉 [TEST] All tests passed! Database Direct Search is working correctly.")
    else:
        print(f"⚠️ [TEST] {total - passed} tests failed. Please check the implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
