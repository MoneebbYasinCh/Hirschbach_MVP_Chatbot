#!/usr/bin/env python3
"""
Test file for SQL Generation Node
Tests the SQL generation with placeholders and BM25 value replacement
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

from Nodes.sql_gen import SQLGenerationNode

def test_environment_setup():
    """Test if environment variables are properly set"""
    print("🔧 [TEST] Checking environment setup...")
    
    required_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY", 
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_API_VERSION"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ [TEST] Missing environment variables: {missing_vars}")
        return False
    else:
        print("✅ [TEST] All environment variables are set")
        return True

def test_node_initialization():
    """Test if the SQL generation node initializes properly"""
    print("\n🔧 [TEST] Testing node initialization...")
    
    try:
        node = SQLGenerationNode()
        print("✅ [TEST] SQL Generation node initialized successfully")
        return True
    except Exception as e:
        print(f"❌ [TEST] Failed to initialize SQL Generation node: {str(e)}")
        return False

def create_test_state_with_metadata():
    """Create a test state with metadata for SQL generation"""
    from langchain_core.messages import HumanMessage
    
    return {
        "messages": [HumanMessage(content="Show me closed claims in California this year")],
        "metadata_rag_results": [
            {
                "column_name": "state",
                "description": "State where claim occurred",
                "data_type": "varchar",
                "score": 0.92,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "claim_status",
                "description": "Current status of the claim",
                "data_type": "varchar",
                "score": 0.88,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "claim_date",
                "description": "Date when claim was filed",
                "data_type": "date",
                "score": 0.90,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "total_incurred",
                "description": "Total amount incurred for the claim",
                "data_type": "decimal",
                "score": 0.85,
                "primary_key": "",
                "foreign_key": ""
            }
        ],
        "llm_check_result": {
            "decision_type": "not_relevant",
            "reasoning": "No relevant KPI found, generate new SQL",
            "confidence": "HIGH"
        }
    }

def create_test_state_without_metadata():
    """Create a test state without metadata"""
    from langchain_core.messages import HumanMessage
    
    return {
        "messages": [HumanMessage(content="Show me claims data")],
        "metadata_rag_results": [],
        "llm_check_result": {}
    }

def create_test_state_without_messages():
    """Create a test state without messages"""
    return {
        "metadata_rag_results": [
            {
                "column_name": "state",
                "description": "State where claim occurred",
                "data_type": "varchar",
                "score": 0.92
            }
        ]
    }

def test_sql_generation_with_valid_data():
    """Test SQL generation with valid metadata"""
    print("\n🔧 [TEST] Testing SQL generation with valid data...")
    
    try:
        node = SQLGenerationNode()
        state = create_test_state_with_metadata()
        
        print(f"[TEST] User query: {state['messages'][0].content}")
        print(f"[TEST] Metadata columns: {len(state['metadata_rag_results'])}")
        
        result_state = node(state)
        
        # Check if state was updated properly
        assert "sql_generation_status" in result_state, "sql_generation_status should be set"
        assert "generated_sql" in result_state, "generated_sql should be set"
        assert "sql_generation_result" in result_state, "sql_generation_result should be set"
        
        if result_state["sql_generation_status"] == "completed":
            print("✅ [TEST] SQL generation completed successfully")
            print(f"[TEST] Generated SQL: {result_state['generated_sql'][:100]}...")
            
            # Check if SQL was generated
            assert "generated_sql" in result_state, "Should have generated SQL"
            assert len(result_state["generated_sql"]) > 0, "Generated SQL should not be empty"
            
            return True
        else:
            print(f"❌ [TEST] SQL generation failed: {result_state.get('sql_generation_error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error during SQL generation test: {str(e)}")
        return False

def test_sql_generation_without_metadata():
    """Test SQL generation without metadata"""
    print("\n🔧 [TEST] Testing SQL generation without metadata...")
    
    try:
        node = SQLGenerationNode()
        state = create_test_state_without_metadata()
        
        result_state = node(state)
        
        # Should handle missing metadata gracefully
        assert result_state["sql_generation_status"] == "error", "Should set error status for missing metadata"
        assert "sql_generation_error" in result_state, "Should set error message"
        
        print("✅ [TEST] SQL generation handled missing metadata correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during missing metadata test: {str(e)}")
        return False

def test_sql_generation_without_messages():
    """Test SQL generation without messages"""
    print("\n🔧 [TEST] Testing SQL generation without messages...")
    
    try:
        node = SQLGenerationNode()
        state = create_test_state_without_messages()
        
        result_state = node(state)
        
        # Should handle missing messages gracefully
        assert result_state["sql_generation_status"] == "error", "Should set error status for missing messages"
        assert "sql_generation_error" in result_state, "Should set error message"
        
        print("✅ [TEST] SQL generation handled missing messages correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during missing messages test: {str(e)}")
        return False

def test_metadata_formatting():
    """Test the metadata formatting helper method"""
    print("\n🔧 [TEST] Testing metadata formatting...")
    
    try:
        node = SQLGenerationNode()
        
        test_metadata = [
            {
                "column_name": "state",
                "description": "State where claim occurred",
                "data_type": "varchar",
                "score": 0.92
            },
            {
                "column_name": "claim_date",
                "description": "Date when claim was filed",
                "data_type": "date",
                "score": 0.90
            }
        ]
        
        formatted = node._format_metadata_for_sql_generation(test_metadata)
        
        assert "state (varchar)" in formatted, "Should include column name and type"
        assert "claim_date (date)" in formatted, "Should include different data types"
        
        print("✅ [TEST] Metadata formatting works correctly")
        print(f"[TEST] Formatted metadata preview:\n{formatted[:200]}...")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during metadata formatting test: {str(e)}")
        return False

def test_column_validation():
    """Test column validation functionality"""
    print("\n🔧 [TEST] Testing column validation...")
    
    try:
        node = SQLGenerationNode()
        
        # Test SQL with valid columns
        valid_sql = "SELECT state, claim_status FROM claims_summary WHERE state = 'CA'"
        available_columns = ['state', 'claim_status', 'claim_date']
        
        result = node._validate_sql_columns(valid_sql, available_columns)
        assert "INVALID COLUMN" not in result, "Should not find invalid columns"
        
        # Test SQL with invalid columns
        invalid_sql = "SELECT state, invalid_column FROM claims_summary WHERE state = 'CA'"
        result = node._validate_sql_columns(invalid_sql, available_columns)
        assert "INVALID COLUMN" in result, "Should detect invalid columns"
        
        print("✅ [TEST] Column validation works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during column validation test: {str(e)}")
        return False

def test_filter_values_validation():
    """Test filter values validation against unique values database"""
    print("\n🔧 [TEST] Testing filter values validation...")
    
    try:
        node = SQLGenerationNode()
        
        # Test SQL with valid filter values (assuming BM25 index is loaded)
        valid_sql = "SELECT state FROM claims_summary WHERE state = 'CA'"
        metadata = [{"column_name": "state", "data_type": "varchar"}]
        result = node._validate_filter_values(valid_sql, metadata)
        
        # Should not have any invalid value comments
        assert "INVALID VALUE" not in result, "Should not find invalid filter values"
        
        # Test SQL with potentially invalid filter values
        invalid_sql = "SELECT state FROM claims_summary WHERE state = 'NonExistentState'"
        result = node._validate_filter_values(invalid_sql, metadata)
        
        # Should either find a match or comment out the line
        assert "INVALID VALUE" in result or "NO MATCH FOUND" in result, "Should detect invalid filter values"
        
        print("✅ [TEST] Filter values validation works correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during filter values validation test: {str(e)}")
        return False

def test_placeholder_replacement():
    """Test placeholder replacement functionality"""
    print("\n🔧 [TEST] Testing placeholder replacement...")
    
    try:
        node = SQLGenerationNode()
        
        # Test SQL with placeholders
        test_sql = """
        SELECT state, COUNT(*) as claim_count
        FROM claims_summary
        WHERE state = {PLACEHOLDER:state:California}
        AND claim_status = {PLACEHOLDER:claim_status:closed}
        AND claim_date >= '2024-01-01'
        GROUP BY state
        """
        
        # Test the replacement method
        metadata = [{"column_name": "state", "data_type": "varchar"}]
        final_sql = node._replace_placeholders_with_bm25(test_sql, "test query", metadata)
        
        # Should have replaced placeholders with actual values
        assert "{PLACEHOLDER" not in final_sql, "Should replace all placeholders"
        # Should contain either the original terms or BM25 matches
        assert "California" in final_sql or "california" in final_sql.lower() or "CA" in final_sql, "Should contain search terms or matches"
        
        print("✅ [TEST] Placeholder replacement works correctly")
        print(f"[TEST] Final SQL: {final_sql[:100]}...")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during placeholder replacement test: {str(e)}")
        return False

def test_sql_extraction():
    """Test SQL extraction from LLM response"""
    print("\n🔧 [TEST] Testing SQL extraction...")
    
    try:
        node = SQLGenerationNode()
        
        # Test response with SQL block
        test_response = """
        Here's the SQL query for your request:
        
        ```sql
        SELECT state, COUNT(*) as claim_count
        FROM claims_summary
        WHERE state = {PLACEHOLDER:state:California}
        AND claim_status = {PLACEHOLDER:claim_status:closed}
        GROUP BY state
        ```
        
        This query will show closed claims by state.
        """
        
        extracted_sql = node._extract_sql_from_response(test_response)
        
        assert "SELECT state" in extracted_sql, "Should extract SELECT statement"
        assert "claims_summary" in extracted_sql, "Should include table name"
        assert "{PLACEHOLDER" in extracted_sql, "Should preserve placeholders"
        
        print("✅ [TEST] SQL extraction works correctly")
        print(f"[TEST] Extracted SQL: {extracted_sql[:100]}...")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during SQL extraction test: {str(e)}")
        return False

def run_all_tests():
    """Run all tests and report results"""
    print("🚀 [TEST] Starting SQL Generation Node Tests")
    print("=" * 50)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Node Initialization", test_node_initialization),
        ("Metadata Formatting", test_metadata_formatting),
        ("Column Validation", test_column_validation),
        ("Filter Values Validation", test_filter_values_validation),
        ("Placeholder Replacement", test_placeholder_replacement),
        ("SQL Extraction", test_sql_extraction),
        ("Valid Data Processing", test_sql_generation_with_valid_data),
        ("Missing Metadata Handling", test_sql_generation_without_metadata),
        ("Missing Messages Handling", test_sql_generation_without_messages)
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
    
    print("\n" + "=" * 50)
    print(f"📊 [TEST] Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 [TEST] All tests passed! SQL Generation node is working correctly.")
    else:
        print(f"⚠️ [TEST] {total - passed} tests failed. Please check the implementation.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
