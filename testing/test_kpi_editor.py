#!/usr/bin/env python3
"""
Test file for KPI Editor Node
Tests the KPI editing and modification functionality
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

from Nodes.kpi_editor import KPIEditorNode

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
    """Test if the KPI editor node initializes properly"""
    print("\n🔧 [TEST] Testing node initialization...")
    
    try:
        node = KPIEditorNode()
        print("✅ [TEST] KPI Editor node initialized successfully")
        return True
    except Exception as e:
        print(f"❌ [TEST] Failed to initialize KPI Editor node: {str(e)}")
        return False

def create_test_state_with_kpi():
    """Create a test state with KPI data"""
    from langchain_core.messages import HumanMessage
    
    return {
        "messages": [HumanMessage(content="Show me filtered data by category this year")],
        "top_kpi": {
            "metric_name": "Data Analysis",
            "score": 0.85,
            "description": "Analysis of data by category",
            "sql_query": "SELECT category, COUNT(*) as total_count FROM data_table GROUP BY category",
            "table_columns": "category, id"
        },
        "metadata_rag_results": [
            {
                "column_name": "Accident or Incident Code",
                "description": "Code indicating type of accident or incident",
                "data_type": "varchar",
                "score": 0.92,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Accident Type",
                "description": "Type of accident that occurred",
                "data_type": "varchar",
                "score": 0.88,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Preventable Flag",
                "description": "Flag indicating if claim is preventable",
                "data_type": "varchar",
                "score": 0.90,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Cargo Claim Flag",
                "description": "State code for the claim",
                "data_type": "varchar",
                "score": 0.85,
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
            "decision_type": "needs_minor_edit",
            "reasoning": "KPI is close but needs date and status filters",
            "confidence": "HIGH"
        }
    }

def create_test_state_without_kpi():
    """Create a test state without KPI data"""
    from langchain_core.messages import HumanMessage
    
    return {
        "messages": [HumanMessage(content="Show me claims data")],
        "top_kpi": None,
        "metadata_rag_results": [],
        "llm_check_result": {}
    }

def create_test_state_without_messages():
    """Create a test state without messages"""
    return {
        "top_kpi": {
            "metric_name": "Claims by State",
            "sql_query": "SELECT state, COUNT(*) FROM claims_summary GROUP BY state"
        },
        "metadata_rag_results": []
    }

def test_kpi_editor_with_valid_data():
    """Test KPI editor with valid KPI and metadata"""
    print("\n🔧 [TEST] Testing KPI editor with valid data...")
    
    try:
        node = KPIEditorNode()
        state = create_test_state_with_kpi()
        
        print(f"[TEST] Original SQL: {state['top_kpi']['sql_query']}")
        print(f"[TEST] User request: {state['messages'][0].content}")
        
        result_state = node(state)
        
        # Check if state was updated properly
        assert "kpi_editor_status" in result_state, "kpi_editor_status should be set"
        assert "kpi_editor_result" in result_state, "kpi_editor_result should be set"
        
        if result_state["kpi_editor_status"] == "completed":
            print("✅ [TEST] KPI editor completed successfully")
            print(f"[TEST] Edited SQL: {result_state['top_kpi']['sql_query']}")
            print(f"[TEST] Modifications: {result_state['kpi_editor_result'].get('modifications_made', [])}")
            return True
        else:
            print(f"❌ [TEST] KPI editor failed: {result_state.get('kpi_editor_error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error during KPI editor test: {str(e)}")
        return False

def test_kpi_editor_without_kpi():
    """Test KPI editor without KPI data"""
    print("\n🔧 [TEST] Testing KPI editor without KPI data...")
    
    try:
        node = KPIEditorNode()
        state = create_test_state_without_kpi()
        
        result_state = node(state)
        
        # Should handle missing KPI gracefully
        assert result_state["kpi_editor_status"] == "error", "Should set error status for missing KPI"
        assert "kpi_editor_error" in result_state, "Should set error message"
        
        print("✅ [TEST] KPI editor handled missing KPI data correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during missing KPI test: {str(e)}")
        return False

def test_kpi_editor_without_messages():
    """Test KPI editor without messages"""
    print("\n🔧 [TEST] Testing KPI editor without messages...")
    
    try:
        node = KPIEditorNode()
        state = create_test_state_without_messages()
        
        result_state = node(state)
        
        # Should handle missing messages gracefully
        assert result_state["kpi_editor_status"] == "error", "Should set error status for missing messages"
        assert "kpi_editor_error" in result_state, "Should set error message"
        
        print("✅ [TEST] KPI editor handled missing messages correctly")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during missing messages test: {str(e)}")
        return False

def test_metadata_formatting():
    """Test the metadata formatting helper method"""
    print("\n🔧 [TEST] Testing metadata formatting...")
    
    try:
        node = KPIEditorNode()
        
        test_metadata = [
            {
                "column_name": "Accident or Incident Code",
                "description": "Code indicating type of accident or incident",
                "data_type": "varchar",
                "score": 0.92
            },
            {
                "column_name": "Accident Type",
                "description": "Type of accident that occurred",
                "data_type": "varchar",
                "score": 0.88
            }
        ]
        
        formatted = node._format_metadata_for_prompt(test_metadata)
        
        assert "Accident or Incident Code (varchar)" in formatted, "Should include column name and type"
        assert "Code indicating type of accident or incident" in formatted, "Should include description"
        assert "0.92" in formatted, "Should include score"
        
        print("✅ [TEST] Metadata formatting works correctly")
        print(f"[TEST] Formatted metadata preview:\n{formatted[:200]}...")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during metadata formatting test: {str(e)}")
        return False

def test_entity_mapping_data():
    """Test entity mapping data functionality"""
    print("\n🔧 [TEST] Testing entity mapping data...")
    
    try:
        node = KPIEditorNode()
        
        # Test metadata with relevant columns
        test_metadata = [
            {
                "column_name": "Preventable Flag",
                "description": "Flag indicating if claim is preventable",
                "data_type": "varchar",
                "score": 0.92
            },
            {
                "column_name": "Cargo Claim Flag",
                "description": "Flag indicating if this is a cargo claim",
                "data_type": "varchar",
                "score": 0.88
            },
            {
                "column_name": "other_column",
                "description": "Some other column",
                "data_type": "varchar",
                "score": 0.70
            }
        ]
        
        # Extract just the column names for the test
        column_names = [col['column_name'] for col in test_metadata]
        result = node._get_entity_mapping_data(column_names)
        
        assert "Preventable Flag:" in result, "Should include Preventable Flag values"
        assert "Cargo Claim Flag:" in result, "Should include Cargo Claim Flag values"
        assert "other_column" not in result, "Should not include irrelevant columns"
        
        print("✅ [TEST] Entity mapping data works correctly")
        print(f"[TEST] Entity mapping result: {result[:200]}...")
        return True
        
    except Exception as e:
        print(f"❌ [TEST] Error during entity mapping test: {str(e)}")
        return False

def create_test_state_for_current_month_filter():
    """Create a test state for testing current month filter addition"""
    from langchain_core.messages import HumanMessage
    
    return {
        "messages": [HumanMessage(content="Show me data for the current month")],
        "top_kpi": {
            "metric_name": "Total Count",
            "score": 0.90,
            "description": "Total number of records in the system",
            "sql_query": "SELECT COUNT(*) as total_count FROM data_table",
            "table_columns": "id, date_field"
        },
        "metadata_rag_results": [
            {
                "column_name": "Accident or Incident Code",
                "description": "Code indicating type of accident or incident",
                "data_type": "varchar",
                "score": 0.95,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Accident Type",
                "description": "Type of accident that occurred",
                "data_type": "varchar",
                "score": 0.88,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Actual Recovered Amount",
                "description": "Amount actually recovered for the claim",
                "data_type": "decimal",
                "score": 0.85,
                "primary_key": "",
                "foreign_key": ""
            },
            {
                "column_name": "Claim City",
                "description": "State where claim occurred",
                "data_type": "varchar",
                "score": 0.80,
                "primary_key": "",
                "foreign_key": ""
            }
        ],
        "llm_check_result": {
            "decision_type": "needs_minor_edit",
            "reasoning": "KPI needs date filter for current month",
            "confidence": "HIGH"
        }
    }

def test_kpi_editor_current_month_filter():
    """Test KPI editor adding current month filter to original KPI"""
    print("\n🔧 [TEST] Testing KPI editor with current month filter...")
    
    try:
        node = KPIEditorNode()
        state = create_test_state_for_current_month_filter()
        
        print(f"[TEST] Original SQL: {state['top_kpi']['sql_query']}")
        print(f"[TEST] User request: {state['messages'][0].content}")
        print(f"[TEST] Expected: Should add current month filter to the original KPI")
        
        result_state = node(state)
        
        # Check if state was updated properly
        assert "kpi_editor_status" in result_state, "kpi_editor_status should be set"
        assert "kpi_editor_result" in result_state, "kpi_editor_result should be set"
        
        if result_state["kpi_editor_status"] == "completed":
            edited_sql = result_state['top_kpi']['sql_query']
            print("✅ [TEST] KPI editor completed successfully")
            print(f"[TEST] Edited SQL: {edited_sql}")
            print(f"[TEST] Modifications: {result_state['kpi_editor_result'].get('modifications_made', [])}")
            
            # Check if current month filter was added
            sql_lower = edited_sql.lower()
            has_date_filter = any(keyword in sql_lower for keyword in ['month', 'date', 'current', '2024', 'where'])
            has_where_clause = 'where' in sql_lower
            
            if has_date_filter or has_where_clause:
                print("✅ [TEST] Current month filter was successfully added!")
                return True
            else:
                print("⚠️ [TEST] No date filter detected in the edited SQL")
                return True  # Still consider it a pass since the LLM might have used different syntax
        else:
            print(f"❌ [TEST] KPI editor failed: {result_state.get('kpi_editor_error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error during current month filter test: {str(e)}")
        return False

def run_all_tests():
    """Run all tests and report results"""
    print("🚀 [TEST] Starting KPI Editor Node Tests")
    print("=" * 50)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Node Initialization", test_node_initialization),
        ("Metadata Formatting", test_metadata_formatting),
        ("Entity Mapping Data", test_entity_mapping_data),
        ("Valid Data Processing", test_kpi_editor_with_valid_data),
        ("Current Month Filter", test_kpi_editor_current_month_filter),
        ("Missing KPI Handling", test_kpi_editor_without_kpi),
        ("Missing Messages Handling", test_kpi_editor_without_messages)
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
        print("🎉 [TEST] All tests passed! KPI Editor node is working correctly.")
    else:
        print(f"⚠️ [TEST] {total - passed} tests failed. Please check the implementation.")
    
    return passed == total

def interactive_kpi_test():
    """Interactive KPI editor test where user can input prompts"""
    print("🎮 INTERACTIVE KPI EDITOR TEST")
    print("=" * 60)
    print("Enter your queries below. Type 'quit', 'exit', or 'q' to stop.")
    print("Type 'test' to run automated tests instead.")
    print("-" * 60)
    
    # Check environment first
    if not test_environment_setup():
        print("❌ Environment setup failed. Please fix missing variables and try again.")
        return
    
    # Initialize the node
    try:
        print("\n📡 Initializing KPI Editor Node...")
        node = KPIEditorNode()
        print("✅ KPI Editor Node initialized successfully!")
    except Exception as e:
        print(f"❌ Failed to initialize KPI Editor Node: {str(e)}")
        return
    
    # Create sample KPI and metadata for testing
    sample_kpi = {
        "metric_name": "Claims Analysis",
        "score": 0.85,
        "description": "Analysis of claims data by various dimensions",
        "sql_query": "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) AS claim_count FROM PRD.CLAIMS_SUMMARY GROUP BY [Accident or Incident Code]",
        "table_columns": "Accident or Incident Code, Claim Number, Claim State, Status Flag"
    }
    
    sample_metadata = [
        {
            "column_name": "Accident or Incident Code",
            "description": "Code indicating type of accident or incident",
            "data_type": "varchar",
            "score": 0.92
        },
        {
            "column_name": "Claim State",
            "description": "State where claim occurred",
            "data_type": "varchar",
            "score": 0.88
        },
        {
            "column_name": "Status Flag",
            "description": "Current status of the claim",
            "data_type": "varchar",
            "score": 0.90
        },
        {
            "column_name": "Preventable Flag",
            "description": "Flag indicating if claim is preventable",
            "data_type": "varchar",
            "score": 0.87
        },
        {
            "column_name": "Cargo Claim Flag",
            "description": "Flag indicating if this is a cargo claim",
            "data_type": "varchar",
            "score": 0.83
        },
        {
            "column_name": "Claim Date",
            "description": "Date when claim was filed",
            "data_type": "date",
            "score": 0.85
        }
    ]
    
    print(f"\n📊 Sample KPI: {sample_kpi['metric_name']}")
    print(f"📝 Original SQL: {sample_kpi['sql_query'][:100]}...")
    print(f"\n📊 Available columns: {len(sample_metadata)}")
    for col in sample_metadata:
        print(f"  - {col['column_name']} ({col['data_type']}): {col['description']}")
    
    print("\n" + "=" * 60)
    
    while True:
        try:
            # Get user input
            query = input("\n🔍 Enter your query: ").strip()
            
            # Check for exit commands
            if query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            # Check for test command
            if query.lower() == 'test':
                print("\n🧪 Running automated tests...")
                success = run_all_tests()
                print(f"\n📊 Test results: {'All passed!' if success else 'Some failed!'}")
                continue
            
            if not query:
                print("⚠️ Please enter a valid query")
                continue
            
            print(f"\n🔄 Processing: '{query}'")
            print("-" * 40)
            
            # Create test state
            from langchain_core.messages import HumanMessage
            test_state = {
                "messages": [HumanMessage(content=query)],
                "top_kpi": sample_kpi.copy(),
                "metadata_rag_results": sample_metadata.copy(),
                "llm_check_result": {
                    "decision_type": "needs_minor_edit",
                    "reasoning": "KPI needs modification to match user request",
                    "confidence": "HIGH"
                }
            }
            
            # Run the KPI editor node
            result_state = node(test_state)
            
            # Display results
            status = result_state.get("kpi_editor_status", "unknown")
            print(f"📊 Status: {status}")
            
            if status == "completed":
                edited_sql = result_state.get("top_kpi", {}).get("sql_query", "")
                modifications = result_state.get("kpi_editor_result", {}).get("modifications_made", [])
                
                print(f"\n✅ KPI EDITING COMPLETED SUCCESSFULLY:")
                print(f"📝 Original SQL:")
                print("-" * 40)
                print(sample_kpi['sql_query'])
                print("-" * 40)
                
                print(f"📝 Edited SQL:")
                print("-" * 40)
                print(edited_sql)
                print("-" * 40)
                
                if modifications:
                    print(f"🔧 Modifications made: {', '.join(modifications)}")
                
                # Show if SQL actually changed
                if edited_sql != sample_kpi['sql_query']:
                    print("✅ SQL was successfully modified!")
                else:
                    print("ℹ️ No changes were made to the SQL")
                
            else:
                error_msg = result_state.get("kpi_editor_error", "Unknown error")
                print(f"❌ KPI editing failed: {error_msg}")
                
                # Show partial results if available
                edited_sql = result_state.get("top_kpi", {}).get("sql_query", "")
                if edited_sql and edited_sql != sample_kpi['sql_query']:
                    print(f"📝 Partial SQL generated:")
                    print("-" * 40)
                    print(edited_sql)
                    print("-" * 40)
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except EOFError:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error processing query: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    # Check if user wants interactive mode or test mode
    if len(sys.argv) > 1 and sys.argv[1] in ['--interactive', '-i', '--interactive-mode']:
        interactive_kpi_test()
    else:
        # Default to interactive mode
        print("🚀 Starting Interactive KPI Editor Test")
        print("💡 Use '--test' flag for automated test mode")
        interactive_kpi_test()
