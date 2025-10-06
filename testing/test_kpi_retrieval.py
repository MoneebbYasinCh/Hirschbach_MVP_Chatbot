#!/usr/bin/env python3
"""
Test script for KPI Retrieval Node
Tests if the KPI retrieval node correctly retrieves values from Azure AI Search
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

def test_kpi_retrieval():
    """Test the KPI retrieval node with sample queries"""
    
    print("=" * 60)
    print(" TESTING KPI RETRIEVAL NODE")
    print("=" * 60)
    
    try:
        # Import the KPI retrieval node
        from Nodes.kpi_retrieval import KPIRetrievalNode
        
        # Initialize the node
        print(" Initializing KPI Retrieval Node...")
        kpi_node = KPIRetrievalNode()
        print(" KPI Retrieval Node initialized successfully!")
        
        # Test queries for claims_summary analysis - 3 tests with the same query
        test_queries = [
            "Show the distribution of claims across different claim categories",
            "Show the distribution of claims across different claim categories", 
            "Show the distribution of claims across different claim categories"
        ]
        
        print(f"\n Testing with {len(test_queries)} runs of the same query...")
        print("Query: 'Show the distribution of claims across different claim categories'")
        print("-" * 60)
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n Test {i}: '{query}'")
            print("-" * 40)
            
            # Create a mock state with the query (using correct state structure)
            mock_state = {
                "user_query": query,  # Primary way to pass user query
                "messages": [
                    {"content": query, "role": "user"}
                ],
                "workflow_status": "in_progress"
            }
            
            try:
                # Run the KPI retrieval node
                result_state = kpi_node(mock_state)
                
                # Check results
                top_kpi = result_state.get("top_kpi")
                retrieval_status = result_state.get("kpi_retrieval_status", "unknown")
                
                print(f" Status: {retrieval_status}")
                
                if top_kpi:
                    print(" Top KPI found:")
                    print(f"   Metric Name: {top_kpi.get('metric_name', 'Unknown')}")
                    print(f"   Description: {top_kpi.get('description', 'No description')[:80]}...")
                    print(f"   SQL Query: {top_kpi.get('sql_query', 'No SQL')[:60]}...")
                    print(f"   Table Columns: {top_kpi.get('table_columns', 'No columns')}")
                else:
                    print(" No KPIs found for this query")
                
            except Exception as e:
                print(f" Error processing query: {e}")
                continue
        
        print("\n" + "=" * 60)
        print(" KPI RETRIEVAL TEST COMPLETED")
        print("=" * 60)
        
        # Test with a more complex state
        print("\n Testing with complex conversation state...")
        complex_state = {
            "user_query": "I want to see claims data for California and Texas",
            "messages": [
                {"content": "Hello", "role": "user"},
                {"content": "Hi! How can I help you with claims analysis?", "role": "assistant"},
                {"content": "I want to see claims data for California and Texas", "role": "user"}
            ],
            "workflow_status": "in_progress"
        }
        
        try:
            result = kpi_node(complex_state)
            top_kpi = result.get("top_kpi")
            print(f" Complex state test: {'Found KPI' if top_kpi else 'No KPI found'}")
            
            if top_kpi:
                print(" Best match:")
                print(f"   Metric: {top_kpi.get('metric_name', 'Unknown')}")
                print(f"   Description: {top_kpi.get('description', 'No description')}")
                
        except Exception as e:
            print(f" Complex state test failed: {e}")
        
        print("\n All tests completed!")
        
    except ImportError as e:
        print(f" Import error: {e}")
        print("Make sure you're running from the project root directory")
    except Exception as e:
        print(f" Unexpected error: {e}")
        import traceback
        traceback.print_exc()

def test_environment_setup():
    """Test if environment variables are properly set"""
    print("\n Testing Environment Setup...")
    print("-" * 40)
    
    required_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY", 
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT",
        "AZURE_SEARCH_SERVICE_ENDPOINT",
        "AZURE_SEARCH_API_KEY",
        "AZURE_SEARCH_INDEX_NAME"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f" {var}: {'*' * 10}...{value[-4:] if len(value) > 10 else value}")
        else:
            print(f" {var}: Not set")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n Missing environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file")
        return False
    else:
        print("\n All required environment variables are set!")
        return True

def interactive_kpi_test():
    """Interactive test where user can input prompts and see results"""
    
    print("=" * 60)
    print(" INTERACTIVE KPI RETRIEVAL TEST")
    print("=" * 60)
    
    try:
        # Import the KPI retrieval node
        from Nodes.kpi_retrieval import KPIRetrievalNode
        
        # Initialize the node
        print(" Initializing KPI Retrieval Node...")
        kpi_node = KPIRetrievalNode()
        print(" KPI Retrieval Node initialized successfully!")
        
        print("\n Enter your queries below. Type 'quit' or 'exit' to stop.")
        print("-" * 60)
        
        while True:
            try:
                # Get user input
                query = input("\n🔍 Enter your query: ").strip()
                
                # Check for exit commands
                if query.lower() in ['quit', 'exit', 'q']:
                    print(" Goodbye!")
                    break
                
                if not query:
                    print(" Please enter a valid query")
                    continue
            except KeyboardInterrupt:
                print("\n Goodbye!")
                break
            except EOFError:
                print("\n Goodbye!")
                break
            
            print(f"\n Processing: '{query}'")
            print("-" * 40)
            
            try:
                # Create mock state
                mock_state = {
                    "user_query": query,
                    "messages": [{"content": query, "role": "user"}],
                    "workflow_status": "in_progress"
                }
                
                # Run the KPI retrieval node
                result_state = kpi_node(mock_state)
                
                # Display results
                top_kpi = result_state.get("top_kpi")
                retrieval_status = result_state.get("kpi_retrieval_status", "unknown")
                
                print(f" Status: {retrieval_status}")
                
                if top_kpi:
                    print("\n TOP KPI FOUND:")
                    print(f" Metric Name: {top_kpi.get('metric_name', 'Unknown')}")
                    print(f" Description: {top_kpi.get('description', 'No description')}")
                    print(f" Table Columns: {top_kpi.get('table_columns', 'No columns')}")
                    print(f" SQL Query:")
                    print(f"      {top_kpi.get('sql_query', 'No SQL')}")
                else:
                    print(" No KPIs found for this query")
                    print(" Try rephrasing your query or using different keywords")
                
            except Exception as e:
                print(f" Error processing query: {e}")
                import traceback
                traceback.print_exc()
        
    except ImportError as e:
        print(f" Import error: {e}")
        print("Make sure you're running from the project root directory")
    except Exception as e:
        print(f" Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    # Check if user wants regular test mode
    if len(sys.argv) > 1 and sys.argv[1] in ['--test', '-t', '--regular']:
        print(" Starting KPI Retrieval Node Tests")
        
        # Test environment first
        if test_environment_setup():
            # Run the main test
            test_kpi_retrieval()
        else:
            print(" Environment setup failed. Please fix missing variables and try again.")
    else:
        # Default to interactive mode
        print(" Starting Interactive KPI Retrieval Test")
        print(" Use '--test' flag for regular test mode")
        
        # Test environment first
        if test_environment_setup():
            # Run interactive test
            interactive_kpi_test()
        else:
            print(" Environment setup failed. Please fix missing variables and try again.")
