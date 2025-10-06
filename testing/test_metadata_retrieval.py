#!/usr/bin/env python3
"""
Test script for Metadata Retrieval Node
Tests if the metadata retrieval node correctly retrieves columns from Azure AI Search
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

def test_metadata_retrieval():
    """Test the metadata retrieval node with sample queries"""
    
    print("=" * 60)
    print(" TESTING METADATA RETRIEVAL NODE")
    print("=" * 60)
    
    try:
        # Import the metadata retrieval node
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        
        # Initialize the node
        print(" Initializing Metadata Retrieval Node...")
        metadata_node = MetadataRetrievalNode()
        print(" Metadata Retrieval Node initialized successfully!")
        
        # Test queries for generic data analysis - 3 tests with the same query
        test_queries = [
            "Show the distribution of records across different categories",
            "Show the distribution of records across different categories", 
            "Show the distribution of records across different categories"
        ]
        
        print(f"\n Testing with {len(test_queries)} runs of the same query...")
        print("Query: 'Show the distribution of records across different categories'")
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
                # Run the metadata retrieval node
                result_state = metadata_node(mock_state)
                
                # Check results
                metadata_results = result_state.get("metadata_rag_results", [])
                retrieval_status = result_state.get("metadata_retrieval_status", "unknown")
                
                print(f" Status: {retrieval_status}")
                print(f" Found {len(metadata_results)} columns")
                
                if metadata_results:
                    print(" Top columns found:")
                    for j, col in enumerate(metadata_results[:5], 1):  # Show top 5
                        print(f"   {j}. {col.get('column_name', 'Unknown')} (score: {col.get('score', 0):.3f})")
                        print(f"      Description: {col.get('description', 'No description')[:80]}...")
                        print(f"      Data Type: {col.get('data_type', 'Unknown')}")
                        print()
                else:
                    print(" No columns found for this query")
                
            except Exception as e:
                print(f" Error processing query: {e}")
                continue
        
        print("\n" + "=" * 60)
        print(" METADATA RETRIEVAL TEST COMPLETED")
        print("=" * 60)
        
        # Test with a more complex state
        print("\n Testing with complex conversation state...")
        complex_state = {
            "user_query": "I want to see data for California and Texas by assignee",
            "messages": [
                {"content": "Hello", "role": "user"},
                {"content": "Hi! How can I help you with data analysis?", "role": "assistant"},
                {"content": "I want to see data for California and Texas by assignee", "role": "user"}
            ],
            "workflow_status": "in_progress"
        }
        
        try:
            result = metadata_node(complex_state)
            metadata_results = result.get("metadata_rag_results", [])
            print(f" Complex state test: Found {len(metadata_results)} columns")
            
            if metadata_results:
                print(" Best matches:")
                for i, col in enumerate(metadata_results[:3], 1):
                    print(f"   {i}. {col.get('column_name', 'Unknown')} (score: {col.get('score', 0):.3f})")
                    print(f"      Description: {col.get('description', 'No description')}")
                
        except Exception as e:
            print(f" Complex state test failed: {e}")
        
        # Test the new generic approach with different query types
        print("\n Testing Generic Semantic Approach...")
        print("-" * 50)
        
        generic_test_queries = [
            "How many closed records in California?",
            "Show total amounts by state", 
            "Records by assignee in 2024",
            "Distribution by category in Texas"
        ]
        
        for query in generic_test_queries:
            print(f"\n Testing: '{query}'")
            test_state = {
                "user_query": query,
                "messages": [{"content": query, "role": "user"}],
                "workflow_status": "in_progress"
            }
            
            try:
                result = metadata_node(test_state)
                metadata_results = result.get("metadata_rag_results", [])
                print(f" Found {len(metadata_results)} columns")
                
                # Show the types of columns found
                if metadata_results:
                    column_types = set()
                    for col in metadata_results:
                        desc = col.get('description', '').lower()
                        if 'status' in desc or 'flag' in desc:
                            column_types.add('status')
                        elif 'state' in desc or 'location' in desc or 'geographic' in desc:
                            column_types.add('location')
                        elif 'amount' in desc or 'incurred' in desc or 'monetary' in desc:
                            column_types.add('amount')
                        elif 'date' in desc or 'time' in desc:
                            column_types.add('date')
                        elif 'name' in desc or 'assignee' in desc or 'adjuster' in desc:
                            column_types.add('person')
                        elif 'type' in desc or 'category' in desc or 'code' in desc:
                            column_types.add('category')
                        elif 'number' in desc or 'id' in desc or 'key' in desc:
                            column_types.add('identifier')
                    
                    print(f" Column types found: {', '.join(sorted(column_types))}")
                
            except Exception as e:
                print(f" Error: {e}")
        
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
        "AZURE_SEARCH_INDEX_NAME_2"
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

def test_simple_retrieval():
    """Test a simple metadata retrieval without the full iterative process"""
    print("\n Testing Simple Metadata Retrieval...")
    print("-" * 50)
    
    try:
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        
        # Initialize the node
        metadata_node = MetadataRetrievalNode()
        
        # Test a simple retrieval
        test_query = "Show me records by state"
        print(f"Testing query: '{test_query}'")
        
        # Test the _retrieve_metadata method directly
        results = metadata_node._retrieve_metadata(test_query, top_k=3)
        
        print(f"Direct retrieval results: {len(results)} columns")
        for i, col in enumerate(results, 1):
            print(f"  {i}. {col.get('column_name', 'Unknown')} (score: {col.get('score', 0):.3f})")
            print(f"     Description: {col.get('description', 'No description')[:60]}...")
        
        return len(results) > 0
        
    except Exception as e:
        print(f" Simple retrieval test failed: {e}")
        return False

def test_generic_descriptions():
    """Test the new generic description generation approach"""
    print("\n Testing Generic Description Generation...")
    print("-" * 50)
    
    try:
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        
        # Initialize the node
        metadata_node = MetadataRetrievalNode()
        
        # Test different query types to see what descriptions are generated
        test_cases = [
            "How many closed records in California?",
            "Show total amounts by state",
            "Records by assignee in 2024",
            "Distribution by category"
        ]
        
        for query in test_cases:
            print(f"\n Testing query: '{query}'")
            
            # Test the query analysis
            requirements = metadata_node._analyze_query_requirements(query)
            print(f" Requirements: {requirements}")
            
            # Test targeted description generation
            descriptions = metadata_node._create_targeted_search_descriptions(query, requirements)
            print(f" Generated descriptions:")
            for i, desc in enumerate(descriptions, 1):
                print(f"      {i}. {desc}")
            
            # Test generic search descriptions
            generic_descriptions = metadata_node._create_search_descriptions(query, [])
            print(f" Generic descriptions:")
            for i, desc in enumerate(generic_descriptions, 1):
                print(f"      {i}. {desc}")
        
        return True
        
    except Exception as e:
        print(f" Generic description test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def interactive_metadata_test():
    """Interactive test where user inputs prompts and gets metadata results"""
    print(" Interactive Metadata Retrieval Test")
    print("="*50)
    
    try:
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        
        # Initialize the node
        print(" Initializing Metadata Retrieval Node...")
        metadata_node = MetadataRetrievalNode()
        print(" Metadata Retrieval Node initialized successfully!")
        
        print("\n Enter your queries below. Type 'quit' or 'exit' to stop.")
        print("-" * 50)
        
        while True:
            # Get user input
            query = input("\n🔍 Enter your query: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                print(" Goodbye!")
                break
            
            if not query:
                print(" Please enter a query.")
                continue
            
            print(f"\n Processing: '{query}'")
            print("-" * 40)
            
            try:
                # Create mock state with the query
                mock_state = {
                    "user_query": query,
                    "messages": [{"content": query, "role": "user"}],
                    "workflow_status": "in_progress"
                }
                
                # First, let's see what descriptions are generated
                print(" Step 1: Analyzing query requirements...")
                requirements = metadata_node._analyze_query_requirements(query)
                
                print(" Step 2: Generating semantic descriptions...")
                descriptions = metadata_node._create_targeted_search_descriptions(query, requirements)
                print(f" Generated {len(descriptions)} descriptions:")
                for i, desc in enumerate(descriptions, 1):
                    print(f"   {i}. \"{desc}\"")
                
                print("\n Step 3: Running semantic searches...")
                # Test each description individually to see what it finds
                for i, description in enumerate(descriptions, 1):
                    print(f"\n   Search {i}: \"{description}\"")
                    try:
                        columns = metadata_node._retrieve_metadata(description, top_k=3)
                        if columns:
                            print(f" Found {len(columns)} columns:")
                            for col in columns:
                                print(f"         - {col.get('column_name', 'Unknown')} (score: {col.get('score', 0):.3f})")
                        else:
                            print(f" No columns found")
                    except Exception as e:
                        print(f" Error: {e}")
                
                print("\n Step 4: Running full metadata retrieval...")
                # Run the metadata retrieval node
                result_state = metadata_node(mock_state)
                
                # Get results
                metadata_results = result_state.get("metadata_rag_results", [])
                retrieval_status = result_state.get("metadata_retrieval_status", "unknown")
                
                print(f" Status: {retrieval_status}")
                print(f" Final result: Found {len(metadata_results)} unique columns")
                
                if metadata_results:
                    print("\n Final Metadata Results:")
                    for i, col in enumerate(metadata_results, 1):
                        print(f"   {i}. {col.get('column_name', 'Unknown')}")
                        print(f"      Description: {col.get('description', 'No description')}")
                        print(f"      Data Type: {col.get('data_type', 'Unknown')}")
                        print(f"      Score: {col.get('score', 0):.3f}")
                        print()
                else:
                    print(" No columns found for this query")
                
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

def quick_interactive_test():
    """Quick interactive test without running all the other tests first"""
    print(" Quick Interactive Metadata Retrieval Test")
    print("="*50)
    
    # Check environment quickly
    required_vars = ["AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_API_KEY", "AZURE_SEARCH_SERVICE_ENDPOINT", "AZURE_SEARCH_API_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f" Missing environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file")
        return
    
    print(" Environment variables found")
    
    # Run interactive test directly
    interactive_metadata_test()

if __name__ == "__main__":
    import sys
    
    # Check if user wants quick mode
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        quick_interactive_test()
    else:
        print(" Starting Metadata Retrieval Node Tests")
        
        # Test environment first
        if test_environment_setup():
            # Test generic descriptions first (doesn't require Azure connection)
            print("\n" + "="*60)
            print(" TESTING GENERIC DESCRIPTION GENERATION")
            print("="*60)
            test_generic_descriptions()
            
            # Test simple retrieval
            if test_simple_retrieval():
                # Run interactive test
                print("\n" + "="*60)
                print(" INTERACTIVE METADATA RETRIEVAL TEST")
                print("="*60)
                interactive_metadata_test()
            else:
                print(" Simple retrieval test failed. Check Azure AI Search connection.")
        else:
            print(" Environment setup failed. Please fix missing variables and try again.")
