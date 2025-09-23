#!/usr/bin/env python3
"""
Interactive Metadata Retrieval Test
Simple script to test metadata retrieval with user input
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv()

def main():
    """Main interactive test function"""
    print("🚀 Interactive Metadata Retrieval Test")
    print("="*50)
    
    # Check environment variables
    required_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY", 
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT",
        "AZURE_SEARCH_SERVICE_ENDPOINT",
        "AZURE_SEARCH_API_KEY",
        "AZURE_SEARCH_INDEX_NAME_2"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file")
        return
    
    print("✅ Environment variables found")
    
    try:
        from Nodes.metadata_retrieval import MetadataRetrievalNode
        
        # Initialize the node
        print("📡 Initializing Metadata Retrieval Node...")
        metadata_node = MetadataRetrievalNode()
        print("✅ Metadata Retrieval Node initialized successfully!")
        
        print("\n💡 Enter your queries below. Type 'quit', 'exit', or 'q' to stop.")
        print("Examples:")
        print("  - How many closed records in California?")
        print("  - Show me records by state")
        print("  - Total amounts by category")
        print("-" * 50)
        
        while True:
            # Get user input
            query = input("\n🔍 Enter your query: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not query:
                print("⚠️ Please enter a query.")
                continue
            
            print(f"\n🔄 Processing: '{query}'")
            print("-" * 40)
            
            try:
                # First, let's see what descriptions are generated
                print("🔍 Step 1: Analyzing query requirements...")
                requirements = metadata_node._analyze_query_requirements(query)
                
                print("🎯 Step 2: Generating semantic descriptions...")
                descriptions = metadata_node._create_targeted_search_descriptions(query, requirements)
                print(f"📝 Generated {len(descriptions)} descriptions:")
                for i, desc in enumerate(descriptions, 1):
                    print(f"   {i}. \"{desc}\"")
                
                print("\n🔍 Step 3: Running semantic searches...")
                # Test each description individually to see what it finds
                for i, description in enumerate(descriptions, 1):
                    print(f"\n   Search {i}: \"{description}\"")
                    try:
                        columns = metadata_node._retrieve_metadata(description, top_k=3)
                        if columns:
                            print(f"      ✅ Found {len(columns)} columns:")
                            for col in columns:
                                print(f"         - {col.get('column_name', 'Unknown')} (score: {col.get('score', 0):.3f})")
                        else:
                            print(f"      ❌ No columns found")
                    except Exception as e:
                        print(f"      ❌ Error: {e}")
                
                print("\n🔄 Step 4: Running full metadata retrieval...")
                # Create mock state with the query
                mock_state = {
                    "user_query": query,
                    "messages": [{"content": query, "role": "user"}],
                    "workflow_status": "in_progress"
                }
                
                # Run the metadata retrieval node
                result_state = metadata_node(mock_state)
                
                # Get results
                metadata_results = result_state.get("metadata_rag_results", [])
                retrieval_status = result_state.get("metadata_retrieval_status", "unknown")
                
                print(f"📊 Status: {retrieval_status}")
                print(f"📈 Final result: Found {len(metadata_results)} unique columns")
                
                if metadata_results:
                    print("\n🎯 Final Metadata Results:")
                    for i, col in enumerate(metadata_results, 1):
                        print(f"   {i}. {col.get('column_name', 'Unknown')}")
                        print(f"      Description: {col.get('description', 'No description')}")
                        print(f"      Data Type: {col.get('data_type', 'Unknown')}")
                        print(f"      Score: {col.get('score', 0):.3f}")
                        print()
                else:
                    print("❌ No columns found for this query")
                
            except Exception as e:
                print(f"❌ Error processing query: {e}")
                import traceback
                traceback.print_exc()
    
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're running from the project root directory")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
