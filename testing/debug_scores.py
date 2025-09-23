#!/usr/bin/env python3
"""
Debug script to investigate scoring issues in KPI retrieval
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

def debug_kpi_scores():
    """Debug the scoring issue in KPI retrieval"""
    
    print("🔍 Debugging KPI Retrieval Scores")
    print("=" * 50)
    
    try:
        from Nodes.kpi_retrieval import KPIRetrievalNode
        
        # Initialize the node
        print("📡 Initializing KPI Retrieval Node...")
        kpi_node = KPIRetrievalNode()
        print("✅ Node initialized!")
        
        # Test with a simple query
        test_query = "Show me claims in California"
        print(f"\n🔍 Testing query: '{test_query}'")
        
        # Create a simple state
        mock_state = {
            "messages": [{"content": test_query, "role": "user"}],
            "task": test_query
        }
        
        # Run the retrieval
        print("\n📊 Running KPI retrieval...")
        result_state = kpi_node(mock_state)
        
        # Check results
        kpi_results = result_state.get("kpi_rag_results", [])
        print(f"\n📈 Found {len(kpi_results)} KPIs")
        
        if kpi_results:
            print("\n🎯 First KPI details:")
            first_kpi = kpi_results[0]
            for key, value in first_kpi.items():
                print(f"   {key}: {value}")
        
        print("\n✅ Debug completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_kpi_scores()
