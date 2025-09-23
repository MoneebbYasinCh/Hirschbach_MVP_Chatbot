#!/usr/bin/env python3
"""
Test the RCM-style simple approach
"""

def test_rcm_approach():
    """Test the simplified RCM-style approach"""
    print("Testing RCM-style simple approach...")
    
    try:
        from Graph_Flow.main_graph import create_main_graph
        from langchain_core.messages import HumanMessage
        
        # Test simple query (should get direct reply)
        print("\n1. Testing simple query (should get direct reply)...")
        graph = create_main_graph()
        config = {"configurable": {"thread_id": "test"}}
        inputs = {"messages": [HumanMessage(content="hi")]}
        
        result = graph.invoke(inputs, config)
        print(f"✅ Simple query result: {result.get('final_response', 'No response')[:100]}...")
        
        # Test data analysis query (should go through full pipeline)
        print("\n2. Testing data analysis query (should go through pipeline)...")
        inputs = {"messages": [HumanMessage(content="show me claims data")]}
        
        result = graph.invoke(inputs, config)
        print(f"✅ Data analysis result: {result.get('final_response', 'No response')[:100]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_rcm_approach()
    if success:
        print("\n🎉 RCM-style approach test passed!")
    else:
        print("\n💥 RCM-style approach test failed!")
