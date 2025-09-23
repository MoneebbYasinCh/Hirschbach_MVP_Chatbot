#!/usr/bin/env python3
"""
Test the fast create-and-run approach
"""

def test_fast_approach():
    """Test the simplified create-and-run approach"""
    print("Testing fast create-and-run approach...")
    
    try:
        from Graph_Flow.main_graph import create_main_graph
        from langchain_core.messages import HumanMessage
        
        # Test graph creation
        print("Creating graph...")
        graph = create_main_graph()
        print("✅ Graph created successfully!")
        
        # Test simple invocation
        print("Testing simple invocation...")
        config = {"configurable": {"thread_id": "test"}}
        inputs = {"messages": [HumanMessage(content="hi")]}
        
        result = graph.invoke(inputs, config)
        print("✅ Graph invocation successful!")
        print(f"Result keys: {list(result.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_fast_approach()
    if success:
        print("\n🎉 Fast approach test passed!")
    else:
        print("\n💥 Fast approach test failed!")
