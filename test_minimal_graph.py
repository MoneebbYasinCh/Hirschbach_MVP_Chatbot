#!/usr/bin/env python3
"""
Test minimal graph to identify the hanging node
"""

def test_minimal_graph():
    """Test with just orchestrator to see if it's the problem"""
    print("Testing minimal graph with just orchestrator...")
    
    try:
        from langchain_core.messages import HumanMessage
        from langgraph.graph import StateGraph, END
        from Nodes.orchestrator import HirschbachOrchestrator
        from State.main_state import HirschbachGraphState
        
        print("1. Creating StateGraph...")
        workflow = StateGraph(HirschbachGraphState)
        
        print("2. Creating orchestrator...")
        orchestrator = HirschbachOrchestrator()
        print("✅ Orchestrator created successfully")
        
        print("3. Adding nodes...")
        workflow.add_node("orchestrator", orchestrator)
        workflow.add_node("end", lambda state: state)
        
        print("4. Adding edges...")
        workflow.add_edge("orchestrator", "end")
        workflow.add_edge("end", END)
        workflow.set_entry_point("orchestrator")
        
        print("5. Compiling...")
        graph = workflow.compile()
        print("✅ Graph compiled successfully")
        
        print("6. Testing invocation...")
        config = {"configurable": {"thread_id": "test"}}
        inputs = {"messages": [HumanMessage(content="hi")]}
        
        result = graph.invoke(inputs, config)
        print(f"✅ Result: {result.get('final_response', 'No response')[:100]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_minimal_graph()
    if success:
        print("\n🎉 Minimal graph test passed!")
    else:
        print("\n💥 Minimal graph test failed!")
