"""
Simple test file for Hirschbach Orchestrator
Quick test to see orchestrator output for different prompts
"""

import sys
import os
import json

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Nodes.orchestrator import HirschbachOrchestrator
from langchain_core.messages import HumanMessage

def test_orchestrator(prompt: str):
    """Test orchestrator with a single prompt and show output"""
    print(f"\n{'='*60}")
    print(f"PROMPT: {prompt}")
    print(f"{'='*60}")
    
    # Create orchestrator
    orchestrator = HirschbachOrchestrator()
    
    # Create test state
    state = {
        "messages": [HumanMessage(content=prompt)],
        "orchestration": {},
        "main_task_queue": [],
        "nl_to_sql_queue": [],
        "nl_to_sql_state": {},
        "completed_tasks": [],
        "snowflake_results": {},
        "redshift_results": {},
        "error_message": "",
        "aggregated_data": [],
        "task_results": {},
        "insights": {},
        "final_response": "",
        "workflow_status": "active"
    }
    
    try:
        # Run orchestrator
        result = orchestrator(state)
        
        # Show key outputs
        print(f"STATUS: {result.get('workflow_status')}")
        print(f"RESPONSE: {result.get('final_response')}")
        
        # Show tasks if created
        orchestration = result.get('orchestration', {})
        if orchestration.get('tasks'):
            print(f"\nTASKS CREATED:")
            for i, task in enumerate(orchestration['tasks'], 1):
                print(f"  {i}. {task.get('description')}")
        
        # Show queue status
        queue = result.get('nl_to_sql_queue', [])
        if queue:
            print(f"\nQUEUE: {len(queue)} tasks queued")
        
        # Show retrieval results
        if result.get('top_kpi'):
            print(f"\nKPI FOUND: {result['top_kpi'].get('metric_name')}")
        
        if result.get('metadata_rag_results'):
            print(f"\nMETADATA: {len(result['metadata_rag_results'])} columns found")
        
    except Exception as e:
        print(f"ERROR: {e}")

def main():
    """Test different types of prompts"""
    
    # Direct reply prompts
    print("TESTING DIRECT REPLY PROMPTS:")
    test_orchestrator("What is a preventable crash rate?")
    test_orchestrator("How does this system work?")
    test_orchestrator("What data do you have access to?")
    
    # Task breakdown prompts
    print("\n\nTESTING TASK BREAKDOWN PROMPTS:")
    test_orchestrator("Show me claims in California")
    test_orchestrator("Which drivers have the most claims?")
    test_orchestrator("What are the highest value claims?")
    test_orchestrator("Show me closed claims by adjuster in Texas")

if __name__ == "__main__":
    main()
