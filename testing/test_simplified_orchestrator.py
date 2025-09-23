"""
Test file for Simplified Hirschbach Orchestrator
Tests the streamlined orchestrator that handles multi-retrieval without task breakdown
"""

import sys
import os
import json

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Nodes.orchestrator import HirschbachOrchestrator
from langchain_core.messages import HumanMessage

def test_simplified_orchestrator(prompt: str, description: str):
    """Test the simplified orchestrator with a single prompt"""
    print(f"\n{'='*80}")
    print(f"TEST: {description}")
    print(f"{'='*80}")
    print(f"PROMPT: {prompt}")
    print(f"{'='*80}")
    
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
        
        # Show retrieval results
        kpi_status = result.get('kpi_retrieval_status', 'Not triggered')
        metadata_status = result.get('metadata_retrieval_status', 'Not triggered')
        print(f"\nRETRIEVAL STATUS:")
        print(f"  KPI Retrieval: {kpi_status}")
        print(f"  Metadata Retrieval: {metadata_status}")
        
        # Show KPI results if available
        if result.get('top_kpi'):
            print(f"\nKPI FOUND:")
            kpi = result['top_kpi']
            print(f"  Metric: {kpi.get('metric_name', 'N/A')}")
            print(f"  Score: {kpi.get('score', 'N/A')}")
            print(f"  Description: {kpi.get('description', 'N/A')[:100]}...")
        
        # Show metadata results if available
        if result.get('metadata_rag_results'):
            print(f"\nMETADATA FOUND: {len(result['metadata_rag_results'])} columns")
            for i, col in enumerate(result['metadata_rag_results'][:3], 1):
                print(f"  {i}. {col.get('column_name', 'N/A')}: {col.get('description', 'N/A')[:50]}...")
        
        # Show any errors
        if result.get('error_message'):
            print(f"\nERROR: {result['error_message']}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Test different types of prompts with the simplified orchestrator"""
    
    print("SIMPLIFIED HIRSCHBACH ORCHESTRATOR TEST")
    print("="*80)
    print("This orchestrator handles multi-retrieval without unnecessary task breakdown")
    print("="*80)
    
    # Direct reply prompts (no retrieval needed)
    print("\n\nTESTING DIRECT REPLY PROMPTS:")
    test_simplified_orchestrator(
        "What is a preventable crash rate?",
        "Direct Reply - General Risk Management Question"
    )
    
    test_simplified_orchestrator(
        "How does this system work?",
        "Direct Reply - Help Question"
    )
    
    # Data analysis prompts (triggers multi-retrieval)
    print("\n\nTESTING DATA ANALYSIS PROMPTS (Multi-Retrieval):")
    test_simplified_orchestrator(
        "Show me claims in California",
        "Data Analysis - Simple Query (KPI + Metadata)"
    )
    
    test_simplified_orchestrator(
        "Which drivers have the most claims?",
        "Data Analysis - Complex Query (KPI + Metadata)"
    )
    
    test_simplified_orchestrator(
        "What are the highest value claims by state?",
        "Data Analysis - Multi-Criteria Query (KPI + Metadata)"
    )
    
    test_simplified_orchestrator(
        "Show me accident trends and driver performance",
        "Data Analysis - Multi-Aspect Query (KPI + Metadata)"
    )
    
    print(f"\n{'='*80}")
    print("TEST COMPLETED")
    print("="*80)
    print("Key Benefits of Simplified Orchestrator:")
    print("✓ No unnecessary task breakdown")
    print("✓ Direct multi-retrieval for data queries")
    print("✓ Cleaner, more efficient processing")
    print("✓ Still handles complex multi-retrieval scenarios")
    print("✓ Better user experience with immediate responses")

if __name__ == "__main__":
    main()
