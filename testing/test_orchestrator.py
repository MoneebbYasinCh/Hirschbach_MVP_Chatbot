"""
Test file for Hirschbach Orchestrator
Tests different types of prompts and shows complete orchestrator output
"""

import sys
import os
import json
from typing import Dict, Any, List

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Nodes.orchestrator import HirschbachOrchestrator
from langchain_core.messages import HumanMessage, AIMessage

class OrchestratorTester:
    """Test class for the Hirschbach Orchestrator"""
    
    def __init__(self):
        self.orchestrator = HirschbachOrchestrator()
    
    def create_test_state(self, user_message: str, conversation_history: List = None) -> Dict[str, Any]:
        """Create a test state with the given user message"""
        messages = []
        
        # Add conversation history if provided
        if conversation_history:
            for msg in conversation_history:
                if isinstance(msg, str):
                    messages.append(HumanMessage(content=msg))
                else:
                    messages.append(msg)
        
        # Add the current user message
        messages.append(HumanMessage(content=user_message))
        
        return {
            "messages": messages,
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
    
    def test_prompt(self, prompt: str, description: str, conversation_history: List = None):
        """Test a single prompt and display results"""
        print(f"\n{'='*80}")
        print(f"TEST: {description}")
        print(f"{'='*80}")
        print(f"PROMPT: {prompt}")
        print(f"{'='*80}")
        
        # Create test state
        state = self.create_test_state(prompt, conversation_history)
        
        # Run orchestrator
        try:
            result_state = self.orchestrator(state)
            
            # Display results
            print(f"WORKFLOW STATUS: {result_state.get('workflow_status', 'Unknown')}")
            print(f"FINAL RESPONSE: {result_state.get('final_response', 'No response')}")
            
            # Show orchestration details if available
            orchestration = result_state.get('orchestration', {})
            if orchestration:
                print(f"\nORCHESTRATION DETAILS:")
                print(f"  Original Input: {orchestration.get('original_input', 'N/A')}")
                print(f"  Routed To: {orchestration.get('routed_to', 'N/A')}")
                
                tasks = orchestration.get('tasks', [])
                if tasks:
                    print(f"  Tasks Created: {len(tasks)}")
                    for i, task in enumerate(tasks, 1):
                        print(f"    Task {i}:")
                        print(f"      ID: {task.get('id', 'N/A')}")
                        print(f"      Description: {task.get('description', 'N/A')}")
                        print(f"      Tools: {task.get('tools', 'N/A')}")
            
            # Show queue status
            nl_to_sql_queue = result_state.get('nl_to_sql_queue', [])
            if nl_to_sql_queue:
                print(f"\nNL_TO_SQL QUEUE: {len(nl_to_sql_queue)} tasks")
                for i, task in enumerate(nl_to_sql_queue, 1):
                    print(f"  Queue Task {i}: {task.get('description', 'N/A')}")
            
            # Show retrieval status
            kpi_status = result_state.get('kpi_retrieval_status', 'Not triggered')
            metadata_status = result_state.get('metadata_retrieval_status', 'Not triggered')
            print(f"\nRETRIEVAL STATUS:")
            print(f"  KPI Retrieval: {kpi_status}")
            print(f"  Metadata Retrieval: {metadata_status}")
            
            # Show any errors
            if result_state.get('error_message'):
                print(f"\nERROR: {result_state['error_message']}")
            
            # Show retrieval results if available
            if result_state.get('top_kpi'):
                print(f"\nTOP KPI RESULT:")
                kpi = result_state['top_kpi']
                print(f"  Metric Name: {kpi.get('metric_name', 'N/A')}")
                print(f"  Score: {kpi.get('score', 'N/A')}")
                print(f"  Description: {kpi.get('description', 'N/A')[:100]}...")
            
            if result_state.get('metadata_rag_results'):
                print(f"\nMETADATA RESULTS: {len(result_state['metadata_rag_results'])} columns")
                for col in result_state['metadata_rag_results'][:3]:  # Show first 3
                    print(f"  - {col.get('column_name', 'N/A')}: {col.get('description', 'N/A')[:50]}...")
            
        except Exception as e:
            print(f"ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def run_all_tests(self):
        """Run comprehensive tests on the orchestrator"""
        print("HIRSCHBACH ORCHESTRATOR TEST SUITE")
        print("="*80)
        
        # Test 1: Direct reply - general question
        self.test_prompt(
            "What is a preventable crash rate?",
            "Direct Reply - General Risk Management Question"
        )
        
        # Test 2: Direct reply - help question
        self.test_prompt(
            "How does this system work?",
            "Direct Reply - Help Question"
        )
        
        # Test 3: Direct reply - capability question
        self.test_prompt(
            "What data do you have access to?",
            "Direct Reply - Capability Question"
        )
        
        # Test 4: Task breakdown - simple data query
        self.test_prompt(
            "Show me claims in California",
            "Task Breakdown - Simple Data Query"
        )
        
        # Test 5: Task breakdown - complex analysis
        self.test_prompt(
            "Which drivers have the most claims this year?",
            "Task Breakdown - Complex Analysis Query"
        )
        
        # Test 6: Task breakdown - trend analysis
        self.test_prompt(
            "What are the accident trends by state?",
            "Task Breakdown - Trend Analysis Query"
        )
        
        # Test 7: Task breakdown - high value claims
        self.test_prompt(
            "Find all claims above $50,000",
            "Task Breakdown - High Value Claims Query"
        )
        
        # Test 8: Task breakdown - multiple criteria
        self.test_prompt(
            "Show me closed claims by adjuster in Texas for 2024",
            "Task Breakdown - Multi-Criteria Query"
        )
        
        # Test 9: Conversation context
        conversation_history = [
            "What is a preventable crash rate?",
            "A preventable crash rate is the percentage of crashes that could have been avoided through proper driver training and safety measures."
        ]
        self.test_prompt(
            "Now show me our current preventable crash rate",
            "Task Breakdown - With Conversation Context",
            conversation_history
        )
        
        # Test 10: Edge case - ambiguous query
        self.test_prompt(
            "Help me understand the data",
            "Edge Case - Ambiguous Query"
        )

def main():
    """Main function to run the orchestrator tests"""
    print("Starting Hirschbach Orchestrator Tests...")
    print("Note: This requires Azure OpenAI credentials to be configured")
    print()
    
    try:
        tester = OrchestratorTester()
        tester.run_all_tests()
        
        print(f"\n{'='*80}")
        print("TEST SUITE COMPLETED")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Failed to initialize tester: {e}")
        print("Make sure Azure OpenAI credentials are configured in your environment")

if __name__ == "__main__":
    main()
