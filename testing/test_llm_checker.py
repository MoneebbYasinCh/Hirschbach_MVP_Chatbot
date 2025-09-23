#!/usr/bin/env python3
"""
Test file for LLM Checker Node
Tests the intelligent decision making and routing logic
"""

import os
import sys
from typing import Dict, Any
from dotenv import load_dotenv

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Load environment variables from .env file
load_dotenv(os.path.join(project_root, '.env'))

from Nodes.llm_checker import LLMCheckerNode

def test_environment_setup():
    """Test if environment variables are properly set"""
    print("🔧 [TEST] Checking environment setup...")
    
    required_vars = [
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_KEY", 
        "AZURE_OPENAI_DEPLOYMENT",
        "AZURE_OPENAI_API_VERSION"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ [TEST] Missing environment variables: {missing_vars}")
        return False
    else:
        print("✅ [TEST] All environment variables are set")
        return True

def test_node_initialization():
    """Test if the LLM checker node initializes properly"""
    print("\n🔧 [TEST] Testing node initialization...")
    
    try:
        node = LLMCheckerNode()
        print("✅ [TEST] LLM Checker node initialized successfully")
        return True
    except Exception as e:
        print(f"❌ [TEST] Failed to initialize node: {e}")
        return False

def create_test_state(task: str, kpi_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Create a test state with the given task and KPI data"""
    # Create a proper message object that mimics the real state structure
    class MockMessage:
        def __init__(self, content):
            self.content = content
    
    state = {
        "messages": [MockMessage(task)],
        "task": task,
        "kpi_retrieval_status": "completed",
        "metadata_retrieval_status": "completed"
    }
    
    if kpi_data:
        state["top_kpi"] = kpi_data
    
    return state

def test_perfect_match_scenario():
    """Test perfect match scenario"""
    print("\n🔧 [TEST] Testing PERFECT_MATCH scenario...")
    
    task = "show me closed claims by state"
    kpi_data = {
        "metric_name": "Closed Claims by State",
        "description": "Shows count of closed claims grouped by state",
        "score": 0.95,
        "sql_query": "SELECT State, COUNT(*) as closed_claims FROM claims_summary WHERE Status_Flag = 'Closed' GROUP BY State",
        "table_columns": "State, Status_Flag"
    }
    
    state = create_test_state(task, kpi_data)
    
    try:
        node = LLMCheckerNode()
        result = node(state)
        
        print(f"📊 [RESULT] Decision: {result['llm_check_result']['decision_type']}")
        print(f"📊 [RESULT] Next Node: {result['next_node']}")
        print(f"📊 [RESULT] Reasoning: {result['llm_check_result']['reasoning']}")
        print(f"📊 [RESULT] Confidence: {result['llm_check_result']['confidence']}")
        
        expected_node = "azure_retrieval"  # Fixed: should be azure_retrieval not aws_retrieval
        if result['next_node'] == expected_node:
            print(f"✅ [TEST] Correctly routed to {expected_node}")
            return True
        else:
            print(f"❌ [TEST] Expected {expected_node}, got {result['next_node']}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error in perfect match test: {e}")
        return False

def test_needs_edit_scenario():
    """Test needs minor edit scenario"""
    print("\n🔧 [TEST] Testing NEEDS_MINOR_EDIT scenario...")
    
    task = "show me closed claims this year by state"
    kpi_data = {
        "metric_name": "Closed Claims by State",
        "description": "Shows count of closed claims grouped by state",
        "score": 0.75,
        "sql_query": "SELECT State, COUNT(*) as closed_claims FROM claims_summary WHERE Status_Flag = 'Closed' GROUP BY State",
        "table_columns": "State, Status_Flag"
    }
    
    state = create_test_state(task, kpi_data)
    
    try:
        node = LLMCheckerNode()
        result = node(state)
        
        print(f"📊 [RESULT] Decision: {result['llm_check_result']['decision_type']}")
        print(f"📊 [RESULT] Next Node: {result['next_node']}")
        print(f"📊 [RESULT] Reasoning: {result['llm_check_result']['reasoning']}")
        print(f"📊 [RESULT] Confidence: {result['llm_check_result']['confidence']}")
        
        expected_node = "kpi_editor"
        if result['next_node'] == expected_node:
            print(f"✅ [TEST] Correctly routed to {expected_node}")
            return True
        else:
            print(f"❌ [TEST] Expected {expected_node}, got {result['next_node']}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error in needs edit test: {e}")
        return False

def test_not_relevant_scenario():
    """Test not relevant scenario"""
    print("\n🔧 [TEST] Testing NOT_RELEVANT scenario...")
    
    task = "show me patient demographics"
    kpi_data = {
        "metric_name": "Closed Claims by State",
        "description": "Shows count of closed claims grouped by state",
        "score": 0.25,
        "sql_query": "SELECT State, COUNT(*) as closed_claims FROM claims_summary WHERE Status_Flag = 'Closed' GROUP BY State",
        "table_columns": "State, Status_Flag"
    }
    
    state = create_test_state(task, kpi_data)
    
    try:
        node = LLMCheckerNode()
        result = node(state)
        
        print(f"📊 [RESULT] Decision: {result['llm_check_result']['decision_type']}")
        print(f"📊 [RESULT] Next Node: {result['next_node']}")
        print(f"📊 [RESULT] Reasoning: {result['llm_check_result']['reasoning']}")
        print(f"📊 [RESULT] Confidence: {result['llm_check_result']['confidence']}")
        
        expected_node = "sql_generation"
        if result['next_node'] == expected_node:
            print(f"✅ [TEST] Correctly routed to {expected_node}")
            return True
        else:
            print(f"❌ [TEST] Expected {expected_node}, got {result['next_node']}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error in not relevant test: {e}")
        return False

def test_no_kpi_scenario():
    """Test scenario when no KPI is found"""
    print("\n🔧 [TEST] Testing NO_KPI scenario...")
    
    task = "show me some data"
    state = create_test_state(task)  # No KPI data
    
    try:
        node = LLMCheckerNode()
        result = node(state)
        
        print(f"📊 [RESULT] Decision: {result['llm_check_result']['decision_type']}")
        print(f"📊 [RESULT] Next Node: {result['next_node']}")
        print(f"📊 [RESULT] Reasoning: {result['llm_check_result']['reasoning']}")
        
        expected_node = "sql_generation"
        if result['next_node'] == expected_node:
            print(f"✅ [TEST] Correctly routed to {expected_node} when no KPI found")
            return True
        else:
            print(f"❌ [TEST] Expected {expected_node}, got {result['next_node']}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error in no KPI test: {e}")
        return False

def test_claims_distribution_scenario():
    """Test the specific claims distribution scenario"""
    print("\n🔧 [TEST] Testing Claims Distribution scenario...")
    
    task = "Show the distribution of claims across different claim categories"
    kpi_data = {
        "metric_name": "Claims by Type (Work Comp, Cargo, Crash)",
        "description": "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently.",
        "score": 0.95,
        "sql_query": "select [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) from PRD.CLAIMS_SUMMARY cs group by [Accident or Incident Code]",
        "table_columns": "Claims_Summary: [Accident or Incident Code], [Claim Number]"
    }
    
    state = create_test_state(task, kpi_data)
    
    try:
        node = LLMCheckerNode()
        result = node(state)
        
        print(f"📊 [RESULT] Decision: {result['llm_check_result']['decision_type']}")
        print(f"📊 [RESULT] Next Node: {result['next_node']}")
        print(f"📊 [RESULT] Reasoning: {result['llm_check_result']['reasoning']}")
        print(f"📊 [RESULT] Confidence: {result['llm_check_result']['confidence']}")
        print(f"📊 [RESULT] KPI Metric: {result['llm_check_result']['kpi_metric']}")
        # Note: kpi_score is not included in llm_check_result, it's in the original KPI data
        
        # This should be a perfect match since the KPI exactly matches the user query
        expected_decision = "perfect_match"
        expected_node = "azure_retrieval"  # Fixed: should be azure_retrieval not aws_retrieval
        
        if result['llm_check_result']['decision_type'] == expected_decision and result['next_node'] == expected_node:
            print(f"✅ [TEST] Correctly identified as {expected_decision} and routed to {expected_node}")
            return True
        else:
            print(f"❌ [TEST] Expected {expected_decision} -> {expected_node}, got {result['llm_check_result']['decision_type']} -> {result['next_node']}")
            return False
            
    except Exception as e:
        print(f"❌ [TEST] Error in claims distribution test: {e}")
        return False

def test_complex_scenarios():
    """Test more complex real-world scenarios"""
    print("\n🔧 [TEST] Testing complex scenarios...")
    
    scenarios = [
        {
            "name": "High Value Claims",
            "task": "show me high value claims over $100k",
            "kpi": {
                "metric_name": "Claims by Amount",
                "description": "Shows claims grouped by amount ranges",
                "score": 0.80,
                "sql_query": "SELECT Amount_Range, COUNT(*) FROM claims_summary GROUP BY Amount_Range",
                "table_columns": "Total_Incurred"
            },
            "expected_decision": "needs_minor_edit"
        },
        {
            "name": "Adjuster Performance",
            "task": "show me adjuster performance metrics",
            "kpi": {
                "metric_name": "Claims by Adjuster",
                "description": "Shows claims count by adjuster",
                "score": 0.90,
                "sql_query": "SELECT Last_Updated_AdjusterName, COUNT(*) FROM claims_summary GROUP BY Last_Updated_AdjusterName",
                "table_columns": "Last_Updated_AdjusterName"
            },
            "expected_decision": "perfect_match"
        },
        {
            "name": "Appointment Scheduling",
            "task": "show me appointment scheduling data",
            "kpi": {
                "metric_name": "Claims by Department",
                "description": "Shows claims count by department",
                "score": 0.15,
                "sql_query": "SELECT Department, COUNT(*) FROM claims_summary GROUP BY Department",
                "table_columns": "Department"
            },
            "expected_decision": "not_relevant"
        }
    ]
    
    results = []
    for scenario in scenarios:
        print(f"\n  📋 Testing: {scenario['name']}")
        state = create_test_state(scenario['task'], scenario['kpi'])
        
        try:
            node = LLMCheckerNode()
            result = node(state)
            
            decision = result['llm_check_result']['decision_type']
            next_node = result['next_node']
            reasoning = result['llm_check_result']['reasoning']
            
            print(f"    📊 Decision: {decision}")
            print(f"    📊 Next Node: {next_node}")
            print(f"    📊 Reasoning: {reasoning[:100]}...")
            
            # Check if decision matches expected
            if decision == scenario['expected_decision']:
                print(f"    ✅ Correct decision: {decision}")
                results.append(True)
            else:
                print(f"    ⚠️  Expected {scenario['expected_decision']}, got {decision}")
                results.append(False)
                
        except Exception as e:
            print(f"    ❌ Error: {e}")
            results.append(False)
    
    return all(results)

def interactive_llm_checker_test():
    """Interactive test for LLM checker - allows manual input"""
    print("\n🔧 [INTERACTIVE] LLM Checker Interactive Test")
    print("=" * 50)
    
    node = LLMCheckerNode()
    
    while True:
        print("\n" + "─" * 50)
        print("Enter test data (or 'quit' to exit):")
        
        # Get user query
        task = input("User Query: ").strip()
        if task.lower() == 'quit':
            break
            
        # Get KPI data
        print("\nKPI Information:")
        kpi_name = input("KPI Name (or press Enter to skip KPI): ").strip()
        
        if kpi_name:
            kpi_description = input("KPI Description: ").strip()
            kpi_sql = input("KPI SQL: ").strip()
            
            kpi_data = {
                "metric_name": kpi_name,
                "description": kpi_description,
                "sql_query": kpi_sql,
                "score": 0.85  # Default score
            }
            
            state = create_test_state(task, kpi_data)
        else:
            state = create_test_state(task)  # No KPI
        
        # Run the test
        print(f"\n🧠 [TESTING] Processing: '{task}'")
        print("─" * 30)
        
        try:
            result = node(state)
            
            print("\n📊 [RESULTS]")
            print(f"  Decision: {result['llm_check_result']['decision_type']}")
            print(f"  Next Node: {result['next_node']}")
            print(f"  Reasoning: {result['llm_check_result']['reasoning']}")
            print(f"  Confidence: {result['llm_check_result']['confidence']}")
            
            if 'kpi_metric' in result['llm_check_result']:
                print(f"  KPI Metric: {result['llm_check_result']['kpi_metric']}")
            if 'kpi_sql' in result['llm_check_result']:
                print(f"  KPI SQL: {result['llm_check_result']['kpi_sql'][:100]}...")
                
        except Exception as e:
            print(f"❌ [ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

def test_specific_problem_cases():
    """Test specific cases that are causing problems"""
    print("\n🔧 [TEST] Testing Specific Problem Cases...")
    
    problem_cases = [
        {
            "name": "Claims Distribution - Exact Match",
            "task": "Show the distribution of claims across different claim categories",
            "kpi": {
                "metric_name": "Claims by Type (Work Comp, Cargo, Crash)",
                "description": "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently.",
                "sql_query": "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) AS claim_count FROM PRD.CLAIMS_SUMMARY GROUP BY [Accident or Incident Code]",
                "score": 0.95
            },
            "expected_decision": "perfect_match",
            "expected_node": "azure_retrieval"
        },
        {
            "name": "Claims Distribution - With Time Filter",
            "task": "Show the distribution of claims across different claim categories this month",
            "kpi": {
                "metric_name": "Claims by Type (Work Comp, Cargo, Crash)",
                "description": "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently.",
                "sql_query": "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) AS claim_count FROM PRD.CLAIMS_SUMMARY GROUP BY [Accident or Incident Code]",
                "score": 0.95
            },
            "expected_decision": "needs_minor_edit",
            "expected_node": "kpi_editor"
        },
        {
            "name": "Completely Different Request",
            "task": "Show me employee payroll information",
            "kpi": {
                "metric_name": "Claims by Type (Work Comp, Cargo, Crash)",
                "description": "Shows the distribution of claims across different claim categories (e.g., Work Compensation, Cargo, Crash). Helps identify which types of claims occur most frequently.",
                "sql_query": "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) AS claim_count FROM PRD.CLAIMS_SUMMARY GROUP BY [Accident or Incident Code]",
                "score": 0.95
            },
            "expected_decision": "not_relevant",
            "expected_node": "sql_generation"
        }
    ]
    
    results = []
    node = LLMCheckerNode()
    
    for case in problem_cases:
        print(f"\n  🧪 Testing: {case['name']}")
        print(f"     Task: {case['task']}")
        print(f"     Expected: {case['expected_decision']} → {case['expected_node']}")
        
        state = create_test_state(case['task'], case['kpi'])
        
        try:
            result = node(state)
            
            actual_decision = result['llm_check_result']['decision_type']
            actual_node = result['next_node']
            
            print(f"     Actual:   {actual_decision} → {actual_node}")
            
            # Check if both decision and routing are correct
            decision_correct = actual_decision == case['expected_decision']
            routing_correct = actual_node == case['expected_node']
            
            if decision_correct and routing_correct:
                print(f"     ✅ PASS - Correct decision and routing")
                results.append(True)
            else:
                print(f"     ❌ FAIL - Decision: {'✓' if decision_correct else '✗'}, Routing: {'✓' if routing_correct else '✗'}")
                results.append(False)
                
                # Show the raw LLM response for debugging
                print(f"     🔍 Debug Info:")
                if 'reasoning' in result['llm_check_result']:
                    print(f"        Reasoning: {result['llm_check_result']['reasoning']}")
                    
        except Exception as e:
            print(f"     ❌ ERROR: {e}")
            results.append(False)
    
    success_rate = sum(results) / len(results) * 100
    print(f"\n📊 [PROBLEM CASES] Success Rate: {success_rate:.1f}% ({sum(results)}/{len(results)})")
    
    return all(results)

def main():
    """Run all tests"""
    print("🚀 [TEST] Starting LLM Checker Node Tests")
    print("=" * 60)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Node Initialization", test_node_initialization),
        ("Specific Problem Cases", test_specific_problem_cases),
        ("Claims Distribution Scenario", test_claims_distribution_scenario),
        ("Perfect Match Scenario", test_perfect_match_scenario),
        ("Needs Edit Scenario", test_needs_edit_scenario),
        ("Not Relevant Scenario", test_not_relevant_scenario),
        ("No KPI Scenario", test_no_kpi_scenario),
        ("Complex Scenarios", test_complex_scenarios)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                passed += 1
                print(f"✅ [TEST] {test_name} PASSED")
            else:
                print(f"❌ [TEST] {test_name} FAILED")
        except Exception as e:
            print(f"❌ [TEST] {test_name} ERROR: {e}")
    
    print(f"\n{'='*60}")
    print(f"📊 [SUMMARY] Tests Passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 [SUCCESS] All tests passed!")
    else:
        print("⚠️  [WARNING] Some tests failed. Check the output above.")
    
    return passed == total

if __name__ == "__main__":
    import sys
    
    # Check if interactive mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "interactive":
        print("🔧 [MODE] Starting Interactive LLM Checker Test")
        interactive_llm_checker_test()
    else:
        print("🔧 [MODE] Running Automated Tests")
        print("💡 [TIP] Use 'python test_llm_checker.py interactive' for interactive testing")
        success = main()
        sys.exit(0 if success else 1)
