#!/usr/bin/env python3
"""
Test each node individually to find the one causing the hang
"""

def test_node_loading():
    """Test each node to find which one hangs during initialization"""
    
    nodes_to_test = [
        ("orchestrator", "from Nodes.orchestrator import HirschbachOrchestrator; HirschbachOrchestrator()"),
        ("kpi_retrieval", "from Nodes.kpi_retrieval import KPIRetrievalNode; KPIRetrievalNode()"),
        ("metadata_retrieval", "from Nodes.metadata_retrieval import MetadataRetrievalNode; MetadataRetrievalNode()"),
        ("llm_checker", "from Nodes.llm_checker import LLMCheckerNode; LLMCheckerNode()"),
        ("kpi_editor", "from Nodes.kpi_editor import KPIEditorNode; KPIEditorNode()"),
        ("sql_generation", "from Nodes.sql_gen import SQLGenerationNode; SQLGenerationNode()"),
        ("azure_retrieval", "from Nodes.azure_retrieval import AzureRetrievalNode; AzureRetrievalNode()"),
        ("insight_generation", "from Nodes.insight_gen import InsightGenerationNode; InsightGenerationNode()"),
    ]
    
    print("Testing each node individually to find the problematic one...")
    
    for node_name, import_code in nodes_to_test:
        print(f"\n--- Testing {node_name} ---")
        try:
            print(f"Importing and creating {node_name}...")
            exec(import_code)
            print(f"✅ {node_name} created successfully")
        except Exception as e:
            print(f"❌ {node_name} failed: {e}")
            return node_name
    
    print("\n🎉 All nodes loaded successfully!")
    return None

if __name__ == "__main__":
    problematic_node = test_node_loading()
    if problematic_node:
        print(f"\n💥 Problematic node: {problematic_node}")
    else:
        print("\n🎉 No problematic nodes found!")

