#!/usr/bin/env python3
"""
Test file for Azure Retrieval Node
Tests the AzureRetrievalNode functionality with the working connection string
"""

import os
import sys
from dotenv import load_dotenv

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv()

def test_azure_retrieval_node():
    """Test the Azure Retrieval Node"""
    print("Testing Azure Retrieval Node")
    print("=" * 50)
    
    try:
        from Nodes.azure_retrieval import AzureRetrievalNode
        
        # Initialize the node
        print("1. Initializing Azure Retrieval Node...")
        node = AzureRetrievalNode()
        
        # Check if connection string is loaded
        if not node.connection_string:
            print("❌ No SQL_CONNECTION_STRING found in environment")
            print("Add this to your .env file:")
            print('SQL_CONNECTION_STRING="Driver={SQL Server};Server=hmldatastore.database.windows.net;Database=Warehouse;Encrypt=yes;TrustServerCertificate=no;UID=BI_Reader;PWD=hepqir-3hybmu-wAvhef"')
            return False
        
        print(f"✅ Connection string loaded: {node.connection_string[:50]}...")
        
        # Test connection validation
        print("\n2. Testing connection validation...")
        is_connected = node._validate_connection()
        if is_connected:
            print("✅ Connection validation successful")
        else:
            print("❌ Connection validation failed")
            return False
        
        # Test simple query execution
        print("\n3. Testing simple query execution...")
        test_query = "SELECT [Accident or Incident Code] AS Type, COUNT(DISTINCT [Claim Number]) as ClaimCount FROM PRD.CLAIMS_SUMMARY cs GROUP BY [Accident or Incident Code]"
        result = node._execute_sql_query(test_query)
        
        if result and result.get('data') is not None:
            print("✅ Query execution successful")
            print(f"   Rows returned: {result.get('row_count', 0)}")
            print(f"   Columns: {result.get('columns', [])}")
            print(f"   Execution time: {result.get('execution_time', 'N/A')}")
            if result.get('data'):
                print(f"   Sample data: {result['data'][0]}")
        else:
            print("❌ Query execution failed")
            if result:
                print(f"   Error: {result.get('error', 'Unknown error')}")
            else:
                print("   Error: Query returned None")
            return False
        
        # Test with sample state (simulating graph workflow)
        print("\n4. Testing with sample state...")
        sample_state = {
            'validated_sql': "SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES",
            'kpi_results': {'test': 'value'},
            'azure_data': None
        }
        
        result_state = node(sample_state)
        
        if result_state and result_state.get('azure_data'):
            print("✅ Node execution with state successful")
            azure_data = result_state['azure_data']
            print(f"   Rows returned: {len(azure_data.get('data', []))}")
            print(f"   Execution time: {azure_data.get('execution_time', 'N/A')} seconds")
        else:
            print("❌ Node execution with state failed")
            return False
        
        print("\n🎉 All tests passed! Azure Retrieval Node is working correctly.")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're in the project root directory")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_connection_string_format():
    """Test if the connection string format is correct"""
    print("\nTesting Connection String Format")
    print("=" * 50)
    
    connection_string = os.getenv("SQL_CONNECTION_STRING")
    if not connection_string:
        print("❌ SQL_CONNECTION_STRING not found")
        return False
    
    print(f"Connection string: {connection_string}")
    
    # Check for required components (Microsoft official format)
    required_parts = [
        "Driver={SQL Server}",  # or {ODBC Driver 18 for SQL Server}
        "Server=hmldatastore.database.windows.net",
        "Database=Warehouse",
        "Encrypt=yes",
        "TrustServerCertificate=no",
        "UID=BI_Reader",
        "PWD="
    ]
    
    missing_parts = []
    for part in required_parts:
        if part not in connection_string:
            missing_parts.append(part)
    
    if missing_parts:
        print(f"❌ Missing required parts: {missing_parts}")
        return False
    else:
        print("✅ Connection string format looks correct")
        return True

if __name__ == "__main__":
    print("Azure Retrieval Node Test Suite")
    print("=" * 60)
    
    # Test 1: Connection string format
    format_ok = test_connection_string_format()
    
    if format_ok:
        # Test 2: Node functionality
        node_ok = test_azure_retrieval_node()
        
        if node_ok:
            print("\n🎉 ALL TESTS PASSED!")
            print("Your Azure Retrieval Node is ready to use in the graph workflow.")
        else:
            print("\n❌ Some tests failed. Check the errors above.")
    else:
        print("\n❌ Connection string format is incorrect. Fix the .env file first.")
