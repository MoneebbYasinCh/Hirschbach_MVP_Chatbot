import os
import sys
import argparse
from typing import Dict, Any
from dotenv import load_dotenv

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Load environment variables
load_dotenv()

from Tools.entity_mapping_tool import EntityMappingTool

def setup_entity_mapping_tool():
    """Initialize the entity mapping tool"""
    try:
        tool = EntityMappingTool()
        print(" [TEST] Entity mapping tool initialized successfully")
        return tool
    except Exception as e:
        print(f" [TEST] Failed to initialize entity mapping tool: {str(e)}")
        return None

def run_entity_mapping_test(tool: EntityMappingTool, column_name: str):
    """Run a single entity mapping test and show detailed results"""
    print(f"\n{'='*80}")
    print(f" [TEST] Getting values for column: '{column_name}'")
    print(f"{'='*80}")
    
    try:
        # Run the tool
        print(f"\n [TEST] Running entity mapping tool...")
        result = tool.get_column_values(column_name)
        
        # Show detailed results
        print(f"\n [TEST] Results:")
        print(f"{''*60}")
        
        if result.get("success", False):
            print(f"\n [COLUMN VALUES]:")
            print(f"   Column: {result['column_name']}")
            print(f"   Values: {result['values']}")
            print(f"   Source: {result['column_info']['source']}")
            print(f"   Distinct count: {result['column_info']['distinct_count']}")
            print(f"   Sample values raw: {result['column_info']['sample_values_raw']}")
            
            print(f"\n [TOOL OUTPUT FOR OTHER NODES]:")
            print(f"   - KPI Editor can use: {len(result['values'])} values from '{result['column_name']}'")
            print(f"   - SQL Generation can use: {len(result['values'])} values from '{result['column_name']}'")
        else:
            print(f"\n [ERROR]:")
            print(f"   Error: {result.get('error', 'Unknown error')}")
            print(f"   Column: {result.get('column_name', 'Unknown')}")
            print(f"   Values: {result.get('values', [])}")
        
        return result
        
    except Exception as e:
        print(f" [TEST] Error during entity mapping: {str(e)}")
        return None

def interactive_mode(tool: EntityMappingTool):
    """Interactive testing mode"""
    print(f"\n{'='*80}")
    print(" [TEST] Interactive Entity Mapping Mode")
    print("Type your query and entities to see how the tool behaves")
    print("Type 'exit' or 'quit' to stop")
    print(f"{'='*80}")
    
    while True:
        try:
            user_input = input("\n💬 Enter your query: ").strip()
            
            if user_input.lower() in ["exit", "quit"]:
                print(" [TEST] Exiting interactive mode")
                break
            
            if not user_input:
                print(" [TEST] Please enter a query")
                continue
            
            # Ask for column name
            column_input = input("📝 Enter column name: ").strip()
            if not column_input:
                print(" [TEST] Please enter a column name")
                continue
            
            # Run the test
            run_entity_mapping_test(tool, column_input)
            
        except KeyboardInterrupt:
            print("\n [TEST] Exiting interactive mode")
            break
        except EOFError:
            print("\n [TEST] Exiting interactive mode")
            break
        except Exception as e:
            print(f" [TEST] Error in interactive mode: {str(e)}")

def run_automated_tests(tool: EntityMappingTool):
    """Run automated tests with various scenarios"""
    print(f"\n{'='*80}")
    print(" [TEST] Running Automated Tests")
    print(f"{'='*80}")
    
    test_cases = [
        "Preventable Flag",
        "Claim State", 
        "Accident Type",
        "Claim Date",
        "Driver Manager",
        "Note Last Updated User"
    ]
    
    for i, column_name in enumerate(test_cases, 1):
        print(f"\n [TEST] Test {i}/{len(test_cases)}")
        run_entity_mapping_test(tool, column_name)
        
        if i < len(test_cases):
            input("\n⏸️ Press Enter to continue to next test...")

def show_tool_info(tool: EntityMappingTool):
    """Show information about the tool and available columns"""
    print(f"\n{'='*80}")
    print("ℹ [TEST] Entity Mapping Tool Information")
    print(f"{'='*80}")
    
    # Show available columns
    columns = tool.get_available_columns()
    print(f"\n [AVAILABLE COLUMNS]:")
    for i, column in enumerate(columns, 1):
        print(f"   {i}. {column}")
    
    # Show column details for a few examples
    print(f"\n [COLUMN DETAILS]:")
    example_columns = ["Preventable Flag", "Claim Status", "Claim State"]
    
    for column in example_columns:
        if column in columns:
            info = tool.get_column_info(column)
            print(f"\n {column}:")
            print(f"      Sample values: {info.get('sample_values', 'N/A')}")
            print(f"      Distinct count: {info.get('distinct_count', 'N/A')}")
            print(f"      Unique values: {info.get('unique_values', [])}")

def main():
    """Main test function"""
    parser = argparse.ArgumentParser(description="Test Entity Mapping Tool")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--test", action="store_true", help="Run automated tests")
    parser.add_argument("--info", action="store_true", help="Show tool information")
    parser.add_argument("--entities", type=str, help="Column name to get values for")
    
    args = parser.parse_args()
    
    # Initialize the tool
    tool = setup_entity_mapping_tool()
    if not tool:
        print(" [TEST] Cannot proceed without tool initialization")
        return
    
    try:
        if args.info:
            # Show tool information
            show_tool_info(tool)
        elif args.entities:
            # Test specific column
            run_entity_mapping_test(tool, args.entities)
        elif args.test:
            # Run automated tests
            run_automated_tests(tool)
        elif args.interactive:
            # Interactive mode
            interactive_mode(tool)
        else:
            # Default to interactive mode
            print("No mode specified. Defaulting to interactive mode.")
            print("Use --test for automated tests, --info for tool information")
            interactive_mode(tool)
    
    except Exception as e:
        print(f" [TEST] Error in main: {str(e)}")

if __name__ == "__main__":
    main()
