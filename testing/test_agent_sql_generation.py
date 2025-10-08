#!/usr/bin/env python3
"""
Test script for Hirschbach AI Agent SQL Generation
Runs each prompt from test_prompts.json through the agent and captures the generated SQL query
Outputs results to an HTML table comparing prompts, expected SQL, and generated SQL
"""

import os
import sys
import json
import logging
import pyodbc
from datetime import datetime
from typing import Dict, Any, List
from langchain_core.messages import HumanMessage, AIMessage
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables
load_dotenv()

# Set up logging to reduce noise
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("langchain").setLevel(logging.WARNING)
logging.getLogger("langgraph").setLevel(logging.WARNING)
logging.getLogger("Graph_Flow").setLevel(logging.WARNING)
logging.getLogger("Nodes").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

from Graph_Flow.main_graph import create_main_graph
from State.main_state import HirschbachGraphState


class SQLGenerationTester:
    """Test class for running agent against test prompts and capturing SQL queries"""

    def __init__(self):
        self.graph = create_main_graph()
        self.test_results = []

    def run_test_for_prompt(self, prompt: str, expected_sql: str) -> Dict[str, Any]:
        """
        Run the agent for a single prompt and capture both expected and generated SQL queries with their data

        Args:
            prompt: The test prompt
            expected_sql: The expected SQL query from test data

        Returns:
            Dict containing prompt, expected_sql, expected_data, generated_sql, generated_data, and status
        """
        print(f"\n=== Testing Prompt: {prompt[:50]}... ===")

        try:
            # First, execute the expected SQL query to get baseline data
            expected_data = []
            try:
                expected_data = self._execute_sql_query(expected_sql)[:5]  # Limit to 5 rows
                print(f"Expected SQL executed successfully: {len(expected_data)} rows")
            except Exception as e:
                print(f"Error executing expected SQL: {str(e)}")
                expected_data = []

            # Create initial state
            messages = [HumanMessage(content=prompt)]
            initial_state = HirschbachGraphState(
                messages=messages,
                user_query=prompt,
                workflow_status="active",
                sql_query_history=[]
            )

            # Run the full graph including azure_retrieval
            result_state = self._run_full_graph_with_retrieval(initial_state)

            # Extract the SQL query and retrieved data
            generated_sql = self._extract_sql_from_state(result_state)
            generated_data = self._extract_retrieved_data_from_state(result_state)

            print(f"Expected SQL: {expected_sql[:100]}...")
            print(f"Expected data: {len(expected_data)} rows")
            print(f"Generated SQL: {generated_sql[:100]}..." if generated_sql else "Generated SQL: None")
            print(f"Generated data: {len(generated_data) if generated_data else 0} rows")

            return {
                "prompt": prompt,
                "expected_sql": expected_sql,
                "expected_data": expected_data,
                "generated_sql": generated_sql or "No SQL generated",
                "generated_data": generated_data or [],
                "status": "success" if generated_sql else "failed",
                "error": None
            }

        except Exception as e:
            print(f"Error testing prompt: {str(e)}")
            return {
                "prompt": prompt,
                "expected_sql": expected_sql,
                "expected_data": [],
                "generated_sql": "Error occurred",
                "generated_data": [],
                "status": "error",
                "error": str(e)
            }

    def _run_full_graph_with_retrieval(self, state: HirschbachGraphState) -> HirschbachGraphState:
        """
        Run the complete graph including azure_retrieval to get both SQL and data

        Args:
            state: Initial state

        Returns:
            Final state after complete execution including data retrieval
        """
        # Run the complete graph with proper configuration
        try:
            # Use the same thread_id as in app.py
            thread_id = "test_conversation"
            config = {"configurable": {"thread_id": thread_id}}
            result = self.graph.invoke(state, config)
            return result
        except Exception as e:
            print(f"Error running full graph: {str(e)}")
            # Return the state as is if there's an error
            return state

    def _extract_sql_from_state(self, state: HirschbachGraphState) -> str:
        """
        Extract the SQL query from the state

        Args:
            state: State after SQL generation

        Returns:
            The SQL query string or None if not found
        """
        # Check multiple possible locations for the SQL query

        # 1. Check generated_sql (from SQL generation or KPI editor)
        generated_sql = state.get("generated_sql")
        if generated_sql and generated_sql.strip():
            print(f"Found SQL in generated_sql: {generated_sql[:50]}...")
            return generated_sql.strip()

        # 2. Check top_kpi (from KPI retrieval, possibly edited)
        top_kpi = state.get("top_kpi")
        if top_kpi and top_kpi.get("sql_query"):
            sql_query = top_kpi["sql_query"].strip()
            print(f"Found SQL in top_kpi: {sql_query[:50]}...")
            return sql_query

        # 3. Check edited_kpi (from KPI editor)
        edited_kpi = state.get("edited_kpi")
        if edited_kpi and edited_kpi.get("sql_query"):
            sql_query = edited_kpi["sql_query"].strip()
            print(f"Found SQL in edited_kpi: {sql_query[:50]}...")
            return sql_query

        # 4. Check sql_modification_result (from SQL modifier)
        sql_modification_result = state.get("sql_modification_result")
        if sql_modification_result and sql_modification_result.get("modified_sql"):
            sql_query = sql_modification_result["modified_sql"].strip()
            print(f"Found SQL in sql_modification_result: {sql_query[:50]}...")
            return sql_query

        print("No SQL query found in state")
        return None

    def _extract_retrieved_data_from_state(self, state: HirschbachGraphState) -> List[Dict[str, Any]]:
        """
        Extract the retrieved data from the state (azure_data)

        Args:
            state: State after complete execution

        Returns:
            List of data rows (truncated to 5 rows) or empty list if no data
        """
        azure_data = state.get("azure_data", {})

        if not azure_data.get("success"):
            print("No successful data retrieval found in state")
            return []

        data = azure_data.get("data", [])
        if not data:
            print("No data found in azure_data")
            return []

        # Truncate to first 5 rows
        truncated_data = data[:5]
        print(f"Extracted {len(truncated_data)} rows of data (showing first 5)")

        return truncated_data

    def _execute_sql_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query directly against Azure SQL Database

        Args:
            sql_query: SQL query to execute

        Returns:
            List of data rows (limited to 5 rows)
        """
        connection_string = os.getenv("SQL_CONNECTION_STRING")
        if not connection_string:
            raise Exception("SQL_CONNECTION_STRING environment variable not set")

        try:
            with pyodbc.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)

                    # Get column names
                    columns = [column[0] for column in cursor.description] if cursor.description else []

                    # Fetch up to 5 rows
                    rows = cursor.fetchall()[:5]

                    # Convert rows to list of dictionaries
                    data = []
                    for row in rows:
                        row_dict = {}
                        for i, value in enumerate(row):
                            # Convert datetime objects to strings for JSON serialization
                            if isinstance(value, datetime):
                                row_dict[columns[i]] = value.isoformat()
                            else:
                                row_dict[columns[i]] = value
                        data.append(row_dict)

                    return data

        except Exception as e:
            raise Exception(f"Database error: {str(e)}")

    def _generate_data_table_html(self, data: List[Dict[str, Any]]) -> str:
        """
        Generate HTML table for retrieved data

        Args:
            data: List of data rows

        Returns:
            HTML string for the data table or message if no data
        """
        if not data:
            return '<div class="no-data">No data retrieved</div>'

        # Get column names from first row
        columns = list(data[0].keys())

        # Start building table
        html = '<div class="data-table-container"><table class="data-table">'

        # Add header row
        html += '<thead><tr>'
        for col in columns:
            html += f'<th>{col}</th>'
        html += '</tr></thead>'

        # Add data rows
        html += '<tbody>'
        for row in data:
            html += '<tr>'
            for col in columns:
                value = row.get(col, '')
                # Truncate long values for display
                if isinstance(value, str) and len(value) > 50:
                    value = value[:50] + '...'
                html += f'<td>{value}</td>'
            html += '</tr>'
        html += '</tbody></table></div>'

        return html

    def run_all_tests(self, test_prompts: List[Dict[str, str]], output_file: str = "test_results.html") -> List[Dict[str, Any]]:
        """
        Run tests for all prompts and update HTML report incrementally

        Args:
            test_prompts: List of test prompt dictionaries
            output_file: Output HTML file path

        Returns:
            List of test results
        """
        results = []

        for i, test_case in enumerate(test_prompts, 1):
            prompt = test_case.get("Prompt", "")
            expected_sql = test_case.get("Query", "")

            if not prompt or not expected_sql:
                print(f"Skipping test case {i}: missing prompt or expected SQL")
                continue

            print(f"\n--- Running test {i}/{len(test_prompts)} ---")
            result = self.run_test_for_prompt(prompt, expected_sql)
            results.append(result)

            # Add result to HTML report immediately
            self.add_test_result_to_html(result, output_file)

        return results

    def initialize_html_report(self, output_file: str = "test_results.html"):
        """
        Initialize empty HTML report file

        Args:
            output_file: Output HTML file path
        """
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hirschbach Agent SQL Generation Test Results</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background-color: #2c3e50;
            color: white;
            padding: 20px;
            text-align: center;
            border-radius: 8px;
            margin-bottom: 20px;
        }}
        .stats {{
            display: flex;
            justify-content: space-around;
            margin-bottom: 20px;
        }}
        .stat-box {{
            background-color: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
            flex: 1;
            margin: 0 10px;
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .stat-label {{
            color: #7f8c8d;
            margin-top: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background-color: white;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th:nth-child(1), td:nth-child(1) {{ width: 20%; }}
        th:nth-child(2), td:nth-child(2) {{ width: 22.5%; }}
        th:nth-child(3), td:nth-child(3) {{ width: 22.5%; }}
        th:nth-child(4), td:nth-child(4) {{ width: 25%; }}
        th {{
            background-color: #000000;
            color: white;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f8f9fa;
        }}
        tr:hover {{
            background-color: #e8f4f8;
        }}
        .status-success {{
            color: #27ae60;
            font-weight: bold;
        }}
        .status-failed {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .status-error {{
            color: #f39c12;
            font-weight: bold;
        }}
        .sql-code {{
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            background-color: #f8f9fa;
            padding: 8px;
            border-radius: 4px;
            border: 1px solid #dee2e6;
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 200px;
            overflow-y: auto;
        }}
        .prompt-text {{
            font-weight: bold;
            color: #2c3e50;
        }}
        .running {{
            background-color: #fff3cd !important;
            border-left: 4px solid #ffc107;
        }}
        .completed {{
            background-color: #d1ecf1 !important;
            border-left: 4px solid #17a2b8;
        }}
        .data-table-container {{
            max-height: 300px;
            width: 100%;
            overflow: auto;
            border: 1px solid #dee2e6;
            border-radius: 4px;
        }}
        .data-table {{
            width: 100%;
            min-width: 300px;
            border-collapse: collapse;
            font-size: 0.85em;
        }}
        .data-table th {{
            background-color: #f8f9fa;
            color: black;
            padding: 8px 12px;
            text-align: left;
            font-weight: bold;
            border-bottom: 2px solid #dee2e6;
            position: sticky;
            top: 0;
        }}
        .data-table td {{
            padding: 6px 12px;
            border-bottom: 1px solid #dee2e6;
            min-width: 80px;
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .data-table tr:nth-child(even) {{
            background-color: #f8f9fa;
        }}
        .data-table tr:hover {{
            background-color: #e8f4f8;
        }}
        .no-data {{
            color: #6c757d;
            font-style: italic;
            text-align: center;
            padding: 20px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Hirschbach AI Agent SQL Generation Test Results</h1>
        <p>Test results comparing expected vs generated SQL queries and their data results</p>
    </div>

    <div class="stats">
        <div class="stat-box">
            <div class="stat-number" id="total-tests">0</div>
            <div class="stat-label">Total Tests</div>
        </div>
        <div class="stat-box">
            <div class="stat-number" id="sql-generated">0</div>
            <div class="stat-label">SQL Generated</div>
        </div>
        <div class="stat-box">
            <div class="stat-number" id="failed">0</div>
            <div class="stat-label">Failed</div>
        </div>
        <div class="stat-box">
            <div class="stat-number" id="errors">0</div>
            <div class="stat-label">Errors</div>
        </div>
    </div>

    <table>
        <thead>
            <tr>
                <th style="width: 15%;">Prompt</th>
                <th style="width: 18%;">Expected SQL Query</th>
                <th style="width: 18%;">Generated SQL Query</th>
                <th style="width: 24%;">Expected Data Results</th>
                <th style="width: 25%;">Generated Data Results</th>
            </tr>
        </thead>
        <tbody id="test-results-body">
        </tbody>
    </table>
</body>
</html>
"""

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"HTML report initialized: {output_file}")

    def add_test_result_to_html(self, result: Dict[str, Any], output_file: str = "test_results.html"):
        """
        Add a single test result to the existing HTML report

        Args:
            result: Single test result
            output_file: Output HTML file path
        """
        # Read the current HTML content
        with open(output_file, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # Find the tbody and add the new row
        tbody_marker = '<tbody id="test-results-body">'
        insert_pos = html_content.find(tbody_marker) + len(tbody_marker)

        status_class = f"status-{result['status']}"

        # Generate HTML tables for both expected and generated data
        expected_data_html = self._generate_data_table_html(result['expected_data'])
        generated_data_html = self._generate_data_table_html(result['generated_data'])

        new_row = f"""
            <tr class="completed">
                <td class="prompt-text">{result['prompt']}</td>
                <td><div class="sql-code">{result['expected_sql']}</div></td>
                <td>
                    <div class="sql-code">{result['generated_sql']}</div>
                    {f'<div class="{status_class}">Status: {result["status"]}</div>' if result['status'] != 'success' else ''}
                    {f'<div class="status-error">Error: {result["error"]}</div>' if result.get('error') else ''}
                </td>
                <td>{expected_data_html}</td>
                <td>{generated_data_html}</td>
            </tr>
"""

        # Insert the new row
        html_content = html_content[:insert_pos] + new_row + html_content[insert_pos:]

        # Update statistics in the HTML
        # We need to count current results by reading the table rows
        tbody_start = html_content.find(tbody_marker)
        tbody_end = html_content.find('</tbody>', tbody_start)
        tbody_content = html_content[tbody_start:tbody_end]

        # Count completed rows (those with class="completed")
        completed_rows = tbody_content.count('class="completed"')

        # Update the stats div
        stats_replacements = [
            ('<div class="stat-number" id="total-tests">0</div>', f'<div class="stat-number" id="total-tests">{completed_rows}</div>'),
            ('<div class="stat-number" id="sql-generated">0</div>', f'<div class="stat-number" id="sql-generated">{completed_rows}</div>'),  # Assume all completed are successful for now
        ]

        for old, new in stats_replacements:
            html_content = html_content.replace(old, new, 1)

        # Write back the updated HTML
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"Added test result to HTML report: {result['prompt'][:50]}...")

    def finalize_html_report(self, results: List[Dict[str, Any]], output_file: str = "test_results.html"):
        """
        Finalize HTML report with correct statistics

        Args:
            results: All test results
            output_file: Output HTML file path
        """
        # Read the current HTML content
        with open(output_file, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # Update final statistics
        success_count = len([r for r in results if r['status'] == 'success'])
        failed_count = len([r for r in results if r['status'] == 'failed'])
        error_count = len([r for r in results if r['status'] == 'error'])

        html_content = html_content.replace(
            '<div class="stat-number" id="total-tests">0</div>',
            f'<div class="stat-number" id="total-tests">{len(results)}</div>'
        )
        html_content = html_content.replace(
            '<div class="stat-number" id="sql-generated">0</div>',
            f'<div class="stat-number" id="sql-generated">{success_count}</div>'
        )
        html_content = html_content.replace(
            '<div class="stat-number" id="failed">0</div>',
            f'<div class="stat-number" id="failed">{failed_count}</div>'
        )
        html_content = html_content.replace(
            '<div class="stat-number" id="errors">0</div>',
            f'<div class="stat-number" id="errors">{error_count}</div>'
        )

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"\nHTML report finalized: {output_file}")


def main():
    """Main function to run the tests"""
    print("Starting Hirschbach Agent SQL Generation Tests...")

    # Load test prompts
    test_prompts_file = os.path.join(os.path.dirname(__file__), "test_prompts.json")

    try:
        with open(test_prompts_file, 'r', encoding='utf-8') as f:
            test_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Test prompts file not found at {test_prompts_file}")
        return
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in test prompts file: {e}")
        return

    print(f"Loaded {len(test_data)} test prompts")

    # Initialize tester
    tester = SQLGenerationTester()

    # Initialize HTML report (clear any existing file)
    output_file = os.path.join(os.path.dirname(__file__), "test_results.html")
    tester.initialize_html_report(output_file)

    # Run all tests (HTML will be updated incrementally)
    results = tester.run_all_tests(test_data, output_file)

    # Finalize HTML report with correct statistics
    tester.finalize_html_report(results, output_file)

    # Print summary
    success_count = len([r for r in results if r['status'] == 'success'])
    failed_count = len([r for r in results if r['status'] == 'failed'])
    error_count = len([r for r in results if r['status'] == 'error'])

    print("\n=== Test Summary ===")
    print(f"Total tests: {len(results)}")
    print(f"SQL generated: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Errors: {error_count}")
    print(f"HTML report: {output_file}")


if __name__ == "__main__":
    # Ensure we're in the virtual environment
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("Warning: Not running in virtual environment. Please activate venv first.")
        print("Run: venv\\Scripts\\activate && python testing\\test_agent_sql_generation.py")
        sys.exit(1)

    main()
