# Orchestrator Test Files

This directory contains test files for the Hirschbach Orchestrator to help you understand its behavior and outputs.

## Files

### 1. `test_orchestrator.py` - Comprehensive Test Suite
- **Purpose**: Detailed testing with full output analysis
- **Features**: 
  - Tests 10 different scenarios
  - Shows complete orchestrator state
  - Displays task breakdown details
  - Shows retrieval results
  - Includes conversation context testing

### 2. `test_orchestrator_simple.py` - Quick Test
- **Purpose**: Simple, focused testing
- **Features**:
  - Tests basic direct reply vs task breakdown
  - Shows key outputs only
  - Easy to run and understand

## How to Run

### Prerequisites
Make sure you have Azure OpenAI credentials configured in your environment:
```bash
# Set these environment variables
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_API_KEY=your_key
AZURE_OPENAI_DEPLOYMENT=your_deployment
AZURE_OPENAI_API_VERSION=your_version
```

### Running the Tests

#### Comprehensive Test Suite
```bash
cd testing
python test_orchestrator.py
```

#### Simple Test
```bash
cd testing
python test_orchestrator_simple.py
```

## What You'll See

### For Direct Reply Prompts (like "What is a preventable crash rate?"):
- **Status**: "complete"
- **Response**: Direct answer from the LLM
- **Tasks**: None created
- **Queue**: Empty
- **Retrieval**: Not triggered

### For Task Breakdown Prompts (like "Show me claims in California"):
- **Status**: "complete"
- **Response**: Professional response about task breakdown
- **Tasks**: 1-3 specific tasks created with:
  - ID (task_1, task_2, etc.)
  - Description (what the task does)
  - Tools (nl_to_sql_generator)
- **Queue**: Tasks added to nl_to_sql_queue
- **Retrieval**: KPI and metadata retrieval triggered
- **Results**: Shows retrieved KPIs and metadata columns

## Understanding the Output

### Task Structure
```json
{
  "id": "task_1",
  "description": "analyze claims count by driver from claims_summary table",
  "tools": ["nl_to_sql_generator"]
}
```

### Orchestration Details
- **Original Input**: The user's original question
- **Routed To**: Which queue the tasks were sent to
- **Tasks**: List of created tasks

### Retrieval Results
- **KPI Results**: Top relevant KPI with metric name, score, description
- **Metadata Results**: Relevant database columns with descriptions

## Test Scenarios Covered

1. **Direct Reply Tests**:
   - General risk management questions
   - Help and capability questions
   - System explanations

2. **Task Breakdown Tests**:
   - Simple data queries
   - Complex analysis requests
   - Trend analysis
   - High-value claims
   - Multi-criteria queries
   - Conversation context

3. **Edge Cases**:
   - Ambiguous queries
   - Error handling

## Troubleshooting

If you get errors:
1. Check Azure OpenAI credentials are set
2. Ensure all dependencies are installed
3. Verify the orchestrator can import properly
4. Check the console output for specific error messages

## Custom Testing

To test your own prompts, modify the test files or create new ones following the same pattern:

```python
from Nodes.orchestrator import HirschbachOrchestrator
from langchain_core.messages import HumanMessage

orchestrator = HirschbachOrchestrator()
state = {
    "messages": [HumanMessage(content="Your prompt here")],
    # ... other state fields
}
result = orchestrator(state)
print(result.get('final_response'))
```
