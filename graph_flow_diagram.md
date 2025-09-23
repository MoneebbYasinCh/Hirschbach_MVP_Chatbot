# Hirschbach Risk Intelligence Graph Flow

## Complete Data Retrieval Flow

```mermaid
graph TD
    A[start] --> B[orchestrator]
    B --> C{workflow_status}
    C -->|complete| D[end]
    C -->|active| E[kpi_retrieval]
    
    E --> F[llm_checker]
    F --> G{llm_decision}
    
    G -->|relevant| H[azure_retrieval]
    G -->|needs_change| I[kpi_editor]
    G -->|irrelevant| J[metadata_retrieval]
    
    I --> H
    J --> K[sql_generation]
    K --> H
    
    H --> L[insight_generation]
    L --> D
    D --> M[END]
```

## Flow Description

1. **Start** → **Orchestrator**: Initializes conversation
2. **Orchestrator Decision**:
   - Direct reply → End (simple questions)
   - Data retrieval → KPI Retrieval (complex data questions)

3. **KPI Retrieval** → **LLM Checker**: Retrieves KPIs and validates them

4. **LLM Checker Decision**:
   - **Relevant**: KPI is good → Azure Retrieval
   - **Needs Change**: KPI needs editing → KPI Editor → Azure Retrieval  
   - **Irrelevant**: KPI not useful → Metadata Retrieval → SQL Generation → Azure Retrieval

5. **Azure Retrieval** → **Insight Generation** → **End**: Final data processing and response

## Key Features

- **Conditional Routing**: Smart decisions at each step
- **Sequential Processing**: Each path leads to data retrieval
- **State Management**: All data passed through state
- **Error Handling**: Graceful fallbacks at each step
