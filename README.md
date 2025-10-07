# Hirschbach AI Risk Intelligence Platform

![Hirschbach Logo](https://img.shields.io/badge/Hirschbach-AI%20Risk%20Intelligence-blue)
![Python](https://img.shields.io/badge/Python-3.12+-green)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red)
![LangGraph](https://img.shields.io/badge/LangGraph-0.0.20+-orange)

## Overview

The **Hirschbach AI Risk Intelligence Platform** is an advanced conversational AI system designed specifically for transportation and logistics risk management. Built using LangGraph orchestration, the platform provides intelligent analysis of claims data, accident trends, and driver risk assessments through natural language queries.

### Key Features

- **Intelligent Query Processing**: Natural language understanding for complex risk intelligence queries
- **Real-time Data Analysis**: Direct connection to Azure SQL databases for live data retrieval
- **Smart Workflow Orchestration**: LangGraph-powered routing between KPI editing and SQL generation
- **AI-Powered Insights**: Automated analysis with business recommendations and risk assessments
- **Query Transparency**: Full visibility into generated SQL queries and execution details
- **Performance Monitoring**: Real-time tracking of execution times and data volumes

### Architecture

The platform uses a sophisticated multi-node architecture:

1. **Orchestrator**: Determines query routing and workflow management
2. **KPI Retrieval**: Searches existing KPIs using Azure AI Search
3. **Metadata Retrieval**: Fetches database schema and column information
4. **LLM Checker**: Intelligently decides between KPI editing vs. SQL generation
5. **KPI Editor**: Modifies existing KPIs to match user requirements
6. **SQL Generator**: Creates new SQL queries from scratch using metadata
7. **Azure Retrieval**: Executes SQL queries against Azure SQL Database
8. **Insight Generation**: Analyzes results and provides business recommendations

## Prerequisites

- **Python 3.12+**
- **Azure OpenAI Service** with GPT-4 deployment
- **Azure AI Search** service
- **Azure SQL Database** with claims data
- **Windows/Linux/macOS** environment

## Installation & Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd Hirschbach_MVP_Chatbot
```

### 2. Create Virtual Environment

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

**Windows (with virtual environment activated):**
```bash
pip install -r requirements.txt
```

**Linux/macOS:**
```bash
source venv/bin/activate && pip install -r requirements.txt
```

### 4. Environment Configuration

Create a `.env` file in the project root directory with the following variables:

```env
# LangSmith Configuration (Optional - for tracing)
LANGSMITH_TRACING=true
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
LANGSMITH_API_KEY=your_langsmith_api_key_here
LANGSMITH_PROJECT=Hirschbach-MVP

# Azure OpenAI Configuration (Required)
AZURE_OPENAI_ENDPOINT=https://your-openai-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your_azure_openai_api_key_here
AZURE_OPENAI_DEPLOYMENT=gpt-4o
AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT=text-embedding-3-small
AZURE_OPENAI_API_VERSION=2024-12-01-preview

# Azure AI Search Configuration (Required)
AZURE_SEARCH_SERVICE_ENDPOINT=https://your-search-service.search.windows.net
AZURE_SEARCH_API_KEY=your_azure_search_api_key_here
AZURE_SEARCH_INDEX_NAME=kpis-hml-mvp
AZURE_SEARCH_INDEX_NAME_2=metadata-hml-mvp

# Azure SQL Database Configuration (Required)
SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=your-server.database.windows.net;Database=your-database;Uid=your-username;Pwd=your-password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;
```

### 5. Data Setup (One-time)

Before running the application, you need to populate the Azure AI Search indexes with your KPI and metadata information:

**Push KPI data to Azure AI Search:**
```bash
python RAG/kpi_rag_push.py
```

**Push metadata to Azure AI Search:**
```bash
python RAG/metadata_rag_push.py
```

## Running the Application

### Start the Streamlit Application

**Windows:**
```bash
streamlit run app.py
```

**Linux/macOS:**
```bash
source venv/bin/activate && streamlit run app.py
```

The application will be available at: `http://localhost:8501`

### Alternative: Direct Python Execution

You can also run the core graph logic directly:

```bash
python Graph_Flow/main_graph.py
```

### Generate Workflow Diagram

The project includes a tool to generate visual diagrams of the LangGraph workflow:

```bash
python Documentation/graph_image.py
```

This will create several files:
- **graph.png**: Visual workflow diagram (PNG image)
- **graph.mmd**: Mermaid diagram source code
- **graph_original.mmd**: Original auto-generated diagram (for debugging)
- **graph_simple.mmd**: Simplified diagram (if needed)

**Requirements for PNG generation:**
- **Pyppeteer** (recommended): `pip install pyppeteer nest-asyncio`
- **Mermaid CLI** (alternative): `npm install -g @mermaid-js/mermaid-cli`

If PNG generation fails, you can:
1. Use the online Mermaid editor at https://mermaid.live
2. Copy the contents from `graph.mmd` and paste them to generate the image
3. Install the required dependencies listed above

## Usage Guide

### Sample Queries

The platform supports various types of risk intelligence queries:

- **Accident Analysis**: "Show me accident trends by state for the last 6 months"
- **Driver Risk Assessment**: "Which drivers have the highest risk of preventable accidents?"
- **Claims Analysis**: "What are the most common types of cargo claims?"
- **Geographic Insights**: "Which regions have the highest claim frequency?"
- **Experience Correlation**: "Analyze accident patterns by driver experience level"

### Query Processing Flow

1. **Input**: User enters natural language query
2. **Orchestration**: System determines if query needs data analysis or direct response
3. **KPI Matching**: Searches existing KPIs for relevant matches
4. **Decision Making**: LLM determines if existing KPI can be used or new SQL needed
5. **Data Retrieval**: Executes optimized SQL query against Azure database
6. **Insight Generation**: AI analyzes results and provides recommendations
7. **Response**: User receives data tables, insights, and SQL transparency

### Understanding Results

Each response includes:

- **Natural Language Summary**: AI-generated explanation of findings
- **Data Tables**: Interactive tables with query results
- **Key Findings**: Bullet-point insights and patterns
- **Recommendations**: Actionable business suggestions
- **Risk Assessment**: Identification of concerning patterns
- **SQL Query**: Full transparency of executed queries
- **Performance Metrics**: Execution time and row counts

## Project Structure

```
Hirschbach_MVP_Chatbot/
├── app.py                          # Main Streamlit application
├── requirements.txt                # Python dependencies
├── .env                           # Environment variables (create this)
│
├── Graph_Flow/                    # LangGraph orchestration
│   └── main_graph.py             # Main workflow definition
│
├── Nodes/                         # Individual processing nodes
│   ├── orchestrator.py           # Query routing and decision making
│   ├── kpi_retrieval.py          # KPI search using Azure AI Search
│   ├── metadata_retrieval.py     # Database schema retrieval
│   ├── llm_checker.py            # Intelligent KPI vs SQL decision
│   ├── kpi_editor.py             # KPI modification logic
│   ├── sql_gen.py                # SQL query generation
│   ├── azure_retrieval.py        # Database query execution
│   └── insight_gen.py            # AI-powered insight generation
│
├── State/                         # State management
│   └── main_state.py             # LangGraph state definition
│
├── RAG/                          # Vector database management
│   ├── kpi_rag_push.py          # Upload KPIs to Azure AI Search
│   ├── metadata_rag_push.py     # Upload metadata to Azure AI Search
│   ├── view_azure_search_index.py # View index contents
│   └── clear_azure_search_index.py # Clear index data
│
├── Tools/                        # Utility tools
│   └── entity_mapping_tool.py   # Entity and value mapping
│
├── utils/                        # Utilities
│   └── logger.py                 # Centralized logging system
│
├── Data/                         # Sample data files
│   ├── KPIs_HML_MVP.csv         # Sample KPI definitions
│   └── MetaData_HML_MVP.csv     # Sample database metadata
│
└── testing/                     # Test files and debugging
    ├── test_*.py                # Individual node tests
    └── debug_*.py               # Debugging utilities
```

## Configuration Details

### Azure OpenAI Setup

1. Create an Azure OpenAI resource in Azure Portal
2. Deploy GPT-4 model (recommended: `gpt-4o`)
3. Deploy text embedding model (`text-embedding-3-small`)
4. Copy endpoint URL and API key to `.env` file

### Azure AI Search Setup

1. Create Azure AI Search service
2. Note the service URL and admin API key
3. The application will automatically create required indexes on first run

### Azure SQL Database Setup

1. Ensure your database contains claims/risk data
2. Create connection string with appropriate permissions
3. The application expects a table structure compatible with transportation claims data

### LangSmith Integration (Optional)

LangSmith provides advanced tracing and monitoring:

1. Sign up at [LangSmith](https://smith.langchain.com/)
2. Create a project named "Hirschbach-MVP"
3. Add API key to `.env` file
4. Set `LANGSMITH_TRACING=true`

## Testing

### Run Individual Node Tests

```bash
# Test orchestrator
python testing/test_orchestrator.py

# Test KPI retrieval
python testing/test_kpi_retrieval.py

# Test SQL generation
python testing/test_sql_generation.py

# Test Azure retrieval
python testing/test_endpoint.py
```

### Debug Tools

```bash
# View Azure Search index contents
python RAG/view_azure_search_index.py

# Debug scoring mechanisms
python testing/debug_scores.py
```

## Logging

The application provides comprehensive logging:

- **Console Output**: Real-time processing updates
- **File Logging**: Detailed logs in `hirschbach_chatbot.log`
- **Node-specific Logging**: Individual node execution tracking
- **Performance Metrics**: Query execution times and data volumes

## Security Considerations

- **Environment Variables**: Never commit `.env` file to version control
- **API Keys**: Rotate keys regularly and use least-privilege access
- **Database Access**: Use read-only connections where possible
- **Network Security**: Ensure proper firewall rules for Azure services

## Troubleshooting

### Common Issues

**1. Virtual Environment Issues**
```bash
# Windows: Ensure execution policy allows scripts
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Recreate virtual environment if corrupted
rmdir /s venv
python -m venv venv
pip install -r requirements.txt
```

**2. Azure Connection Issues**
- Verify all Azure service endpoints are correct
- Check API keys are valid and not expired
- Ensure Azure services are in the same region for optimal performance

**3. Database Connection Issues**
- Verify SQL connection string format
- Check firewall rules allow your IP address
- Ensure database user has appropriate permissions

**4. Missing Dependencies**
```bash
# Reinstall all dependencies
pip install --upgrade -r requirements.txt
```

### Performance Optimization

- **Token Management**: Large datasets are automatically truncated for LLM processing
- **Caching**: Consider implementing Redis caching for frequently accessed KPIs
- **Database Indexing**: Ensure proper indexes on frequently queried columns
- **Concurrent Requests**: Streamlit handles multiple users automatically

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Follow SOLID principles and existing code patterns
4. Add comprehensive tests for new functionality
5. Update documentation as needed
6. Submit a pull request

### Code Standards

- **SOLID Principles**: All code must follow Single Responsibility, Open/Closed, Liskov Substitution, Interface Segregation, and Dependency Inversion principles
- **Type Hints**: Use type annotations for all function parameters and returns
- **Error Handling**: Implement proper exception handling with custom exceptions
- **Logging**: Use the centralized logging system for all output
- **Testing**: Write unit tests for all new functionality

## License

This project is proprietary software developed for Hirschbach Transportation. All rights reserved.

## Support

For technical support or questions:

- **Internal Teams**: Contact the AI Development Team
- **Issues**: Use the internal issue tracking system
- **Documentation**: Refer to internal confluence pages

---

**Built for Hirschbach Transportation's Risk Intelligence Initiative**