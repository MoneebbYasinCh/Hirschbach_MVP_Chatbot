# Quick Start Guide

## Prerequisites
- Python 3.12+
- Git
- Azure OpenAI, Azure AI Search, and Azure SQL Database configured

## Setup Instructions

### 1. Clone Repository
```bash
git clone https://github.com/MoneebbYasinCh/Hirschbach_MVP_Chatbot.git
cd Hirschbach_MVP_Chatbot
```

### 2. Create Virtual Environment

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Mac/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Environment Configuration
Create a `.env` file in the project root with your Azure credentials:

```env
# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=https://your-openai-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your_azure_openai_api_key_here
AZURE_OPENAI_DEPLOYMENT=gpt-4o
AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT=text-embedding-3-small
AZURE_OPENAI_API_VERSION=2024-12-01-preview

# Azure AI Search Configuration
AZURE_SEARCH_SERVICE_ENDPOINT=https://your-search-service.search.windows.net
AZURE_SEARCH_API_KEY=your_azure_search_api_key_here
AZURE_SEARCH_INDEX_NAME=kpis-hml-mvp
AZURE_SEARCH_INDEX_NAME_2=metadata-hml-mvp

# Azure SQL Database Configuration
SQL_CONNECTION_STRING=Driver={ODBC Driver 18 for SQL Server};Server=your-server.database.windows.net;Database=your-database;Uid=your-username;Pwd=your-password;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;

# Optional: LangSmith Tracing
LANGSMITH_TRACING=true
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
LANGSMITH_API_KEY=your_langsmith_api_key_here
LANGSMITH_PROJECT=Hirschbach-MVP
```

### 5. Setup Data (One-time)
```bash
python RAG/kpi_rag_push.py
python RAG/metadata_rag_push.py
```

## Running the Application

### Windows Users
1. **Activate virtual environment (one-time per session):**
   ```bash
   venv\Scripts\activate
   ```

2. **Start the application:**
   ```bash
   streamlit run app.py
   ```

3. **Open browser:** Navigate to `http://localhost:8501`

### Mac/Linux Users
1. **Activate virtual environment (one-time per session):**
   ```bash
   source venv/bin/activate
   ```

2. **Start the application:**
   ```bash
   streamlit run app.py
   ```

3. **Open browser:** Navigate to `http://localhost:8501`

## Additional Commands

### Generate Workflow Diagram
```bash
python Documentation/graph_image.py
```

### Run Tests
```bash
python testing/test_orchestrator.py
python testing/test_kpi_retrieval.py
python testing/test_sql_generation.py
```

### View Azure Search Index
```bash
python RAG/view_azure_search_index.py
```

## Troubleshooting

### Virtual Environment Issues
**Windows:**
```bash
# If activation fails, set execution policy
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Mac/Linux:**
```bash
# If activation fails, check Python path
which python
python --version
```

### Common Issues
- **Port already in use:** Change port with `streamlit run app.py --server.port 8502`
- **Missing dependencies:** Run `pip install -r requirements.txt` again
- **Azure connection issues:** Verify all environment variables in `.env` file

## Sample Queries
Try these queries in the application:
- "Show claim counts grouped by accident or incident type."
- "Show claim counts grouped by accident or incident type."
- "Please tell me the number of closed claims for current calender week"

