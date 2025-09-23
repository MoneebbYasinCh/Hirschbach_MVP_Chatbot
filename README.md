# Hirschbach Trucking Assistant

A Streamlit-based UI interface for the Hirschbach Trucking Operations Assistant with orchestrator node integration.

## Features

- 🚛 **Trucking Operations Focus**: Specialized for logistics and transportation tasks
- 🤖 **AI-Powered Orchestration**: Intelligent task decomposition and routing
- 💬 **Interactive Chat Interface**: User-friendly Streamlit UI
- 📊 **Data Analysis**: Support for trucking operations data queries and analysis
- 🔄 **Graph-Based Workflow**: LangGraph-powered conversation flow

## Quick Start

### 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Or run the setup script
python setup.py
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```env
# OpenAI API Key (required)
OPENAI_API_KEY=your_openai_api_key_here

# Pinecone API Key (required)
PINECONE_API_KEY=your_pinecone_api_key_here

# Pinecone Index Names (optional)
PINECONE_INDEX_NAME=hirschbach-mvp-kpi
PINECONE_INDEX_NAME_2=metadata-activityfact
```

### 3. Run the Application

```bash
streamlit run app.py
```

Open your browser to `http://localhost:8501`

## Project Structure

```
├── app.py                 # Streamlit UI interface
├── main_graph.py          # Main graph with start, stop, orchestrator nodes
├── Nodes/
│   └── orchestrator.py    # Orchestrator node implementation
├── State/
│   └── main_state.py      # State definitions
├── RAG/                   # RAG-related scripts
├── requirements.txt       # Python dependencies
├── setup.py              # Setup script
└── README.md             # This file
```

## Usage

### Chat Interface

The Streamlit interface provides:

- **Main Chat Area**: Interactive conversation with the trucking operations assistant
- **Sidebar**: System status and conversation management
- **Orchestration Details**: View task breakdown and analysis
- **Status Indicators**: Real-time workflow status

### Example Queries

**Operations Analysis:**
- "How are we performing this month?"
- "Show me delivery performance"
- "What's our fuel efficiency trend?"

**Specific Queries:**
- "Which driver has the best safety record?"
- "How many loads were delivered on time?"
- "Show me maintenance schedules"

**General Information:**
- "What is load planning?"
- "How can you help me?"
- "Explain route optimization"

## Architecture

### Main Graph Flow

```
Start → Orchestrator → Stop
```

- **Start Node**: Initializes conversation state
- **Orchestrator Node**: Analyzes input, decomposes tasks, routes to tools
- **Stop Node**: Finalizes conversation and generates response

### Orchestrator Features

- **Input Analysis**: Determines if direct reply or tool usage is needed
- **Task Decomposition**: Breaks complex requests into structured tasks
- **Tool Selection**: Routes tasks to appropriate processing tools
- **Queue Management**: Manages task queues for different subgraphs

## Development

### Adding New Nodes

1. Create node class in `Nodes/` directory
2. Implement `__call__` method that takes and returns state
3. Add to graph in `main_graph.py`

### State Management

The system uses a centralized state object (`RCMGraphState`) that includes:
- Messages and conversation history
- Orchestration plans and task queues
- Database results and aggregated data
- Workflow status and error handling

## API Keys

### OpenAI API Key
1. Go to [OpenAI Platform](https://platform.openai.com/api-keys)
2. Create a new API key
3. Add to `.env` file

### Pinecone API Key
1. Go to [Pinecone Console](https://app.pinecone.io/)
2. Get your API key
3. Add to `.env` file

## Troubleshooting

### Common Issues

1. **Missing API Keys**: Ensure both OpenAI and Pinecone keys are set in `.env`
2. **Import Errors**: Run `pip install -r requirements.txt`
3. **Port Conflicts**: Change `STREAMLIT_SERVER_PORT` in `.env`

### Debug Mode

Enable debug logging by setting environment variable:
```bash
export STREAMLIT_LOGGER_LEVEL=debug
streamlit run app.py
```

## License

This project is part of the Hirschbach MVP system.