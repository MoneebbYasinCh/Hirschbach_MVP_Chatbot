from typing import Dict, Any, List
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_openai import AzureChatOpenAI
import json
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class HirschbachOrchestrator:
    """
    Orchestrator node for Hirschbach Trucking Assistant
    Handles input processing, task decomposition, and routing to appropriate tools
    """
    
    def __init__(self):
        # Initialize logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize Azure OpenAI client
        api_version = os.getenv("AZURE_OPENAI_API_VERSION")
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
        if api_version:
            self.llm = AzureChatOpenAI(
                azure_deployment=deployment_name,
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                api_version=api_version,
                temperature=0.4
            )
        else:
            # Let Azure OpenAI use its default version
            self.llm = AzureChatOpenAI(
                azure_deployment=deployment_name,
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                temperature=0.4
            )

    def _format_history_as_text(self, messages: List[BaseMessage]) -> str:
        """Format conversation history as text with context analysis."""
        if not messages:
            return ""
        
        lines: List[str] = []
        context_topics = []
        data_requests = []
        
        for msg in messages:
            role = "User" if isinstance(msg, HumanMessage) else ("Assistant" if isinstance(msg, AIMessage) else "System")
            content = str(getattr(msg, "content", "")).strip()
            
            # Limit content length for readability
            if len(content) > 150:
                content = content[:150] + "..."
            
            lines.append(f"- {role}: {content}")
            
            # Extract context for better decision making
            if isinstance(msg, HumanMessage):
                content_lower = content.lower()
                context_topics.append(content_lower)
                
                # Track data-related requests
                if any(keyword in content_lower for keyword in ["show", "get", "find", "claims", "data", "report", "analyze"]):
                    data_requests.append(content_lower)
        
        # Add context summary for better decision making
        if context_topics:
            recent_topics = context_topics[-3:]  # Last 3 user queries
            lines.append(f"\n--- CONTEXT: Recent user interests: {', '.join(recent_topics)} ---")
            
        if data_requests:
            lines.append(f"--- DATA REQUESTS: {len(data_requests)} data-related queries in conversation ---")
        
        return "\n".join(lines)
    
    def _format_sql_history(self, sql_history: List[Dict[str, Any]]) -> str:
        """Format SQL query history for context"""
        if not sql_history:
            return "No previous SQL queries generated."
        
        formatted_history = []
        for i, entry in enumerate(sql_history, 1):
            question = entry.get("user_question", "Unknown question")
            sql_query = entry.get("generated_sql", "No SQL generated")
            source = entry.get("source", "unknown")
            timestamp = entry.get("timestamp", "Unknown time")
            
            # Truncate long SQL queries for readability
            if len(sql_query) > 200:
                sql_query = sql_query[:200] + "..."
            
            formatted_history.append(f"{i}. Question: \"{question}\"")
            formatted_history.append(f"   Generated SQL: {sql_query}")
            formatted_history.append(f"   Source: {source} | Time: {timestamp}")
            formatted_history.append("")  # Empty line for separation
        
        return "\n".join(formatted_history)
    
    def _check_for_sql_modification(self, user_input: str, sql_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check if the current query can be satisfied by modifying a previous SQL query"""
        
        if not sql_history:
            return {"should_modify": False}
        
        # Get the most recent SQL query
        last_sql_entry = sql_history[-1]
        last_question = last_sql_entry.get("user_question", "").lower()
        last_sql = last_sql_entry.get("generated_sql", "")
        current_input = user_input.lower()
        
        # Define temporal modification patterns
        temporal_patterns = {
            "last_week": ["last week", "previous week", "week before", "past week"],
            "last_month": ["last month", "previous month", "month before", "past month"],
            "last_quarter": ["last quarter", "previous quarter", "quarter before"],
            "last_year": ["last year", "previous year", "year before"],
            "today": ["today", "this day"],
            "yesterday": ["yesterday", "day before"],
            "this_month": ["this month", "current month"],
            "this_quarter": ["this quarter", "current quarter"],
            "this_year": ["this year", "current year"]
        }
        
        # Check if current input is a temporal follow-up
        detected_period = None
        for period, patterns in temporal_patterns.items():
            if any(pattern in current_input for pattern in patterns):
                detected_period = period
                break
        
        # Check if it's a simple follow-up pattern
        follow_up_patterns = ["what about", "how about", "show me for", "and for", "also for"]
        is_follow_up = any(pattern in current_input for pattern in follow_up_patterns)
        
        if detected_period and (is_follow_up or len(current_input.split()) <= 5):
            # This looks like a temporal modification request
            
            # Check if the previous query is compatible (contains time-related elements)
            time_indicators = ["this week", "current week", "this month", "current month", 
                             "this quarter", "this year", "today", "week", "month", "quarter", "year"]
            
            has_time_context = any(indicator in last_question for indicator in time_indicators)
            has_date_in_sql = any(date_func in last_sql.upper() for date_func in 
                                ["DATEPART", "GETDATE", "DATEADD", "YEAR", "MONTH", "DAY"])
            
            if has_time_context or has_date_in_sql:
                print(f"[ORCHESTRATOR] SQL modification opportunity detected:")
                print(f"  Previous: {last_question}")
                print(f"  Current: {user_input}")
                print(f"  Detected period: {detected_period}")
                
                return {
                    "should_modify": True,
                    "modification_type": "temporal_change",
                    "target_period": detected_period,
                    "base_sql": last_sql,
                    "base_question": last_sql_entry.get("user_question", ""),
                    "new_question": user_input
                }
        
        return {"should_modify": False}
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main orchestrator function - decides between direct reply or data analysis
        
        Args:
            state: Current state containing messages and other context
            
        Returns:
            Updated state with orchestration results
        """
        self.logger.info("[ORCHESTRATOR] Processing user input...")
        print("[ORCHESTRATOR] Processing user input...")
        
        # Get user query from state (preferred) or fallback to last message
        user_input = state.get("user_query", "")
        if not user_input:
            # Fallback to last message if user_query not in state
            messages = state.get("messages", [])
            if messages:
                latest_message = messages[-1]
                if isinstance(latest_message, HumanMessage):
                    user_input = latest_message.content
        
        if not user_input:
            self.logger.warning("[ORCHESTRATOR] No user query found in state")
            return state
        
        self.logger.info(f"[ORCHESTRATOR] User input: {user_input[:100]}...")
        print(f"[ORCHESTRATOR] User input: {user_input[:100]}...")
        
        # Get conversation history for context
        messages = state.get("messages", [])
        print(f"[ORCHESTRATOR] Received {len(messages)} messages in conversation")
        
        history_text = self._format_history_as_text(messages[:-1])  # Exclude current message
        print(f"[ORCHESTRATOR DEBUG] History being passed to LLM:")
        print(f"[ORCHESTRATOR DEBUG] {history_text}")
        print(f"[ORCHESTRATOR DEBUG] Current user input: {user_input}")
        
        # Decide: Direct reply or data analysis?
        if self._should_reply_directly(user_input, history_text):
            self.logger.info("[ORCHESTRATOR] Decided to reply directly")
            print("[ORCHESTRATOR] Decided to reply directly")
            
            # Generate direct response and end workflow
            response = self._generate_direct_response(user_input, history_text, state)
            ai_message = AIMessage(content=response)
            state["messages"].append(ai_message)
            state["final_response"] = response
            state["workflow_status"] = "complete"
            
            self.logger.info(f"[ORCHESTRATOR] Direct response generated: {response[:100]}...")
            print(f"[ORCHESTRATOR] Direct response generated: {response[:100]}...")
        else:
            self.logger.info("[ORCHESTRATOR] Decided to perform data analysis")
            print("[ORCHESTRATOR] Decided to perform data analysis")
            
            # Check if this is a follow-up query that can modify previous SQL
            sql_modification_result = self._check_for_sql_modification(user_input, state.get("sql_query_history", []))
            
            if sql_modification_result["should_modify"]:
                # Route to direct SQL modification instead of full pipeline
                print(f"[ORCHESTRATOR] Detected SQL modification request: {sql_modification_result['modification_type']}")
                state["sql_modification_request"] = sql_modification_result
                state["workflow_status"] = "active"
                
                # Debug: Confirm state is set
                print(f"[ORCHESTRATOR DEBUG] Set sql_modification_request in state: {sql_modification_result}")
                print(f"[ORCHESTRATOR DEBUG] State keys after setting: {list(state.keys())}")
                
                # Set up orchestration for SQL modification path
                state["orchestration"] = {
                    "decision": "sql_modification",
                    "user_input": user_input,
                    "requires_retrieval": False,
                    "requires_sql_generation": False,
                    "requires_sql_modification": True,
                    "requires_azure_retrieval": True,
                    "original_input": user_input,
                    "routed_to": "sql_modification,azure_retrieval"
                }
            else:
                # Generate response about data analysis and continue workflow
                response = self._create_data_analysis_response(user_input)
                ai_message = AIMessage(content=response)
                state["messages"].append(ai_message)
                state["final_response"] = response
                state["workflow_status"] = "active"  # Continue to next nodes
                
                # Set up orchestration metadata for downstream nodes
                state["orchestration"] = {
                    "decision": "data_analysis",
                    "user_input": user_input,
                    "requires_retrieval": True,
                    "requires_sql_generation": True,
                    "requires_azure_retrieval": True,
                    "original_input": user_input,
                    "routed_to": "kpi_retrieval,metadata_retrieval,sql_generation,azure_retrieval"
                }
        
        return state
    
    
    def _should_reply_directly(self, user_input: str, history_text: str) -> bool:
        """
        Use LLM to decide whether to reply directly or perform data analysis with conversation context
        
        Args:
            user_input: The user's input text
            history_text: Conversation history
            
        Returns:
            True if should reply directly, False if needs data analysis
        """
        prompt = f"""
        You are an AI Risk Intelligence assistant for Hirschbach's fleet risk management. Analyze this user input and decide whether to reply directly or perform data analysis.
        
        Available data: You have access to one table called 'claims_summary' which contains aggregated data of claims on Claim Number level.
        
        CONVERSATION CONTEXT:
        {history_text}

        CURRENT USER INPUT: "{user_input}"
        
        IMPORTANT: Consider the conversation context when deciding. Look for:
        - Follow-up questions that build on previous data requests
        - Clarifications or refinements of earlier queries
        - References to "that", "those", "more", "also", "what about" that imply continuation
        - Context from previous exchanges that make the current input data-related
        - Questions about "last message" or "previous" refer to messages BEFORE the current one
        
        DIRECT_REPLY for:
        - General questions about risk management concepts
        - Definitions and explanations of safety terms
        - Help and capability questions
        - Simple process explanations
        - General information requests about the platform
        - Questions about claims data structure or capabilities
        - Questions about conversation history ("what questions did I ask", "what queries were generated", "show me previous SQL")
        
        DATA_ANALYSIS for:
        - Any request for claims data analysis
        - Queries about specific claims, drivers, accidents, or incidents
        - "Show me", "get", "find" requests about claims data
        - Reports and analytics on claims
        - Questions about claims performance, trends, or patterns
        - Any data query that would require database analysis
        - Follow-up questions that reference previous data analysis
        - Refinements or modifications of previous data requests
        
        Context-aware examples:
        - Previous: "Show me claims data" → Current: "What about last month?" → DATA_ANALYSIS
        - Previous: "Claims by state" → Current: "Can you filter by California?" → DATA_ANALYSIS
        - Previous: "What is a claim?" → Current: "How do I use this system?" → DIRECT_REPLY
        - Previous: "Claims analysis" → Current: "Show me more details" → DATA_ANALYSIS
        - Previous: "Risk data" → Current: "What about Texas specifically?" → DATA_ANALYSIS
        - Previous: "Show me accident trends by state" → Current: "Tell me more about it" → DATA_ANALYSIS
        - Previous: "Claims data analysis" → Current: "Give me more information" → DATA_ANALYSIS
        - Previous: "Risk analysis" → Current: "Expand on that" → DATA_ANALYSIS
        
        Standard examples:
        - "What is preventable crash rate?" → DIRECT_REPLY
        - "How does claims data work?" → DIRECT_REPLY
        - "What questions did I ask you?" → DIRECT_REPLY
        - "Show me all the SQL queries you generated" → DIRECT_REPLY
        - "What were my previous questions and their queries?" → DIRECT_REPLY
        - "Show me claims in California" → DATA_ANALYSIS
        - "Which drivers have the most claims?" → DATA_ANALYSIS
        - "What are the accident trends?" → DATA_ANALYSIS
        - "Find claims above $10,000" → DATA_ANALYSIS
        
        Respond with only: DIRECT_REPLY or DATA_ANALYSIS
        """
        
        response = self.llm.invoke(prompt)
        decision = response.content.strip().upper()
        
        return decision == "DIRECT_REPLY"
    
    def _create_data_analysis_response(self, user_input: str) -> str:
        """
        Create response for data analysis requests
        
        Args:
            user_input: The user's input text
            
        Returns:
            Response string about data analysis
        """
        # Simple, direct response - no verbose messaging
        return ""
    
    
    
    def _generate_direct_response(self, user_input: str, history_text: str, state: Dict[str, Any]) -> str:
        """
        Generate a context-aware direct response for simple queries
        
        Args:
            user_input: The user's input text
            history_text: Conversation history
            state: Current state containing SQL query history
            
        Returns:
            Direct response string
        """
        # Debug: Check SQL query history
        sql_history = state.get("sql_query_history", [])
        print(f"[ORCHESTRATOR DEBUG] SQL query history has {len(sql_history)} entries")
        
        prompt = f"""
        You are an AI Risk Intelligence assistant for Hirschbach's fleet risk management. Provide a helpful, direct response to this user query.
        
        Available data: You have access to one table called 'claims_summary' which contains aggregated data of claims on Claim Number level.
        
        CONVERSATION HISTORY (messages that happened BEFORE the current query):
        {history_text}
        
        SQL QUERY HISTORY (previous questions and their generated SQL queries):
        {self._format_sql_history(state.get("sql_query_history", []))}

        CURRENT USER QUERY: "{user_input}"
        
        IMPORTANT: When the user asks about their "last message" or "previous message", they are referring to their most recent message in the CONVERSATION HISTORY above, NOT the current query.
        
        IMPORTANT: When the user asks about "questions I asked" or "generated queries", use the SQL QUERY HISTORY above to provide specific questions and their actual SQL queries.
        
        Example:
        - If CONVERSATION HISTORY shows: "User: hello how are you" and "Assistant: Hello! I'm here to help"
        - And CURRENT USER QUERY is: "What was my last message?"
        - Then the answer should be: "Your last message was 'hello how are you'"
        
        Guidelines:
        - Keep your response concise and professional
        - Consider the conversation context - reference previous topics if relevant
        - Focus on claims data analysis and risk management
        - If asking about capabilities, explain the claims data analysis features
        - Use clear formatting and structure
        - Reference Hirschbach's claims data when relevant
        - Emphasize data-driven insights from claims analysis
        - If this appears to be a follow-up question, acknowledge the context
        - Build upon previous conversation points when appropriate
        - When asked about "last message" or "previous message", refer to the message BEFORE the current one in the conversation history
        
        Context-aware response examples:
        - If user previously asked about claims and now asks "what else can you do?", mention additional claims analysis capabilities
        - If conversation has been about risk management, tailor explanations to that context
        - Reference specific topics mentioned earlier in the conversation
        
        Response:
        """
        
        response = self.llm.invoke(prompt)
        return response.content.strip()
    
    
# Factory function to create orchestrator instance
def create_orchestrator() -> HirschbachOrchestrator:
    """
    Factory function to create a Hirschbach Orchestrator instance
    
    Returns:
        HirschbachOrchestrator instance
    """
    return HirschbachOrchestrator() 