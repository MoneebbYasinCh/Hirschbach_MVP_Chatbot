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
        
        # Initialize optimized conversation history for context management
        # Only stores essential messages: user queries and final AI responses (no data/results)
        self.conversation_history: List[BaseMessage] = []
        
        # Store SQL queries separately for context without overwhelming the conversation
        self.sql_context_history: List[Dict[str, str]] = []
        
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

    def _extract_essential_messages(self, messages: List[BaseMessage]) -> List[BaseMessage]:
        """
        Extract only essential messages for conversation history.
        Filters out data-heavy responses and keeps only user queries and final AI responses.
        
        Args:
            messages: All messages from the state
            
        Returns:
            Filtered list of essential messages
        """
        essential_messages = []
        
        for msg in messages:
            if isinstance(msg, HumanMessage):
                # Always keep user messages
                essential_messages.append(msg)
            elif isinstance(msg, AIMessage):
                # Keep AI messages but filter out data-heavy content
                content = msg.content
                
                # Skip if this looks like a data processing message (empty or very short)
                if not content or len(content.strip()) < 10:
                    continue
                    
                # Create a clean AI message without data references
                clean_content = self._clean_ai_message_content(content)
                clean_ai_message = AIMessage(content=clean_content)
                essential_messages.append(clean_ai_message)
        
        return essential_messages
    
    def _clean_ai_message_content(self, content: str) -> str:
        """
        Clean AI message content to remove data-heavy sections.
        
        Args:
            content: Original AI message content
            
        Returns:
            Cleaned content suitable for conversation history
        """
        # Remove common data-heavy patterns
        lines = content.split('\n')
        cleaned_lines = []
        
        skip_patterns = [
            '**Data Error:**',
            'Total rows:',
            'execution_time',
            'rows_returned',
            'query_executed'
        ]
        
        for line in lines:
            # Skip lines that contain data-heavy patterns
            if any(pattern in line for pattern in skip_patterns):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines).strip()
    
    def _update_conversation_history(self, messages: List[BaseMessage]) -> None:
        """
        Update the persistent conversation history with new essential messages only.
        
        Args:
            messages: Current messages from the state
        """
        # Extract only essential messages (no data-heavy content)
        essential_messages = self._extract_essential_messages(messages)
        
        # If the incoming messages list is shorter, conversation was cleared
        if len(essential_messages) < len(self.conversation_history):
            print("[ORCHESTRATOR DEBUG] Conversation was cleared, resetting history")
            self.conversation_history = essential_messages.copy()
            self.sql_context_history.clear()
        elif len(essential_messages) > len(self.conversation_history):
            # Update conversation history - only add new essential messages
            new_messages = essential_messages[len(self.conversation_history):]
            self.conversation_history.extend(new_messages)
            print(f"[ORCHESTRATOR DEBUG] Updated conversation history: {len(self.conversation_history)} total essential messages")
        # If lengths are equal, no update needed
    
    def _add_sql_context(self, user_query: str, sql_query: str) -> None:
        """
        Add SQL query to context history for reference.
        
        Args:
            user_query: The user's original question
            sql_query: The generated SQL query
        """
        if sql_query and sql_query.strip():
            self.sql_context_history.append({
                "user_query": user_query,
                "sql_query": sql_query[:500] + "..." if len(sql_query) > 500 else sql_query  # Limit SQL length
            })
            # Keep only last 10 SQL queries to prevent context overflow
            if len(self.sql_context_history) > 10:
                self.sql_context_history = self.sql_context_history[-10:]
            print(f"[ORCHESTRATOR DEBUG] Added SQL context: {len(self.sql_context_history)} queries stored")
    
    def _track_sql_from_state(self, state: Dict[str, Any], user_query: str) -> None:
        """
        Track SQL queries from the state for context.
        
        Args:
            state: Current state that may contain SQL queries
            user_query: The user's query associated with the SQL
        """
        # Check for generated SQL in various state locations
        sql_query = None
        
        # Check generated_sql field
        if state.get("generated_sql"):
            sql_query = state["generated_sql"]
        
        # Check top_kpi for SQL
        elif state.get("top_kpi", {}).get("sql_query"):
            sql_query = state["top_kpi"]["sql_query"]
        
        # Check azure_data for executed query
        elif state.get("azure_data", {}).get("query_executed"):
            sql_query = state["azure_data"]["query_executed"]
        
        if sql_query:
            self._add_sql_context(user_query, sql_query)
    
    def _populate_sql_context_from_state(self, state: Dict[str, Any]) -> None:
        """
        Populate SQL context history from state's SQL query history if available.
        This ensures we have SQL context from previous executions.
        
        Args:
            state: Current state containing SQL query history
        """
        sql_query_history = state.get("sql_query_history", [])
        
        if not sql_query_history:
            return
            
        # Convert state SQL history to our SQL context format
        for entry in sql_query_history:
            user_question = entry.get("user_question", "")
            generated_sql = entry.get("generated_sql", "")
            
            if user_question and generated_sql:
                # Check if this SQL query is already in our context
                already_exists = any(
                    ctx.get("user_query") == user_question and ctx.get("sql_query") == generated_sql
                    for ctx in self.sql_context_history
                )
                
                if not already_exists:
                    self._add_sql_context(user_question, generated_sql)
        
        print(f"[ORCHESTRATOR DEBUG] Populated SQL context from state: {len(self.sql_context_history)} total queries")
    
    def _ensure_state_sql_history(self, state: Dict[str, Any], sql_modification_result: Dict[str, Any]) -> None:
        """
        Ensure the state has SQL query history for the SQL modifier to use as fallback.
        
        Args:
            state: Current state
            sql_modification_result: The SQL modification request containing base SQL info
        """
        if "sql_query_history" not in state:
            state["sql_query_history"] = []
        
        # Add the base SQL query to state history if not already present
        base_question = sql_modification_result.get("base_question", "")
        base_sql = sql_modification_result.get("base_sql", "")
        
        if base_question and base_sql:
            # Check if this entry already exists
            existing_entry = None
            for entry in state["sql_query_history"]:
                if (entry.get("user_question") == base_question and 
                    entry.get("generated_sql") == base_sql):
                    existing_entry = entry
                    break
            
            if not existing_entry:
                # Add the base SQL query to state history
                sql_entry = {
                    "user_question": base_question,
                    "generated_sql": base_sql,
                    "source": "orchestrator_context",
                    "timestamp": self._get_current_timestamp()
                }
                state["sql_query_history"].append(sql_entry)
                print(f"[ORCHESTRATOR DEBUG] Added base SQL to state history for SQL modifier fallback")
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def clear_conversation_history(self) -> None:
        """Clear the conversation history and SQL context."""
        self.conversation_history.clear()
        self.sql_context_history.clear()
        print("[ORCHESTRATOR DEBUG] Conversation history and SQL context cleared")
        
    def _get_conversation_context(self, current_user_input: str) -> str:
        """
        Get conversation context excluding the current user input.
        
        Args:
            current_user_input: The current user input to exclude from history
            
        Returns:
            Formatted conversation history as text
        """
        # Find all messages except the current user input
        context_messages = []
        
        for msg in self.conversation_history:
            # Skip the current user input message
            if isinstance(msg, HumanMessage) and msg.content.strip() == current_user_input.strip():
                continue
            context_messages.append(msg)
        
        return self._format_history_as_text(context_messages)
    
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
    
    def _format_sql_context_history(self) -> str:
        """Format SQL context history for LLM context"""
        if not self.sql_context_history:
            return "No previous SQL queries generated."
        
        formatted_history = []
        for i, entry in enumerate(self.sql_context_history, 1):
            user_query = entry.get("user_query", "Unknown question")
            sql_query = entry.get("sql_query", "No SQL generated")
            
            formatted_history.append(f"{i}. Question: \"{user_query}\"")
            formatted_history.append(f"   Generated SQL: {sql_query}")
            formatted_history.append("")  # Empty line for separation
        
        return "\n".join(formatted_history)
    
    def _format_sql_history(self, sql_history: List[Dict[str, Any]]) -> str:
        """Format SQL query history for context (legacy method for compatibility)"""
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
    
    def _check_for_sql_modification_from_context(self, user_input: str) -> Dict[str, Any]:
        """Check if the current query can be satisfied by modifying a previous SQL query using intelligent LLM analysis"""
        
        if not self.sql_context_history:
            print("[ORCHESTRATOR DEBUG] No SQL context history available for modification check")
            return {"should_modify": False}
        
        # Get the most recent SQL query from context history
        last_sql_entry = self.sql_context_history[-1]
        last_question = last_sql_entry.get("user_query", "")
        last_sql = last_sql_entry.get("sql_query", "")
        
        print(f"[ORCHESTRATOR DEBUG] Checking SQL modification for: '{user_input}'")
        print(f"[ORCHESTRATOR DEBUG] Last SQL query was for: '{last_question}'")
        print(f"[ORCHESTRATOR DEBUG] Last SQL: {last_sql[:100]}...")
        
        # Use LLM to intelligently detect temporal modification opportunities
        return self._llm_detect_temporal_modification(user_input, last_question, last_sql)
    
    def _llm_detect_temporal_modification(self, current_input: str, previous_question: str, previous_sql: str) -> Dict[str, Any]:
        """Use LLM to intelligently detect if current input is a temporal modification of previous query"""
        
        prompt = f"""
        You are an intelligent SQL query analyzer. Analyze whether the current user input is requesting a temporal modification of a previous query.
        
        PREVIOUS QUESTION: "{previous_question}"
        PREVIOUS SQL QUERY: {previous_sql}
        
        CURRENT USER INPUT: "{current_input}"
        
        Determine if the current input is asking for the same type of analysis but for a different time period.
        
        Examples of temporal modifications:
        - Previous: "drivers with most crashes last quarter" → Current: "what about this quarter" ✓
        - Previous: "claims data this month" → Current: "show me last month" ✓  
        - Previous: "accidents this week" → Current: "also tell me about this week" ✓
        - Previous: "risk analysis today" → Current: "what about yesterday" ✓
        - Previous: "claims by state" → Current: "what about California" ✗ (location, not temporal)
        - Previous: "driver analysis" → Current: "show me vehicles" ✗ (different entity)
        
        ANALYSIS CRITERIA:
        1. Is the current input referencing a different time period than the previous query?
        2. Is the core analysis request similar (same metrics, same entity type)?
        3. Does the previous SQL contain date/time filtering that can be modified?
        4. Is this a follow-up question (uses phrases like "what about", "also", "how about")?
        
        Respond with a JSON object:
        {{
            "should_modify": true/false,
            "confidence": "high"/"medium"/"low",
            "temporal_reference": "detected time period (e.g., 'this_week', 'last_month', 'yesterday')",
            "reasoning": "brief explanation of your decision",
            "modification_type": "temporal_change" or null
        }}
        
        Only return the JSON, no other text.
        """
        
        try:
            print(f"[ORCHESTRATOR] Using LLM for intelligent temporal detection...")
            response = self.llm.invoke(prompt)
            result_text = response.content.strip()
            
            # Parse JSON response
            import json
            result = json.loads(result_text)
            
            print(f"[ORCHESTRATOR DEBUG] LLM Analysis Result:")
            print(f"  Should modify: {result.get('should_modify')}")
            print(f"  Confidence: {result.get('confidence')}")
            print(f"  Temporal reference: {result.get('temporal_reference')}")
            print(f"  Reasoning: {result.get('reasoning')}")
            
            if result.get("should_modify") and result.get("confidence") in ["high", "medium"]:
                return {
                    "should_modify": True,
                    "modification_type": result.get("modification_type", "temporal_change"),
                    "target_period": result.get("temporal_reference"),
                    "base_sql": previous_sql,
                    "base_question": previous_question,
                    "new_question": current_input,
                    "confidence": result.get("confidence"),
                    "reasoning": result.get("reasoning")
                }
            else:
                print(f"[ORCHESTRATOR DEBUG] No temporal modification detected: {result.get('reasoning')}")
                return {"should_modify": False}
                
        except Exception as e:
            print(f"[ORCHESTRATOR] Error in LLM temporal detection: {str(e)}")
            # Fallback to simple pattern matching if LLM fails
            return self._fallback_temporal_detection(current_input, previous_question, previous_sql)
    
    def _fallback_temporal_detection(self, current_input: str, previous_question: str, previous_sql: str) -> Dict[str, Any]:
        """Fallback temporal detection using simple patterns if LLM fails"""
        print("[ORCHESTRATOR DEBUG] Using fallback temporal detection...")
        
        # Simple check for temporal words and follow-up patterns
        temporal_words = ["week", "month", "quarter", "year", "today", "yesterday", "this", "last", "current", "previous"]
        follow_up_words = ["what about", "how about", "also", "and", "show me for"]
        
        current_lower = current_input.lower()
        has_temporal = any(word in current_lower for word in temporal_words)
        has_follow_up = any(phrase in current_lower for phrase in follow_up_words)
        has_date_sql = any(func in previous_sql.upper() for func in ["DATEPART", "GETDATE", "DATEADD"])
        
        if has_temporal and (has_follow_up or len(current_input.split()) <= 6) and has_date_sql:
            return {
                "should_modify": True,
                "modification_type": "temporal_change", 
                "target_period": "detected_temporal_reference",
                "base_sql": previous_sql,
                "base_question": previous_question,
                "new_question": current_input
            }
        
        return {"should_modify": False}
    
    def _check_for_sql_modification(self, user_input: str, sql_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Check if the current query can be satisfied by modifying a previous SQL query (legacy method)"""
        
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
            "this_week": ["this week", "current week"],
            "last_month": ["last month", "previous month", "month before", "past month"],
            "this_month": ["this month", "current month"],
            "last_quarter": ["last quarter", "previous quarter", "quarter before"],
            "this_quarter": ["this quarter", "current quarter"],
            "last_year": ["last year", "previous year", "year before"],
            "this_year": ["this year", "current year"],
            "today": ["today", "this day"],
            "yesterday": ["yesterday", "day before"]
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
        
        # Update conversation history with current messages
        messages = state.get("messages", [])
        self._update_conversation_history(messages)
        print(f"[ORCHESTRATOR] Received {len(messages)} messages in conversation")
        
        # Track any SQL queries from previous executions for context
        self._track_sql_from_state(state, user_input)
        
        # Also populate SQL context from state's SQL query history if available
        self._populate_sql_context_from_state(state)
        
        # Get conversation context excluding current user input
        history_text = self._get_conversation_context(user_input)
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
            sql_modification_result = self._check_for_sql_modification_from_context(user_input)
            
            if sql_modification_result["should_modify"]:
                # Route to direct SQL modification instead of full pipeline
                print(f"[ORCHESTRATOR] Detected SQL modification request: {sql_modification_result['modification_type']}")
                state["sql_modification_request"] = sql_modification_result
                state["workflow_status"] = "active"
                
                # Ensure state SQL query history is populated for SQL modifier fallback
                self._ensure_state_sql_history(state, sql_modification_result)
                
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
                    "routed_to": "sql_modification,azure_retrieval",
                    "use_history_in_prompts": True  # SQL modifier should use history for context
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
                    "routed_to": "kpi_retrieval,metadata_retrieval,sql_generation,azure_retrieval",
                    "use_history_in_prompts": False  # New unrelated query - don't use history in prompts
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
        # Debug: Check SQL context history
        print(f"[ORCHESTRATOR DEBUG] SQL context history has {len(self.sql_context_history)} entries")
        
        # Get user messages from conversation history for "last message" queries
        user_messages = []
        for msg in self.conversation_history:
            if isinstance(msg, HumanMessage):
                user_messages.append(msg.content.strip())
        
        # Remove the current user input from the list to get previous messages
        previous_user_messages = [msg for msg in user_messages if msg != user_input.strip()]
        
        print(f"[ORCHESTRATOR DEBUG] Found {len(previous_user_messages)} previous user messages")
        if previous_user_messages:
            print(f"[ORCHESTRATOR DEBUG] Last user message: {previous_user_messages[-1]}")
            print(f"[ORCHESTRATOR DEBUG] First user message: {previous_user_messages[0] if previous_user_messages else 'None'}")
        
        prompt = f"""
        You are an AI Risk Intelligence assistant for Hirschbach's fleet risk management. Provide a helpful, direct response to this user query.
        
        Available data: You have access to one table called 'claims_summary' which contains aggregated data of claims on Claim Number level.
        
        CONVERSATION HISTORY (messages that happened BEFORE the current query):
        {history_text}
        
        SQL QUERY HISTORY (previous questions and their generated SQL queries):
        {self._format_sql_context_history()}

        CURRENT USER QUERY: "{user_input}"
        
        IMPORTANT MESSAGE CONTEXT:
        - User's previous messages (excluding current): {previous_user_messages}
        - Last user message: "{previous_user_messages[-1] if previous_user_messages else 'No previous messages'}"
        - First user message: "{previous_user_messages[0] if previous_user_messages else 'No previous messages'}"
        
        IMPORTANT: When the user asks about their "last message" or "previous message", they are referring to their most recent message BEFORE the current query.
        
        IMPORTANT: When the user asks about "questions I asked" or "generated queries", use the SQL QUERY HISTORY above to provide specific questions and their actual SQL queries.
        
        Example responses:
        - If user asks "What was my last message?" → Answer with the last message from the previous messages list above
        - If user asks "What was my first message?" → Answer with the first message from the previous messages list above
        - If user asks about conversation history → Reference the CONVERSATION HISTORY above
        
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
        - For message history queries, use the specific message context provided above
        
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