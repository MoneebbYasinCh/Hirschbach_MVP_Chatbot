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
        """Format conversation history as text."""
        if not messages:
            return ""
        lines: List[str] = []
        for msg in messages:
            role = "User" if isinstance(msg, HumanMessage) else ("Assistant" if isinstance(msg, AIMessage) else "System")
            content = str(getattr(msg, "content", "")).strip()
            lines.append(f"- {role}: {content}")
        return "\n".join(lines)
    
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
        history_text = self._format_history_as_text(messages[:-1])  # Exclude current message
        
        # Decide: Direct reply or data analysis?
        if self._should_reply_directly(user_input, history_text):
            self.logger.info("[ORCHESTRATOR] Decided to reply directly")
            print("[ORCHESTRATOR] Decided to reply directly")
            
            # Generate direct response and end workflow
            response = self._generate_direct_response(user_input, history_text)
            ai_message = AIMessage(content=response)
            state["messages"].append(ai_message)
            state["final_response"] = response
            state["workflow_status"] = "complete"
            
            self.logger.info(f"[ORCHESTRATOR] Direct response generated: {response[:100]}...")
            print(f"[ORCHESTRATOR] Direct response generated: {response[:100]}...")
        else:
            self.logger.info("[ORCHESTRATOR] Decided to perform data analysis")
            print("[ORCHESTRATOR] Decided to perform data analysis")
            
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
        Use LLM to decide whether to reply directly or perform data analysis
        
        Args:
            user_input: The user's input text
            history_text: Conversation history
            
        Returns:
            True if should reply directly, False if needs data analysis
        """
        prompt = f"""
        You are an AI Risk Intelligence assistant for Hirschbach's fleet risk management. Analyze this user input and decide whether to reply directly or perform data analysis.
        
        Available data: You have access to one table called 'claims_summary' which contains aggregated data of claims on Claim Number level.
        
        Conversation context:
        {history_text}

        User input: "{user_input}"
        
        DIRECT_REPLY for:
        - General questions about risk management concepts
        - Definitions and explanations of safety terms
        - Help and capability questions
        - Simple process explanations
        - General information requests about the platform
        - Questions about claims data structure or capabilities
        
        DATA_ANALYSIS for:
        - Any request for claims data analysis
        - Queries about specific claims, drivers, accidents, or incidents
        - "Show me", "get", "find" requests about claims data
        - Reports and analytics on claims
        - Questions about claims performance, trends, or patterns
        - Any data query that would require database analysis
        
        Examples:
        - "What is preventable crash rate?" → DIRECT_REPLY
        - "How does claims data work?" → DIRECT_REPLY
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
        return "Analyzing claims data..."
    
    
    
    def _generate_direct_response(self, user_input: str, history_text: str) -> str:
        """
        Generate a direct response for simple queries
        
        Args:
            user_input: The user's input text
            history_text: Conversation history
            
        Returns:
            Direct response string
        """
        prompt = f"""
        You are an AI Risk Intelligence assistant for Hirschbach's fleet risk management. Provide a helpful, direct response to this user query.
        
        Available data: You have access to one table called 'claims_summary' which contains aggregated data of claims on Claim Number level.
        
        Conversation context:
        {history_text}

        User query: "{user_input}"
        
        Guidelines:
        - Keep your response concise and professional
        - Focus on claims data analysis and risk management
        - If asking about capabilities, explain the claims data analysis features
        - Use clear formatting and structure
        - Reference Hirschbach's claims data when relevant
        - Emphasize data-driven insights from claims analysis
        
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