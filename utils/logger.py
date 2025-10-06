#!/usr/bin/env python3
"""
Centralized logging utility for Hirschbach MVP Chatbot
Provides consistent logging across all nodes with proper formatting and length management
"""

import logging
import os
from typing import Optional

class HirschbachLogger:
    """Centralized logger for Hirschbach MVP Chatbot with enhanced formatting"""

    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.node_name = self._extract_node_name(name)

        if not self.logger.handlers:
            self._setup_logger()

        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        self.logger.setLevel(level_map.get(log_level.upper(), logging.INFO))

    def _extract_node_name(self, full_name: str) -> str:
        """Extract clean node name from full logger name"""
        parts = full_name.split('.')
        if len(parts) > 1:
            return parts[-1].upper().replace('_', ' ')
        return full_name.upper()

    def _setup_logger(self):
        """Set up logging configuration"""
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )

        log_file = os.path.join(os.getcwd(), 'hirschbach_chatbot.log')
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

    def _create_separator(self, title: str, width: int = 80) -> str:
        """Create a formatted separator line"""
        if len(title) >= width - 4:
            return f"={title}="

        padding = width - len(title) - 2
        left_padding = padding // 2
        right_padding = padding - left_padding

        return f"{'=' * left_padding}{title}{'=' * right_padding}"

    def _truncate_message(self, message: str, max_length: int = 200) -> str:
        """Truncate long messages while preserving readability"""
        if len(message) <= max_length:
            return message

        truncated = message[:max_length]
        last_space = truncated.rfind(' ')
        last_comma = truncated.rfind(',')

        if last_space > max_length * 0.7:
            return message[:last_space] + "..."
        elif last_comma > max_length * 0.7:
            return message[:last_comma] + "..."
        else:
            return truncated + "..."

    def node_initialized(self, message: str = ""):
        """Log node initialization with proper formatting"""
        separator = self._create_separator(f"{self.node_name} INITIALIZED")
        self.logger.info(f"\n{separator}")
        if message:
            self.logger.info(f"  {message}")

    def node_started(self, message: str = ""):
        """Log node execution start"""
        separator = self._create_separator(f"{self.node_name} STARTED")
        self.logger.info(f"\n{separator}")
        if message:
            self.logger.info(f"  {message}")

    def node_completed(self, message: str = "", details: Optional[str] = None):
        """Log node completion with optional details"""
        separator = self._create_separator(f"{self.node_name} COMPLETED")
        self.logger.info(f"\n{separator}")
        if message:
            self.logger.info(f"  {message}")
        if details:
            truncated_details = self._truncate_message(details, 300)
            self.logger.info(f"  Details: {truncated_details}")

    def node_processing(self, message: str, details: Optional[str] = None):
        """Log node processing with optional details"""
        self.logger.info(f"  🔄 {self.node_name}: {message}")
        if details:
            truncated_details = self._truncate_message(details, 200)
            self.logger.info(f"     └─ {truncated_details}")

    def node_debug(self, message: str, data: Optional[any] = None):
        """Log debug information with optional data truncation"""
        self.logger.debug(f"  🔍 {self.node_name} DEBUG: {message}")
        if data is not None:
            data_str = str(data)
            if len(data_str) > 150:
                data_str = data_str[:150] + "..."
            self.logger.debug(f"     └─ Data: {data_str}")

    def node_warning(self, message: str, details: Optional[str] = None):
        """Log warnings with optional details"""
        self.logger.warning(f"  ⚠️  {self.node_name} WARNING: {message}")
        if details:
            self.logger.warning(f"     └─ {details}")

    def node_error(self, message: str, error: Optional[Exception] = None):
        """Log errors with optional exception details"""
        self.logger.error(f"  ❌ {self.node_name} ERROR: {message}")
        if error:
            self.logger.error(f"     └─ Exception: {type(error).__name__}: {str(error)}")

    def node_success(self, message: str, details: Optional[str] = None):
        """Log successful operations"""
        self.logger.info(f"  ✅ {self.node_name}: {message}")
        if details:
            truncated_details = self._truncate_message(details, 200)
            self.logger.info(f"     └─ {truncated_details}")

    def log_sql_query(self, query: str, execution_time: Optional[str] = None):
        """Log SQL queries with length management"""
        if len(query) > 300:
            truncated_query = query[:300] + "... [TRUNCATED]"
            self.logger.info(f"  📄 {self.node_name} SQL Query (truncated): {truncated_query}")
        else:
            self.logger.info(f"  📄 {self.node_name} SQL Query: {query}")

        if execution_time:
            self.logger.info(f"     └─ Execution time: {execution_time}")

    def log_data_summary(self, data_info: dict):
        """Log data summary information"""
        rows = data_info.get('rows_returned', 0)
        columns = len(data_info.get('columns', []))
        execution_time = data_info.get('execution_time', 'N/A')

        self.logger.info(f"  📊 {self.node_name} Data Summary:")
        self.logger.info(f"     └─ Rows: {rows:,}")
        self.logger.info(f"     └─ Columns: {columns}")
        self.logger.info(f"     └─ Execution time: {execution_time}")

    def log_llm_response(self, response: str, truncated: bool = True):
        """Log LLM responses with truncation"""
        if truncated and len(response) > 200:
            response = response[:200] + "... [TRUNCATED]"

        self.logger.info(f"  🤖 {self.node_name} LLM Response: {response}")

    def log_workflow_status(self, status: str, details: Optional[str] = None):
        """Log workflow status changes"""
        self.logger.info(f"  🔄 {self.node_name} Workflow: {status}")
        if details:
            self.logger.info(f"     └─ {details}")

def get_logger(node_name: str) -> HirschbachLogger:
    """Get or create a logger for a specific node"""
    return HirschbachLogger(f"Hirschbach.Nodes.{node_name}")

def log_node_initialization(node_name: str, message: str = ""):
    """Log node initialization"""
    logger = get_logger(node_name)
    logger.node_initialized(message)

def log_node_execution(node_name: str, message: str = ""):
    """Log node execution start"""
    logger = get_logger(node_name)
    logger.node_started(message)

def log_node_completion(node_name: str, message: str = "", details: str = None):
    """Log node completion"""
    logger = get_logger(node_name)
    logger.node_completed(message, details)

def log_sql_generation(node_name: str, sql_query: str, execution_time: str = None):
    """Log SQL generation with proper formatting"""
    logger = get_logger(node_name)
    logger.log_sql_query(sql_query, execution_time)

def log_data_retrieval(node_name: str, data_info: dict):
    """Log data retrieval results"""
    logger = get_logger(node_name)
    logger.log_data_summary(data_info)

def log_error(node_name: str, message: str, error: Exception = None):
    """Log errors with proper formatting"""
    logger = get_logger(node_name)
    logger.node_error(message, error)

def log_debug(node_name: str, message: str, data: any = None):
    """Log debug information"""
    logger = get_logger(node_name)
    logger.node_debug(message, data)

def log_warning(node_name: str, message: str, details: str = None):
    """Log warnings"""
    logger = get_logger(node_name)
    logger.node_warning(message, details)

def log_success(node_name: str, message: str, details: str = None):
    """Log successful operations"""
    logger = get_logger(node_name)
    logger.node_success(message, details)

def log_workflow_status(node_name: str, status: str, details: str = None):
    """Log workflow status"""
    logger = get_logger(node_name)
    logger.log_workflow_status(status, details)

def log_llm_interaction(node_name: str, response: str):
    """Log LLM interactions"""
    logger = get_logger(node_name)
    logger.log_llm_response(response)
