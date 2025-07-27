"""
DBT Automation Tool

A comprehensive tool for automating dbt project generation using OpenAI API
and configuration files.
"""

__version__ = "1.0.0"
__author__ = "DBT Automation Team"

# Expose public API for dbt_automation
from .llm_client import LLMClient
from .dbt_project_generator import DBTProjectGenerator
from .schema_generator import SchemaGenerator
from .schema_reader import SchemaReader
from .config import Config
from .sqlfluff_formatter import SQLFluffFormatter

__all__ = [
    "Config",
    "DBTProjectGenerator",
    "SchemaGenerator",
    "SchemaReader",
    "SQLFluffFormatter"
] 