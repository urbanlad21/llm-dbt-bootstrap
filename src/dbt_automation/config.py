"""
Configuration management for DBT automation tool.

This module provides centralized configuration management with environment variable
support, validation, and best practices for dbt project generation.
"""

import os
import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class Config:
    """
    Centralized configuration management for DBT automation tool.
    
    This class manages all configuration settings including OpenAI API settings,
    dbt project configuration, file paths, and best practices.
    
    Attributes:
        OPENAI_API_KEY: OpenAI API key for model generation
        OPENAI_MODEL: OpenAI model to use (default: gpt-4)
        PROJECT_ROOT: Root directory for generated dbt project
        SOURCE_CSV_PATH: Path to source tables CSV file
        SCHEMA_DEFINITIONS_PATH: Path to schema definitions CSV file
        MAPPING_YAML_PATH: Path to mapping YAML file
        PROMPTS_PATH: Path to prompt templates directory
    """
    
    # OpenAI Configuration
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4")
    OPENAI_MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", "4000"))
    OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.1"))
    
    # DBT Project Configuration
    PROJECT_ROOT = os.getenv("PROJECT_ROOT", "./dbt_project")
    PROJECT_NAME = os.getenv("PROJECT_NAME", "dbt_automation_project")
    PROJECT_VERSION = os.getenv("PROJECT_VERSION", "1.0.0")
    DBT_PROFILE_NAME = os.getenv("DBT_PROFILE_NAME", "dbt_automation_profile")
    
    # Database Configuration
    DATABASE_TYPE = os.getenv("DATABASE_TYPE", "snowflake")
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "")
    
    # File Paths
    SOURCE_CSV_PATH = os.getenv("SOURCE_CSV_PATH", "./config/source_tables.csv")
    SCHEMA_DEFINITIONS_PATH = os.getenv("SCHEMA_DEFINITIONS_PATH", "./config/schema_definitions.csv")
    MAPPING_YAML_PATH = os.getenv("MAPPING_YAML_PATH", "./config/table_mappings.yaml")
    PROMPTS_PATH = os.getenv("PROMPTS_PATH", "./prompts")
    
    # Generic Prompts
    MODEL_GENERATION_PROMPT = os.getenv(
        "MODEL_GENERATION_PROMPT",
        "You are an expert dbt developer. Create a production-ready dbt model."
    )
    CODE_REVIEW_PROMPT = os.getenv(
        "CODE_REVIEW_PROMPT", 
        "You are a senior dbt developer. Review the following code for quality and best practices."
    )
    UNIT_TEST_PROMPT = os.getenv(
        "UNIT_TEST_PROMPT",
        "You are an expert dbt developer. Generate comprehensive unit tests for the following model."
    )
    
    # DBT Best Practices
    DBT_BEST_PRACTICES = {
        "naming_conventions": {
            "staging": "stg_",
            "intermediate": "int_",
            "dimensions": "dim_",
            "facts": "fact_",
            "marts": "marts/"
        },
        "materialization_strategies": {
            "staging": "view",
            "intermediate": "table",
            "dimensions": "table",
            "facts": "table"
        },
        "incremental_strategies": {
            "merge": "merge",
            "insert_overwrite": "insert_overwrite"
        },
        "testing_strategies": {
            "generic_tests": ["not_null", "unique", "accepted_values", "relationships"],
            "custom_tests": ["dbt_utils.expression_is_true", "dbt_utils.string_length"]
        }
    }
    
    # SQLFluff Configuration
    SQLFLUFF_CONFIG = {
        "dialect": "snowflake",
        "rules": "L001,L002,L003,L004,L005,L006,L007,L008,L009,L010,L011,L012,L013,L014,L015,L016,L017,L018,L019,L020,L021,L022,L023,L024,L025,L026,L027,L028,L029,L030,L031,L032,L033,L034,L035,L036,L037,L038,L039,L040,L041,L042,L043,L044,L045,L046,L047,L048,L049,L050",
        "max_line_length": 120,
        "indent_unit": "space",
        "indent_size": 4
    }
    
    # Schema Constraints
    SCHEMA_CONSTRAINTS = {
        "not_null": "not_null",
        "unique": "unique", 
        "primary_key": "unique",
        "foreign_key": "relationships",
        "accepted_values": "accepted_values",
        "string_length": "dbt_utils.string_length",
        "range_check": "dbt_utils.expression_is_true",
        "pattern_check": "dbt_utils.expression_is_true",
        "date_range": "dbt_utils.expression_is_true"
    }
    
    @classmethod
    def validate_config(cls) -> bool:
        """
        Validate configuration settings.
        
        Returns:
            bool: True if configuration is valid, False otherwise
            
        Raises:
            ValueError: If required configuration is missing
        """
        logger.info("Validating configuration...")
        
        # Check OpenAI API key
        if not cls.OPENAI_API_KEY:
            logger.warning("OPENAI_API_KEY is not set. OpenAI features will be disabled.")
        
        # Check project root
        if not cls.PROJECT_ROOT:
            raise ValueError("PROJECT_ROOT is required")
        
        # Check file paths exist
        required_paths = [
            cls.SOURCE_CSV_PATH,
            cls.SCHEMA_DEFINITIONS_PATH,
            cls.MAPPING_YAML_PATH
        ]
        
        for path in required_paths:
            if not os.path.exists(path):
                logger.warning(f"Configuration file not found: {path}")
        
        # Check prompts directory
        if not os.path.exists(cls.PROMPTS_PATH):
            logger.warning(f"Prompts directory not found: {cls.PROMPTS_PATH}")
        
        logger.info("Configuration validation completed")
        return True 
    
    @classmethod
    def get_project_structure(cls) -> dict:
        """
        Get the standard dbt project structure.
        
        Returns:
            dict: Project structure configuration
        """
        return {
            "models": {
                "staging": "models/staging",
                "intermediate": "models/intermediate", 
                "marts": {
                    "dimensions": "models/marts/dimensions",
                    "facts": "models/marts/facts"
                }
            },
            "macros": "macros",
            "tests": "tests",
            "docs": "docs",
            "schemas": "schemas",
            "target": "target",
            "logs": "logs"
        }
    
    @classmethod
    def get_database_config(cls) -> dict:
        """
        Get database configuration based on database type.
        
        Returns:
            dict: Database configuration
        """
        if cls.DATABASE_TYPE.lower() == "snowflake":
            return {
                "type": "snowflake",
                "account": cls.SNOWFLAKE_ACCOUNT,
                "user": cls.SNOWFLAKE_USER,
                "password": cls.SNOWFLAKE_PASSWORD,
                "role": cls.SNOWFLAKE_ROLE,
                "database": cls.SNOWFLAKE_DATABASE,
                "warehouse": cls.SNOWFLAKE_WAREHOUSE,
                "schema": cls.SNOWFLAKE_SCHEMA
            }
        else:
            raise ValueError(f"Unsupported database type: {cls.DATABASE_TYPE}")
    
    @classmethod
    def get_prompt_template(cls, prompt_type: str) -> str:
        """
        Get prompt template by type.
        
        Args:
            prompt_type: Type of prompt (model_generation, code_review, unit_test)
            
        Returns:
            str: Prompt template content
            
        Raises:
            ValueError: If prompt type is not supported
        """
        try:
            prompt_file = Path(cls.PROMPTS_PATH) / f"{prompt_type}.txt"
            
            if prompt_file.exists():
                with open(prompt_file, 'r', encoding='utf-8') as f:
                    return f.read()
        except Exception as e:
            logger.warning(f"Could not read prompt file for {prompt_type}: {e}")
        
        # Fallback to environment variable or default
        if prompt_type == "model_generation":
            return cls.MODEL_GENERATION_PROMPT
        elif prompt_type == "code_review":
            return cls.CODE_REVIEW_PROMPT
        elif prompt_type == "unit_test":
            return cls.UNIT_TEST_PROMPT
        else:
            raise ValueError(f"Unsupported prompt type: {prompt_type}")
    
    @classmethod
    def update_config(cls, **kwargs) -> None:
        """
        Update configuration settings.
        
        Args:
            **kwargs: Configuration key-value pairs to update
        """
        for key, value in kwargs.items():
            if hasattr(cls, key):
                setattr(cls, key, value)
                logger.info(f"Updated configuration: {key} = {value}")
            else:
                logger.warning(f"Unknown configuration key: {key}")
    
    @classmethod
    def get_config_summary(cls) -> dict:
        """
        Get a summary of current configuration.
        
        Returns:
            dict: Configuration summary
        """
        return {
            "openai": {
                "model": cls.OPENAI_MODEL,
                "max_tokens": cls.OPENAI_MAX_TOKENS,
                "temperature": cls.OPENAI_TEMPERATURE,
                "api_key_set": bool(cls.OPENAI_API_KEY)
            },
            "project": {
                "root": cls.PROJECT_ROOT,
                "name": cls.PROJECT_NAME,
                "version": cls.PROJECT_VERSION
            },
            "database": {
                "type": cls.DATABASE_TYPE,
                "account": cls.SNOWFLAKE_ACCOUNT if cls.DATABASE_TYPE == "snowflake" else None
            },
            "files": {
                "source_csv": cls.SOURCE_CSV_PATH,
                "schema_csv": cls.SCHEMA_DEFINITIONS_PATH,
                "mapping_yaml": cls.MAPPING_YAML_PATH,
                "prompts": cls.PROMPTS_PATH
            }
        } 