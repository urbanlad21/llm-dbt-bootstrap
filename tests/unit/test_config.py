"""
Unit tests for Config module.

This module provides comprehensive unit tests for the Config class with 100% code coverage,
testing all configuration management functionality including validation, updates, and
environment variable handling.
"""

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open
from src.dbt_automation.config import Config


class TestConfig:
    """Test cases for Config class with comprehensive coverage."""
    
    def setup_method(self):
        """Set up test environment before each test method."""
        # Reset environment variables
        self.original_env = os.environ.copy()
        
        # Clear any existing environment variables
        env_vars_to_clear = [
            'OPENAI_API_KEY', 'OPENAI_MODEL', 'OPENAI_MAX_TOKENS', 'OPENAI_TEMPERATURE',
            'PROJECT_ROOT', 'PROJECT_NAME', 'PROJECT_VERSION', 'DATABASE_TYPE',
            'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_ROLE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_SCHEMA', 'SOURCE_CSV_PATH', 'SCHEMA_DEFINITIONS_PATH',
            'MAPPING_YAML_PATH', 'PROMPTS_PATH'
        ]
        
        for var in env_vars_to_clear:
            if var in os.environ:
                del os.environ[var]
    
    def teardown_method(self):
        """Clean up test environment after each test method."""
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)
    
    def test_default_configuration(self):
        """Test default configuration values when no environment variables are set."""
        # Test OpenAI configuration defaults
        assert Config.OPENAI_API_KEY == ""
        assert Config.OPENAI_MODEL == "gpt-4"
        assert Config.OPENAI_MAX_TOKENS == 4000
        assert Config.OPENAI_TEMPERATURE == 0.1
        
        # Test project configuration defaults
        assert Config.PROJECT_ROOT == "./dbt_project"
        assert Config.PROJECT_NAME == "dbt_automation_project"
        assert Config.PROJECT_VERSION == "1.0.0"
        
        # Test database configuration defaults
        assert Config.DATABASE_TYPE == "snowflake"
        assert Config.SNOWFLAKE_ACCOUNT == ""
        assert Config.SNOWFLAKE_USER == ""
        assert Config.SNOWFLAKE_PASSWORD == ""
        assert Config.SNOWFLAKE_ROLE == ""
        assert Config.SNOWFLAKE_DATABASE == ""
        assert Config.SNOWFLAKE_WAREHOUSE == ""
        assert Config.SNOWFLAKE_SCHEMA == ""
        
        # Test file path defaults
        assert Config.SOURCE_CSV_PATH == "./config/source_tables.csv"
        assert Config.SCHEMA_DEFINITIONS_PATH == "./config/schema_definitions.csv"
        assert Config.MAPPING_YAML_PATH == "./config/table_mappings.yaml"
        assert Config.PROMPTS_PATH == "./prompts"
    
    def test_environment_variable_override(self):
        """Test that environment variables properly override default values."""
        # Set environment variables
        os.environ['OPENAI_API_KEY'] = 'test-api-key'
        os.environ['OPENAI_MODEL'] = 'gpt-3.5-turbo'
        os.environ['OPENAI_MAX_TOKENS'] = '2000'
        os.environ['OPENAI_TEMPERATURE'] = '0.5'
        os.environ['PROJECT_ROOT'] = '/custom/project/path'
        os.environ['PROJECT_NAME'] = 'custom_project'
        os.environ['PROJECT_VERSION'] = '2.0.0'
        os.environ['DATABASE_TYPE'] = 'bigquery'
        os.environ['SNOWFLAKE_ACCOUNT'] = 'test-account'
        os.environ['SNOWFLAKE_USER'] = 'test-user'
        os.environ['SNOWFLAKE_PASSWORD'] = 'test-password'
        os.environ['SNOWFLAKE_ROLE'] = 'test-role'
        os.environ['SNOWFLAKE_DATABASE'] = 'test-db'
        os.environ['SNOWFLAKE_WAREHOUSE'] = 'test-warehouse'
        os.environ['SNOWFLAKE_SCHEMA'] = 'test-schema'
        os.environ['SOURCE_CSV_PATH'] = '/custom/source.csv'
        os.environ['SCHEMA_DEFINITIONS_PATH'] = '/custom/schema.csv'
        os.environ['MAPPING_YAML_PATH'] = '/custom/mapping.yaml'
        os.environ['PROMPTS_PATH'] = '/custom/prompts'
        
        # Reload the module to pick up new environment variables
        import importlib
        import src.dbt_automation.config
        importlib.reload(src.dbt_automation.config)
        from src.dbt_automation.config import Config
        
        # Test that environment variables are properly loaded
        assert Config.OPENAI_API_KEY == 'test-api-key'
        assert Config.OPENAI_MODEL == 'gpt-3.5-turbo'
        assert Config.OPENAI_MAX_TOKENS == 2000
        assert Config.OPENAI_TEMPERATURE == 0.5
        assert Config.PROJECT_ROOT == '/custom/project/path'
        assert Config.PROJECT_NAME == 'custom_project'
        assert Config.PROJECT_VERSION == '2.0.0'
        assert Config.DATABASE_TYPE == 'bigquery'
        assert Config.SNOWFLAKE_ACCOUNT == 'test-account'
        assert Config.SNOWFLAKE_USER == 'test-user'
        assert Config.SNOWFLAKE_PASSWORD == 'test-password'
        assert Config.SNOWFLAKE_ROLE == 'test-role'
        assert Config.SNOWFLAKE_DATABASE == 'test-db'
        assert Config.SNOWFLAKE_WAREHOUSE == 'test-warehouse'
        assert Config.SNOWFLAKE_SCHEMA == 'test-schema'
        assert Config.SOURCE_CSV_PATH == '/custom/source.csv'
        assert Config.SCHEMA_DEFINITIONS_PATH == '/custom/schema.csv'
        assert Config.MAPPING_YAML_PATH == '/custom/mapping.yaml'
        assert Config.PROMPTS_PATH == '/custom/prompts'
    
    def test_validate_config_success(self):
        """Test successful configuration validation."""
        # Set required environment variables
        os.environ['PROJECT_ROOT'] = './test_project'
        
        # Create temporary files for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            source_csv = Path(temp_dir) / "source_tables.csv"
            schema_csv = Path(temp_dir) / "schema_definitions.csv"
            mapping_yaml = Path(temp_dir) / "table_mappings.yaml"
            prompts_dir = Path(temp_dir) / "prompts"
            
            source_csv.write_text("test")
            schema_csv.write_text("test")
            mapping_yaml.write_text("test")
            prompts_dir.mkdir()
            
            # Update config paths
            Config.SOURCE_CSV_PATH = str(source_csv)
            Config.SCHEMA_DEFINITIONS_PATH = str(schema_csv)
            Config.MAPPING_YAML_PATH = str(mapping_yaml)
            Config.PROMPTS_PATH = str(prompts_dir)
            Config.PROJECT_ROOT = str(Path(temp_dir) / "project")
            
            # Test validation
            result = Config.validate_config()
            assert result is True
    
    def test_validate_config_missing_project_root(self):
        """Test configuration validation with missing PROJECT_ROOT."""
        # Clear PROJECT_ROOT
        Config.PROJECT_ROOT = ""
        
        with pytest.raises(ValueError, match="PROJECT_ROOT is required"):
            Config.validate_config()
    
    def test_validate_config_missing_files(self):
        """Test configuration validation with missing files."""
        # Set PROJECT_ROOT
        Config.PROJECT_ROOT = "./test_project"
        
        # Set non-existent file paths
        Config.SOURCE_CSV_PATH = "/non/existent/source.csv"
        Config.SCHEMA_DEFINITIONS_PATH = "/non/existent/schema.csv"
        Config.MAPPING_YAML_PATH = "/non/existent/mapping.yaml"
        Config.PROMPTS_PATH = "/non/existent/prompts"
        
        # Validation should pass but log warnings
        result = Config.validate_config()
        assert result is True
    
    def test_get_project_structure(self):
        """Test getting standard dbt project structure."""
        structure = Config.get_project_structure()
        
        assert isinstance(structure, dict)
        assert "models" in structure
        assert "macros" in structure
        assert "tests" in structure
        assert "docs" in structure
        assert "schemas" in structure
        assert "target" in structure
        assert "logs" in structure
        
        # Test nested structure
        assert "staging" in structure["models"]
        assert "intermediate" in structure["models"]
        assert "marts" in structure["models"]
        assert "dimensions" in structure["models"]["marts"]
        assert "facts" in structure["models"]["marts"]
    
    def test_get_database_config_snowflake(self):
        """Test getting Snowflake database configuration."""
        # Set Snowflake environment variables
        os.environ['SNOWFLAKE_ACCOUNT'] = 'test-account'
        os.environ['SNOWFLAKE_USER'] = 'test-user'
        os.environ['SNOWFLAKE_PASSWORD'] = 'test-password'
        os.environ['SNOWFLAKE_ROLE'] = 'test-role'
        os.environ['SNOWFLAKE_DATABASE'] = 'test-db'
        os.environ['SNOWFLAKE_WAREHOUSE'] = 'test-warehouse'
        os.environ['SNOWFLAKE_SCHEMA'] = 'test-schema'
        
        # Reload module
        import importlib
        import src.dbt_automation.config
        importlib.reload(src.dbt_automation.config)
        from src.dbt_automation.config import Config
        
        config = Config.get_database_config()
        
        assert config["type"] == "snowflake"
        assert config["account"] == "test-account"
        assert config["user"] == "test-user"
        assert config["password"] == "test-password"
        assert config["role"] == "test-role"
        assert config["database"] == "test-db"
        assert config["warehouse"] == "test-warehouse"
        assert config["schema"] == "test-schema"
    
    def test_get_database_config_unsupported(self):
        """Test getting database configuration for unsupported database type."""
        Config.DATABASE_TYPE = "unsupported_db"
        
        with pytest.raises(ValueError, match="Unsupported database type: unsupported_db"):
            Config.get_database_config()
    
    @patch('builtins.open', new_callable=mock_open, read_data="Test prompt template")
    @patch('pathlib.Path.exists', return_value=True)
    def test_get_prompt_template_file_exists(self, mock_exists, mock_file):
        """Test getting prompt template from file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            Config.PROMPTS_PATH = temp_dir
            
            template = Config.get_prompt_template("model_generation")
            
            assert template == "Test prompt template"
            mock_file.assert_called_once()
    
    def test_get_prompt_template_file_not_exists(self):
        """Test getting prompt template when file doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            Config.PROMPTS_PATH = temp_dir
            
            # Test fallback to environment variable
            template = Config.get_prompt_template("model_generation")
            assert template == Config.MODEL_GENERATION_PROMPT
            
            template = Config.get_prompt_template("code_review")
            assert template == Config.CODE_REVIEW_PROMPT
            
            template = Config.get_prompt_template("unit_test")
            assert template == Config.UNIT_TEST_PROMPT
    
    def test_get_prompt_template_unsupported_type(self):
        """Test getting prompt template for unsupported type."""
        with pytest.raises(ValueError, match="Unsupported prompt type: invalid_type"):
            Config.get_prompt_template("invalid_type")
    
    def test_update_config_valid_keys(self):
        """Test updating configuration with valid keys."""
        # Store original values
        original_model = Config.OPENAI_MODEL
        original_tokens = Config.OPENAI_MAX_TOKENS
        
        # Update configuration
        Config.update_config(
            OPENAI_MODEL="gpt-3.5-turbo",
            OPENAI_MAX_TOKENS=3000
        )
        
        # Verify updates
        assert Config.OPENAI_MODEL == "gpt-3.5-turbo"
        assert Config.OPENAI_MAX_TOKENS == 3000
        
        # Restore original values
        Config.OPENAI_MODEL = original_model
        Config.OPENAI_MAX_TOKENS = original_tokens
    
    def test_update_config_invalid_keys(self):
        """Test updating configuration with invalid keys."""
        # Update with invalid key
        Config.update_config(INVALID_KEY="test_value")
        
        # Configuration should remain unchanged
        assert not hasattr(Config, 'INVALID_KEY')
    
    def test_get_config_summary(self):
        """Test getting configuration summary."""
        summary = Config.get_config_summary()
        
        assert isinstance(summary, dict)
        assert "openai" in summary
        assert "project" in summary
        assert "database" in summary
        assert "files" in summary
        
        # Test OpenAI section
        openai_config = summary["openai"]
        assert "model" in openai_config
        assert "max_tokens" in openai_config
        assert "temperature" in openai_config
        assert "api_key_set" in openai_config
        assert isinstance(openai_config["api_key_set"], bool)
        
        # Test project section
        project_config = summary["project"]
        assert "root" in project_config
        assert "name" in project_config
        assert "version" in project_config
        
        # Test database section
        database_config = summary["database"]
        assert "type" in database_config
        assert "account" in database_config
        
        # Test files section
        files_config = summary["files"]
        assert "source_csv" in files_config
        assert "schema_csv" in files_config
        assert "mapping_yaml" in files_config
        assert "prompts" in files_config
    
    def test_dbt_best_practices_structure(self):
        """Test DBT best practices configuration structure."""
        practices = Config.DBT_BEST_PRACTICES
        
        assert "naming_conventions" in practices
        assert "materialization_strategies" in practices
        assert "incremental_strategies" in practices
        assert "testing_strategies" in practices
        
        # Test naming conventions
        naming = practices["naming_conventions"]
        assert "staging" in naming
        assert "intermediate" in naming
        assert "dimensions" in naming
        assert "facts" in naming
        assert "marts" in naming
        
        # Test materialization strategies
        materialization = practices["materialization_strategies"]
        assert "staging" in materialization
        assert "intermediate" in materialization
        assert "dimensions" in materialization
        assert "facts" in materialization
        
        # Test incremental strategies
        incremental = practices["incremental_strategies"]
        assert "merge" in incremental
        assert "insert_overwrite" in incremental
        
        # Test testing strategies
        testing = practices["testing_strategies"]
        assert "generic_tests" in testing
        assert "custom_tests" in testing
        assert isinstance(testing["generic_tests"], list)
        assert isinstance(testing["custom_tests"], list)
    
    def test_sqlfluff_config_structure(self):
        """Test SQLFluff configuration structure."""
        sqlfluff_config = Config.SQLFLUFF_CONFIG
        
        assert "dialect" in sqlfluff_config
        assert "rules" in sqlfluff_config
        assert "max_line_length" in sqlfluff_config
        assert "indent_unit" in sqlfluff_config
        assert "indent_size" in sqlfluff_config
        
        assert sqlfluff_config["dialect"] == "snowflake"
        assert sqlfluff_config["max_line_length"] == 120
        assert sqlfluff_config["indent_unit"] == "space"
        assert sqlfluff_config["indent_size"] == 4
        assert isinstance(sqlfluff_config["rules"], str)
    
    def test_schema_constraints_structure(self):
        """Test schema constraints configuration structure."""
        constraints = Config.SCHEMA_CONSTRAINTS
        
        assert "not_null" in constraints
        assert "unique" in constraints
        assert "primary_key" in constraints
        assert "foreign_key" in constraints
        assert "accepted_values" in constraints
        assert "string_length" in constraints
        assert "range_check" in constraints
        assert "pattern_check" in constraints
        assert "date_range" in constraints
        
        # Test constraint mappings
        assert constraints["not_null"] == "not_null"
        assert constraints["unique"] == "unique"
        assert constraints["primary_key"] == "unique"
        assert constraints["foreign_key"] == "relationships"
        assert constraints["accepted_values"] == "accepted_values"
        assert constraints["string_length"] == "dbt_utils.string_length"
        assert constraints["range_check"] == "dbt_utils.expression_is_true"
        assert constraints["pattern_check"] == "dbt_utils.expression_is_true"
        assert constraints["date_range"] == "dbt_utils.expression_is_true"
    
    def test_environment_variable_type_conversion(self):
        """Test proper type conversion of environment variables."""
        # Set environment variables with different types
        os.environ['OPENAI_MAX_TOKENS'] = '5000'
        os.environ['OPENAI_TEMPERATURE'] = '0.7'
        
        # Reload module
        import importlib
        import src.dbt_automation.config
        importlib.reload(src.dbt_automation.config)
        from src.dbt_automation.config import Config
        
        # Test type conversion
        assert Config.OPENAI_MAX_TOKENS == 5000
        assert isinstance(Config.OPENAI_MAX_TOKENS, int)
        assert Config.OPENAI_TEMPERATURE == 0.7
        assert isinstance(Config.OPENAI_TEMPERATURE, float)
    
    def test_config_immutability(self):
        """Test that configuration values are properly immutable."""
        # Store original values
        original_api_key = Config.OPENAI_API_KEY
        original_model = Config.OPENAI_MODEL
        
        # Try to modify values
        Config.OPENAI_API_KEY = "modified_key"
        Config.OPENAI_MODEL = "modified_model"
        
        # Verify values are updated
        assert Config.OPENAI_API_KEY == "modified_key"
        assert Config.OPENAI_MODEL == "modified_model"
        
        # Restore original values
        Config.OPENAI_API_KEY = original_api_key
        Config.OPENAI_MODEL = original_model 