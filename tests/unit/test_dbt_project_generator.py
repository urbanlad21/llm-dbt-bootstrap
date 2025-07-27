"""
Unit tests for DBTProjectGenerator class
"""
import pytest
import tempfile
import os
import yaml
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd

from src.dbt_automation.dbt_project_generator import DBTProjectGenerator
from src.dbt_automation.config import Config


@pytest.fixture
def sample_mapping_yaml(tmp_path):
    mapping_content = """
staging_models:
  - name: stg_customers
    source_table: customers
    columns:
      - name: id
        transformation: "cast to bigint"
"""
    file_path = tmp_path / "mapping.yaml"
    file_path.write_text(mapping_content)
    return str(file_path)

@pytest.fixture
def sample_source_csv(tmp_path):
    csv_content = (
        "table_name,source_schema,source_database,file_format,location,description,refresh_frequency,partition_by,cluster_by\n"
        "customers,raw_data,analytics,CSV,s3://bucket/raw/customers/,Customer data from CRM,daily,date,created_at\n"
        "orders,raw_data,analytics,CSV,s3://bucket/raw/orders/,Order data from e-commerce,daily,date,order_date\n"
    )
    file_path = tmp_path / "source.csv"
    file_path.write_text(csv_content)
    return str(file_path)

class TestDBTProjectGenerator:
    """Test cases for DBTProjectGenerator class"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def generator(self, temp_dir):
        """Create a DBTProjectGenerator instance with temp directory"""
        Config.PROJECT_ROOT = temp_dir
        return DBTProjectGenerator()
    
    def test_init(self):
        """Test DBTProjectGenerator initialization"""
        generator = DBTProjectGenerator()
        assert generator.llm_client is not None
        assert generator.project_root == Path(Config.PROJECT_ROOT)
        assert generator.schema_generator is not None
        assert generator.schema_reader is not None
        assert generator.sqlfluff_formatter is not None
    
    def test_create_project_structure(self, tmp_path):
        """Test project structure creation"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)  # Ensure project_root is updated
        generator.create_project_structure()
        
        # Check if directories were created
        expected_dirs = [
            "models", "macros", "tests", "docs", "logs", "target"
        ]
        
        for dir_name in expected_dirs:
            dir_path = tmp_path / dir_name
            assert dir_path.exists(), f"Directory {dir_name} was not created"
            assert dir_path.is_dir(), f"{dir_name} is not a directory"
        
        # Only check for top-level directories
        for dir_name in ["models", "macros", "tests", "docs", "logs", "target"]:
            dir_path = tmp_path / dir_name
            assert dir_path.exists(), f"Directory {dir_name} was not created"
        # packages.yml is no longer created, so we do not check for it
    
    def test_generate_external_tables(self, tmp_path, sample_source_csv):
        """Test external tables generation (sources.yml)"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)
        generator.generate_external_tables(sample_source_csv)
        
        # Check if sources.yml was created
        sources_path = tmp_path / "models" / "analytics" / "raw_data" / "sources.yml"
        assert sources_path.exists(), "sources.yml was not created"
        
        # Check sources.yml content
        with open(sources_path, 'r') as f:
            content = f.read()
        
        assert "version: 2" in content
        assert "sources:" in content
        assert "raw_data" in content
        assert "customers" in content
        assert "s3://bucket/raw/customers/" in content
        assert "s3://bucket/raw/orders/" in content
        assert "partitions:" in content
        assert "cluster_by:" in content
    
    def test_generate_external_tables_missing_file(self, generator):
        """Test external tables generation with missing CSV file"""
        with pytest.raises(FileNotFoundError):
            generator.generate_external_tables("nonexistent.csv")
    
    def test_generate_external_tables_incomplete_data(self, tmp_path):
        """Test external tables generation with incomplete data"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)
        # Only one row, missing some optional fields and database
        csv_content = (
            "table_name,source_schema,file_format,location,description\n"
            "customers,raw_data,CSV,s3://bucket/raw/customers/,Customer data from CRM\n"
        )
        file_path = tmp_path / "source.csv"
        file_path.write_text(csv_content)
        generator.generate_external_tables(str(file_path))
        # Should be created in models/default/raw_data/sources.yml
        sources_path = tmp_path / "models" / "default" / "raw_data" / "sources.yml"
        assert sources_path.exists(), "sources.yml should be created even with incomplete data"
        # No assertion on content, as source entry is always created
    
    @patch("src.dbt_automation.dbt_project_generator.LLMClient")
    def test_generate_models_from_mapping_no_openai(self, mock_llm, tmp_path, sample_mapping_yaml):
        """Test complex models generation when OpenAI is not available"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)
        generator.llm_client = mock_llm()
        generator.llm_client.is_available.return_value = False
        # Patch config to be a real dict, not a MagicMock
        generator.llm_client.config = {
            "api_url": "http://fake-url",
            "api_key": "fake-key",
            "model": "fake-model",
            "temperature": 0.2,
            "top_p": 1.0,
            "max_tokens": 100
        }
        # Patch generate_response to return a valid string content
        generator.llm_client.generate_response.return_value = {"choices": [{"message": {"content": "-- No code generated"}}]}
        result = generator.generate_models_from_mapping(sample_mapping_yaml)
        assert result is None or result == False
    
    @patch("src.dbt_automation.dbt_project_generator.LLMClient")
    def test_generate_models_from_mapping_with_openai(self, mock_llm, tmp_path, sample_mapping_yaml):
        """Test complex models generation when OpenAI is available"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)
        generator.llm_client = mock_llm()
        generator.llm_client.is_available.return_value = True
        generator.llm_client.generate_response.return_value = {"choices": [{"message": {"content": "SELECT 1"}}]}
        generator.llm_client.config = {
            "api_url": "http://fake-url",
            "api_key": "fake-key",
            "model": "fake-model",
            "temperature": 0.2,
            "top_p": 1.0,
            "max_tokens": 100
        }
        try:
            generator.generate_models_from_mapping(sample_mapping_yaml)
        except Exception:
            pytest.fail("generate_models_from_mapping raised unexpectedly!")
    
    def test_generate_schema_files(self, generator, sample_mapping_yaml):
        """Test schema files generation"""
        generator.create_project_structure()
        
        # Mock schema reader and generator
        generator.schema_reader.read_schema_definitions = Mock()
        generator.schema_reader.read_table_mappings = Mock()
        generator.schema_reader.table_mappings = {
            'models': [
                {
                    'name': 'dim_customers',
                    'type': 'marts',
                    'mart_type': 'dimensions'
                }
            ]
        }
        generator.schema_reader.validate_schema = Mock(return_value=[])
        generator.schema_generator.generate_schema_yml = Mock(return_value="version: 2\nmodels:\n- name: test_model\n")
        
        generator.generate_schema_files(sample_mapping_yaml)
        
        # Check if schema files were created
        dimensions_schema = generator.project_root / "models" / "marts" / "dimensions" / "schema.yml"
        assert dimensions_schema.exists(), "Dimensions schema file should be created"
        
        # Check content was written
        with open(dimensions_schema, 'r') as f:
            content = f.read()
        assert "version: 2" in content
        assert "models:" in content
    
    def test_generate_schema_files_with_validation_issues(self, generator, sample_mapping_yaml):
        """Test schema files generation with validation issues"""
        generator.create_project_structure()
        
        # Mock schema reader with validation issues
        generator.schema_reader.read_schema_definitions = Mock()
        generator.schema_reader.read_table_mappings = Mock()
        generator.schema_reader.table_mappings = {
            'models': [
                {
                    'name': 'dim_customers',
                    'type': 'marts',
                    'mart_type': 'dimensions'
                }
            ]
        }
        generator.schema_reader.validate_schema = Mock(return_value=["Issue 1", "Issue 2"])
        generator.schema_generator.generate_schema_yml = Mock(return_value="version: 2\nmodels:\n- name: test_model\n")
        
        generator.generate_schema_files(sample_mapping_yaml)
        
        # Should still create schema files even with validation issues
        dimensions_schema = generator.project_root / "models" / "marts" / "dimensions" / "schema.yml"
        assert dimensions_schema.exists(), "Schema file should be created even with validation issues"
        
        # Check content was written
        with open(dimensions_schema, 'r') as f:
            content = f.read()
        assert "version: 2" in content
        assert "models:" in content
    
    @patch("src.dbt_automation.dbt_project_generator.LLMClient")
    def test_run_full_generation(self, mock_llm, tmp_path, sample_source_csv, sample_mapping_yaml):
        """Test full generation process"""
        generator = DBTProjectGenerator()
        generator.config.PROJECT_ROOT = str(tmp_path)
        generator.project_root = Path(generator.config.PROJECT_ROOT)
        generator.llm_client = mock_llm()
        generator.llm_client.is_available.return_value = False
        generator.llm_client.config = {
            "api_url": "http://fake-url",
            "api_key": "fake-key",
            "model": "fake-model",
            "temperature": 0.2,
            "top_p": 1.0,
            "max_tokens": 100
        }
        # Patch generate_response to return a valid string content
        generator.llm_client.generate_response.return_value = {"choices": [{"message": {"content": "-- No code generated"}}]}
        try:
            generator.run_full_generation(sample_source_csv, sample_mapping_yaml)
        except Exception:
            pytest.fail("run_full_generation raised unexpectedly!")
    
    # The following tests are for removed methods and are now skipped
    @pytest.mark.skip(reason="generate_staging_models is removed")
    def test_generate_staging_models_no_openai(self):
        pass

    @pytest.mark.skip(reason="generate_staging_models is removed")
    def test_generate_staging_models_with_openai(self):
        pass

    @pytest.mark.skip(reason="_generate_sources_yml is removed")
    def test_generate_sources_yml_content(self):
        pass

    @pytest.mark.skip(reason="_generate_sources_yml is removed")
    def test_generate_sources_yml_no_partitions_clustering(self):
        pass 