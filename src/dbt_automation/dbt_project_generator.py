import os
import yaml
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Any
from .config import Config
from .llm_client import LLMClient
from .schema_generator import SchemaGenerator
from .schema_reader import SchemaReader
from .sqlfluff_formatter import SQLFluffFormatter
import subprocess
import json

logger = logging.getLogger(__name__)

class IndentDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentDumper, self).increase_indent(flow, False)

class DBTProjectGenerator:
    """Main class for generating dbt projects"""
    
    def __init__(self):
        self.config = Config()
        self.llm_client = LLMClient()
        self.schema_generator = SchemaGenerator()
        self.schema_reader = SchemaReader()
        self.sqlfluff_formatter = SQLFluffFormatter()
        self.project_root = Path(self.config.PROJECT_ROOT)
        
    def create_project_structure(self):
        """Create the basic dbt project structure"""
        logger.info("Creating dbt project structure...")
        
        # Create only essential directories - let user specify subdirectories in YAML
        directories = [
            "models",
            "macros",
            "tests",
            "docs",
            "logs",
            "target"
        ]
        
        for directory in directories:
            (self.project_root / directory).mkdir(parents=True, exist_ok=True)
        
        # Create macros
        # self._create_macros() # Removed as per edit hint
        
        logger.info("Project structure created successfully")
    

    

    
    # def _create_macros(self): # Removed as per edit hint
    #     """Create essential dbt macros""" # Removed as per edit hint
    #     macros = { # Removed as per edit hint
    #         "generate_schema_name.sql": """ # Removed as per edit hint
    # {% macro generate_schema_name(custom_schema_name, node) -%} # Removed as per edit hint
    #     {%- set default_schema = target.schema -%} # Removed as per edit hint
    #     {%- if custom_schema_name is none -%} # Removed as per edit hint
    #         {{ default_schema }} # Removed as per edit hint
    #     {%- else -%} # Removed as per edit hint
    #         {{ default_schema }}_{{ custom_schema_name | trim }} # Removed as per edit hint
    #     {%- endif -%} # Removed as per edit hint
    # {%- endmacro %} # Removed as per edit hint
    # """, # Removed as per edit hint
    #         "generate_alias_name.sql": """ # Removed as per edit hint
    # {% macro generate_alias_name(custom_alias_name=none, node=none) -%} # Removed as per edit hint
    #     {%- if custom_alias_name is none -%} # Removed as per edit hint
    #         {{ node.name }} # Removed as per edit hint
    #     {%- else -%} # Removed as per edit hint
    #         {{ custom_alias_name | trim }} # Removed as per edit hint
    #     {%- endif -%} # Removed as per edit hint
    # {%- endmacro %} # Removed as per edit hint
    # """ # Removed as per edit hint
    #     } # Removed as per edit hint
        
    #     for filename, content in macros.items(): # Removed as per edit hint
    #         with open(self.project_root / "macros" / filename, 'w') as f: # Removed as per edit hint
    #             f.write(content.strip()) # Removed as per edit hint
    
    def generate_external_tables(self, csv_path=None):
        """Generate sources.yml for external tables, with flexible structure and optional fields."""
        csv_path = csv_path or self.config.SOURCE_CSV_PATH
        df = pd.read_csv(csv_path)
        # Load schema definitions for fallback columns
        schema_def_path = self.config.SCHEMA_DEFINITIONS_PATH
        schema_df = pd.read_csv(schema_def_path) if os.path.exists(schema_def_path) else None
        for _, row in df.iterrows():
            database = str(row.get('source_database', 'default')).strip()
            schema = str(row.get('source_schema', 'public')).strip()
            table_name = str(row['table_name']).strip()
            description = str(row.get('description', '')).strip()
            location = str(row.get('location', '')).strip()
            file_format = str(row.get('file_format', '')).strip()
            # Optional fields
            partition_by = str(row.get('partition_by', '')).strip()
            cluster_by = str(row.get('cluster_by', '')).strip()
            refresh_frequency = str(row.get('refresh_frequency', '')).strip()

            # Build table dict
            table_dict = {
                'name': table_name,
                'description': description,
                'external': {
                    'location': location,
                    'file_format': file_format
                }
            }
            if partition_by:
                table_dict['external']['partitions'] = [
                    {'name': partition_by, 'data_type': 'date'}
                ]
            if cluster_by:
                table_dict['external']['cluster_by'] = [cluster_by]
            if refresh_frequency:
                table_dict['external']['refresh_frequency'] = refresh_frequency

            # Columns: prefer mapping YAML, else fallback to schema_definitions.csv
            columns = []
            mapping_path = self.config.MAPPING_YAML_PATH
            found_in_mapping = False
            if os.path.exists(mapping_path):
                with open(mapping_path, 'r') as f:
                    mapping = yaml.safe_load(f)
                for stg in mapping.get('staging_models', []):
                    if stg.get('name') == f'stg_{table_name}' or stg.get('source_table') == table_name:
                        for col in stg.get('columns', []):
                            col_dict = {
                                'name': col['name'],
                                'data_type': col.get('data_type', ''),
                                'description': col.get('description', '')
                            }
                            if 'quote' in col:
                                col_dict['quote'] = col['quote']
                            if 'alias' in col:
                                col_dict['alias'] = col['alias']
                            if 'expression' in col:
                                col_dict['expression'] = col['expression']
                            columns.append(col_dict)
                        found_in_mapping = True
                        break
            # Fallback to schema_definitions.csv if no columns found in mapping
            if not columns and schema_df is not None:
                filtered = schema_df[(schema_df['schema_name'] == schema) & (schema_df['table_name'] == table_name)]
                for _, scol in filtered.iterrows():
                    col_dict = {
                        'name': scol['column_name'],
                        'data_type': scol['data_type']
                    }
                    # Only add description if present and not null/empty
                    desc = scol.get('description', None)
                    if pd.notnull(desc) and str(desc).strip():
                        col_dict['description'] = str(desc).strip()
                    # Only add expression if present and not null/empty
                    expr = scol.get('expression', None)
                    if pd.notnull(expr) and str(expr).strip():
                        col_dict['expression'] = str(expr).strip()
                    # Add dbt tests
                    tests = []
                    is_pk = scol.get('is_primary_key', False)
                    is_nullable = scol.get('is_nullable', True)
                    # Convert to bool if string
                    if isinstance(is_pk, str):
                        is_pk = is_pk.lower() == 'true'
                    if isinstance(is_nullable, str):
                        is_nullable = is_nullable.lower() == 'true'
                    if is_pk:
                        tests = ['unique', 'not_null']
                    elif not is_nullable:
                        tests = ['not_null']
                    if tests:
                        col_dict['tests'] = tests
                    columns.append(col_dict)
            if columns:
                table_dict['columns'] = columns

            # Place sources.yml in dbt_project/models/{database}/{schema}/sources.yml
            target_dir = Path(self.config.PROJECT_ROOT) / 'models' / database / schema
            target_dir.mkdir(parents=True, exist_ok=True)
            sources_yml_path = target_dir / 'sources.yml'

            # If file exists, load and append; else, create new
            if sources_yml_path.exists():
                with open(sources_yml_path, 'r') as f:
                    sources_yml = yaml.safe_load(f) or {}
            else:
                sources_yml = {'version': 2, 'sources': []}

            # Find or create source entry for this schema
            source_entry = None
            for src in sources_yml['sources']:
                if src.get('name') == schema:
                    source_entry = src
                    break
            if not source_entry:
                source_entry = {'name': schema, 'description': f'External tables in {schema} schema', 'tables': []}
                sources_yml['sources'].append(source_entry)

            # Add or update table
            # Remove existing table with same name
            source_entry['tables'] = [t for t in source_entry['tables'] if t.get('name') != table_name]
            source_entry['tables'].append(table_dict)

            # Write back
            with open(sources_yml_path, 'w', encoding='utf-8') as f:
                yaml.dump(
                    sources_yml,
                    f,
                    Dumper=IndentDumper,
                    sort_keys=False,
                    default_flow_style=False,
                    allow_unicode=True,
                    indent=2,
                    explicit_start=True
            )
            
            # Lint the YAML file after writing
            try:
                result = subprocess.run(['yamllint', str(sources_yml_path)], capture_output=True, text=True)
                if result.returncode != 0:
                    print(f"YAML linting issues in {sources_yml_path}:")
                    print(result.stdout)
                else:
                    print(f"YAML lint passed for {sources_yml_path}")
            except Exception as e:
                print(f"YAML linting could not be performed: {e}")

        print('✅ External tables generated successfully!')
    
    # def _generate_sources_yml(self, sources: Dict) -> str: # Removed as per edit hint
    #     """Generate sources.yml content""" # Removed as per edit hint
    #     content = "version: 2\n\nsources:\n" # Removed as per edit hint
        
    #     for schema_name, schema_data in sources.items(): # Removed as per edit hint
    #         content += f"\n  - name: {schema_name}\n" # Removed as per edit hint
    #         content += f"    description: {schema_data['description']}\n" # Removed as per edit hint
    #         content += "    tables:\n" # Removed as per edit hint
            
    #         for table in schema_data['tables']: # Removed as per edit hint
    #             content += f"\n      - name: {table['name']}\n" # Removed as per edit hint
    #             content += f"        description: {table['description']}\n" # Removed as per edit hint
    #             content += "        external:\n" # Removed as per edit hint
    #             content += f"          location: {table['external']['location']}\n" # Removed as per edit hint
    #             content += f"          file_format: {table['external']['file_format']}\n" # Removed as per edit hint
                
    #             # Add partitions if present # Removed as per edit hint
    #             if 'partitions' in table['external']: # Removed as per edit hint
    #                 content += "          partitions:\n" # Removed as per edit hint
    #                 for partition in table['external']['partitions']: # Removed as per edit hint
    #                     content += f"            - name: {partition['name']}\n" # Removed as per edit hint
    #                     content += f"              data_type: {partition['data_type']}\n" # Removed as per edit hint
                
    #             # Add clustering if present # Removed as per edit hint
    #             if 'cluster_by' in table['external']: # Removed as per edit hint
    #                 content += f"          cluster_by: {table['external']['cluster_by']}\n" # Removed as per edit hint
        
    #     return content # Removed as per edit hint
    
    def generate_models_from_mapping(self, mapping_yaml_path: str = None):
        """Generate all models (staging and complex) from mapping YAML, and log LLM API requests."""
        mapping_yaml_path = mapping_yaml_path or self.config.MAPPING_YAML_PATH
        with open(mapping_yaml_path, 'r') as f:
            mappings = yaml.safe_load(f)
        models = mappings.get('staging_models', []) + mappings.get('models', [])
        for mapping in models:
            model_name = mapping['name']
            model_type = mapping.get('type', 'staging')
            # All marts models go directly under models/marts/
            if model_type == "marts":
                model_dir = self.project_root / "models" / "marts"
            else:
                model_dir = self.project_root / "models" / model_type
            model_dir.mkdir(parents=True, exist_ok=True)
            model_path = model_dir / f"{model_name}.sql"
            # Prepare LLM API request
            prompt = f"Generate dbt model for {model_name} with mapping: {json.dumps(mapping)}"
            llm_config = self.llm_client.config
            url = llm_config["api_url"]
            api_key = llm_config["api_key"]
            headers = [
                "Content-Type: application/json",
                f"Authorization: Bearer {api_key}"
            ]
            payload = {
                "model": llm_config["model"],
                "messages": [{"role": "user", "content": prompt}],
                "temperature": llm_config.get("temperature", 0.2),
                "top_p": llm_config.get("top_p", 1.0),
                "max_tokens": llm_config.get("max_tokens", 100)
            }
            # Log the request
            os.makedirs(self.project_root / "logs", exist_ok=True)
            log_path = self.project_root / "logs" / f"model_generation_{model_name}.log"
            with open(log_path, "w", encoding="utf-8") as logf:
                logf.write(f"URL: {url}\n")
                logf.write(f"Headers: {json.dumps(headers)}\n")
                logf.write(f"Payload: {json.dumps(payload, indent=2)}\n")
            # Make the LLM call for model code
            response = self.llm_client.generate_response(prompt)
            sql_code = response.get("choices", [{}])[0].get("message", {}).get("content", "-- No code generated")
            # Make the LLM call for tester prompt
            tester_prompt = f"Suggest checks a developer should do before deploying the dbt model {model_name}. Return as a checklist."
            tester_response = self.llm_client.generate_response(tester_prompt)
            tester_comment = tester_response.get("choices", [{}])[0].get("message", {}).get("content", "No tester suggestions.")
            # Comment out the entire file
            commented_sql = '\n'.join([f'-- {line}' for line in tester_comment.splitlines()]) + '\n' + '\n'.join([f'-- {line}' for line in sql_code.splitlines()])
            with open(model_path, "w", encoding="utf-8") as f:
                f.write(commented_sql)
            # Optionally format with sqlfluff
            self.sqlfluff_formatter.format_sql(str(model_path))
    
    def generate_schema_files(self, mapping_yaml_path: str, schema_definitions_path: str = None):
        """Generate schema.yml files from mapping YAML and schema definitions"""
        logger.info("Generating schema files from mapping YAML and schema definitions...")
        
        # Read schema definitions if provided
        if schema_definitions_path and os.path.exists(schema_definitions_path):
            self.schema_reader.read_schema_definitions(schema_definitions_path)
            logger.info(f"Loaded schema definitions from {schema_definitions_path}")
        else:
            logger.info("Proceeding with YAML-based schema generation only")
        
        # Read table mappings
        self.schema_reader.read_table_mappings(mapping_yaml_path)
        
        # Group models by their directory
        model_groups = {}
        
        for mapping in self.schema_reader.table_mappings.get('models', []):
            model_name = mapping.get('name')
            model_type = mapping.get('type', 'marts')
            mart_type = mapping.get('mart_type', 'dimensions')
            
            if model_type == "marts":
                group_key = f"models/{model_type}/{mart_type}"
            else:
                group_key = f"models/{model_type}"
            
            if group_key not in model_groups:
                model_groups[group_key] = []
            
            model_groups[group_key].append(mapping)
        
        # Generate schema files for each group
        for group_path, group_mappings in model_groups.items():
            schema_content = self.schema_generator.generate_schema_yml(group_mappings)
            
            schema_path = self.project_root / group_path / "schema.yml"
            schema_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(schema_path, 'w') as f:
                f.write(schema_content)
            
            logger.info(f"Generated schema file: {schema_path}")
        
        # Validate schema definitions
        validation_issues = self.schema_reader.validate_schema()
        if validation_issues:
            logger.warning("Schema validation issues found:")
            for issue in validation_issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("Schema validation passed")
    
    def generate_unit_tests(self, mapping_yaml_path: str):
        """Generate unit tests for models"""
        logger.info("Generating unit tests for models...")
        
        with open(mapping_yaml_path, 'r') as f:
            mappings = yaml.safe_load(f)
        
        for mapping in mappings.get('models', []):
            model_name = mapping.get('name')
            expected_behavior = mapping.get('expected_behavior', '')
            
            if not model_name:
                continue
            
            # Read the generated model code
            model_type = mapping.get('type', 'marts')
            mart_type = mapping.get('mart_type', 'dimensions')
            
            if model_type == "marts":
                model_path = self.project_root / "models" / model_type / mart_type / f"{model_name}.sql"
            else:
                model_path = self.project_root / "models" / model_type / f"{model_name}.sql"
            
            if not model_path.exists():
                logger.warning(f"Model file not found for testing: {model_path}")
                continue
            
            with open(model_path, 'r') as f:
                model_code = f.read()
            
            try:
                # Generate unit tests using LLM
                test_code = self.llm_client.generate_unit_tests(
                    model_name, model_code, expected_behavior
                )
                
                # Write test file
                test_path = self.project_root / "tests" / f"test_{model_name}.sql"
                with open(test_path, 'w') as f:
                    f.write(test_code)
                
                logger.info(f"Generated unit tests: test_{model_name}.sql")
                
            except Exception as e:
                logger.error(f"Error generating tests for {model_name}: {e}")
    
    def run_full_generation(self, csv_path: str = None, mapping_yaml_path: str = None):
        """Run the complete dbt project generation process"""
        logger.info("Starting full dbt project generation...")
        
        # Validate configuration
        self.config.validate_config()
        
        # Create project structure
        self.create_project_structure()
        
        # Generate external tables from CSV
        if csv_path or self.config.SOURCE_CSV_PATH:
            csv_path = csv_path or self.config.SOURCE_CSV_PATH
            self.generate_external_tables(csv_path)
        
        # Generate staging models
        if mapping_yaml_path or self.config.MAPPING_YAML_PATH:
            mapping_yaml_path = mapping_yaml_path or self.config.MAPPING_YAML_PATH
            self.generate_models_from_mapping(mapping_yaml_path)
            self.generate_schema_files(mapping_yaml_path, self.config.SCHEMA_DEFINITIONS_PATH)
            self.generate_unit_tests(mapping_yaml_path)
        
        logger.info("dbt project generation completed successfully!") 