import yaml
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SchemaGenerator:
    """Generate schema.yml files with constraints and documentation"""
    
    def __init__(self):
        self.default_tests = {
            "not_null": ["id", "created_at", "updated_at"],
            "unique": ["id"],
            "accepted_values": {},
            "relationships": {}
        }
    
    def generate_schema_yml(self, model_mappings: List[Dict]) -> str:
        """Generate schema.yml content for a group of models"""
        schema_content = {
            "version": 2,
            "models": []
        }
        
        for mapping in model_mappings:
            model_schema = self._generate_model_schema(mapping)
            schema_content["models"].append(model_schema)
        
        return yaml.dump(schema_content, default_flow_style=False, sort_keys=False)
    
    def _generate_model_schema(self, mapping: Dict) -> Dict:
        """Generate schema for a single model"""
        model_name = mapping.get('name', '')
        description = mapping.get('description', f'Model for {model_name}')
        columns = mapping.get('columns', [])
        
        model_schema = {
            "name": model_name,
            "description": description,
            "config": {
                "contract": {
                    "enforced": True
                }
            },
            "columns": []
        }
        
        # Add columns without tests
        for column in columns:
            column_schema = self._generate_column_schema(column)
            model_schema["columns"].append(column_schema)
        
        return model_schema
    
    def _generate_column_schema(self, column: Dict) -> Dict:
        """Generate schema for a single column"""
        column_name = column.get('name', '')
        column_type = column.get('type', 'string')
        description = column.get('description', f'Column {column_name}')
        transformation = column.get('transformation', '')
        
        column_schema = {
            "name": column_name,
            "description": description
        }
        
        # Add transformation information to description if present
        if transformation:
            column_schema["description"] = f"{description} (Transformation: {transformation})"
        
        return column_schema
        
        # Add not null test for required columns
        if column.get('required', False) or column_name in self.default_tests["not_null"]:
            column_schema["tests"].append({
                "not_null": {
                    "config": {"severity": "error"}
                }
            })
        
        # Add unique test for primary keys
        if column.get('primary_key', False) or column_name in self.default_tests["unique"]:
            column_schema["tests"].append({
                "unique": {
                    "config": {"severity": "error"}
                }
            })
        
        # Add accepted values test
        accepted_values = column.get('accepted_values', [])
        if accepted_values:
            column_schema["tests"].append({
                "accepted_values": {
                    "values": accepted_values,
                    "config": {"severity": "warn"}
                }
            })
        
        # Add relationship test
        relationship = column.get('relationship', {})
        if relationship:
            column_schema["tests"].append({
                "relationships": {
                    "to": relationship.get('to', ''),
                    "field": relationship.get('field', ''),
                    "config": {"severity": "error"}
                }
            })
        
        # Add custom tests
        custom_tests = column.get('custom_tests', [])
        for test in custom_tests:
            column_schema["tests"].append(test)
        
        # Add data quality constraints
        self._add_data_quality_constraints(column_schema, column)
        
        return column_schema
    
    def _add_data_quality_constraints(self, column_schema: Dict, column: Dict):
        """Add data quality constraints to column schema"""
        column_name = column.get('name', '')
        
        # String length constraints
        max_length = column.get('max_length')
        if max_length:
            column_schema["tests"].append({
                "dbt_utils.string_length": {
                    "max_length": max_length,
                    "config": {"severity": "warn"}
                }
            })
        
        # Numeric range constraints
        min_value = column.get('min_value')
        max_value = column.get('max_value')
        if min_value is not None or max_value is not None:
            range_test = {
                "dbt_utils.expression_is_true": {
                    "expression": f"{{{{ ref('{column_name}') }}}}"
                }
            }
            
            if min_value is not None and max_value is not None:
                range_test["dbt_utils.expression_is_true"]["expression"] += f" >= {min_value} and {{{{ ref('{column_name}') }}}} <= {max_value}"
            elif min_value is not None:
                range_test["dbt_utils.expression_is_true"]["expression"] += f" >= {min_value}"
            elif max_value is not None:
                range_test["dbt_utils.expression_is_true"]["expression"] += f" <= {max_value}"
            
            column_schema["tests"].append(range_test)
        
        # Date range constraints
        min_date = column.get('min_date')
        max_date = column.get('max_date')
        if min_date or max_date:
            date_test = {
                "dbt_utils.expression_is_true": {
                    "expression": f"{{{{ ref('{column_name}') }}}}"
                }
            }
            
            if min_date and max_date:
                date_test["dbt_utils.expression_is_true"]["expression"] += f" >= '{min_date}' and {{{{ ref('{column_name}') }}}} <= '{max_date}'"
            elif min_date:
                date_test["dbt_utils.expression_is_true"]["expression"] += f" >= '{min_date}'"
            elif max_date:
                date_test["dbt_utils.expression_is_true"]["expression"] += f" <= '{max_date}'"
            
            column_schema["tests"].append(date_test)
        
        # Pattern matching for strings
        pattern = column.get('pattern')
        if pattern:
            column_schema["tests"].append({
                "dbt_utils.expression_is_true": {
                    "expression": f"{{{{ ref('{column_name}') }}}} ~ '{pattern}'",
                    "config": {"severity": "warn"}
                }
            })
    
    def generate_sources_yml(self, source_mappings: List[Dict]) -> str:
        """Generate sources.yml content"""
        sources_content = {
            "version": 2,
            "sources": []
        }
        
        for source_mapping in source_mappings:
            source_schema = self._generate_source_schema(source_mapping)
            sources_content["sources"].append(source_schema)
        
        return yaml.dump(sources_content, default_flow_style=False, sort_keys=False)
    
    def _generate_source_schema(self, source_mapping: Dict) -> Dict:
        """Generate schema for a source"""
        source_name = source_mapping.get('name', '')
        database = source_mapping.get('database', '')
        schema = source_mapping.get('schema', '')
        tables = source_mapping.get('tables', [])
        
        source_schema = {
            "name": source_name,
            "description": source_mapping.get('description', f'Source {source_name}'),
            "database": database,
            "schema": schema,
            "tables": []
        }
        
        for table in tables:
            table_schema = self._generate_source_table_schema(table)
            source_schema["tables"].append(table_schema)
        
        return source_schema
    
    def _generate_source_table_schema(self, table: Dict) -> Dict:
        """Generate schema for a source table"""
        table_name = table.get('name', '')
        description = table.get('description', f'Table {table_name}')
        columns = table.get('columns', [])
        
        table_schema = {
            "name": table_name,
            "description": description,
            "columns": []
        }
        
        for column in columns:
            column_schema = self._generate_column_schema(column)
            table_schema["columns"].append(column_schema)
        
        return table_schema 