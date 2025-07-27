import pandas as pd
import yaml
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SchemaReader:
    """Read and parse schema definitions from CSV files"""
    
    def __init__(self):
        self.schema_definitions = {}
        self.table_mappings = {}
    
    def read_schema_definitions(self, csv_path: str) -> Dict:
        """Read schema definitions from CSV file"""
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Read {len(df)} schema definitions from {csv_path}")
            
            # Group by table
            tables = {}
            for _, row in df.iterrows():
                schema_name = row.get('schema_name', '')
                table_name = row.get('table_name', '')
                column_name = row.get('column_name', '')
                
                if not all([schema_name, table_name, column_name]):
                    logger.warning(f"Skipping incomplete row: {row}")
                    continue
                
                table_key = f"{schema_name}.{table_name}"
                
                if table_key not in tables:
                    tables[table_key] = {
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'columns': []
                    }
                
                # Parse column definition
                column = self._parse_column_definition(row)
                tables[table_key]['columns'].append(column)
            
            self.schema_definitions = tables
            return tables
            
        except Exception as e:
            logger.error(f"Error reading schema definitions: {e}")
            raise
    
    def _parse_column_definition(self, row: pd.Series) -> Dict:
        """Parse a single column definition from CSV row"""
        # Handle boolean values that might be read as actual booleans or strings
        def parse_boolean(value, default=False):
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() == 'true'
            else:
                return default
        
        column = {
            'name': row.get('column_name', ''),
            'type': row.get('data_type', 'string'),
            'description': row.get('description', ''),
            'required': not parse_boolean(row.get('is_nullable', True), True),  # is_nullable=false means required=true
            'primary_key': parse_boolean(row.get('is_primary_key', False), False),
            'unique': parse_boolean(row.get('is_unique', False), False),
            'default_value': row.get('default_value', ''),
            'tests': []
        }
        
        # Add not null test for required columns
        if column['required']:
            column['tests'].append({
                'not_null': {
                    'config': {'severity': 'error'}
                }
            })
        
        # Add unique test for unique columns
        if column['unique']:
            column['tests'].append({
                'unique': {
                    'config': {'severity': 'error'}
                }
            })
        
        # Add primary key test
        if column['primary_key']:
            column['tests'].append({
                'unique': {
                    'config': {'severity': 'error'}
                }
            })
        
        # Add data type constraints
        data_type = row.get('data_type', '')
        if data_type:
            column['tests'].append(self._get_data_type_test(data_type))
        
        return column
    
    def _get_data_type_test(self, data_type: str) -> Dict:
        """Get appropriate data type test based on data type"""
        data_type_lower = data_type.lower()
        
        if 'bigint' in data_type_lower or 'int' in data_type_lower:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': f"{{{{ ref('column_name') }}}} ~ '^[0-9]+$'",
                    'config': {'severity': 'error'}
                }
            }
        elif 'decimal' in data_type_lower or 'numeric' in data_type_lower:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': f"{{{{ ref('column_name') }}}} ~ '^[0-9]+\\.?[0-9]*$'",
                    'config': {'severity': 'error'}
                }
            }
        elif 'date' in data_type_lower:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': f"{{{{ ref('column_name') }}}} ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'",
                    'config': {'severity': 'error'}
                }
            }
        elif 'timestamp' in data_type_lower:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': f"{{{{ ref('column_name') }}}} ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'",
                    'config': {'severity': 'error'}
                }
            }
        elif 'boolean' in data_type_lower:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': f"{{{{ ref('column_name') }}}} in (true, false)",
                    'config': {'severity': 'error'}
                }
            }
        
        return {}
    
    def _get_range_test(self, min_value: str, max_value: str) -> Dict:
        """Get range test based on min and max values"""
        expression_parts = []
        
        if min_value:
            expression_parts.append(f"{{{{ ref('column_name') }}}} >= {min_value}")
        
        if max_value:
            expression_parts.append(f"{{{{ ref('column_name') }}}} <= {max_value}")
        
        if expression_parts:
            return {
                'dbt_utils.expression_is_true': {
                    'expression': ' and '.join(expression_parts),
                    'config': {'severity': 'warn'}
                }
            }
        
        return {}
    
    def read_table_mappings(self, yaml_path: str) -> Dict:
        """Read table mappings from YAML file"""
        try:
            with open(yaml_path, 'r') as f:
                mappings = yaml.safe_load(f)
            
            self.table_mappings = mappings
            return mappings
            
        except Exception as e:
            logger.error(f"Error reading table mappings: {e}")
            raise
    
    def get_model_schema(self, model_name: str) -> Dict:
        """Get schema for a specific model"""
        # First check if it's in the table mappings
        if self.table_mappings and 'models' in self.table_mappings:
            for model in self.table_mappings['models']:
                if model.get('name') == model_name:
                    return model
        
        # Then check schema definitions
        for table_key, table_def in self.schema_definitions.items():
            if table_def['table_name'] == model_name:
                return {
                    'name': model_name,
                    'description': f"Model for {model_name}",
                    'columns': table_def['columns']
                }
        
        return None
    
    def generate_schema_for_model(self, model_name: str, model_type: str = 'marts') -> str:
        """Generate schema.yml content for a specific model"""
        model_schema = self.get_model_schema(model_name)
        
        if not model_schema:
            logger.warning(f"No schema found for model: {model_name}")
            return ""
        
        schema_content = {
            'version': 2,
            'models': [model_schema]
        }
        
        return yaml.dump(schema_content, default_flow_style=False, sort_keys=False)
    
    def get_all_models(self) -> List[str]:
        """Get list of all model names"""
        models = []
        
        # From table mappings
        if self.table_mappings and 'models' in self.table_mappings:
            models.extend([model.get('name') for model in self.table_mappings['models']])
        
        # From schema definitions
        models.extend([table_def['table_name'] for table_def in self.schema_definitions.values()])
        
        return list(set(models))  # Remove duplicates
    
    def validate_schema(self) -> List[str]:
        """Validate schema definitions and return any issues"""
        issues = []
        
        # Check for duplicate column names within tables
        for table_key, table_def in self.schema_definitions.items():
            column_names = [col['name'] for col in table_def['columns']]
            duplicates = [name for name in set(column_names) if column_names.count(name) > 1]
            
            if duplicates:
                issues.append(f"Duplicate column names in {table_key}: {duplicates}")
        
        # Check for missing required fields
        for table_key, table_def in self.schema_definitions.items():
            for col in table_def['columns']:
                if not col.get('name'):
                    issues.append(f"Missing column name in {table_key}")
                if not col.get('type'):
                    issues.append(f"Missing data type for column {col.get('name')} in {table_key}")
        
        return issues 