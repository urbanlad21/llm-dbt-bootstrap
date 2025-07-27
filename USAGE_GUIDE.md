# DBT Automation System - Usage Guide

## Quick Start

### 1. Setup Environment

```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env_example.txt .env
# Edit .env with your OpenAI API key and other settings
```

### 2. Create Sample Files

```bash
python main.py create-samples
```

This creates:
- `mappings/source_tables.csv` - Source table definitions
- `mappings/table_mappings.yaml` - Model mappings and transformations
- `prompts/model_generation.txt` - Custom prompt template

### 3. Generate Complete Project

```bash
python main.py generate
```

## Step-by-Step Workflow

### Step 1: Define Source Tables

Create a CSV file with your source table definitions:

```csv
table_name,source_schema,source_database,file_format,location,description
customers,raw_data,analytics,CSV,s3://bucket/raw/customers/,Customer data from CRM
orders,raw_data,analytics,CSV,s3://bucket/raw/orders/,Order data from e-commerce
products,raw_data,analytics,CSV,s3://bucket/raw/products/,Product catalog data
```

### Step 2: Define Model Mappings

Create a YAML file with your model definitions:

```yaml
# Staging models (simple transformations)
staging_models:
  - name: customers
    source_table: stg_customers
    transformations:
      - column_name: customer_id
        transformation: cast(customer_id as integer)
      - column_name: email
        transformation: lower(email)
      - column_name: created_at
        transformation: cast(created_at as timestamp)

# Complex models (using OpenAI)
models:
  - name: dim_customers
    type: marts
    mart_type: dimensions
    description: Customer dimension table
    source_tables:
      - stg_customers
    business_logic: |
      Create a customer dimension with deduplication and latest record selection
    expected_behavior: |
      Should contain one record per customer with the most recent information
    columns:
      - name: customer_id
        type: integer
        description: Unique customer identifier
        primary_key: true
        required: true
      - name: email
        type: string
        description: Customer email address
        required: true
        max_length: 255
```

### Step 3: Generate Components

#### Generate External Tables
```bash
python main.py external-tables -c mappings/source_tables.csv
```

#### Generate Staging Models
```bash
python main.py staging-models -m mappings/table_mappings.yaml
```

#### Generate Complex Models (requires OpenAI API)
```bash
python main.py models -m mappings/table_mappings.yaml
```

#### Generate Schema Files
```bash
python main.py schemas -m mappings/table_mappings.yaml
```

#### Generate Unit Tests
```bash
python main.py tests -m mappings/table_mappings.yaml
```

### Step 4: Format and Validate

#### Format SQL Files
```bash
python main.py format-sql -f dbt_project/models/staging/stg_customers.sql
```

#### Lint SQL Files
```bash
python main.py lint-sql -f dbt_project/models/staging/stg_customers.sql
```

## Advanced Usage

### Custom Prompts

You can override default prompts by creating custom files in the `prompts/` directory:

#### Custom Model Generation Prompt
Create `prompts/model_generation.txt`:
```
You are an expert dbt developer. Create a production-ready dbt model.

Project Context: {project_context}
Source Tables: {source_tables}
Mapping Details: {mapping_details}

Requirements:
1. Follow dbt best practices and naming conventions
2. Use proper incremental logic where appropriate
3. Include comprehensive documentation
4. Handle edge cases and data quality issues
5. Use appropriate dbt macros and functions
6. Ensure proper error handling
7. Optimize for performance
8. Include proper tests and constraints

Generate clean, maintainable, and production-ready SQL code.
```

#### Custom Code Review Prompt
Create `prompts/code_review.txt`:
```
Review the following dbt model code for:
1. SQL syntax correctness
2. dbt best practices compliance
3. Performance optimization opportunities
4. Code readability and maintainability
5. Proper use of dbt macros and functions
6. Error handling and edge cases

Code to review:
{code}

Provide specific improvements and suggestions.
```

#### Custom Test Generation Prompt
Create `prompts/unit_test_generation.txt`:
```
Generate comprehensive unit tests for the following dbt model:

Model Name: {model_name}
Model Code: {model_code}
Expected Behavior: {expected_behavior}

Requirements:
1. Test all business logic
2. Include edge cases
3. Test data quality constraints
4. Use dbt test framework
5. Include both positive and negative test cases
6. Test data transformations

Generate tests in dbt test format.
```

### Schema Constraints

The system automatically generates schema constraints based on your column definitions:

```yaml
columns:
  - name: customer_id
    type: integer
    description: Primary key
    primary_key: true
    required: true
  - name: email
    type: string
    description: Email address
    required: true
    max_length: 255
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
  - name: age
    type: integer
    description: Customer age
    min_value: 0
    max_value: 120
  - name: status
    type: string
    description: Customer status
    accepted_values: ["active", "inactive", "pending"]
  - name: customer_type_id
    type: integer
    description: Customer type reference
    relationship:
      to: ref('dim_customer_types')
      field: customer_type_id
```

### Generated Schema Example

```yaml
version: 2
models:
  - name: dim_customers
    description: Customer dimension table
    columns:
      - name: customer_id
        description: Unique customer identifier
        tests:
          - dbt_utils.type_bigint:
              config:
                severity: error
          - not_null:
              config:
                severity: error
          - unique:
              config:
                severity: error
      - name: email
        description: Customer email address
        tests:
          - dbt_utils.type_string:
              config:
                severity: error
          - not_null:
              config:
                severity: error
          - dbt_utils.string_length:
              max_length: 255
              config:
                severity: warn
          - dbt_utils.expression_is_true:
              expression: "{{ ref('email') }} ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'"
              config:
                severity: warn
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | Your OpenAI API key | Required |
| `OPENAI_MODEL` | OpenAI model to use | `gpt-4` |
| `OPENAI_MAX_TOKENS` | Maximum tokens for responses | `4000` |
| `OPENAI_TEMPERATURE` | Temperature for generation | `0.1` |
| `PROJECT_NAME` | Name of the dbt project | `dbt_automated_project` |
| `PROJECT_ROOT` | Project root directory | `./dbt_project` |
| `DBT_PROFILE_NAME` | dbt profile name | `default` |
| `DBT_TARGET` | dbt target environment | `dev` |

### Database Configuration

The system generates a `profiles.yml` template. Update it with your database credentials:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_snowflake_account
      user: your_snowflake_user
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: your_snowflake_role
      database: your_snowflake_database
      warehouse: your_snowflake_warehouse
      schema: your_snowflake_schema
```

## Best Practices

### 1. Naming Conventions

- **Models**: Use snake_case
- **Staging**: Prefix with `stg_`
- **Intermediate**: Prefix with `int_`
- **Dimensions**: Prefix with `dim_`
- **Facts**: Prefix with `fact_`

### 2. Model Organization

```
models/
├── staging/          # Raw data transformations
├── intermediate/     # Complex transformations
└── marts/
    ├── dimensions/   # Descriptive attributes
    └── facts/        # Measurable events
```

### 3. Testing Strategy

- **Column-level tests**: Data type, not null, unique, relationships
- **Model-level tests**: Business logic validation
- **Custom tests**: Domain-specific validations

### 4. Documentation

- **Schema files**: Column descriptions and constraints
- **Model documentation**: Business context and logic
- **Test documentation**: Expected behavior and edge cases

## Troubleshooting

### Common Issues

1. **OpenAI API Key Error**
   - Ensure `OPENAI_API_KEY` is set in your `.env` file
   - Verify the API key is valid and has sufficient credits

2. **SQLFluff Not Found**
   - The tool will automatically install SQLFluff if needed
   - Or install manually: `pip install sqlfluff`

3. **File Not Found Errors**
   - Ensure input files exist in the specified paths
   - Use `python main.py create-samples` to generate sample files

4. **Permission Errors**
   - Ensure write permissions for the project directory
   - Check file permissions for input files

### Validation

Run validation to check your setup:
```bash
python main.py validate
```

This will check:
- Configuration validity
- OpenAI API connectivity
- SQLFluff installation
- Required dependencies

## Examples

### Complete Workflow Example

1. **Create sample files**:
```bash
python main.py create-samples
```

2. **Edit the generated files** with your specific requirements

3. **Generate the complete project**:
```bash
python main.py generate
```

4. **Review and customize** the generated models

5. **Run dbt commands**:
```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

### Custom Workflow Example

1. **Initialize project structure**:
```bash
python main.py init
```

2. **Generate external tables**:
```bash
python main.py external-tables -c mappings/source_tables.csv
```

3. **Generate staging models**:
```bash
python main.py staging-models -m mappings/table_mappings.yaml
```

4. **Generate complex models** (with OpenAI):
```bash
python main.py models -m mappings/table_mappings.yaml
```

5. **Generate schema files**:
```bash
python main.py schemas -m mappings/table_mappings.yaml
```

6. **Generate unit tests**:
```bash
python main.py tests -m mappings/table_mappings.yaml
```

## Support

For support and questions:
- Check the troubleshooting section
- Review the examples and documentation
- Create an issue in the repository 