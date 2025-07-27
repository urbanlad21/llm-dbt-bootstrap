# DBT Automation Tool - Folder Structure Guide

This document explains the organized folder structure for the DBT automation tool, making it easy for developers to modify configurations without touching Python code.

## 📁 Folder Structure

```
cursor_trail/
├── config/                          # Configuration files (CSV, YAML)
│   ├── schema_definitions.csv       # Comprehensive table schema definitions
│   ├── source_tables.csv            # External table definitions
│   └── table_mappings.yaml          # Model mappings and transformations
├── prompts/                         # Customizable prompt templates
│   ├── model_generation.txt         # Prompt for model generation
│   └── code_review.txt              # Prompt for code review
├── src/                             # Python source code
│   └── dbt_automation/              # Main package
│       ├── __init__.py
│       ├── config.py                # Configuration management
│       ├── llm_client.py            # LLM API integration (use this, not openai_client.py)
│       ├── dbt_project_generator.py # Main project generator
│       ├── schema_generator.py      # Schema file generation
│       ├── schema_reader.py         # Schema definitions parser
│       └── sqlfluff_formatter.py    # SQL formatting and linting
├── main.py                          # CLI interface
├── requirements.txt                 # Python dependencies
├── README.md                        # Main documentation
├── USAGE_GUIDE.md                   # Usage instructions
└── .gitignore                       # Git ignore rules
```

## 🔧 Configuration Files

### 1. `config/schema_definitions.csv`
- Table schema definitions (see code for current columns)

### 2. `config/source_tables.csv`
- External table definitions for dbt-external-tables

### 3. `config/table_mappings.yaml`
- Model mappings and transformations for staging and complex models

## 📝 Prompt Templates

### 1. `prompts/model_generation.txt`
- Template for generating dbt models using LLM

### 2. `prompts/code_review.txt`
- Template for reviewing generated code using LLM

## 🗂️ Model Output Structure

- Models are generated in:
  ```
  dbt_project/models/{type}/{model_name}.sql
  ```
  - For `type: marts`, models go directly under `models/marts/` (no mart_type subfolder)
  - For custom types (e.g., `vinay`), models go under `models/vinay/`

- **After model generation, a tester prompt is called and its response is commented at the top of each .sql file. The entire file is commented out for review.**

## 📝 LLM API Call Logs

- LLM API call logs are written to:
  ```
  dbt_project/logs/model_generation_{model_name}.log
  ```

## 🚀 Usage Examples

### 1. Generate External Tables (sources.yml)
```bash
python main.py external-tables -c config/source_tables.csv
```

### 2. Generate All Models (with tester prompt, commented out)
```bash
python main.py generate -c config/source_tables.csv -m config/table_mappings.yaml
```

### 3. Check the Output
```bash
# Check sources.yml
cat dbt_project/models/analytics/raw_data/sources.yml

# Check generated model SQL files (all commented out)
cat dbt_project/models/marts/fact_orders.sql
cat dbt_project/models/vinay/fact_order_items.sql

# Check LLM API call logs
cat dbt_project/logs/model_generation_*.log
```

### 4. (Optional) Lint YAML Files
```bash
yamllint dbt_project/models/analytics/raw_data/sources.yml
```

## 📋 Best Practices
- Keep configuration files in `config/`
- Store prompts in `prompts/`
- Use descriptive file names
- Maintain version control
- Document changes

## 🔧 Configuration Management

### Environment Variables
```bash
# Set custom paths
export SOURCE_CSV_PATH="./config/source_tables.csv"
export SCHEMA_DEFINITIONS_PATH="./config/schema_definitions.csv"
export MAPPING_YAML_PATH="./config/table_mappings.yaml"
export PROMPTS_PATH="./prompts"

# Set LLM configuration
export OPENAI_API_KEY="your-api-key"
export OPENAI_MODEL="gpt-4"
```

This organized structure makes it easy for developers to:
- Modify table definitions without touching code
- Customize prompts for different use cases
- Maintain clear separation of concerns
- Version control configuration changes
- Collaborate on schema design
- Iterate quickly on model generation 