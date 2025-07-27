# DBT Project Boilerplate Generator (LLM-powered)

This tool generates boilerplate code to **kickstart a dbt project** using configuration files and LLM-driven model generation. It is designed for rapid prototyping and scaffolding, not for full production automation.

## Features

- 🚀 **Boilerplate dbt Project Generation**: Quickly scaffold a dbt project structure
- 📊 **External Table Source Generation**: Create `sources.yml` from CSV definitions
- 🤖 **LLM-Powered Model Generation**: Use your LLM API to generate initial model SQL from YAML mappings
- 📝 **Tester Prompt**: After model generation, an LLM prompt suggests checks a developer should do before deploying; the entire model file is commented out for review
- 🗂️ **LLM API Call Logging**: All LLM requests are logged for transparency
- ✨ **YAML and SQL Formatting**: Output is dbt-compatible and linted
- 🏗️ **Best Practices**: Follows dbt conventions for structure and naming

## Installation

```bash
# Clone the repository
# (replace <repository-url> with your repo URL)
git clone <repository-url>
cd dbt-automation-tool

# Create and activate a virtual environment (recommended)
python -m venv .venv
.\.venv\Scripts\activate  # Windows
# or
source .venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# (Optional) Install yamllint for YAML linting
pip install yamllint
```

## Quick Start

1. **Prepare your config files:**
   - `config/source_tables.csv` — external table definitions
   - `config/schema_definitions.csv` — table/column schema
   - `config/table_mappings.yaml` — model mappings
2. **Generate external tables (sources.yml):**
   ```bash
   python main.py external-tables -c config/source_tables.csv
   ```
3. **Generate all models (with tester prompt, commented out):**
   ```bash
   python main.py generate -c config/source_tables.csv -m config/table_mappings.yaml
   ```
4. **Check the output:**
   - `dbt_project/models/{type}/{model_name}.sql` (all commented out, with tester suggestions)
   - `dbt_project/models/analytics/raw_data/sources.yml`
   - `dbt_project/logs/model_generation_{model_name}.log` (LLM API call logs)

## Usage

### Main Commands

```bash
python main.py external-tables -c config/source_tables.csv
python main.py generate -c config/source_tables.csv -m config/table_mappings.yaml
```

### What Happens?
- **sources.yml** is generated for your external tables
- **Model SQL files** are generated using your LLM API, with a tester prompt response at the top (all lines commented out)
- **LLM API call logs** are written to `dbt_project/logs/`

### Example: Commented-out Model File
```sql
-- Checklist before deploying this model:
-- 1. Review join conditions for correctness
-- 2. Validate column data types
-- 3. Ensure incremental logic is correct
-- ...
-- select ... from ...
-- ...
```

## Folder Structure

```
cursor_trail/
├── config/
│   ├── schema_definitions.csv
│   ├── source_tables.csv
│   └── table_mappings.yaml
├── prompts/
│   ├── model_generation.txt
│   └── code_review.txt
├── src/
│   └── dbt_automation/
│       ├── __init__.py
│       ├── config.py
│       ├── llm_client.py
│       ├── dbt_project_generator.py
│       ├── schema_generator.py
│       ├── schema_reader.py
│       └── sqlfluff_formatter.py
├── main.py
├── requirements.txt
├── README.md
├── USAGE_GUIDE.md
└── .gitignore
```

## LLM API Integration
- All LLM calls are made via `src/dbt_automation/llm_client.py` using your custom API endpoint (see `config/llm_config.json`)
- Each model generation and tester prompt call is logged in `dbt_project/logs/`

## Best Practices
- Use this tool to **scaffold** your dbt project and models
- Review and edit the generated, commented-out SQL before deploying
- Use the tester prompt suggestions as a checklist
- Keep your config and mapping files in version control

## Limitations
- This tool is for **boilerplate generation only**; it does not create production-ready dbt code
- All generated models are commented out for safety
- No unit test or code review generation is included

## Contributing
- Fork the repository
- Create a feature branch
- Make your changes
- Add/adjust tests if needed
- Submit a pull request

## License
MIT License

## Support
- Create an issue in the repository for questions or bugs 