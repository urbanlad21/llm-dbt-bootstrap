#!/usr/bin/env python3
"""
DBT Project Automation Tool
Automates the generation of dbt projects using OpenAI API and mapping documents.
"""

import click
import logging
import os
import sys
import signal
import threading
from pathlib import Path
from src.dbt_automation.dbt_project_generator import DBTProjectGenerator
from src.dbt_automation.config import Config
from src.dbt_automation.sqlfluff_formatter import SQLFluffFormatter

def timeout_handler(signum, frame):
    """Handle timeout signal"""
    raise TimeoutError("Operation timed out after 50 seconds")

def run_with_timeout(func, timeout=50):
    """Run a function with timeout"""
    result = [None]
    exception = [None]
    
    def target():
        try:
            result[0] = func()
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    
    if thread.is_alive():
        raise TimeoutError(f"Operation timed out after {timeout} seconds")
    
    if exception[0]:
        raise exception[0]
    
    return result[0]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    """DBT Project Automation Tool"""
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate configuration
    try:
        Config.validate_config()
    except ValueError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--csv-path', '-c', help='Path to source tables CSV file')
@click.option('--mapping-path', '-m', help='Path to mapping YAML file')
@click.option('--project-root', '-p', help='Project root directory')
def generate(csv_path, mapping_path, project_root):
    """Generate a complete dbt project"""
    try:
        # Update config if provided
        if project_root:
            Config.PROJECT_ROOT = project_root
        if csv_path:
            Config.SOURCE_CSV_PATH = csv_path
        if mapping_path:
            Config.MAPPING_YAML_PATH = mapping_path
        
        def run_generation():
            # Initialize generator
            generator = DBTProjectGenerator()
            
            # Run full generation
            generator.run_full_generation()
        
        run_with_timeout(run_generation, timeout=50)
        
        click.echo("✅ dbt project generated successfully!")
        click.echo(f"Project location: {Config.PROJECT_ROOT}")
        
    except TimeoutError:
        click.echo("❌ Operation timed out after 50 seconds", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error generating project: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--csv-path', '-c', required=True, help='Path to source tables CSV file')
@click.option('--project-root', '-p', help='Project root directory')
def external_tables(csv_path, project_root):
    """Generate external table models from CSV"""
    try:
        if project_root:
            Config.PROJECT_ROOT = project_root
        
        generator = DBTProjectGenerator()
        generator.create_project_structure()
        generator.generate_external_tables(csv_path)
        
        click.echo("✅ External tables generated successfully!")
        
    except Exception as e:
        click.echo(f"❌ Error generating external tables: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--mapping-path', '-m', required=True, help='Path to mapping YAML file')
@click.option('--project-root', '-p', help='Project root directory')
def staging_models(mapping_path, project_root):
    """Generate staging models from mapping YAML"""
    try:
        if project_root:
            Config.PROJECT_ROOT = project_root
        
        def generate_staging():
            generator = DBTProjectGenerator()
            generator.create_project_structure()
            generator.generate_staging_models(mapping_path)
        
        run_with_timeout(generate_staging, timeout=50)
        
        click.echo("✅ Staging models generated successfully!")
        
    except TimeoutError:
        click.echo("❌ Operation timed out after 50 seconds", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Error generating staging models: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--mapping-path', '-m', required=True, help='Path to mapping YAML file')
@click.option('--project-root', '-p', help='Project root directory')
def models(mapping_path, project_root):
    """Generate models using OpenAI from mapping YAML"""
    try:
        if project_root:
            Config.PROJECT_ROOT = project_root
        
        generator = DBTProjectGenerator()
        generator.create_project_structure()
        generator.generate_models_from_mapping(mapping_path)
        
        click.echo("✅ Models generated successfully!")
        
    except Exception as e:
        click.echo(f"❌ Error generating models: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--mapping-path', '-m', required=True, help='Path to mapping YAML file')
@click.option('--schema-csv', '-s', help='Path to schema definitions CSV file')
@click.option('--project-root', '-p', help='Project root directory')
def schemas(mapping_path, schema_csv, project_root):
    """Generate schema.yml files from mapping YAML and schema definitions"""
    try:
        if project_root:
            Config.PROJECT_ROOT = project_root
        
        generator = DBTProjectGenerator()
        generator.create_project_structure()
        generator.generate_schema_files(mapping_path, schema_csv or Config.SCHEMA_DEFINITIONS_PATH)
        
        click.echo("✅ Schema files generated successfully!")
        
    except Exception as e:
        click.echo(f"❌ Error generating schema files: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--mapping-path', '-m', required=True, help='Path to mapping YAML file')
@click.option('--project-root', '-p', help='Project root directory')
def tests(mapping_path, project_root):
    """Generate unit tests for models"""
    try:
        if project_root:
            Config.PROJECT_ROOT = project_root
        
        generator = DBTProjectGenerator()
        generator.create_project_structure()
        generator.generate_unit_tests(mapping_path)
        
        click.echo("✅ Unit tests generated successfully!")
        
    except Exception as e:
        click.echo(f"❌ Error generating unit tests: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--sql-file', '-f', required=True, help='Path to SQL file to format')
@click.option('--dialect', '-d', default='snowflake', help='SQL dialect for formatting')
def format_sql(sql_file, dialect):
    """Format SQL file using SQLFluff"""
    try:
        formatter = SQLFluffFormatter(dialect)
        
        # Check if SQLFluff is installed
        if not formatter.check_sqlfluff_installed():
            click.echo("SQLFluff not found. Installing...")
            formatter.install_sqlfluff()
        
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_code = f.read()
        
        # Format SQL
        formatted_code = formatter.format_sql(sql_code)
        
        # Write back to file
        with open(sql_file, 'w') as f:
            f.write(formatted_code)
        
        click.echo(f"✅ SQL file formatted successfully: {sql_file}")
        
    except Exception as e:
        click.echo(f"❌ Error formatting SQL: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--sql-file', '-f', required=True, help='Path to SQL file to lint')
@click.option('--dialect', '-d', default='snowflake', help='SQL dialect for linting')
def lint_sql(sql_file, dialect):
    """Lint SQL file using SQLFluff"""
    try:
        formatter = SQLFluffFormatter(dialect)
        
        # Check if SQLFluff is installed
        if not formatter.check_sqlfluff_installed():
            click.echo("SQLFluff not found. Installing...")
            formatter.install_sqlfluff()
        
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_code = f.read()
        
        # Lint SQL
        lint_results = formatter.lint_sql(sql_code)
        
        if "error" in lint_results:
            click.echo(f"❌ Linting error: {lint_results['error']}", err=True)
            sys.exit(1)
        
        # Display results
        if lint_results:
            click.echo("🔍 SQL Linting Results:")
            for file_result in lint_results:
                if file_result.get('violations'):
                    click.echo(f"File: {file_result['filepath']}")
                    for violation in file_result['violations']:
                        click.echo(f"  Line {violation['line_no']}: {violation['description']}")
                else:
                    click.echo("✅ No violations found!")
        else:
            click.echo("✅ No violations found!")
        
    except Exception as e:
        click.echo(f"❌ Error linting SQL: {e}", err=True)
        sys.exit(1)

@cli.command()
def init():
    """Initialize a new dbt project structure"""
    try:
        generator = DBTProjectGenerator()
        generator.create_project_structure()
        
        click.echo("✅ dbt project structure initialized successfully!")
        click.echo(f"Project location: {Config.PROJECT_ROOT}")
        
    except Exception as e:
        click.echo(f"❌ Error initializing project: {e}", err=True)
        sys.exit(1)

@cli.command()
def validate():
    """Validate configuration and dependencies"""
    try:
        # Validate config
        Config.validate_config()
        click.echo("✅ Configuration is valid")
        
        # Check SQLFluff
        formatter = SQLFluffFormatter()
        if formatter.check_sqlfluff_installed():
            click.echo("✅ SQLFluff is installed")
        else:
            click.echo("⚠️  SQLFluff is not installed")
        
        # Check OpenAI connection
        from src.dbt_automation.openai_client import OpenAIClient
        client = OpenAIClient()
        click.echo("✅ OpenAI client initialized")
        
        click.echo("✅ All validations passed!")
        
    except Exception as e:
        click.echo(f"❌ Validation failed: {e}", err=True)
        sys.exit(1)



if __name__ == '__main__':
    cli() 