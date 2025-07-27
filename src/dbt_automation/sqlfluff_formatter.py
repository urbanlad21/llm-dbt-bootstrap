import subprocess
import tempfile
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class SQLFluffFormatter:
    """SQLFluff formatter for dbt SQL code"""
    
    def __init__(self, dialect: str = "snowflake"):
        self.dialect = dialect
        self.config = {
            "dialect": dialect,
            "rules": "L001,L002,L003,L004,L005,L006,L007,L008,L009,L010,L011,L012,L013,L014,L015,L016,L017,L018,L019,L020,L021,L022,L023,L024,L025,L026,L027,L028,L029,L030,L031,L032,L033,L034,L035,L036,L037,L038,L039,L040,L041,L042,L043,L044,L045,L046,L047,L048,L049,L050",
            "max_line_length": 88,
            "indent": 4,
            "comma_style": "trailing",
            "operator_new_lines": "before"
        }
    
    def format_sql(self, sql_code: str) -> str:
        """
        Format SQL code using SQLFluff
        
        Args:
            sql_code: Raw SQL code to format
            
        Returns:
            Formatted SQL code
        """
        try:
            # Create temporary file with SQL code
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(sql_code)
                temp_file_path = temp_file.name
            
            # Format using SQLFluff
            formatted_code = self._run_sqlfluff_fix(temp_file_path)
            
            # Clean up temporary file
            os.unlink(temp_file_path)
            
            return formatted_code
            
        except Exception as e:
            logger.warning(f"SQLFluff formatting failed: {e}. Returning original code.")
            return sql_code
    
    def _run_sqlfluff_fix(self, file_path: str) -> str:
        """Run SQLFluff fix command"""
        try:
            # Build SQLFluff command
            cmd = [
                "sqlfluff", "fix",
                "--dialect", self.dialect,
                "--rules", self.config["rules"],
                "--config", f"max_line_length={self.config['max_line_length']}",
                file_path
            ]
            
            # Run SQLFluff
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # Read the formatted file
                with open(file_path, 'r') as f:
                    return f.read()
            else:
                logger.warning(f"SQLFluff fix failed: {result.stderr}")
                # Try to read the original file
                with open(file_path, 'r') as f:
                    return f.read()
                    
        except subprocess.TimeoutExpired:
            logger.warning("SQLFluff formatting timed out")
            with open(file_path, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error running SQLFluff: {e}")
            with open(file_path, 'r') as f:
                return f.read()
    
    def lint_sql(self, sql_code: str) -> dict:
        """
        Lint SQL code using SQLFluff
        
        Args:
            sql_code: SQL code to lint
            
        Returns:
            Dictionary with linting results
        """
        try:
            # Create temporary file with SQL code
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
                temp_file.write(sql_code)
                temp_file_path = temp_file.name
            
            # Lint using SQLFluff
            lint_results = self._run_sqlfluff_lint(temp_file_path)
            
            # Clean up temporary file
            os.unlink(temp_file_path)
            
            return lint_results
            
        except Exception as e:
            logger.error(f"SQLFluff linting failed: {e}")
            return {"error": str(e)}
    
    def _run_sqlfluff_lint(self, file_path: str) -> dict:
        """Run SQLFluff lint command"""
        try:
            cmd = [
                "sqlfluff", "lint",
                "--dialect", self.dialect,
                "--format", "json",
                file_path
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # Parse JSON output
                import json
                return json.loads(result.stdout)
            else:
                return {"error": result.stderr}
                
        except subprocess.TimeoutExpired:
            return {"error": "Linting timed out"}
        except Exception as e:
            return {"error": str(e)}
    
    def check_sqlfluff_installed(self) -> bool:
        """Check if SQLFluff is installed and available"""
        try:
            result = subprocess.run(
                ["sqlfluff", "--version"],
                capture_output=True,
                text=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def install_sqlfluff(self):
        """Install SQLFluff if not available"""
        if not self.check_sqlfluff_installed():
            logger.info("SQLFluff not found. Installing...")
            try:
                subprocess.run(
                    ["pip", "install", "sqlfluff"],
                    check=True,
                    capture_output=True
                )
                logger.info("SQLFluff installed successfully")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install SQLFluff: {e}")
                raise 