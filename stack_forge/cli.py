import typer
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = typer.Typer()


@app.command()
def up():
    logger.info("Stack Forge is up and running!")


@app.command()
def down():
    logger.info("Stack Forge is shutting down...")


@app.command()
def init():
    logger.info("Initializing Stack Forge...")

    config_file = Path("stack_forge.yaml")
    if config_file.exists():
        logger.warning("'stack_forge.yaml' already exists.")
    else:
        DEFAULT_YAML_CONTENT = """# Default Stack Forge Configuration
        
version: "1.0"

services:
  postgres: true
  airbyte: true
  dbt: true
  dagster: false
  kafka: false
  spark: false
  trino: false
    
service_config:
  postgres:
    db_name: "mds_local_db"
    user: "admin"
    password: "password"
    port: 5432
  airbyte:
    port: 8000
    
dbt:
  project_dir: "./dbt_project"
        """
        config_file.write_text(DEFAULT_YAML_CONTENT.strip())
        logger.info("Created default 'stack_forge.yaml'.")

    logger.info("Stack Forge initialized.")
