"""
Simple configuration loader for league settings
"""
import yaml
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


def load_league_config(config_path: str = "config/league_config.yaml") -> Dict[str, Any]:
    """
    Load league configuration from YAML file
    
    Args:
        config_path: Path to configuration file (relative to project root)
        
    Returns:
        Dictionary with complete configuration
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If YAML parsing fails
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Please create config/league_config.yaml with league settings"
        )
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"✓ Configuration loaded from {config_path}")
        return config
        
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML configuration: {e}")
        raise


def get_active_config() -> Dict[str, Any]:
    """
    Get active league, season, and ETL configuration
    
    Returns:
        Dictionary with:
        - league_id: int
        - league_name: str
        - season_id: int
        - season_name: str
        - max_pages: int
        - batch_size: int
        - start_date: str | None
        - end_date: str | None
        - last_extraction_date: str | None
    """
    config = load_league_config()
    
    active_config = {
        # League info
        'league_id': config['active_league']['league_id'],
        'league_name': config['active_league']['league_name'],
        'country': config['active_league']['country'],
        'country_id': config['active_league']['country_id'],
        
        # Season info
        'season_id': config['active_season']['season_id'],
        'season_name': config['active_season']['name'],
        'season_year': config['active_season']['year'],
        
        # ETL parameters
        'max_pages': config['etl'].get('max_pages', 25),
        'batch_size': config['etl'].get('batch_size', 50),
        'start_date': config['etl'].get('start_date'),
        'end_date': config['etl'].get('end_date'),
        'last_extraction_date': config['etl'].get('last_extraction_date'),
    }
    
    return active_config