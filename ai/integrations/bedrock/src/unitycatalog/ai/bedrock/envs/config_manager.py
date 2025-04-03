import json
import logging
import os

logger = logging.getLogger(__name__)


def get_config_file_path():
    """Get the path to the configuration file."""
    # Find the directory of the current module
    base_dir = os.getcwd()
    config_path = os.path.join(base_dir, "config.json")
    return config_path


def load_config():
    """
    Load the configuration from the config file.

    Returns:
        dict: The configuration as a dictionary, or an empty dict if the file doesn't exist
    """
    config_path = get_config_file_path()

    if not os.path.exists(config_path):
        logger.info(f"Configuration file not found at {config_path}")
        return {}

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            logger.info(f"Loaded configuration from {config_path}")
            return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return {}


def save_config(config):
    """
    Save the configuration to the config file.

    Args:
        config (dict): The configuration to save

    Returns:
        bool: True if the save was successful, False otherwise
    """
    config_path = get_config_file_path()

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(config_path), exist_ok=True)

        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
            logger.info(f"Saved configuration to {config_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration: {e}")
        return False
