from pathlib import Path
from mkdocs.plugins import BasePlugin


class MaterialPlausiblePlugin(BasePlugin):
    def on_config(self, config):
        config.theme.dirs.insert(0, Path(__file__).parent / "templates")
