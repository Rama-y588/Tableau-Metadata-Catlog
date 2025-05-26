from dataclasses import dataclass
from typing import List, Literal
from datetime import datetime

@dataclass
class FullModeConfig:
    enabled: bool = True
    batch_size: int = 50
    output_path: str = './output/full'
    include_metadata: bool = True

@dataclass
class ProjectModeConfig:
    enabled: bool = True
    project_paths: List[str] = None
    recursive: bool = True
    output_path: str = './output/projects'

    def __post_init__(self):
        if self.project_paths is None:
            self.project_paths = ['Enterprise/Finance', 'Enterprise/Sales']

@dataclass
class WorkbookModeConfig:
    enabled: bool = True
    output_path: str = './output/workbooks'
    include_views: bool = True
    include_data_sources: bool = True

@dataclass
class TableauConfig:
    server_url: str = 'https://your-tableau-server'
    site_id: str = 'your-site-id'
    api_version: str = '3.12'

@dataclass
class ExportConfig:
    formats: List[Literal['json', 'csv', 'excel']] = None
    default_format: Literal['json', 'csv', 'excel'] = 'json'
    timestamp_format: str = '%Y-%m-%d_%H-%M-%S'

    def __post_init__(self):
        if self.formats is None:
            self.formats = ['json', 'csv']

@dataclass
class CatalogConfig:
    modes: dict = None
    tableau: TableauConfig = None
    export: ExportConfig = None

    def __post_init__(self):
        if self.modes is None:
            self.modes = {
                'full': FullModeConfig(),
                'project': ProjectModeConfig(),
                'workbook': WorkbookModeConfig()
            }
        if self.tableau is None:
            self.tableau = TableauConfig()
        if self.export is None:
            self.export = ExportConfig()

# Default configuration
default_config = CatalogConfig() 