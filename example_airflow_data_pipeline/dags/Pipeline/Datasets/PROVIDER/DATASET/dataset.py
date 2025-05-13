import pandas as pd
from common_library.common_datasets import BaseDataset, DatasetConfig
import common_library.common_config as config
import yaml
import os
import logging

log = logging.getLogger(__name__)


class Provider_Dataset_Example(BaseDataset):
    def __init__(self):
        config_yaml_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dataset_definition.yaml")
        self.config = DatasetConfig(yaml.safe_load(open(config_yaml_path, "r").read()))
        self.schedule = config.staging_pipeline_schedule_daily

    def PerformDataTransformations(self, df: pd.DataFrame):
        return df
