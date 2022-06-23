from app_logger import logging
from app_exception import AppException
import os
import sys
from app_entity import DataIngestionConfig,TrainingPipelineConfig, DataValidationConfig
from app_util import read_yaml_file
from datetime import datetime

ROOT_DIR = os.getcwd()

CURRENT_TIME_STAMP = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
# Varibale declaration

# Data Ingestion related variables
DATASET_SCHEMA_COLUMNS_KEY = "columns"
DATASET_SCHEMA_TARGET_COLUMN_KEY = "target_column"


DATA_INGESTION_ARTIFACT_DIR = "data_ingestion"
DATA_INGESTION_CONFIG_KEY = "data_ingestion_config"
DATA_INGESTION_DOWNLOAD_URL_KEY = "dataset_download_url"
DATA_INGESTION_RAW_DATA_DIR_KEY = "raw_data_dir"
DATA_INGESTION_ZIP_DOWNLOAD_DIR_KEY = "zip_download_dir"
DATA_INGESTION_DIR_NAME_KEY = "ingested_dir"
DATA_INGESTION_SCHEMA_FILE_DIR_KEY = "schema_file_name"



# Data Validation related variables
DATA_VALIDATION_ARTIFACT_DIR = "data_validation"
DATA_VALIDATION_CONFIG_KEY = "data_validation_config"
DATA_VALIDATION_TRAIN_DIR_KEY = "validated_train_dir"
DATA_VALIDATION_TEST_DIR_KEY = "validated_test_dir"



# Training pipeline related variable
TRAINING_PIPELINE_CONFIG_KEY = "training_pipeline_config"
TRAINING_PIPELINE_ARTIFACT_DIR_KEY = "artifact_dir"
TRAINING_PIPELINE_NAME_KEY = "pipeline_name"

CONFIG_DIR = "config"
CONFIG_FILE_NAME = "config.yaml"
CONFIG_FILE_PATH = os.path.join(ROOT_DIR, CONFIG_DIR, CONFIG_FILE_NAME)


class AppConfiguration:
    def __init__(self, config_file_path: str = CONFIG_FILE_PATH,current_time_stamp:str=CURRENT_TIME_STAMP):
        """
        Initializes the AppConfiguration class.
        config_file_path: str
        By default it will accept default config file path.
        """
        try:
            self.config_info = read_yaml_file(file_path=config_file_path)
            self.training_pipeline_config = self.get_training_pipeline_config()
            self.time_stamp = current_time_stamp
        except Exception as e:
            raise AppException(e, sys) from e


    def get_data_ingestion_config(self) -> DataIngestionConfig:
        try:
            artifact_dir = os.path.join(
                self.training_pipeline_config.artifact_dir, DATA_INGESTION_ARTIFACT_DIR, self.time_stamp)

            data_ingestion_config = self.config_info[DATA_INGESTION_CONFIG_KEY]
            raw_data_dir = os.path.join(
                artifact_dir, data_ingestion_config[DATA_INGESTION_RAW_DATA_DIR_KEY])
            zip_download_dir = os.path.join(
                artifact_dir, data_ingestion_config[DATA_INGESTION_ZIP_DOWNLOAD_DIR_KEY])

            # ingested_dir_name = os.path.join(artifact_dir,
            #                                  data_ingestion_config[DATA_INGESTION_DIR_NAME_KEY])

            schema_file_path = os.path.join(
                ROOT_DIR, data_ingestion_config[DATA_INGESTION_SCHEMA_FILE_DIR_KEY])                                 


            response = DataIngestionConfig(dataset_download_url=data_ingestion_config[DATA_INGESTION_DOWNLOAD_URL_KEY],
                                           raw_data_dir=raw_data_dir,
                                           zip_download_dir=zip_download_dir,
                                           schema_file_path = schema_file_path
                                           )
            logging.info(f"Data Ingestion Config: {response}")

            return response
        except Exception as e:
            raise AppException(e, sys) from e

    

    def get_data_validation_config(self) -> DataValidationConfig:
        try:
            artifact_dir = os.path.join(
                self.training_pipeline_config.artifact_dir, DATA_VALIDATION_ARTIFACT_DIR, self.time_stamp)

            data_vaidation_config = self.config_info[DATA_VALIDATION_CONFIG_KEY]

            validated_train_dir = os.path.join(
                artifact_dir, data_vaidation_config[DATA_VALIDATION_TRAIN_DIR_KEY])
            
            validated_test_dir = os.path.join(
                artifact_dir, data_vaidation_config[DATA_VALIDATION_TEST_DIR_KEY])
           
            response = DataValidationConfig(validated_train_dir=validated_train_dir,
                                            validated_test_dir=validated_test_dir)
            logging.info(response)
            return response
        except Exception as e:
            raise AppException(e, sys) from e







    def get_training_pipeline_config(self) -> TrainingPipelineConfig:
        try:
            training_pipeline_config = self.config_info[TRAINING_PIPELINE_CONFIG_KEY]
            artifact_dir = os.path.join(
                ROOT_DIR, training_pipeline_config[TRAINING_PIPELINE_NAME_KEY],
                training_pipeline_config[TRAINING_PIPELINE_ARTIFACT_DIR_KEY])
            response = TrainingPipelineConfig(artifact_dir=artifact_dir)
            logging.info(f"Training Pipeline Config: {response}")
            return response
        except Exception as e:
            raise AppException(e, sys) from e
