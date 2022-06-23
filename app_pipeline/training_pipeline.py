from app_logger import logging
from app_exception import AppException
from app_config import AppConfiguration
import os
import sys
from app_entity import DataIngestionArtifact, DataValidationArtifact
from app_src import DataIngestion, DataValidation


class TrainingPipeline:

    def __init__(self, app_config: AppConfiguration = AppConfiguration()):
        """
        TrainingPipeline constructor
        app_config: AppConfiguration

        """
        try:
            self.app_config = app_config

        except Exception as e:
            raise AppException(e, sys) from e
            

    def start_data_ingestion(self) -> DataIngestionArtifact:
        """
        Start data ingestion 
        """
        try:
            data_ingestion = DataIngestion(
                data_ingestion_config=self.app_config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise AppException(e, sys) from e


    def start_data_validation(self, data_ingestion_artifact: DataIngestionArtifact) -> DataValidationArtifact:
        """
        Starts data validation
        data_ingestion_artifact: DataIngestionArtifact
        """
        try:
            data_validation = DataValidation(
                data_validation_config=self.app_config.get_data_validation_config(),
                data_ingestion_artifact=data_ingestion_artifact)

            return data_validation.initiate_data_validation()
        except Exception as e:
            raise AppException(e, sys) from e


    

    def start_training_pipeline(self):
        try:
            logging.info("Starting training pipeline")
            data_ingestion_artifact = self.start_data_ingestion()

            data_validation_artifact = self.start_data_validation(
                data_ingestion_artifact=data_ingestion_artifact)

            logging.info("Training pipeline completed")

        except Exception as e:
            raise AppException(e, sys) from e