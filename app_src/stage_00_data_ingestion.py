from app_entity.artifact_entity import DataIngestionArtifact
from app_logger import logging
from app_exception import AppException
from app_entity import DataIngestionConfig
import sys,os
import zipfile
from six.moves import urllib

class DataIngestion:
    

    def __init__(self, data_ingestion_config: DataIngestionConfig):
        """
        DataIngestion Intialization
        data_ingestion_config: DataIngestionConfig 
        """
        try:
            logging.info(f"{'='*20}Data Ingestion log started.{'='*20} ")
            self.data_ingestion_config=data_ingestion_config
        except Exception as e:
            raise AppException(e, sys) from e

    

    def download_complaint_data(self):
        """
        Fetch complaint data from the url
        
        """
        try:
            
            complaint_file_url = self.data_ingestion_config.dataset_download_url
            zip_download_dir = self.data_ingestion_config.zip_download_dir
            os.makedirs(zip_download_dir, exist_ok=True)
            zip_file_path = os.path.join(zip_download_dir, "consumer_complaints.zip")
            logging.info(f"Downloading complaint data from {complaint_file_url} into file {zip_file_path}")
            urllib.request.urlretrieve(complaint_file_url,zip_file_path)
            logging.info(f"Downloaded complaint data from {complaint_file_url} into file {zip_file_path}")
            return zip_file_path
        except Exception as e:
            raise AppException(e, sys) from e



    def extract_zip_file(self,zip_file_path: str)->DataIngestionArtifact:
        """
        zip_file_path: str
        Extracts the zip file into the raw data directory
        Function returns None
        """
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            os.makedirs(raw_data_dir, exist_ok=True)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(raw_data_dir)
            logging.info(f"Extracted zip file: {zip_file_path} into dir: {raw_data_dir}")
            
            raw_data_file_name = os.listdir(raw_data_dir)[0]
            data_ingestion_artifact = DataIngestionArtifact(raw_data_file_name= os.path.join(raw_data_dir, raw_data_file_name),
            schema_file_path = self.data_ingestion_config.schema_file_path,
            is_ingested=True,
            message="Data Ingestion completed and data extracted into raw data directory")
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")

            return data_ingestion_artifact
        except Exception as e:
            raise AppException(e,sys) from e


    

    def initiate_data_ingestion(self) ->DataIngestionArtifact:
        try:
            downloaded_file_path = self.download_complaint_data()
            outputs = self.extract_zip_file(zip_file_path=downloaded_file_path)
            logging.info(f"{'='*20}Data Ingestion log completed.{'='*20} \n\n")
            return outputs
        except Exception as e:
            raise AppException(e, sys) from e




        


