from app_logger import logging
from app_exception import AppException
from app_entity import DataValidationConfig, DataIngestionArtifact, DataValidationArtifact
import os
import sys
import pandas as pd
from sklearn.model_selection import train_test_split
from app_config import DATASET_SCHEMA_TARGET_COLUMN_KEY
from app_util import read_yaml_file


class DataValidation:
    def __init__(self, data_validation_config: DataValidationConfig,
                 data_ingestion_artifact: DataIngestionArtifact):
        try:
            logging.info(f"{'='*20}Data Validation log started.{'='*20} ")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise AppException(e, sys) from e

    
    @staticmethod
    def condition_parser(x,target_conditions):
        if x in target_conditions:
            return x
        else:
            return "Others"


    def cleanup_raw_data(self, df):
        try:
            print(type(df))
            # reading schema file path
            schema_file_path = self.data_ingestion_artifact.schema_file_path
            dataset_schema = read_yaml_file(schema_file_path)
            # TARGET_COLUMN
            target_column = dataset_schema[DATASET_SCHEMA_TARGET_COLUMN_KEY]
            #removing missing values
            df_new = df.dropna(subset=['consumer_complaint_narrative'])
            df_new = df_new[[target_column, 'consumer_complaint_narrative']]
            
            # Preprocessing Y Column
            # We are only going to be classifying conditions for which the count of reviews are more than 2128.
            count_df = df_new[[target_column,'consumer_complaint_narrative']].groupby(target_column).aggregate({'consumer_complaint_narrative':'count'}).reset_index().sort_values('consumer_complaint_narrative',ascending=False)
            target_conditions = count_df[count_df['consumer_complaint_narrative']>=13][target_column].values
            df_new[target_column] = df_new[target_column].apply(lambda x: DataValidation.condition_parser(x,target_conditions)) 

            return df_new


        except Exception as e:
            raise AppException(e, sys) from e


    
    def split_data_as_train_test(self)->DataValidationArtifact:
        try:
            os.makedirs(self.data_validation_config.validated_train_dir, exist_ok=True)
            os.makedirs(self.data_validation_config.validated_test_dir, exist_ok=True)
            df = pd.read_csv(self.data_ingestion_artifact.raw_data_file_name)
            logging.info(f"Raw data shape: {df.shape}")
            df = df.sample(3000)    #change the number with respect to your machine configuration & you can remove this line also
            df = self.cleanup_raw_data(df)
            logging.info(f"Clean data shape: {df.shape}")
            X_train, X_test = train_test_split(df, test_size=0.20, random_state=42)
            X_train.to_csv(os.path.join(self.data_validation_config.validated_train_dir, "complaint.csv"), index=None, header=True)
            X_test.to_csv(os.path.join(self.data_validation_config.validated_test_dir, "complaint.csv"), index=None, header=True)
            logging.info(f"Data splitted into train & test")


            train_file_path = os.path.join(self.data_validation_config.validated_train_dir, "complaint.csv")
            test_file_path = os.path.join(self.data_validation_config.validated_test_dir, "complaint.csv")

            data_validation_artifact = DataValidationArtifact(train_file_path=train_file_path,
            test_file_path=test_file_path,
            message="Data validation completed and data set has been splited into train and test")
            logging.info(f"Data validation artifact:[{data_validation_artifact}]")

            return data_validation_artifact

        except Exception as e:
            raise AppException(e, sys) from e



    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            outputs = self.split_data_as_train_test()
     
            logging.info(f"{'='*20}Data Validation log ended.{'='*20} ")
            return outputs

        except Exception as e:
            raise AppException(e, sys) from e

