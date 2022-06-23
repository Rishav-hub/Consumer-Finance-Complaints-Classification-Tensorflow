from collections import namedtuple

DataIngestionConfig = namedtuple("DatasetConfig", ["dataset_download_url",
                                                   "raw_data_dir",
                                                   "zip_download_dir",
                                                   "schema_file_path"
                                                  ])

DataValidationConfig = namedtuple("DataValidationConfig", ["validated_train_dir",
                                                           "validated_test_dir"])


TrainingPipelineConfig = namedtuple("TrainingPipelineConfig", ["artifact_dir"])