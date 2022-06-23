from collections import namedtuple

DataIngestionArtifact = namedtuple("DataIngestionArtifact", [
    "raw_data_file_name", "schema_file_path", "is_ingested", "message"])

DataValidationArtifact = namedtuple("DataValidationArtifact", [
    "train_file_path", "test_file_path", "message"])