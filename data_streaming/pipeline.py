# data_streaming/pipeline.py
from typing import Iterator
from itertools import islice
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession, DataFrame, functions as F

from data_ingestion import DataIngestion
from utils import logging


class Pipeline(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    spark: SparkSession = Field(
        default_factory=lambda: (
            SparkSession.builder.appName("DataStreaming")
            # controls how big each partition (chunk) read from parquet is
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .getOrCreate()
        )
    )

    def load(self) -> DataFrame:
        df = DataIngestion(session=self.spark).load_kaggle_dataset()
        return df.withColumn("transaction_ts", F.col("transaction_ts").cast("timestamp"))

    def get_transactions(self, df: DataFrame) -> Iterator[dict]:
        # Only one partition is in driver memory at a time
        for row in df.toLocalIterator():
            yield row.asDict()

    def get_batches(self, df: DataFrame, size: int = 1000) -> Iterator[list[dict]]:
        rows = self.get_transactions(df)
        while batch := list(islice(rows, size)):
            yield batch

    def process_data(self) -> None:
        try:
            df = self.load()
            for batch in self.get_batches(df, size=1000):
                logging.info(f"Processing {len(batch)} transactions, first: {batch[0]}")
                # ... do work on the batch here ...
        except Exception as e:
            logging.error(f"Interrupted due to:\n{e}")
            raise


if __name__ == "__main__":
    Pipeline().process_data()
    