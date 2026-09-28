# data_streaming/data_ingestion.py
from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import SparkSession, DataFrame
from datasets import load_dataset
import pyarrow as pa
import pyarrow.parquet as pq
import os

from utils import logging, create_path


class DataIngestion(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    session: SparkSession
    data_path: str | None = None
    batch_size: int = 50_000
    save_dir: str = Field(
        default_factory=lambda: os.path.join(os.getenv("ARTIFACTS", "./artifacts"), "dataset")
    )

    def model_post_init(self, __context) -> None:
        create_path(self.save_dir)

    @property
    def parquet_dir(self) -> str:
        return os.path.join(self.save_dir, "train")

    def load_kaggle_dataset(self) -> DataFrame:
        try:
            done_marker = os.path.join(self.parquet_dir, "_SUCCESS")

            if not os.path.exists(done_marker):
                self._download_in_chunks()
                open(done_marker, "w").close()

            return self.session.read.parquet(self.parquet_dir)

        except Exception as e:
            logging.error(f"Could not download dataset: {e}")
            raise

    def _download_in_chunks(self) -> None:
        if self.data_path is None:
            self.data_path = os.getenv("KAGGLE_DATASET")

        create_path(self.parquet_dir)
        logging.info(f"Streaming dataset {self.data_path} from Hugging Face...")

        stream = load_dataset(self.data_path, split="train", streaming=True)

        for i, batch in enumerate(stream.iter(batch_size=self.batch_size)):
            table = pa.Table.from_pydict(batch)
            pq.write_table(table, os.path.join(self.parquet_dir, f"part-{i:05d}.parquet"))
            logging.info(f"Wrote chunk {i} ({table.num_rows} rows)")