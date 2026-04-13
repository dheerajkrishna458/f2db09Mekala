import logging
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class DeltaTableHandler(logging.Handler):
    """
    A logging.Handler that buffers log records in memory
    and flushes them to a Delta table as a single batch.
    """

    def __init__(self, spark: SparkSession, table_name: str,
                 run_id: str = None, correlation_id:str = None, level=logging.INFO):
        super().__init__(level)
        self.spark = spark
        self.table_name = table_name
        self.run_id = run_id or str(uuid.uuid4())
        self.correlation_id = correlation_id
        self._buffer = []

        self._schema = StructType([
            StructField("databricks_run_id",StringType(),    False),
            StructField("correlation_id",   StringType(),    True),
            StructField("code_message",     StringType(),    True),
            StructField("created_dtm",      TimestampType(), False),
        ])

    def emit(self, record: logging.LogRecord):
        """Capture each log record into the in-memory buffer."""
        self._buffer.append((
            self.run_id,
            self.correlation_id,
            self.format(record),
            datetime.utcfromtimestamp(record.created),
        ))

    def flush(self):
        """Write all buffered records to the Delta table in one batch."""
        if not self._buffer:
            return
        try:
            df = self.spark.createDataFrame(self._buffer, schema=self._schema)
            df.write.format("delta").mode("append").saveAsTable(self.table_name)
            print(f"[DeltaTableHandler] Code log flush successful to table {self.table_name}")
        except Exception as e:
            print(f"[DeltaTableHandler] Failed to flush logs: {e}")
        finally:
            self._buffer.clear()