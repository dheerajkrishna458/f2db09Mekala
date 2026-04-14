from pyspark.sql import SparkSession

class SessionManager:
    """SessionManager provides utility methods for managing Spark sessions and accessing Databricks utilities."""

    @staticmethod
    def get_session(app_name: str = "VeleraETL"):
        """Creates or retrieves a SparkSession with the specified application name."""
        return SparkSession.builder.appName(app_name).getOrCreate()

    @staticmethod
    def get_dbutils(spark):
        """Returns a DBUtils instance for the given Spark session."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(spark)
        except Exception:
            return None