import os
from airflow.models import Connection
from airflow import settings
from airflow.operators.bash import BashOperator


class SparkManager:
    def __init__(self):
        """Initialize SparkManager with environment variables and ensure connection exists."""
        self.spark_master = os.getenv(
            "SPARK_MASTER_URL", "spark://spark-master:7077poop"
        )
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
        self.minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "admin123")

        # Ensure spark_default connection exists
        self.ensure_spark_connection()

    def ensure_spark_connection(self):
        """Check if spark_default exists, and if not, create it."""
        session = settings.Session()
        conn_id = "spark_default"

        # Check if the connection already exists
        existing_conn = (
            session.query(Connection).filter(Connection.conn_id == conn_id).first()
        )
        if not existing_conn:
            print(f"🟢 Creating missing Airflow connection: {conn_id}")

            # Create a new Airflow connection
            new_conn = Connection(
                conn_id=conn_id,
                conn_type="spark",
                host=self.spark_master,
            )

            session.add(new_conn)
            session.commit()

        session.close()

    def submit_spark_job(self, dag, task_id, application_path):
        """
        Returns a BashOperator for a DAG that triggers spark-submit via docker exec on the spark-master container.

        Args:
            dag: Airflow DAG object.
            task_id: Name of the Spark task.
            application_path: Path to the Spark script inside MinIO (e.g., s3a://pipelines/your_script.py).

        Returns:
            BashOperator instance.
        """
        bash_command = (
            "docker exec spark-master spark-submit "
            f"--master {self.spark_master} "
            f"--conf spark.master={self.spark_master} "
            f"--conf spark.hadoop.fs.s3a.endpoint={self.minio_endpoint} "
            f"--conf spark.hadoop.fs.s3a.access.key={self.minio_access_key} "
            f"--conf spark.hadoop.fs.s3a.secret.key={self.minio_secret_key} "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.jars.packages=io.delta:delta-core_2.12:2.3.0 "
            "--executor-cores 2 --executor-memory 2g --driver-memory 1g "
            "--name arrow-spark "
            f"{application_path}"
        )

        return BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
        )
