import hashlib
import uuid

def read_raw_data(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

def get_unique_output_path(base_path: str, prefix: str = "predict") -> str:
    uid = str(uuid.uuid4())
    hash_val = hashlib.sha256(uid.encode()).hexdigest()[:10]
    return f"{base_path}/{prefix}_{hash_val}"