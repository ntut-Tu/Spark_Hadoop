import subprocess

def build_and_submit(path):

    # TODO: Job need to be register in order to be track by web UI
    try:
        cmd = [
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--deploy-mode", "client",
            "--conf", "spark.driver.host=spark-master",
            "/app/batch_predict.py", # TODO batch_predict.py
            "--input", path
        ]
        completed = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return {
            "stdout": completed.stdout,
            "stderr": completed.stderr
        }
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Spark 任務失敗: {e.stderr}")