from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

SPARK_SUBMIT = "/opt/bitnami/spark/bin/spark-submit"
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")


@app.route("/submit", methods=["POST"])
def submit_pyspark_job():
    print("Received request to submit PySpark job")
    data = request.get_json()
    script_path = data.get("script_path")
    print(data)

    if not script_path or not os.path.exists(script_path):
        return jsonify({"error": "Invalid or missing script_path"}), 400

    try:
        cmd = [SPARK_SUBMIT, script_path]

        result = subprocess.run(cmd, capture_output=True, text=True)
        # return jsonify({"result": "Yes"}), (200)
        return jsonify(
            {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode,
            }
        ), (200 if result.returncode == 0 else 500)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
