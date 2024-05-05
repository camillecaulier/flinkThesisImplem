# http://localhost:8081/jobs/{job_id}/metrics?get={metric}
# curl http://localhost:8081/jobs/

import requests
import time
# URL for Flink REST API
# flink_url = 'http://localhost:8081'
flink_url = "http://127.0.0.1:8081"

def get_running_job():
    response = requests.get(f"{flink_url}/jobs/")

    jobs_data = response.json()
    for job in jobs_data['jobs']:
        if job['status'] == 'RUNNING':
            return job['id']

def get_job_metrics(job_id):

    if job_id is None:
        return "None"
    metrics_url = f"{flink_url}/jobs/{job_id}"
    response = requests.get(metrics_url)
    metrics_data = response.json()

    print(metrics_data)


while True:
    none_count = 0
    job_running = get_running_job()
    try:

        if get_job_metrics(job_running) == "None":
            none_count += 1
            if none_count == 200:
                print("No running job found")
                break
        else:
            none_count = 0

    except Exception as e:
        print(f"Failed to fetch metrics: {e}")
    time.sleep(0.25)
