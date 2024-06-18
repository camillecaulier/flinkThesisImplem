import requests
import time

# URL for Flink REST API
flink_url = "http://127.0.0.1:8081"
print("starting fake prometheus")

def get_running_job():
    response = requests.get(f"{flink_url}/jobs/")
    jobs_data = response.json()
    for job in jobs_data['jobs']:
        if job['status'] == 'RUNNING':
            return job['id']
    return None

def get_all_data(job_id):
    if job_id is None:
        return None
    metrics_url = f"{flink_url}/jobs/{job_id}"
    response = requests.get(metrics_url)
    metrics_data = response.json()
    data = {}
    data['job name'] =  metrics_data['name']

    data[vertex_ids] = get_vertex_ids(job_id)
    print(data)
    return data
def get_vertex_ids(job_id) -> dict:
    if job_id is None:
        return None
    metrics_url = f"{flink_url}/jobs/{job_id}"
    response = requests.get(metrics_url)
    metrics_data = response.json()
    # print(metrics_data)

    vertex_ids = {}
    for vertex_data in metrics_data['vertices']:
        vertex_ids[vertex_data['name']] = vertex_data['id']
    # print("vertex_ids: {}".format(vertex_ids))
    return vertex_ids

# def get_vertices_metrics(job_id, vertex_ids):
#     if job_id is None:
#         return None
#     for vertex_name, vertex_id in vertex_ids.items():
#         subtask_ids = get_subtask_ids(job_id, vertex_id)
#         print("vertex name: " + vertex_name)
#         for subtask_id in subtask_ids:
#             subtask_data = get_subtask_data(job_id, vertex_id, subtask_id)
#             if 'errors' in subtask_data or subtask_data is None:
#                 print(f"Error in subtask data for vertex {vertex_name}, subtask {subtask_id}: {subtask_data.get('errors', 'Unknown error')}")
#                 return None
#             print(f"Metrics for vertex {vertex_name}, subtask {subtask_id}: {subtask_data}")
#
#             accumulated_backpressure = get_accumulated_backpressure(job_id, vertex_id, subtask_id)
#             if 'errors' in accumulated_backpressure or accumulated_backpressure is None:
#                 print(f"Error in subtask data for vertex {vertex_name}, subtask {subtask_id}: {accumulated_backpressure.get('errors', 'Unknown error')}")
#                 return None
#             print(f"Metrics for vertex {vertex_name}, subtask {subtask_id}: {accumulated_backpressure}")
#

def get_vertices_metrics(job_id, vertex_ids):
    if job_id is None:
        return None
    for vertex_name, vertex_id in vertex_ids.items():
        subtask_ids = get_subtask_ids(job_id, vertex_id)
        print("vertex name: " + vertex_name)
        for subtask_id in subtask_ids:
            busyness_per_second = get_busyness_per_second(job_id, vertex_id, subtask_id)
            accumulated_backpressure = get_accumulated_backpressure(job_id, vertex_id, subtask_id)
            if 'errors' in accumulated_backpressure or accumulated_backpressure is None:
                print(
                    f"Error in subtask data for vertex {vertex_name}, subtask {subtask_id}: {accumulated_backpressure.get('errors', 'Unknown error')}")
                return None
            print(f"Metrics for vertex {vertex_name}, subtask {subtask_id}: {accumulated_backpressure}")


def get_subtask_ids(job_id, vertex_id):
    print(job_id, vertex_id)
    if job_id is None:
        return None
    response = requests.get(f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}").json()
    subtask_ids = [subtask['subtask'] for subtask in response.get('subtasks', [])]
    # print("subtask names: " + str(subtask_ids))
    return subtask_ids

def get_busyness_per_second(job_id, vertex_id, subtask_id):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=busyTimeMsPerSecond"
    # request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics"

    # print(request_string)
    response = requests.get(request_string).json()
    # print("data subtask")
    # print(response)
    return response

def get_accumulated_backpressure(job_id, vertex_id, subtask_id):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=accumulateBackPressuredTimeMs"

    response = requests.get(request_string).json()
    # print("data subtask")
    # print(response)
    return response

def get_max_soft_back_pressure_time(job_id, vertex_id, subtask_id):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=maxSoftBackPressureTimeMs"

    response = requests.get(request_string).json()

    return response

def get_max_hard_back_pressure_time(job_id, vertex_id, subtask_id):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=maxHardBackPressureTimeMs"
    response = requests.get(request_string).json()

    return response



while True:
    none_count = 0
    # Main execution
    job_running = get_running_job()
    if job_running:
        vertex_ids = get_vertex_ids(job_running)
        if vertex_ids:
            get_vertices_metrics(job_running, vertex_ids)

        none_count = 0


    else:
        none_count += 1
        if none_count == 100:
            break




    time.sleep(0.25)

#
# job_running = get_running_job()
# if job_running:
#     vertex_ids = get_vertex_ids(job_running)
#     if vertex_ids:
#         get_vertices_metrics(job_running, vertex_ids)
