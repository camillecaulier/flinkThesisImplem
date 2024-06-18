import requests
import time

# URL for Flink REST API
flink_url = "http://127.0.0.1:8081"
print("starting fake prometheus")
# NAME = 'name'
# JOB_NAME = 'job name'
accumulate_str = "accumulate"

# data = {job name, vertex names {vertex names: vertex id},
# vertexname: {subplots, subplots no, 'soft'{subplot id: val} , 'hard'{subplot id} , accumulated {subplot id}}}
def get_running_job():
    try:
        response = requests.get(f"{flink_url}/jobs/")
        jobs_data = response.json()
        print(jobs_data)
        for job in jobs_data['jobs']:
            if job['status'] == 'RUNNING':
                return job['id']
        return None
    except:
        return None
def set_up_data(job_id):
    if job_id is None:
        return None
    # get the preliminary data
    metrics_url = f"{flink_url}/jobs/{job_id}"
    response = requests.get(metrics_url)
    metrics_data = response.json()
    data = {}
    data['job name'] =  metrics_data['name']
    data['job_id'] = job_id

    data['vertex names'] = get_vertex_ids(job_id)
    # print(data)

    for vertex_name, vertex_id in data['vertex names'].items():
        subtask_ids = get_subtask_ids(job_id, vertex_id)
        data[vertex_name] = {'name': vertex_name, 'id': vertex_id}
        data[vertex_name]['subtask_ids'] = subtask_ids
        data[vertex_name]['no subtask'] = len(subtask_ids)

    # print(data)

    return data


def get_vertex_ids(job_id) -> dict:
    if job_id is None:
        return None
    metrics_url = f"{flink_url}/jobs/{job_id}"
    response = requests.get(metrics_url)
    metrics_data = response.json()

    vertex_ids = {}
    for vertex_data in metrics_data['vertices']:
        vertex_ids[vertex_data['name']] = vertex_data['id']

    return vertex_ids


def get_all_data(data, job_id):
    # job_id = data['job_id']
    for vertex_name, vertex_id in data['vertex names'].items():
        accumulate_dic, soft_dic, hard_dic, input_queue_dic = get_vertices_metrics(job_id, vertex_id, data[vertex_name]['subtask_ids'], vertex_name)
        data[vertex_name]["accumulate"] = accumulate_dic
        data[vertex_name]["soft"] = soft_dic
        data[vertex_name]["hard"] = hard_dic
        data[vertex_name]["input"] = input_queue_dic
    return data

def get_vertices_metrics(job_id, vertex_id,subtask_ids, vertex_name):
    if job_id is None:
        return None

    # print("vertex name: " + vertex_name)
    accumulate_dic = {}
    soft_dic = {}
    hard_dic = {}
    input_queue_dic = {}
    for subtask_id in subtask_ids:
        # busyness_per_second = get_busyness_per_second(job_id, vertex_id, subtask_id)
        accumulated_backpressure = get_accumulated_backpressure(job_id, vertex_id, subtask_id)
        soft_backpressure = get_max_soft_back_pressure_time(job_id, vertex_id, subtask_id)
        hard_backpressure = get_max_hard_back_pressure_time(job_id, vertex_id, subtask_id)
        input_queue=  get_buffer_input_queue_size(job_id, vertex_id, subtask_id)
        # print(accumulated_backpressure, soft_backpressure, hard_backpressure)
        #if value is not in the returned value just put empty
        if accumulated_backpressure == [] or accumulated_backpressure == None:
            accumulate_dic[subtask_id] = []
        else:
            accumulate_dic[subtask_id] = int(accumulated_backpressure[0]['value'])

        if soft_backpressure == [] or soft_backpressure is None:
            soft_dic[subtask_id] = []
        else:
            soft_dic[subtask_id] = int(soft_backpressure[0]['value'])
        if hard_backpressure == [] or hard_backpressure is None:
            hard_dic[subtask_id] = []
        else:
            hard_dic[subtask_id] = int(hard_backpressure[0]['value'])

        if input_queue == [] or input_queue is None:
            input_queue_dic[subtask_id] = []
        else:
            input_queue_dic[subtask_id] = int(input_queue[0]['value'])


    return accumulate_dic, soft_dic, hard_dic, input_queue_dic


def get_subtask_ids(job_id, vertex_id):
    # print("job id and vertex id ",job_id, vertex_id)
    if job_id is None:
        return None
    response = requests.get(f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}").json()
    subtask_ids = [subtask['subtask'] for subtask in response.get('subtasks', [])]
    # print("subtask names: " + str(subtask_ids))
    return subtask_ids

def get_busyness_per_second(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=busyTimeMsPerSecond"
    # request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics"

    # print(request_string)
    response = requests.get(request_string).json()
    # print("data subtask")
    # print(vertex_name,response)
    return response

def get_accumulated_backpressure(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=accumulateBackPressuredTimeMs"

    response = requests.get(request_string).json()
    # print("data subtask")
    # print(vertex_name,response)
    return response

def get_max_soft_back_pressure_time(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=maxSoftBackPressureTimeMs"

    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def get_max_hard_back_pressure_time(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=maxHardBackPressureTimeMs"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def get_buffer_input_queue_size(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=buffers.inputQueueLength"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response



while True:
    none_count = 0
    # Main execution
    job_running = get_running_job()
    if job_running and job_running != None:
        data = set_up_data(job_running)
        data = get_all_data(data, job_id=job_running)
        print(data)

        none_count = 0


    else:
        none_count += 1
        if none_count == 100:
            break




    time.sleep(0.25)


# job_running = get_running_job()
# if job_running:
#     data = set_up_data(job_running)
#     data = get_all_data(data, job_id= job_running)
#     print(data)
    # vertex_ids = get_vertex_ids(job_running)
    # if vertex_ids:
    #     get_vertices_metrics(job_running, vertex_ids)
