import requests
import time

# URL for Flink REST API
flink_url = "http://127.0.0.1:8081"
print("starting fake prometheus")
# NAME = 'name'
# JOB_NAME = 'job name'
accumulateStr = "accumulate"
# softStr = "soft"
hardStr = "hard"
# inputStr = "input"
numBytesOutTotalStr = "numBytesOutTotal"
# numBytesOutPerSecondStr = "numBytesOutPerSecond"
numRecordsOutTotalStr = "numRecordsOutTotal"
# numRecordsOutPerSecondStr = "numRecordsOutPerSecond"
numBytesInTotalStr = "numBytesInTotal"
# numBytesInPerSecondStr = "numBytesInPerSecond"
numRecordsInTotalStr = "numRecordsInTotal"
# numRecordsInPerSecondStr = "numRecordsInPerSecond"

list_of_metrics = ["accumulate",
                   # "soft",
                   "hard",
                   # "input",
                   "numBytesOutTotal",
                   # "numBytesOutPerSecond",
                   "numRecordsOutTotal",
                   # "numRecordsOutPerSecond",
                   "numBytesInTotal",
                   # "numBytesInPerSecond",
                   "numRecordsInTotal",
                   # "numRecordsInPerSecond"
                   ]
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

        vertex_data = get_vertices_metrics(job_id, vertex_id, data[vertex_name]['subtask_ids'], vertex_name)
        for metric in list_of_metrics:
            data[vertex_name][metric] = vertex_data[metric]


    return data

def get_vertices_metrics(job_id, vertex_id,subtask_ids, vertex_name):
    if job_id is None:
        return None

    verticesData = {}
    for metric in list_of_metrics:
        verticesData[metric] = {}
    # seeWhatWeCanGet(job_id, vertex_id, 0)
    for subtask_id in subtask_ids:
        # busyness_per_second = get_busyness_per_second(job_id, vertex_id, subtask_id)
        accumulated_backpressure = get_accumulated_backpressure(job_id, vertex_id, subtask_id)
        # soft_backpressure = get_max_soft_back_pressure_time(job_id, vertex_id, subtask_id)
        hard_backpressure = get_max_hard_back_pressure_time(job_id, vertex_id, subtask_id)
        # input_queue=  get_buffer_input_queue_size(job_id, vertex_id, subtask_id)

        numBytesOutTotal = get_num_bytes_out(job_id, vertex_id, subtask_id)
        # numBytesOutPerSecond = get_num_bytes_out_per_second(job_id, vertex_id, subtask_id)
        numRecordsOutTotal = get_num_records_out(job_id, vertex_id, subtask_id)
        # numRecordsOutPerSecond = get_num_records_out_per_second(job_id, vertex_id, subtask_id)
        numBytesInTotal = get_num_bytes_in(job_id, vertex_id, subtask_id)
        # numBytesInPerSecond= get_num_bytes_in_per_second(job_id, vertex_id, subtask_id)
        numRecordsInTotal = get_num_records_in(job_id, vertex_id,subtask_id)
        # numRecordsInPerSecond = get_num_records_in_per_second(job_id, vertex_id, subtask_id)



        # print(accumulated_backpressure, soft_backpressure, hard_backpressure)
        #if value is not in the returned value just put empty

        verticesData[accumulateStr][subtask_id] = treatData(accumulated_backpressure)
        # verticesData[softStr][subtask_id] = treatData(soft_backpressure)
        verticesData[hardStr][subtask_id] = treatData(hard_backpressure)
        # verticesData[inputStr][subtask_id] = treatData(input_queue)
        verticesData[numBytesOutTotalStr][subtask_id] = treatData(numBytesOutTotal)
        # verticesData[numBytesOutPerSecondStr][subtask_id] = treatData(numBytesOutPerSecond)
        verticesData[numRecordsOutTotalStr][subtask_id] = treatData(numRecordsOutTotal)
        # verticesData[numRecordsOutPerSecondStr][subtask_id] = treatData(numRecordsOutPerSecond)
        verticesData[numBytesInTotalStr][subtask_id] = treatData(numBytesInTotal)
        # verticesData[numBytesInPerSecondStr][subtask_id] = treatData(numBytesInPerSecond)
        verticesData[numRecordsInTotalStr][subtask_id] = treatData(numRecordsInTotal)
        # verticesData[numRecordsInPerSecondStr][subtask_id] = treatData(numRecordsInPerSecond)


    return verticesData


def treatData(data):
    # print(data)
    if data == [] or data is None:
        return []
    else:
        value = data[0]['value']

        if '.' in value:
            return float(value)
        else:
            return int(value)



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

def get_num_bytes_out(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numBytesOut"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def get_num_bytes_out_per_second(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numBytesOutPerSecond"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response


def get_num_records_out(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numRecordsOut"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def get_num_records_out_per_second(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numRecordsOutPerSecond"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response


def get_num_bytes_in(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numBytesIn"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response
def get_num_bytes_in_per_second(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numBytesInPerSecond"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def get_num_records_in(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numRecordsIn"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response


def get_num_records_in_per_second(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics?get=numRecordsInPerSecond"
    response = requests.get(request_string).json()
    # print(vertex_name,response)
    return response

def seeWhatWeCanGet(job_id, vertex_id, subtask_id, vertex_name = ""):
    request_string = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_id}/metrics"
    response = requests.get(request_string).json()
    print(response)
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




    time.sleep(2)



# job_running = get_running_job()
# if job_running and job_running != None:
#     data = set_up_data(job_running)
#     data = get_all_data(data, job_id=job_running)
#     print(data)


#python3 ./src/main/python/fakePrometheus.py

