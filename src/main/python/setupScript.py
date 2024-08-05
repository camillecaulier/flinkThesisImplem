import os
import subprocess
import yaml
import glob
import tarfile


def configure_flink(flink_dir, task_slots=4, task_managers=24, memory_size='4096m'):
    # Path to the Flink configuration file
    conf_path = os.path.join(flink_dir, 'conf', 'flink-conf.yaml')
    workers_path = os.path.join(flink_dir, 'conf', 'workers')

    with open(conf_path, 'r+') as file:
        lines = file.readlines()
        file.seek(0)
        task_slot_found = False
        memory_size_found = False
        for line in lines:
            if 'taskmanager.numberOfTaskSlots' in line:
                line = 'taskmanager.numberOfTaskSlots: {}\n'.format(task_slots)
                task_slot_found = True
            elif 'taskmanager.memory.process.size' in line:
                line = 'taskmanager.memory.process.size: {}\n'.format(memory_size)
                memory_size_found = True
            file.write(line)
        if not task_slot_found:
            file.write('\ntaskmanager.numberOfTaskSlots: {}\n'.format(task_slots))
        if not memory_size_found:
            file.write('taskmanager.memory.process.size: {}\n'.format(memory_size))

    with open(workers_path, 'w') as file:
        for i in range(task_managers):
            file.write('localhost\n')

    print("Flink configuration has been updated")


# def configure_flink(flink_dir, task_slots=8, workers_n=8, memory_size='4096m'):
#     # Path to the Flink configuration file
#     conf_path = os.path.join(flink_dir, 'conf', 'flink-conf.yaml')
#     workers_path = os.path.join(flink_dir, 'conf', 'workers')
#
#     with open(conf_path, 'r') as file:
#         config = yaml.safe_load(file)
#
#     # Update task slots
#     config['taskmanager']['numberOfTaskSlots'] = task_slots
#
#     # Update memory size
#     config['taskmanager']['memory']['process']['size'] = memory_size
#
#     with open(conf_path, 'w') as file:
#         yaml.safe_dump(config, file)
#
#     # Update workers file
#     with open(workers_path, 'w') as file:
#         for i in range(workers_n):
#             file.write('localhost\n')
#
#     print("Flink configuration has been updated")

def install_packages():
    # Install required Python packages using subprocess
    subprocess.run(['pip', 'install', 'fabric', 'pandas', 'ipywidgets', 'papermill'], check=True)

def extract_tar_files(pattern):
    if os.path.exists("flink-1.18.1"):
        print("file already downloaded")
        return
    # Get list of all tar files matching the pattern
    tar_files = glob.glob(pattern)

    # Loop through each file and extract its contents
    for tar_file in tar_files:
        try:
            with tarfile.open(tar_file, 'r:gz') as file:
                print(f"Extracting {tar_file}...")
                file.extractall()  # Extracts to the current directory
                print(f"Extracted {tar_file} successfully.")
        except Exception as e:
            print(f"Failed to extract {tar_file}: {str(e)}")

def download_flink():
    # Check if Flink package is already downloaded
    if not os.path.exists("flink-1.18.1-bin-scala_2.12.tgz"):
        subprocess.run(['curl', '-O', '--insecure', 'https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz'], check=True)
        subprocess.run(['tar', '-xvzf', 'flink-1.18.1-bin-scala_2.12.tgz'], check=True)

def verify_java_version():
    try:
        # Check the current Java version
        result = subprocess.run(['java', '-version'], capture_output=True, text=True, check=True)
        output = result.stderr
        if '11' in output:
            print("Java 11 is correctly installed.")
        else:
            print("Java 11 is not installed. Current version:")
            print(output)
    except subprocess.CalledProcessError as e:
        print(f"Failed to check Java version: {str(e)}")

# Run the functions
install_packages()
download_flink()
extract_tar_files('flink-*.tgz')
configure_flink('flink-1.18.1')
verify_java_version()