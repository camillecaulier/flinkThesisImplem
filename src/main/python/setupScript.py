import os
import subprocess


def configure_flink(flink_dir, task_slots= 8, workers_n = 8):
    # Path to the Flink configuration file
    conf_path = os.path.join(flink_dir, 'conf', 'flink-conf.yaml')
    workers_path = os.path.join(flink_dir, 'conf', 'workers')
    with open(conf_path, 'r+') as file:
        lines = file.readlines()
        file.seek(0)
        task_slot_found = False
        for line in lines:
            if 'taskmanager.numberOfTaskSlots' in line:
                line = 'taskmanager.numberOfTaskSlots: {}\n'.format(task_slots)
                task_slot_found = True
            file.write(line)
        if not task_slot_found:
            file.write('\ntaskmanager.numberOfTaskSlots: 4\n')

    with open(workers_path, 'w') as file:
        for i in range(workers_n):
            file.write('localhost\n')

    print("Flink configuration has been updated")


def install_packages():
    # Install required Python packages using subprocess
    subprocess.run(['pip', 'install', 'fabric', 'pandas', 'ipywidgets', 'papermill'], check=True)

import tarfile
import glob
import os
def extract_tar_files(pattern):
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

extract_tar_files('flink-*.tgz')
def download_flink():
    #Check if Flink package is already downloaded
    if not os.path.exists("flink-1.18.1-bin-scala_2.12.tgz"):
        subprocess.run(['curl', '-O', '--insecure', 'https://dlcdn.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz'], check=True)
        subprocess.run(['tar', '-xvzf', 'flink-1.18.1-bin-scala_2.12.tgz'], check=True)

install_packages()
download_flink()
configure_flink('flink-1.18.1')