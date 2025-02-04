import os
from hdfs import InsecureClient

# HDFS Namenode WebHDFS URL (Use your VM's IP)
HDFS_VM_IP = "100.66.117.85"
HDFS_NAMENODE_URL = f"http://{HDFS_VM_IP}:9870"
HDFS_USER = "hadoop"

# Local directory for tables
local_directory_path = r"C:\Users\ACER\Desktop\DE Project\Tables"

# HDFS target directory
hdfs_target_directory = "/user/DE_Project/"

# Initialize HDFS client
client = InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)

def upload_directory(local_dir, hdfs_dir):
    """Uploads all files from local_dir to hdfs_dir in HDFS."""
    
    # Ensure HDFS directory exists
    client.makedirs(hdfs_dir)

    # Iterate through local directory and upload files
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            hdfs_file_path = os.path.join(hdfs_dir, os.path.relpath(local_file_path, local_dir)).replace("\\", "/")

            # Upload file to HDFS
            with open(local_file_path, "rb") as file_data:
                client.write(hdfs_file_path, file_data, overwrite=True)

            print(f"Uploaded: {local_file_path} → {hdfs_file_path}")

if __name__ == "__main__":
    if os.path.exists(local_directory_path):
        upload_directory(local_directory_path, hdfs_target_directory)
        print("All files successfully uploaded to HDFS:", hdfs_target_directory)
    else:
        print(f"Error: Local directory '{local_directory_path}' does not exist.")

