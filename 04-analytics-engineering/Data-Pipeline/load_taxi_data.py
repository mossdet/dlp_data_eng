import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import pandas as pd
import time


SERVICES = ["yellow", "green"] # yellow, green
YEARS = [2019, 2020] # 2019, 2020
MONTHS = [f"{i:02d}" for i in range(1, 13)] 

#Change this to your bucket name
BUCKET_NAME = "dlp-dateng-04-bucket"  
#If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/dlp/Documents/data_eng_ed/dlp_data_eng/GCP_Keys/dlp-dateng-04-d79fa6efa9dd.json"  

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

DOWNLOAD_DIR = "./Downloads/"
CHUNK_SIZE = 8 * 1024 * 1024  
PARALL_WORKERS = 1


client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)



def download_file(args):
    service=args[0]
    year=args[1]
    month=args[2]
    filename = f"{service}_tripdata_{year}-{month}.csv.gz"
    url = f"{BASE_URL}{service}/{filename}"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def convert_to_parquet(file_path):
    try:
        # read compressed file as parquet file
        print(f"Converting {file_path}...")
        df = pd.read_csv(file_path, compression='gzip')

        # Remove compressed file
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Removed: {file_path}")

        file_path = file_path.replace('.csv.gz', '.parquet')
        df.to_parquet(file_path, engine='pyarrow')
        print(f"Converted and saved: {file_path}")
        return file_path
    
    except Exception as e:
        print(f"Failed to convert {file_path}: {e}")
        return None
    

def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE  
    
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")
            
            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")
        
        time.sleep(5)  
    
    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":

    for s in SERVICES:
        for y in YEARS:
            args = ((s,y,m) for m in MONTHS)
            with ThreadPoolExecutor(max_workers=PARALL_WORKERS) as executor:
                file_paths = list(executor.map(download_file, args))
            
            with ThreadPoolExecutor(max_workers=PARALL_WORKERS) as executor:
                file_paths = executor.map(convert_to_parquet, filter(None, file_paths))  # Remove None values

            # with ThreadPoolExecutor(max_workers=PARALL_WORKERS) as executor:
            #     executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")