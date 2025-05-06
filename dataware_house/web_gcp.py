import io
import os
import requests
import pandas as pd
from google.cloud import storage

# Initial URL and bucket name
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dana-kestra-202504")

def upload_to_gcs(bucket_name, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(f"[UPLOAD] Uploaded {local_file} to gs://{bucket_name}/{object_name}")

def web_to_gcs(year, service):
    for i in range(12):
        month = f"{i+1:02d}"
        csv_file = f"{service}_tripdata_{year}-{month}.csv.gz"
        parquet_file = csv_file.replace(".csv.gz", ".parquet")

        url = f"{init_url}{service}/{csv_file}"
        print(f"[DOWNLOAD] Attempting to download: {url}")
        response = requests.get(url)

        if response.status_code != 200:
            print(f"[SKIP] File not found: {url}")
            continue

        with open(csv_file, "wb") as f:
            f.write(response.content)
        print(f"[SUCCESS] Downloaded {csv_file}")

        df = pd.read_csv(csv_file, compression='gzip')
        df.to_parquet(parquet_file, engine='pyarrow')
        print(f"[CONVERT] Converted to Parquet: {parquet_file}")

        gcs_path = f"{service}/{parquet_file}"
        upload_to_gcs(BUCKET, gcs_path, parquet_file)

        os.remove(csv_file)
        os.remove(parquet_file)
        print(f"[CLEANUP] Removed local files {csv_file} and {parquet_file}")

# def run():
#     for service in ["green", "yellow"]:
#         for year in ["2019", "2020"]:
#             web_to_gcs(year, service)

#     web_to_gcs("2019", "fhv")

def run():
    web_to_gcs("2019", "fhv")
    

if __name__ == "__main__":
    run()