#Define log_recorder() function
from datetime import datetime, timedelta
def log_recorder(message):
    t = datetime.now() - timedelta(hours=5)
    print(f"{t}: {message}")
log_recorder("Screener downloader started execution.")

#Import packages
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import parametermanager_v1
import requests

#Get project secrets
def get_secret(project_id, secret_id, version_id="latest"):
    """
    Retrieves a secret from Google Secret Manager.

    Args:
        project_id (str): The ID of the Google Cloud project containing the secret.
        secret_id (str): The name of the secret to retrieve.
        version_id (str): The version of the secret to retrieve. Defaults to "latest".

    Returns:
        str: The secret value.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

#Get project parameter
def get_parameter(project_id, parameter_id, version_id="unique", location="global"):
    """
    Retrieves a parameter value from Google Cloud Parameter Manager.

    Args:
        project_id (str): The GCP project identifier.
        parameter_id (str): The name of the parameter to retrieve.
        version_id (str, optional): The specific version of the parameter to fetch. Defaults to "unique".
        location (str, optional): The GCP location for the parameter. Defaults to "global".

    Returns:
        str: The value of the requested parameter.
    """
    client = parametermanager_v1.ParameterManagerClient()
    name = client.parameter_version_path(project_id, location, parameter_id, version_id)
    request = parametermanager_v1.GetParameterVersionRequest(name=name)
    response = client.get_parameter_version(request=request)
    return response.payload.data.decode("UTF-8")

def download_screener(project_id, bucket_name, daily_blob_name, view, token):
    """
    Downloads the stock screener data from the Finviz Elite API and uploads it to a Google Cloud Storage (GCS) bucket in text/csv format.

    Args:
        project_id (str): Your Google Cloud Project ID for authentication with GCS.
        bucket_name (str): Name of the GCS bucket where the CSV file will be uploaded.
        daily_blob_name (str): Path and filename in the bucket where the daily CSV will be saved.
        view (str): Query string for the Finviz screener URL specifying the view type and filters.
        token (str): Authentication token for Finviz Elite API.

    Returns:
        None
    """
    view = get_parameter(project_id, view)
    token = get_secret(project_id, token)
    url = "https://elite.finviz.com/export/screener?" + view + "&auth=" + token
    response = requests.get(url)

    # Initialize GCS client
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Upload the response content directly to daily_blob_name in GCS as text/csv
    daily_blob = bucket.blob(daily_blob_name)
    log_recorder(f"Uploading downloaded CSV content to gs://{bucket_name}/{daily_blob_name} as text/csv...")

    daily_blob.upload_from_string(
        response.content,
        content_type="text/csv"
    )

    log_recorder(f"Daily CSV uploaded successfully to GCS in text/csv format.")


def main():
    #SET VARIABLES

    #Get project ID
    import google.auth
    credentials, project_id = google.auth.default()
    
    # Retrieve GCS bucket name from Parameter Manager
    try:
        gcs_bucket_name = get_parameter(project_id, parameter_id="v2_bucket_name")
        log_recorder(f"Retrieved GCS bucket name: {gcs_bucket_name}")
    except Exception as e:
        log_recorder(f"Failed to retrieve GCS bucket name from Secret Manager: {e}")
        raise
    
    #Get daily blob path
    adjusted_time = datetime.now() - timedelta(hours=5)
    today_date_str = adjusted_time.strftime("%Y-%m-%d %H:%M:%S")
    gcs_daily_blob = gcs_bucket_name+"/daily_raw/"+today_date_str+".csv"

    #Hardocoded variables
    view="v2_finviz_view"
    token="v2_finviz_api_token"
    
    try:
        log_recorder("Downloading CSV...")
        download_screener(
            bucket_name=gcs_bucket_name,
            daily_blob_name=gcs_daily_blob,
            project_id=project_id,
            view=view,
            token=token
        )
        log_recorder("Download was successful...")
    except Exception as e:
        log_recorder(f"Script failed: {e}")

if __name__ == "__main__":
    main()