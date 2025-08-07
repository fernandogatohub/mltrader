from datetime import datetime, timedelta
start_time = datetime.now() - timedelta(hours=5)
print(f"{start_time}: Screener downloader started execution")

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import glob
import tempfile
from google.cloud import storage
from google.cloud import secretmanager
import pandas as pd
import json

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

def initialize_driver(download_dir, headless=True):
    """
    Initializes undetected_chromedriver with specified download directory and headless mode.

    Args:
        download_dir (str): The temporary directory for downloads.
        headless (bool): Whether to run Chrome in headless mode.

    Returns:
        undetected_chromedriver.Chrome: The initialized WebDriver instance.
    """

    chrome_prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    # Set up undetected_chromedriver options
    options = uc.ChromeOptions()
    options.headless = headless
    options.add_argument("--no-sandbox") # Required for running as root in some environments
    options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems
    options.add_experimental_option("prefs", chrome_prefs)

    # Initialize undetected_chromedriver
    driver = uc.Chrome(options=options,force_update=True)
    print("Browser launched.")
    return driver

def login(driver, project_id):
    """
    Logs into StockAnalysis.com using credentials from Secret Manager.

    Args:
        driver (undetected_chromedriver.Chrome): The WebDriver instance.
        project_id (str): Your Google Cloud Project ID for Secret Manager.

    Returns:
        None
    """
    # Fetch sensitive data using the get_secret function
    try:
        email = get_secret(project_id, "stockanalysis_email")
        password = get_secret(project_id, "stockanalysis_password")
        print("Successfully retrieved credentials from Secret Manager.")
    except Exception as e:
        print(f"Failed to retrieve secrets from Secret Manager: {e}")
        print("Please ensure the secrets exist and the VM's service account has 'Secret Manager Secret Accessor' role.")
        raise

    # Visit login page
    print("Navigating to login page...")
    driver.get("https://stockanalysis.com/login/")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "email"))
    )
    time.sleep(2)

    # Fill in credentials
    print("Filling in credentials...")
    driver.find_element(By.NAME, "email").send_keys(email)
    driver.find_element(By.NAME, "password").send_keys(password)

    # Submit the form
    print("Clicking login button...")
    login_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Log In')]")
    login_button.click()
    time.sleep(30)

def download_stock_screener_csv_to_gcs(driver, bucket_name, daily_blob_name, temp_download_dir, project_id):
    """
    Navigates to the screener, downloads the CSV, and uploads to GCS.

    Args:
        driver (undetected_chromedriver.Chrome): The WebDriver instance.
        bucket_name (str): The name of your Google Cloud Storage bucket.
        daily_blob_name (str): The desired path/name for the daily CSV in the GCS bucket.
        temp_download_dir (str): The temporary directory where the CSV will be downloaded.
        project_id (str): Your Google Cloud Project ID for GCS operations.

    Returns:
        None
    """
    try:
        # Navigate to the StockAnalysis screener page
        print("Navigating to the StockAnalysis screener page...")
        driver.get("https://stockanalysis.com/stocks/screener/")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(., 'ML View')]"))
        )

        # Click on the "ML View" tab
        print("Waiting for the 'ML View' tab to be clickable...")
        ml_view_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'ML View')]"))
        )
        print("Clicking 'ML View' tab...")
        ml_view_tab.click()
        time.sleep(3)

        # --- Attempt to click the primary download button that opens the dropdown ---
        download_button_clicked = False
        print("Attempting to click the 'Download' button to open the dropdown...")

        # Strategy: Try to find a button that *opens* the dropdown.
        # Prioritize a button with exact text 'Download', then one containing 'Download'
        button_xpaths_to_try = [
            "//button[text()='Download']", # Exact text 'Download'
            "//button[contains(., 'Download')]" # Contains 'Download'
        ]

        for xpath in button_xpaths_to_try:
            try:
                download_trigger_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                print(f"Clicking button found with XPath: {xpath}...")
                download_trigger_button.click()
                download_button_clicked = True
                time.sleep(2) 
                break
            except Exception:
                print(f"  Button with XPath '{xpath}' not found or not clickable.")
        
        if not download_button_clicked:
            print("No primary download button found or clickable to open the dropdown. Proceeding, assuming 'Download to CSV' might be directly visible.")

        if download_button_clicked:
            print("Waiting for the dropdown menu to appear...")
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'dropdown-menu') or contains(@class, 'sa-dropdown') or @role='menu']")) # Added @role='menu' for more generic dropdown detection
                )
                print("Dropdown menu detected.")
                time.sleep(2)
            except Exception as e:
                print(f"Dropdown menu did not appear after clicking download button: {e}")
                print("Proceeding to 'Download to CSV' option directly.")

        print("Waiting for the 'Download to CSV' option to appear and be clickable...")
        # Modified XPath to look for both 'a' and 'button' tags
        download_csv_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Download to CSV')] | //button[contains(., 'Download to CSV')]"))
        )
        
        print("Clicking 'Download to CSV' option...")
        download_csv_option.click()

        print("Download initiated. Waiting for the CSV file to appear in the temporary directory...")
        downloaded_file_path = None
        for _ in range(240): # Wait time
            csv_files = glob.glob(os.path.join(temp_download_dir, "*.csv"))
            if csv_files:
                downloaded_file_path = csv_files[0]
                print(f"Daily CSV file found: {downloaded_file_path}")
                break
            time.sleep(1)
        else:
            raise FileNotFoundError("Daily CSV file did not appear in the download directory within the expected time.")

        print("Reading daily CSV and adding datetime column...")
        try:
            # Read the daily CSV into a pandas DataFrame
            dtypes_file_path = "screener_dtypes.json"
            with open(dtypes_file_path, 'r') as f:
                data_types = json.load(f)
            new_daily_df = pd.read_csv(downloaded_file_path,dtype=data_types)

            # Convert percentage columns to float
            percent_columns_path = "percent_columns.json"
            with open(percent_columns_path) as f:
                percent_columns = json.load(f)
            for i in percent_columns:
                new_daily_df[i] = new_daily_df[i].str.replace('%', '').astype(float)/100

            # Add a new column with the download datetime
            download_datetime = datetime.now() - timedelta(hours=5)
            new_daily_df['download_datetime'] = download_datetime
            
            # Save the modified DataFrame to the new path
            modified_file_path = os.path.join(temp_download_dir, "modified_" + os.path.basename(downloaded_file_path))
            new_daily_df.to_csv(modified_file_path, index=False)
            print(f"Modified daily CSV saved to: {modified_file_path}")

        except Exception as e:
            print(f"Failed to modify CSV with datetime column: {e}")
            raise

        # Initialize GCS client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        # Upload daily CSV
        daily_blob = bucket.blob(daily_blob_name)
        print(f"Uploading daily CSV {modified_file_path} to gs://{bucket_name}/{daily_blob_name}...")
        daily_blob.upload_from_filename(modified_file_path)
        print(f"Daily CSV uploaded successfully to GCS.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        print("Closing the browser...")
        driver.quit()
        print("Browser closed.")

def main():
    #SET VARIABLES
    import google.auth
    credentials, project_id = google.auth.default() # Inferred project ID
    
    # Retrieve GCS bucket name from Secret Manager
    try:
        gcs_bucket_name = get_secret(project_id, "bucket_name") # Assuming you have a secret named 'bucket_name'
        print(f"Retrieved GCS bucket name: {gcs_bucket_name}")
    except Exception as e:
        print(f"Failed to retrieve GCS bucket name from Secret Manager: {e}")
        raise

    adjusted_time = datetime.now() - timedelta(hours=5)
    today_date_str = adjusted_time.strftime("%Y-%m-%d %H:%M:%S")
    gcs_daily_blob = gcs_bucket_name+"/daily_raw/"+today_date_str+".csv"
    
    # Use a temporary directory for the entire operation
    tdd = tempfile.TemporaryDirectory()
    try:
        # Pass the path string of the temporary directory
        print("Initializing driver...")
        driver = initialize_driver(download_dir=tdd.name, headless=True) # Set headless=False for local debugging with GUI

        # Log in
        print("Loging in...")
        login(driver=driver, project_id=project_id)

        # Download and upload
        print("Downloading CSV...")
        download_stock_screener_csv_to_gcs(
            driver=driver,
            bucket_name=gcs_bucket_name,
            daily_blob_name=gcs_daily_blob,
            temp_download_dir=tdd.name, # Pass the path string
            project_id=project_id
        )
        print("Download was successful...")
    except Exception as e:
        print(f"Script failed: {e}")
        # Ensure driver is quit even if an error occurs before finally block in download function
        if 'driver' in locals() and driver:
            driver.quit()
    finally:
        tdd.cleanup() # Ensure the temporary directory is cleaned up
        print("Temporary directory was cleaned")

if __name__ == "__main__":
    main()