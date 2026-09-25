#Define log_recorder() function
from datetime import datetime, timedelta
def log_recorder(message):
    t = datetime.now() - timedelta(hours=5)
    print(f"{t}: {message}")
log_recorder("Screener downloader started execution.")

#Import packages
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
from google.cloud import parametermanager_v1
import pandas as pd
import json
import subprocess

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
    """Retrieves a parameter from Google Cloud Parameter Manager."""
    client = parametermanager_v1.ParameterManagerClient()
    name = client.parameter_version_path(project_id, location, parameter_id, version_id)
    request = parametermanager_v1.GetParameterVersionRequest(name=name)
    response = client.get_parameter_version(request=request)
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

    # Use subprocess to get the Chrome version and extract the major version number
    try:
        version_output = subprocess.check_output(['google-chrome', '--version'], universal_newlines=True)
        chrome_major_version = int(version_output.split(' ')[2].split('.')[0])
        log_recorder(f"Detected Chrome major version: {chrome_major_version}")
    except (FileNotFoundError, IndexError, ValueError) as e:
        log_recorder(f"Could not determine Chrome version: {e}")
        # Fallback to a common major version if autodetection fails
        chrome_major_version = 138
        log_recorder(f"Falling back to a default Chrome major version: {chrome_major_version}")

    log_recorder("Setting up chrome and undetected_chromedriver...")
    download_dir = os.path.abspath(download_dir)
    options = uc.ChromeOptions()
    # Do not set options.headless / add_experimental_option: both leak automation
    # and make Cloudflare stick on "Just a moment...".
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--disable-blink-features=AutomationControlled")
    if headless:
        options.add_argument("--headless=new")

    log_recorder("Launching browser...")
    driver = uc.Chrome(
        options=options,
        version_main=chrome_major_version,
        use_subprocess=True,
    )
    driver.set_page_load_timeout(90)
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": download_dir},
        )
    except Exception as e:
        log_recorder(f"Could not set download directory via CDP: {e}")
    log_recorder("Browser launched.")
    return driver


def wait_for_login_form(driver, timeout=120):
    """Wait out Cloudflare's interstitial, then return the email input."""
    deadline = time.time() + timeout
    last_title = None
    email_selector = (By.CSS_SELECTOR, "form#login_form input[name='email']")
    while time.time() < deadline:
        title = driver.title or ""
        if title != last_title:
            log_recorder(f"Page title: {title!r}")
            last_title = title
        title_l = title.lower()
        if "just a moment" in title_l or "attention required" in title_l:
            time.sleep(2)
            continue
        try:
            email_input = driver.find_element(*email_selector)
            if email_input.is_displayed():
                return email_input
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Login form not found after Cloudflare wait. url={driver.current_url!r} title={driver.title!r}"
    )

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
        email = get_secret(project_id, "v2_finviz_email")
        password = get_secret(project_id, "v2_finviz_password")
        log_recorder("Successfully retrieved credentials from Secret Manager.")
    except Exception as e:
        log_recorder(f"Failed to retrieve secrets from Secret Manager: {e}")
        log_recorder("Please ensure the secrets exist and the VM's service account has 'Secret Manager Secret Accessor' role.")
        raise

    # Warm the Cloudflare cookie on the homepage, then open login.
    log_recorder("Navigating to Finviz homepage...")
    driver.get("https://finviz.com/")
    time.sleep(5)
    log_recorder("Navigating to login page...")
    driver.get("https://finviz.com/login-email")
    try:
        email_input = wait_for_login_form(driver, timeout=120)
    except Exception:
        log_recorder(
            f"Login form not found. url={driver.current_url!r} title={driver.title!r}"
        )
        raise
    time.sleep(2)

    # Fill in credentials
    log_recorder("Filling in credentials...")
    email_input.clear()
    email_input.send_keys(email)
    driver.find_element(By.CSS_SELECTOR, "form#login_form input[name='password']").send_keys(password)

    # Submit the form
    log_recorder("Clicking login button...")
    login_button = driver.find_element(By.CSS_SELECTOR, "form#login_form button[type='submit']")
    login_button.click()
    time.sleep(30)

#working_directory
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
        log_recorder("Navigating to the StockAnalysis screener page...")
        driver.get("https://elite.finviz.com/screener?v=151&ft=4&preset=s151740538")
        '''
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(., 'ML View')]"))
        )

        # Click on the "ML View" tab
        log_recorder("Waiting for the 'ML View' tab to be clickable...")
        ml_view_tab = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'ML View')]"))
        )
        log_recorder("Clicking 'ML View' tab...")
        ml_view_tab.click()
        time.sleep(3)
        '''
        # --- Attempt to click the primary export button that opens the dropdown ---
        download_button_clicked = False
        log_recorder("Attempting to click the 'Export' button to open the dropdown...")

        # Strategy: Try to find a button that *opens* the dropdown.
        # Prioritize a button with exact text 'Download', then one containing 'Download'
        button_xpaths_to_try = [
            "//button[text()='Export']" # Exact text 'Download'
            #"//button[contains(., 'Download')]" # Contains 'Download'
        ]

        for xpath in button_xpaths_to_try:
            try:
                download_trigger_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                log_recorder(f"Clicking button found with XPath: {xpath}...")
                download_trigger_button.click()
                download_button_clicked = True
                time.sleep(2) 
                break
            except Exception:
                log_recorder(f"Button with XPath '{xpath}' not found or not clickable.")
        
        if not download_button_clicked:
            log_recorder("No primary download button found or clickable to open the dropdown. Proceeding, assuming 'Download to CSV' might be directly visible.")

        '''
        if download_button_clicked:
            log_recorder("Waiting for the dropdown menu to appear...")
            try:
                WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'dropdown-menu') or contains(@class, 'sa-dropdown') or @role='menu']")) # Added @role='menu' for more generic dropdown detection
                )
                log_recorder("Dropdown menu detected.")
                time.sleep(2)
            except Exception as e:
                log_recorder(f"Dropdown menu did not appear after clicking download button: {e}")
                log_recorder("Proceeding to 'Download to CSV' option directly.")

        log_recorder("Waiting for the 'Download to CSV' option to appear and be clickable...")
        # Modified XPath to look for both 'a' and 'button' tags
        download_csv_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Download to CSV')] | //button[contains(., 'Download to CSV')]"))
        )

        log_recorder("Clicking 'Download to CSV' option...")
        download_csv_option.click()
        '''

        log_recorder("Download initiated. Waiting for the CSV file to appear in the temporary directory...")
        downloaded_file_path = None
        for _ in range(360): # Wait time
            csv_files = glob.glob(os.path.join(temp_download_dir, "*.csv"))
            if csv_files:
                downloaded_file_path = csv_files[0]
                log_recorder(f"Daily CSV file found: {downloaded_file_path}")
                break
            time.sleep(1)
        else:
            raise FileNotFoundError("Daily CSV file did not appear in the download directory within the expected time.")
        
        '''
        log_recorder("Reading daily CSV and adding datetime column...")
        try:
            # Read the daily CSV into a pandas DataFrame
            dtypes_file_path = working_directory+r"/screener_dtypes.json"
            with open(dtypes_file_path, 'r') as f:
                data_types = json.load(f)
            new_daily_df = pd.read_csv(downloaded_file_path,dtype=data_types)

            # Convert percentage columns to float
            percent_columns_path = working_directory+r"/percent_columns.json"
            with open(percent_columns_path) as f:
                percent_columns = json.load(f)
            for i in percent_columns:
                new_daily_df[i] = new_daily_df[i].str.rstrip('%').astype(float) / 100

            # Add a new column with the download datetime
            download_datetime = datetime.now() - timedelta(hours=5)
            new_daily_df.loc[:, 'download_datetime'] = download_datetime
            
            # Save the modified DataFrame to the new path
            modified_file_path = os.path.join(temp_download_dir, "modified_" + os.path.basename(downloaded_file_path))
            new_daily_df.to_csv(modified_file_path, index=False)
            log_recorder(f"Modified daily CSV saved to: {modified_file_path}")

        except Exception as e:
            log_recorder(f"Failed to modify CSV with datetime column: {e}")
            raise
        '''
        # Initialize GCS client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        # Upload daily CSV
        daily_blob = bucket.blob(daily_blob_name)
        log_recorder(f"Uploading daily CSV {downloaded_file_path} to gs://{bucket_name}/{daily_blob_name}...")
        daily_blob.upload_from_filename(downloaded_file_path)
        log_recorder(f"Daily CSV uploaded successfully to GCS.")

    except Exception as e:
        log_recorder(f"An error occurred: {e}")
        raise
    finally:
        log_recorder("Closing the browser...")
        driver.quit()
        log_recorder("Browser closed.")

def main():
    #SET VARIABLES
    import google.auth
    credentials, project_id = google.auth.default() # Inferred project ID
    
    # Retrieve GCS bucket name from Secret Manager
    try:
        #gcs_bucket_name = get_secret(project_id, "v2_bucket_name")
        gcs_bucket_name = get_parameter(project_id, parameter_id="v2_bucket_name")
        log_recorder(f"Retrieved GCS bucket name: {gcs_bucket_name}")
    except Exception as e:
        log_recorder(f"Failed to retrieve GCS bucket name from Secret Manager: {e}")
        raise

    adjusted_time = datetime.now() - timedelta(hours=5)
    today_date_str = adjusted_time.strftime("%Y-%m-%d %H:%M:%S")
    gcs_daily_blob = gcs_bucket_name+"/daily_raw/"+today_date_str+".csv"
    #working_directory = get_secret(project_id, 'v2_working_directory', version_id="latest")
    #working_directory = get_parameter(project_id, parameter_id="v2_working_directory")
    
    # Use a temporary directory for the entire operation
    tdd = tempfile.TemporaryDirectory()
    try:
        # Pass the path string of the temporary directory
        log_recorder("Initializing driver...")
        driver = initialize_driver(download_dir=tdd.name, headless=True) # Set headless=False for local debugging with GUI

        # Log in
        log_recorder("Loging in...")
        login(driver=driver, project_id=project_id)

        # Download and upload
        log_recorder("Downloading CSV...")
        #working_directory
        download_stock_screener_csv_to_gcs(
            driver=driver,
            bucket_name=gcs_bucket_name,
            daily_blob_name=gcs_daily_blob,
            temp_download_dir=tdd.name,
            project_id=project_id
        )
        log_recorder("Download was successful...")
    except Exception as e:
        log_recorder(f"Script failed: {e}")
        # Ensure driver is quit even if an error occurs before finally block in download function
        if 'driver' in locals() and driver:
            driver.quit()
    finally:
        tdd.cleanup() # Ensure the temporary directory is cleaned up
        log_recorder("Temporary directory was cleaned")

if __name__ == "__main__":
    main()