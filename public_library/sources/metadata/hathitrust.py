import json
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path

import duckdb
import pandas as pd
import requests
from tqdm import tqdm

import licensed_pile

METADATA_PATH = (
    Path(__file__).resolve().parent.parent.parent / "data" / "metadata" / "hathitrust"
)


def get_filelist():
    """
    Parse the JSON file from the given URL to extract and sort full datasets based on the date in the filename.

    Args:
        url (str): The URL to the JSON file.

    Returns:
        dict: The latest full dataset entry.
    """
    # Load the JSON data from the URL
    url = "https://www.hathitrust.org/files/hathifiles/hathi_file_list.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Failed to load JSON file. Status code: {response.status_code}")
        return None

    # Function to extract date from filename
    def extract_date(filename):
        match = re.search(r"(\d{8})", filename)
        if match:
            return datetime.strptime(match.group(1), "%Y%m%d")
        return None

    # Filter full datasets and sort by date extracted from filename
    full_datasets = [entry for entry in data if entry["full"]]
    full_datasets.sort(key=lambda x: extract_date(x["filename"]), reverse=True)

    # Return the latest full dataset
    return full_datasets[0] if full_datasets else None


def get_latest_dataset(dataset_entry):
    """
    Download the latest full dataset using requests.

    Args:
        dataset_entry (dict): The dataset entry to download.
    """
    if dataset_entry:
        url = dataset_entry["url"]
        local_filename = (
            METADATA_PATH / dataset_entry["filename"]
        )  # Modify the local filename to include the METADATA_PATH
        print(f"Downloading {local_filename} from {url}")

        # Check if the file already exists on disk
        if local_filename.exists():
            print(f"Dataset already exists on disk: {local_filename}")
            return

        response = requests.get(url, stream=True)
        total_size = int(response.headers.get("content-length", 0))

        # Set up tqdm progress bar
        progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)

        with open(local_filename, "wb") as file:
            for data in response.iter_content(chunk_size=4096):
                file.write(data)
                progress_bar.update(len(data))

        progress_bar.close()
        print(f"Download completed: {local_filename}")
    else:
        print("No full dataset found in the JSON file.")


def download_header_file():
    """
    Download the TSV header file from the given URL and save it to the specified path.

    Args:
        url (str): The URL of the TSV header file.
        save_path (str): The path to save the downloaded file.

    Returns:
        bool: True if the download is successful, False otherwise.
    """
    url = "https://www.hathitrust.org/files/hathifiles/hathi_field_list.txt"
    save_path = METADATA_PATH / "hathi_field_list.txt"
    if save_path.exists():
        print(f"Header file already exists on disk: {save_path}")
        return True
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, "wb") as file:
            file.write(response.content)
        print(f"Download completed: {save_path}")
        return True
    else:
        print(f"Failed to download header file. Status code: {response.status_code}")
        return False


def convert_to_parquet():
    # Read the header file into a pandas dataframe
    header_file_path = METADATA_PATH / "hathi_field_list.txt"
    header_df = pd.read_csv(header_file_path, sep="\t")

    # Read the latest dataset file into a DuckDB table
    latest_dataset_file_path = METADATA_PATH / latest_full_dataset["filename"]
    parquet_file_path = METADATA_PATH / latest_dataset_file_path.name.replace(
        ".txt.gz", ".parquet"
    )

    # Check if the parquet file already exists on disk
    if parquet_file_path.exists():
        print(f"Parquet file already exists on disk: {parquet_file_path}")
        return

    # Export the DuckDB table to parquet
    columns_list = ", ".join([f"'{col}'" for col in header_df.columns])
    delimiter = '"\\t"'
    query = f"COPY (SELECT * FROM read_csv_auto('{latest_dataset_file_path}', delim={delimiter}, names = [{columns_list}])) TO '{parquet_file_path}'"

    con = duckdb.connect()
    con.execute(query)
    con.close()


download_header_file()
latest_full_dataset = get_filelist()
get_latest_dataset(latest_full_dataset)
convert_to_parquet()
