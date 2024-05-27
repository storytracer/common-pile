import json
import os
from pathlib import Path
from time import time

import click
import pandas as pd
import requests
from dateutil.parser import parse
from furl import furl
from tqdm import tqdm

from licensed_pile import logs

data_path = Path(__file__).resolve().parent / "data"
downloads_path = data_path / "downloads"
exports_path = data_path / "exports"

downloads_path.mkdir(parents=True, exist_ok=True)
exports_path.mkdir(parents=True, exist_ok=True)

logger = logs.get_logger("books_metadata")


class BooksMetadataSource:
    def __init__(self, source_url):
        self.source_url = source_url
    
    def download(self):
        pass
    
    def export(self):
        # Implement export to parquet using DuckDB
        pass
    
    def db(self):
        # Return the appropriate DuckDB instance querying all parquet files
        pass
    
class BooksMetadataSourceHathifiles(BooksMetadataSource):
    def __init__(self, source_url):
        super().__init__(source_url)
    
    def download(self):
        # Implement the download logic for Hathifiles source
        pass
    
    def export(self):
        # Implement the export logic for Hathifiles source
        pass


class BooksMetadataDownloader:
    def __init__(self, source, snapshot):
        pass
    
    def start_download(self):
        base_furl = furl(self.base_url)
        logger.info(f"Downloading metadata from URL: {base_furl.url}")
        try:
            response = self.session.get(base_furl.url)
            response.raise_for_status()
            json_data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Failed to download base URL:", str(e))


    def check_existing_files(self):
        pass
    
class BooksMetadataExporter:
    def __init__(self, source, snapshot):


@click.group("books_metadata", context_settings={"show_default": True})
def main():
    pass


@main.command()
@click.option("--source", required=True, help="Dataset name")
@click.option("--snapshot", required=True, help="Snapshot name")
def download(base_url, snapshot):
    downloader = BooksMetadataDownloader(base_url, snapshot)
    downloader.start_download()
    logger.info(
        f"Downloaded {downloader.progress_bar.total} pages. {downloader.existing_pages_count} files already exist."
    )


@click.option(
    "--snapshot",
    required=True,
    help="Snapshot name",
)
@main.command()
def export(snapshot):
    parser = BooksMetadataExporter(snapshot)
    df = parser.parse_files()
    export_csv = exports_path / f"{snapshot}.csv"
    df.to_csv(export_csv, index=False)
    logger.info(f"Exported metadata saved to {export_csv}")


if __name__ == "__main__":
    logs.configure_logging("loc_books")
    main()
