import functools
import json
import multiprocessing.dummy as mp
from pathlib import Path

import duckdb
import pandas as pd
from internetarchive import ArchiveSession
from tqdm import tqdm

from licensed_pile import logs

SOURCE_PATH = Path(__file__).resolve().parent.parent
METADATA_PATH = SOURCE_PATH / "data" / "metadata"
IA_METADATA_PATH = METADATA_PATH / "ia"
DATASET_NAME = "hathi_ia_pd_us_1929"
DATASET_PATH = IA_METADATA_PATH / DATASET_NAME

MAX_PARALLEL_DOWNLOADS = 10

session = ArchiveSession()

logger = logs.get_logger("public_library")


def fetch_metadata(ark_hash):
    query = f"identifier-ark:*{ark_hash}"
    json_path = DATASET_PATH / f"{ark_hash}.json"

    try:
        results = session.search_items(query=query, max_retries=3)
        for item in results:
            if item:
                ocaid = item.get("identifier", None)
                if ocaid:
                    metadata = session.get_item(ocaid).metadata
                    if metadata and not json_path.exists():
                        with open(json_path, "w") as f:
                            json.dump(metadata, f)
                        return True
                else:
                    logger.info(f"{ark_hash}: no OCAID in search results")
            else:
                logger.info(f"{ark_hash}: no search results")
    except Exception as e:
        logger.error(f"{ark_hash}: {str(e)}")
        pass

    return False


def hashes_to_download(ark_hashes):
    existing_hashes = [path.stem for path in DATASET_PATH.glob("*.json")]
    return [ark_hash for ark_hash in ark_hashes if ark_hash not in existing_hashes]


def download_metadata_for_ark_hashes(ark_hashes):
    outstanding_hashes = hashes_to_download(ark_hashes)
    total_hashes_count = len(ark_hashes)
    outstanding_hashes_count = len(outstanding_hashes)
    downloaded_hashes_count = total_hashes_count - outstanding_hashes_count

    print(
        f"{downloaded_hashes_count} metadata files already exist. Downloading {outstanding_hashes_count} new files..."
    )
    with tqdm(total=len(ark_hashes), initial=downloaded_hashes_count) as pbar:
        with mp.Pool(MAX_PARALLEL_DOWNLOADS) as pool:
            results = pool.imap(functools.partial(fetch_metadata), outstanding_hashes)
            for result in results:
                pbar.update(1)


def merge_metadata_files():
    metadata_files = IA_METADATA_PATH.glob("*.json")
    metadata_jsonl_file = IA_METADATA_PATH / f"{DATASET_NAME}.jsonl"
    for file in metadata_files:
        with open(file, "r") as f:
            with open(metadata_jsonl_file, "a") as out:
                json.dump(json.load(f), out)
                out.write("\n")


def export_parquet():
    db = IA_METADATA_PATH / f"{DATASET_NAME}.duckdb"
    parquet_path = IA_METADATA_PATH / f"{DATASET_NAME}.parquet"
    con = duckdb.connect(str(db))
    con.execute(f"COPY ia_metadata TO '{parquet_path}'")
    con.close()


def generate_download_links():
    parquet_path = IA_METADATA_PATH / f"{DATASET_NAME}.parquet"
    df = pd.read_parquet(parquet_path, columns=["identifier"])
    ocaids = df["identifier"].tolist()
    link_template = "https://archive.org/download/{ocaid}/{ocaid}_djvu.txt"
    links = [link_template.format(ocaid=ocaid) for ocaid in ocaids]
    with open(IA_METADATA_PATH / f"{DATASET_NAME}_links.txt", "w") as f:
        for link in links:
            f.write(f"{link}\n")


def download_metadata_for_hathitrust_parquet(parquet_file):
    try:
        df = pd.read_parquet(parquet_file)
        ark_hashes = df["htid"].apply(lambda x: x.split("/")[-1]).tolist()
        download_metadata_for_ark_hashes(ark_hashes)
    except Exception as e:
        print(f"An error occurred processing {parquet_file}: {str(e)}")


if __name__ == "__main__":
    logs.configure_logging("public_library")
