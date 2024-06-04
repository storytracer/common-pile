import datetime
import functools
import json
import multiprocessing.dummy as mp
import unicodedata
from pathlib import Path

import click
import duckdb
import msgspec
import pandas as pd
from internetarchive import ArchiveSession
from tqdm import tqdm

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_PATH = Path(__file__).resolve().parent.parent
METADATA_PATH = SOURCE_PATH / "data" / "metadata"
IA_METADATA_PATH = METADATA_PATH / "ia"
DATASET_NAME = "hathi_ia_pd_us_1929"
DATASET_METADATA_PATH = IA_METADATA_PATH / DATASET_NAME
DATASET_BOOKS_PATH = SOURCE_PATH / "data" / "books" / "ia" / DATASET_NAME
DATASET_DOLMA_PATH = SOURCE_PATH / "data" / "dolma" / DATASET_NAME / "documents"

MAX_PARALLEL_DOWNLOADS = 10

session = ArchiveSession()
logger = logs.get_logger("public_library")


@click.group()
@click.option("--hathi-metadata-parquet", type=click.Path(exists=True), required=True)
@click.pass_context
def cli(ctx, hathi_metadata_parquet):
    ctx.ensure_object(dict)
    ctx.obj["hathi_metadata_parquet"] = hathi_metadata_parquet
    logs.configure_logging("public_library")


def fetch_metadata(ark_hash):
    query = f"identifier-ark:*{ark_hash}"
    json_path = DATASET_METADATA_PATH / f"{ark_hash}.json"

    try:
        results = session.search_items(query=query, max_retries=3)
        for item in results:
            ocaid = item.get("identifier")
            if ocaid:
                metadata = session.get_item(ocaid).metadata
                if metadata and not json_path.exists():
                    with open(json_path, "w") as f:
                        json.dump(metadata, f)
                    return True
    except Exception as e:
        logger.error(f"{ark_hash}: {str(e)}")
    return False


def hashes_to_download(ark_hashes):
    existing_hashes = {path.stem for path in DATASET_METADATA_PATH.glob("*.json")}
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
            for _ in results:
                pbar.update(1)


@cli.command()
def export_jsonl():
    metadata_files = DATASET_METADATA_PATH.glob("*.json")
    metadata_jsonl_path = IA_METADATA_PATH / f"{DATASET_NAME}.jsonl"
    with open(metadata_jsonl_path, "w") as metadata_jsonl_file:
        for metadata_file_path in metadata_files:
            with open(metadata_file_path, "r") as metadata_file:
                data = json.dumps(json.load(metadata_file))
                metadata_jsonl_file.write(data + "\n")


@cli.command()
@click.pass_context
def export_duckdb(ctx):
    ia_metadata_jsonl_path = IA_METADATA_PATH / f"{DATASET_NAME}.jsonl"
    db = IA_METADATA_PATH / f"{DATASET_NAME}.duckdb"
    db.unlink(missing_ok=True)
    con = duckdb.connect(str(db))
    downloaded_ark_hashes = [
        metadata_file.stem for metadata_file in DATASET_METADATA_PATH.glob("*.json")
    ]
    downloaded_ocaids = [
        path.stem.split("_djvu")[0] for path in DATASET_BOOKS_PATH.glob("*_djvu.txt")
    ]
    downloaded_ark_hashes_df = pd.DataFrame(downloaded_ark_hashes, columns=["ark_hash"])
    downloaded_ocaids_df = pd.DataFrame(downloaded_ocaids, columns=["ocaid"])
    con.execute(
        f"CREATE TABLE metadata_hathi AS SELECT * FROM '{ctx.obj['hathi_metadata_parquet']}'"
    )
    con.execute(
        f"CREATE TABLE metadata_ia AS SELECT * FROM read_json('{ia_metadata_jsonl_path}', ignore_errors=True, union_by_name=True)"
    )
    con.execute(
        "CREATE TABLE metadata_ids (ark_hash VARCHAR, htid VARCHAR, ia_ark_id VARCHAR, ocaid VARCHAR)"
    )
    con.execute(
        "CREATE TABLE downloads (ocaid VARCHAR, htid VARCHAR, ia_ark_id VARCHAR, title VARCHAR, author VARCHAR, year INT, place VARCHAR, language VARCHAR)"
    )
    con.execute(
        "INSERT INTO metadata_ids (ark_hash) SELECT ark_hash FROM downloaded_ark_hashes_df"
    )
    con.execute(
        "UPDATE metadata_ids SET htid = (SELECT htid FROM metadata_hathi WHERE ends_with(htid, ark_hash) = TRUE)"
    )
    con.execute(
        'UPDATE metadata_ids SET ia_ark_id = (SELECT "identifier-ark" FROM metadata_ia WHERE ends_with("identifier-ark", ark_hash) = TRUE)'
    )
    con.execute(
        'UPDATE metadata_ids SET ocaid = (SELECT identifier FROM metadata_ia WHERE "identifier-ark" = ia_ark_id)'
    )
    con.execute("INSERT INTO downloads (ocaid) SELECT ocaid FROM downloaded_ocaids_df")
    con.execute(
        "UPDATE downloads SET htid = (SELECT htid FROM metadata_ids WHERE ocaid = downloads.ocaid)"
    )
    con.execute(
        "UPDATE downloads SET ia_ark_id = (SELECT ia_ark_id FROM metadata_ids WHERE ocaid = downloads.ocaid)"
    )
    con.execute(
        "UPDATE downloads SET title = (SELECT title FROM metadata_hathi WHERE htid = downloads.htid)"
    )
    con.execute(
        "UPDATE downloads SET author = (SELECT author FROM metadata_hathi WHERE htid = downloads.htid)"
    )
    con.execute(
        "UPDATE downloads SET year = (SELECT rights_date_used FROM metadata_hathi WHERE htid = downloads.htid)"
    )
    con.execute(
        "UPDATE downloads SET place = (SELECT pub_place FROM metadata_hathi WHERE htid = downloads.htid)"
    )
    con.execute(
        "UPDATE downloads SET language = (SELECT lang FROM metadata_hathi WHERE htid = downloads.htid)"
    )
    con.close()


@cli.command()
def export_parquet():
    db = IA_METADATA_PATH / f"{DATASET_NAME}.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        f"COPY metadata_ids TO '{IA_METADATA_PATH / (DATASET_NAME + '_metadata_ids.parquet')}'"
    )
    con.execute(
        f"COPY metadata_hathi TO '{IA_METADATA_PATH / (DATASET_NAME + '_metadata_hathi.parquet')}'"
    )
    con.execute(
        f"COPY metadata_ia TO '{IA_METADATA_PATH / (DATASET_NAME + '_metadata_ia.parquet')}'"
    )
    con.execute(
        f"COPY downloads TO '{IA_METADATA_PATH / (DATASET_NAME + '_downloads.parquet')}'"
    )
    con.close()


def format_dolma(df_row):
    row = df_row[1]
    ocaid = row["ocaid"]
    filepath = DATASET_BOOKS_PATH / f"{ocaid}_djvu.txt"
    if filepath.exists():
        with open(filepath, encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()
            json_encoded_text = msgspec.json.encode(raw_text)
            json_decoded_text = msgspec.json.decode(json_encoded_text)
            text = json_decoded_text

        dolma_data = {
            "id": ocaid,
            "text": text,
            "source": "public_library",
            "added": datetime.datetime.utcnow().isoformat(),
            "metadata": {
                "title": row["title"],
                "author": row["author"],
                "year": row["year"],
                "place": row["place"],
                "language": row["language"],
                "htid": row["htid"],
                "ia_ark_id": row["ia_ark_id"],
                "license": str(PermissiveLicenses.PD),
                "hathi_url": f"https://babel.hathitrust.org/cgi/pt?id={row['htid']}",
                "ia_url": f"https://archive.org/details/{ocaid}",
                "text_file_url": f"https://archive.org/download/{ocaid}/{ocaid}_djvu.txt",
            },
        }

        return dolma_data


@cli.command()
def export_dolma(shard_size=1):
    db = IA_METADATA_PATH / f"{DATASET_NAME}.duckdb"
    con = duckdb.connect(str(db))
    downloads_df = con.execute("SELECT * FROM downloads").fetchdf()
    results = map(functools.partial(format_dolma), downloads_df.iterrows())

    DATASET_DOLMA_PATH.mkdir(parents=True, exist_ok=True)

    to_dolma(
        results,
        DATASET_DOLMA_PATH,
        "public_library_hathi_ia_pd_us_1929.jsonl.gz",
        shard_size,
    )
    logger.info(
        f"Exported {len(downloads_df)} text files in dolma format to {DATASET_DOLMA_PATH}"
    )


@cli.command()
def generate_links():
    parquet_path = IA_METADATA_PATH / f"{DATASET_NAME}.parquet"
    df = pd.read_parquet(parquet_path, columns=["identifier"])
    link_template = "https://archive.org/download/{ocaid}/{ocaid}_djvu.txt"
    with open(IA_METADATA_PATH / f"{DATASET_NAME}_links.txt", "w") as f:
        for ocaid in df["identifier"]:
            link = link_template.format(ocaid=ocaid)
            f.write(link + "\n")


def download_metadata(parquet_file):
    try:
        df = pd.read_parquet(parquet_file)
        ark_hashes = df["htid"].apply(lambda x: x.split("/")[-1]).tolist()
        download_metadata_for_ark_hashes(ark_hashes)
    except Exception as e:
        logger.error(f"An error occurred processing {parquet_file}: {str(e)}")


if __name__ == "__main__":
    cli()
