import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import jsonlines
import trafilatura
import requests
from furl import furl
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_NAME = "usgpo"

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mirror-url", required=False, help="URL to GovInfo file mirror")
    parser.add_argument(
        "--links-file", required=True, help="Path to links file (jsonl)"
    )
    parser.add_argument(
        "--output-dir",
        default=f"data/{SOURCE_NAME}/v0",
        help="Path to output directory",
    )
    parser.add_argument(
        "--filename",
        default="usgpo.jsonl.gz",
        help="The base filename for the USGPO Dolma dataset",
    )
    parser.add_argument(
        "--shard-size", type=int, default=1, help="Size, in GB, for each shard"
    )
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    args = parser.parse_args()
    return args


def download_file(file_url, mirror_url):
    response = None
    
    if mirror_url is not None:
        file_furl = furl(file_url)
        mirror_furl = furl(mirror_url)
        
        file_path = file_furl.path
        mirror_furl.path = mirror_furl.path / file_path
        
        try:
            response = requests.get(mirror_furl.url)
        except Exception as e:
            response = None
            raise Exception(f"Could not download file from mirror: {mirror_furl.url}")
        
    if response is None or response.status_code != 200:
        try:
            response = requests.get(file_url)
        except Exception as e:
            response = None
            raise Exception(f"Could not download file from govinfo.gov: {file_url}")
        
    if response is None or response.status_code != 200:
        return None

    text = response.text
    return text


def parse_html(html):
    # Most documents are pre-formatted text inside of the a <pre> tag
    # For the rest of the documents, we use trafilatura to extract to markdown
    soup = BeautifulSoup(html, "html.parser")
    pre_tag = soup.find("pre")
    if pre_tag:
        text = pre_tag.get_text().strip()
    else:
        text = trafilatura.extract(html, output_format="markdown")
    return text


def construct_record(mirror_url, file):
    logger = logs.get_logger("usgpo")
    records = []
    
    try:
        html_links = file.get("html_links")
        if html_links is None or len(html_links) == 0:
            return records
        
        if len(html_links) == 1:
            link = html_links[0]
            
            html = download_file(link, mirror_url)
            text = parse_html(html)
            
            record = {
                "collection": file["collection"],
                "id": file["package_id"],
                "title": file["title"],
                "date": file["date"],
                "author": file["author"],
                "text": text,
                "source": SOURCE_NAME,
                "added": datetime.datetime.utcnow().isoformat(),
                "metadata": {"license": str(PermissiveLicenses.PD), "url": link},
            }
            records.append(record)
        elif len(html_links) > 1:
            granules = file.get("granules")
            if granules is None:
                return records
            
            for granule in granules:
                links = granule.get("links")
                if links:
                    html_link = links.get("HTML rendition")
                    if html_link:
                        link = html_link
                        
                        html = download_file(link, mirror_url)
                        text = parse_html(html)
                        id = f"{file['package_id']}/{granule['granule_id']}"
                
                        record = {
                            "collection": file["collection"],
                            "id": id,
                            "title": file["title"],
                            "date": file["date"],
                            "author": file["author"],
                            "text": text,
                            "source": SOURCE_NAME,
                            "added": datetime.datetime.utcnow().isoformat(),
                            "metadata": {"license": str(PermissiveLicenses.PD), "url": link},
                        }
                        
                        records.append(record)
    except Exception as e:
        logger.error(f"Failed to download package {file['package_id']}: {e}")
        return records
                
    return records


def generate_records(args):
    with jsonlines.open(args.links_file, mode="r") as reader:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(construct_record, args.mirror_url, file) for file in reader
            ]
            for future in as_completed(futures):
                records = future.result()
                for record in records:
                    if record is not None:
                        yield record


def main(args):
    to_dolma(generate_records(args), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
