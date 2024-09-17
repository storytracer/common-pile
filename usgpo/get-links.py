import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import jsonlines
from tqdm.auto import tqdm
from utils import api_query

from licensed_pile import logs


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in ISO8601 format (yyyy-MM-dd'T'HH:mm:ss'Z')",
    )
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=20, help="Number of threads")
    parser.add_argument(
        "--collections",
        nargs="+",
        default=[
            "BILLS", # Congressional Bills
            # "BILLSTATUS", # Congressional Bill Status [No relevant full text content]
            # "BILLSUM", Congressional Bill Summaries [No relevant full text content]
            "BUDGET", # United States Budget
            "CCAL", # Congressional Calendars
            "CDIR", # Congressional Directory
            "CDOC", # Congressional Documents
            "CFR", # Code of Federal Regulations
            "CHRG", # Congressional Hearings
            # "CMR", # Congressionally Mandated Reports [No relevant full text content]
            # "COMPS", # Statutes Compilations [No relevant full text content]
            "CPD", # Compilation of Presidential Documents
            "CPRT", # Congressional Committee Prints 
            "CREC", # Congressional Record
            # "CRECB", # Congressional Record (Bound Edition) [Mostly duplicate of CREC]
            # "CRI", # Congressional Record Index [No relevant full text content]
            "CRPT", # Congressional Reports
            # "CZIC", # Coastal Zone Information Center [No relevant full text content]
            # "ECFR", # Electronic Code of Federal Regulations [No relevant full text content]
            # "ECONI", # Economic Indicators [No relevant full text content]
            # "ERIC", # Education Reports from ERIC [No relevant full text content]
            # "ERP", # Economic Report of the President [No relevant full text content]
            "FR", # Federal Register
            "GAOREPORTS", # Government Accountability Office Reports and Comptroller General Decisions
            # "GOVMAN", # United States Government Manual [No relevant full text content]
            # "GOVPUB", # Bulk Submission [No relevant full text content]
            "GPO", # Additional Government Publications
            # "HJOURNAL", # Journal of the House of Representatives [No relevant full text content]
            # "HMAN", # House Rules and Manual [No relevant full text content]
            # "HOB", # History of Bills [No relevant full text content]
            # "LSA", # List of CFR Sections Affected [No relevant full text content]
            # "PAI", # Privacy Act Issuances [No relevant full text content]
            "PLAW", # Public and Private Laws
            # "PPP", # Public Papers of the Presidents of the United States [No relevant full text content]
            # "SERIALSET", # Congressional Serial Set [No relevant full text content]
            # "SJOURNAL", # Journal of the Senate [No relevant full text content]
            # "SMAN", # Senate Manual [No relevant full text content]
            "STATUTE", # Statutes at Large
            "USCODE", # United States Code
            # "USCOURTS", # United States Courts Opinions [Not included to avoid duplication with CourtListener]
        ],
    )
    args = parser.parse_args()
    return args


def get_packages(api_key, collections, start_date):
    logger = logs.get_logger("usgpo")

    url = f"https://api.govinfo.gov/published/{start_date}"
    offset_mark = "*"
    packages = []
    pbar = tqdm()
    while url is not None:
        response = api_query(
            url,
            headers={"accept": "application/json"},
            params={
                "api_key": args.api_key,
                "offsetMark": offset_mark,
                "pageSize": 1000,
                "collection": ",".join(collections),
            },
        )
        if response.status_code == 200:
            output = response.json()

            for record in output["packages"]:
                packages.append(record)
                pbar.update(1)

            url = output["nextPage"]
            offset_mark = None
            # Sleep since a sudden burst of requests seems to result in erroneous rate-limiting
            time.sleep(5)
        else:
            logger.error(
                f"get_packages received status code {response.status_code} for query {url}"
            )
            break
    return packages


def get_file_links(api_key, package):
    package_id = package["packageId"]
    response = api_query(
        f"https://api.govinfo.gov/packages/{package_id}/summary",
        headers={"accept": "application/json"},
        params={"api_key": args.api_key},
    )
    if response.status_code == 200:
        output = response.json()
        return output.get("download")
    return None


def get_package_metadata(api_key, package):
    record = {
        "title": package.get("title"),
        "package_id": package.get("packageId"),
        "date": package.get("dateIssued"),
        "category": package.get("category"),
        "author": package.get("governmentAuthor1"),
        "publisher": package.get("publisher"),
        "links": get_file_links(api_key, package),
    }
    return record


def main(args):
    logger = logs.get_logger("usgpo")
    os.makedirs(args.output_dir, exist_ok=True)

    # Get packages from the specified USGPO collections from `args.start_date` to current day
    logger.info(f"Getting packages from the following collections: {args.collections}")
    packages = get_packages(args.api_key, args.collections, args.start_date)

    logger.info(f"Getting package metadata and writing out to {args.output_dir}")
    with jsonlines.open(
        os.path.join(args.output_dir, "links.jsonl"), mode="w", flush=True
    ) as writer:
        # Spawn multiple worker threads to get the metadata associated with all packages
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            metadata_futures_to_package = {
                executor.submit(get_package_metadata, args.api_key, package): package
                for package in packages
            }

            # Write out package metadata to file
            for metadata_future in tqdm(as_completed(metadata_futures_to_package)):
                package = metadata_futures_to_package[metadata_future]
                try:
                    record = metadata_future.result()
                except Exception as e:
                    logger.error(f"Package {package} raised exception {e}")
                    continue
                writer.write(record)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
