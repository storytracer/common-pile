import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import jsonlines
from tqdm.auto import tqdm
from utils import api_query
from licensed_pile import logs
from lxml import etree
import dotenv
import os
from pathlib import Path
from redislite import Redis
from requests_cache import CachedSession, RedisCache, NEVER_EXPIRE

dotenv.load_dotenv()

redis_directory = Path("data/cache/redis")
redis_directory.mkdir(parents=True, exist_ok=True)

backend = RedisCache(connection=Redis(f"{redis_directory}/session.db"))
session = CachedSession(expire_after=NEVER_EXPIRE, backend=backend)

MODS_NS = {'mods': 'http://www.loc.gov/mods/v3'}

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
    parser.add_argument("--pages", type=int, help="Number of pages to download")
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
            "CRECB", # Congressional Record (Bound Edition)
            # "CRI", # Congressional Record Index [No relevant full text content]
            "CRPT", # Congressional Reports
            "CZIC", # Coastal Zone Information Center
            # "ECFR", # Electronic Code of Federal Regulations [No relevant full text content]
            "ECONI", # Economic Indicators
            # "ERIC", # Education Reports from ERIC [No relevant full text content]
            "ERP", # Economic Report of the President
            "FR", # Federal Register
            "GAOREPORTS", # Government Accountability Office Reports and Comptroller General Decisions
            "GOVMAN", # United States Government Manual [No relevant full text content]
            # "GOVPUB", # Bulk Submission [No relevant full text content]
            "GPO", # Additional Government Publications
            "HJOURNAL", # Journal of the House of Representatives
            "HMAN", # House Rules and Manual
            # "HOB", # History of Bills [No relevant full text content]
            # "LSA", # List of CFR Sections Affected [No relevant full text content]
            # "PAI", # Privacy Act Issuances [No relevant full text content]
            "PLAW", # Public and Private Laws
            "PPP", # Public Papers of the Presidents of the United States
            # "SERIALSET", # Congressional Serial Set [No relevant full text content]
            # "SJOURNAL", # Journal of the Senate [No relevant full text content]
            "SMAN", # Senate Manual
            "STATUTE", # Statutes at Large
            "USCODE", # United States Code
            # "USCOURTS", # United States Courts Opinions [Duplicate of CourtListener]
        ],
    )
    args = parser.parse_args()
    return args


def get_packages(api_key, collections, start_date, page_limit):
    logger = logs.get_logger("usgpo")

    url = f"https://api.govinfo.gov/published/{start_date}"
    offset_mark = "*"
    packages = []
    pbar = tqdm()
    page_counter = 0
    while url is not None:
        response = api_query(
            url,
            headers={"accept": "application/json"},
            params={
                "api_key": api_key,
                "offsetMark": offset_mark,
                "pageSize": 1000,
                "collection": ",".join(collections),
            },
        )

        if response.status_code == 200:
            output = response.json()
            page_counter += 1

            for record in output["packages"]:
                packages.append(record)
                pbar.update(1)

            if page_limit and page_counter >= page_limit:
                break
            else:
                url = output["nextPage"]
                offset_mark = None
                
        else:
            logger.error(
                f"get_packages received status code {response.status_code} for query {url}"
            )
            break
    return packages

def mods_find(element, path):
    """Helper function to find a single element with MODS namespace."""
    return element.find(path, namespaces=MODS_NS)

def mods_findall(element, path):
    """Helper function to find all elements with MODS namespace."""
    return element.findall(path, namespaces=MODS_NS)

def mods_findtext(element, path):
    """Helper function to find text of an element with MODS namespace."""
    return element.findtext(path, namespaces=MODS_NS)

def get_text(element):
    """Helper function to get text from an element."""
    if element is not None:
        return element.text
    return None

def extract_links(root):
    """Extract links from the location element."""
    if root is None:
        return None

    links = {}
    for url_elem in mods_findall(root, 'mods:url'):
        display_label = url_elem.get('displayLabel')
        url = url_elem.text
        if display_label and url:
            links[display_label] = url
    return links

def extract_name(root, name_role):
    """Extract the name of the author or publisher based on role."""
    for name_elem in mods_findall(root, 'mods:name'):
        role_terms = mods_findall(name_elem, 'mods:role/mods:roleTerm')
        for role_term in role_terms:
            if role_term.get('type') == 'text' and role_term.text == name_role:
                name_parts = mods_findall(name_elem, 'mods:namePart')
                name = ' '.join([np.text for np in name_parts if np.text])
                return name
    return None

def extract_title(root):
    """Construct the full title from MODS elements."""
    search_title = mods_findtext(root, './mods:extension/mods:searchTitle')
    if search_title is not None:
        stripped_lines = "".join([line.strip() for line in search_title.splitlines()]).rstrip(";")
        parts = stripped_lines.split(";")
        stripped_parts = [part.strip() for part in parts]
        cleaned_search_title = " – ".join(stripped_parts)
        title = cleaned_search_title
        
        return title
    
    title_info = mods_find(root, './mods:titleInfo')
    if title_info is not None:
        title = " ".join([line.strip() for line in ''.join(title_info.itertext()).strip().splitlines()])
        
        return title
    
    return None

def extract_granules(root, package_id):
    """Extract granule information from related items."""
    granules = []
    related_items = mods_findall(root, 'mods:relatedItem')
    for item in related_items:
        if item.get('type') == 'constituent':
            location_elem = mods_find(item, 'mods:location')
            links = extract_links(location_elem)

            extension_elements = mods_findall(item, 'mods:extension')

            granule_id = None
            sequence_no = None
            granule_class = None

            for extension in extension_elements:
                if granule_id is None:
                    access_id_elem = mods_find(extension, 'mods:accessId')
                    granule_id = get_text(access_id_elem)
                if granule_class is None:
                    granule_class_elem = mods_find(extension, 'mods:granuleClass')
                    granule_class = get_text(granule_class_elem)
                if sequence_no is None:
                    sequence_no_elem = mods_find(extension, 'mods:sequenceNumber')
                    sequence_no_text = get_text(sequence_no_elem)
                    sequence_no = int(sequence_no_text) if sequence_no_text is not None else None

            granule = {
                "package_id": package_id,
                "granule_id": granule_id,
                "sequence_no": sequence_no,
                "granule_class": granule_class,
                "title": extract_title(item),
                "links": links,
            }
            granules.append(granule)

    if not granules:
        return None

    granules.sort(key=lambda x: x["sequence_no"] or 0)
    return granules

def extract_html_links(record):
    html_links = []
    record_links = record.get("links")
    if record_links is not None:
        key = "HTML rendition"
        package_html = record_links.get(key)
        if package_html is not None:
            html_links.append(package_html)
        else:
            granules = record.get("granules")
            if granules is not None:
                for granule in record["granules"]:
                    granuleLinks = granule.get("links")
                    if granuleLinks is not None:
                        granuleHTML = granuleLinks.get(key)
                        if granuleHTML is not None:
                            html_links.append(granuleHTML)
                        
    if len(html_links) == 0:
        return None
                        
    return html_links

def get_package_metadata(package):
    package_id = package["packageId"]
    mods_url = f"https://www.govinfo.gov/metadata/pkg/{package_id}/mods.xml"
    response = session.get(mods_url)
    if response.status_code == 200:
        mods_root = etree.fromstring(response.content)
        package_title = package["title"]

        extension_elements = mods_findall(mods_root, './mods:extension')

        collection_code = None
        access_id = None

        for extension in extension_elements:
            if collection_code is None:
                doc_class_elem = mods_find(extension, 'mods:collectionCode')
                collection_code = get_text(doc_class_elem)
            if access_id is None:
                access_id_elem = mods_find(extension, 'mods:accessId')
                access_id = get_text(access_id_elem)

        origin_info = mods_find(mods_root, 'mods:originInfo')
        location_elem = mods_find(mods_root, 'mods:location')
        links = extract_links(location_elem)

        record = {
            "collection": collection_code,
            "package_id": access_id or package_id,
            "title": extract_title(mods_root) or package_title,
            "date": mods_findtext(origin_info, 'mods:dateIssued') if origin_info is not None else None,
            "author": extract_name(mods_root, "author"),
            "publisher": extract_name(mods_root, "publisher"),
            "links": links,
            "granules": extract_granules(mods_root, package_id),
        }
        record["html_links"] = extract_html_links(record)
        return record

def main(args):
    logger = logs.get_logger("usgpo")
    os.makedirs(args.output_dir, exist_ok=True)

    # Get packages from the specified USGPO collections from `args.start_date` to current day
    logger.info(f"Getting packages from the following collections: {args.collections}")
    packages = get_packages(args.api_key, args.collections, args.start_date, args.pages)

    logger.info(f"Getting package metadata and writing out to {args.output_dir}")
    with jsonlines.open(
        os.path.join(args.output_dir, "links.jsonl"), mode="w", flush=True
    ) as writer:
        # Spawn multiple worker threads to get the metadata associated with all packages
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            metadata_futures_to_package = {
                executor.submit(get_package_metadata, package): package
                for package in packages
            }

            # Write out package metadata to file
            for metadata_future in tqdm(as_completed(metadata_futures_to_package)):
                package = metadata_futures_to_package[metadata_future]
                try:
                    record = metadata_future.result()
                    if record:
                        writer.write(record)
                except Exception as e:
                    logger.error(f"Package {package} raised exception {e}")
                    continue

if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)