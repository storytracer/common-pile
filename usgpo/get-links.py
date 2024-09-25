import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import jsonlines
from tqdm.auto import tqdm
from utils import api_query
from licensed_pile import logs
import xmltodict
import dotenv
import os
from redislite import Redis
from requests_cache import CachedSession, RedisCache, NEVER_EXPIRE

dotenv.load_dotenv()

backend = RedisCache(connection=Redis("data/redis/session.db"))
session = CachedSession(expire_after=NEVER_EXPIRE, backend=backend)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in ISO8601 format (yyyy-MM-dd'T'HH:mm:ss'Z')",
    )
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=50, help="Number of threads")
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

            for record in output["packages"]:
                packages.append(record)
                pbar.update(1)
                page_counter += 1

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

def merge_tag(element):
    if element is None:
        return None
    
    data = {}
    if isinstance(element, list):
        for element in element:
            data.update(element)
    else:
        data = element
    return data

def read_links(element):
    if element is None:
        return None
    
    data = {}
    if isinstance(element, list):
        for element in element:
            display_label = element.get("@displayLabel")
            url = read_text(element)
            data[display_label] = url
    return data

def read_list(element):
    if element is None:
        return []
    
    if isinstance(element, list):
        return element
    
    return [element]

def read_name(element, name_role):
    names = []
    if isinstance(element, list):
        names = element
    elif element is not None:
        names = [element]
        
    for name in names:
        roles = read_list(name.get("role"))
        for role in roles:
            roleTerms = read_list(role.get("roleTerm"))
            for roleTerm in roleTerms:
                if roleTerm.get("@type") == "text":
                    role = roleTerm.get("#text")
                    if role == name_role:
                        nameParts = read_list(name.get("namePart"))
                        authorName = " ".join(nameParts)
                        
                        return authorName
                    
def read_title(element, host_title=None):
    full_title = None
    title = None
    
    if host_title is None:
        citation = None
        citation_key = "preferred citation"
        
        identifiers = read_list(element.get("identifier"))
        if identifiers:
            for id in identifiers:
                if id.get("@type") == citation_key:
                    citation = id.get("#text")
                    break
                
        if citation is None:
            related_items = read_list(element.get("relatedItem"))
            host_items = [item for item in related_items if item.get("@type") == "host"]
            if len(host_items) > 0:
                for item in host_items:
                    host_identifiers = read_list(item.get("identifier"))
                    for id in host_identifiers:
                        if id.get("@type") == citation_key:
                            citation = id.get("#text")
                            break
        
        if citation:
            host_title = citation
        
    extension = merge_tag(element.get("extension"))
    if extension:
        shortTitle = read_text(extension.get("shortTitle"))
        if shortTitle is not None:
            title = shortTitle
        else:
            titleInfo = element.get("titleInfo")
            if titleInfo:
                titleInfoTitle = titleInfo.get("title")
                part_number = titleInfo.get("partNumber")
                
                if titleInfoTitle is None and part_number:
                    title = part_number
                elif titleInfoTitle is not None and part_number is None:
                    title = titleInfoTitle
                elif titleInfoTitle is not None and part_number is not None:
                    if titleInfoTitle == part_number:
                        title = titleInfoTitle
                    else:
                        title = f"{part_number}: {titleInfoTitle}"
            else:
                searchTitle = read_text(extension.get("searchTitle"))
                if searchTitle is not None:
                    full_title = searchTitle
    
    if full_title is None:
        if host_title and title:
            full_title = f"{host_title} – {title}"
        elif title:
            full_title = title
    
    return full_title

def read_text(element):
    if element is None:
        return None
    
    if isinstance(element, str):
        return element
    
    tags = []
    if isinstance(element, dict):
        tags = [element]
    
    if isinstance(element, list):
        tags = element
    
    if len(tags) > 0:
        textParts = []
        for tag in tags:
            if isinstance(tag, str):
                textParts.append(tag)
            elif isinstance(tag, dict):      
                tag_text = tag.get("#text")
                if tag_text is not None:
                    textParts.append(tag_text)
        text = " ".join(textParts)
        return text
    
    return None

def read_granules(elements, package_id, host_title=None):
    if len(elements) == 0:
        return None
    
    granules = []
    for element in elements:
        type = element.get("@type")
        if type and type == "constituent":
            location = element.get("location").get("url")
            extension = merge_tag(element.get("extension"))
            sequence_no = int(extension.get("sequenceNumber")) if extension.get("sequenceNumber") else 0
            granule = {
                "package_id": package_id,
                "granule_id": extension.get("accessId"),
                "sequence_no": sequence_no,
                "granule_class": extension.get("granuleClass"),
                "title": read_title(element, host_title),
                "links": read_links(location),
            }
            granules.append(granule)
    granules = sorted(granules, key=lambda x: (x["sequence_no"]))
            
    if len(granules) == 0:
        return None
    
    return granules

def read_html_links(record):
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
        record = {}
        metadata = xmltodict.parse(response.text)
        mods = metadata.get("mods")
        extension = merge_tag(mods.get("extension"))
        originInfo = merge_tag(mods.get("originInfo"))
        location = merge_tag(mods.get("location"))
        links = read_links(location.get("url")) if location and location.get("url") else None
        relatedItems = read_list(mods.get("relatedItem"))
        name = mods.get("name")
        package_title = read_title(mods)
        record = {
            "collection": extension.get("docClass"),
            "package_id": extension.get("accessId"),
            "title": package_title,
            "date": read_text(originInfo.get("dateIssued")),
            "author": read_name(name, "author"),
            "publisher": read_name(name, "publisher"),
            "links": links,
            "granules": read_granules(relatedItems, package_id, package_title),
        }
        record["html_links"] = read_html_links(record)
        # print(record)
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
                except Exception as e:
                    logger.error(f"Package {package} raised exception {e}")
                    continue
                writer.write(record)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
