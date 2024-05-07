"""Utilities for parsing news data."""

import re

from bs4 import BeautifulSoup, NavigableString
from usp.tree import sitemap_tree_for_homepage

FORMATTED_STRING_TAGS = ("em", "a", "i", "strong", "span")


def build_url_index(base_url, keyword=None):
    if keyword is None:
        keyword = [""]
    elif isinstance(keyword, str):
        keyword = [keyword]

    tree = sitemap_tree_for_homepage(base_url)
    page_index = [
        page.url
        for key in keyword
        for idx, page in enumerate(tree.all_pages())
        if key in page.url
    ]
    return page_index


def parse_page(
    html, tag="div", attrs=None, formatted_string_tags=FORMATTED_STRING_TAGS
):
    soup = BeautifulSoup(html, "html.parser")
    attrs = attrs if attrs is not None else {}

    text = [soup.title.get_text() if soup.title else ""]
    # Search for author
    author = None
    # Calling `re.compile(...)` repeatedly in a this processing function (once
    # for each document) is ok because the resulting compiled re object is cached and reused
    if byline := soup.find(
        class_=re.compile("(byline|post-author|posted-by|article__source|author)")
    ):
        author = byline.get_text().strip()
        text.append(author)

    # Search for dateline
    date = None
    if dateline := soup.find(
        "time", class_=re.compile("(title|entry-date|date|timestamp|time)")
    ):
        date = dateline.get_text().strip()
        text.append(date)
    elif dateline := soup.find("div", class_=re.compile("(timestamps|post-date|date)")):
        date = dateline.get_text().strip()
        text.append(date)
    elif dateline := soup.find("span", class_=re.compile("(date|posted-on)")):
        date = dateline.get_text().strip()
        text.append(date)
    elif dateline := soup.find(class_=re.compile("article__date")):
        date = dateline.get_text().strip()
        text.append(date)
    elif dateline := soup.find("time"):
        date = dateline.get_text().strip()
        text.append(date)

    # Adapted from
    # https://github.com/bltlab/mot/blob/63ef942f2a4cc7fff5823b4cdefbccc5c7464b5f/extraction/extracttext.py#L540-L558
    article = soup.find_all(tag, attrs={k: re.compile(v) for k, v in attrs.items()})
    for a in article:
        p_tag = a.find_all("p")
        for p in p_tag:
            split_p = []
            text_pieces = []
            for child in p.children:
                if isinstance(child, NavigableString):
                    text_pieces.extend(child.split("\n"))
                elif child.name == "br":
                    split_p.append("".join(text_pieces))
                    text_pieces = []
                elif child.name in formatted_string_tags:
                    text_pieces.extend(child.get_text())

            # Remaining pieces
            if text_pieces:
                split_p.append("".join(text_pieces))
            text_article = [
                article_paragraph
                for s in split_p
                if is_valid(article_paragraph := s.strip()) and article_paragraph
            ]
            text.extend(text_article)

    return "\n".join(text), date, author


def is_valid(text: str) -> bool:
    """
    Simple check to eliminate and filter obviously bad text in paragraph tags.
    """
    text = text.strip()
    text = " ".join(text.split())
    if not text:
        return False
    elif text.startswith("Attention Required! | Cloudflare"):
        return False
    elif text.startswith("403 Forbidden"):
        return False
    else:
        return True


def url_to_filename(url: str) -> str:
    """Remove parts of url string we don't want or can't use as a filename"""
    base = (
        url.replace("?", "_")
        .replace(",", "_")
        .replace("=", "_")
        .replace("https://www.", "")
        .replace("http://www.", "")
        .replace("https://", "")
        .replace("/", "_")
    )
    return re.sub(r"\s+", "_", base)
