#!/usr/bin/env sh

# CC BY
python parse_pages.py --license CC-BY --index_path data/pages/360info/pagelist.jsonl --source_name 360info --output_dir data/news/v0/documents --tag div --attrs '{"class": "copy main-copy"}'
python parse_pages.py --license CC-BY --index_path data/pages/africasacountry/pagelist.jsonl --source_name africasacountry --output_dir data/news/v0/documents --tag article --attrs '{"class": "po__article"}'
python parse_pages.py --license CC-BY --index_path data/pages/altnews/pagelist.jsonl --source_name altnews --output_dir data/news/v0/documents --tag div --attrs '{"id": "primary"}'
python parse_pages.py --license CC-BY --index_path data/pages/balkandiskurs/pagelist.jsonl --source_name balkandiskurs --output_dir data/news/v0/documents --tag div --attrs '{"class": "entry-content"}'
python parse_pages.py --license CC-BY --index_path data/pages/factly/pagelist.jsonl --source_name factly --output_dir data/news/v0/documents --tag div --attrs '{"class": "post-content-right"}'
python parse_pages.py --license CC-BY --index_path data/pages/fides/pagelist.jsonl --source_name fides --output_dir data/news/v0/documents --tag div --attrs '{"id": "news"}'
python parse_pages.py --license CC-BY --index_path data/pages/freedom/pagelist.jsonl --source_name freedom --output_dir data/news/v0/documents --tag div --attrs '{"class": "blog-page"}'
python parse_pages.py --license CC-BY --index_path data/pages/globalvoices/pagelist.jsonl --source_name globalvoices --output_dir data/news/v0/documents --tag div --attrs '{"class": "entry-container"}'
python parse_pages.py --license CC-BY --index_path data/pages/meduza/pagelist.jsonl --source_name meduza --output_dir data/news/v0/documents --tag div --attrs '{"class": "article"}'
python parse_pages.py --license CC-BY --index_path data/pages/mekongeye/pagelist.jsonl --source_name mekongeye --output_dir data/news/v0/documents --tag div --attrs '{"class": "main-content"}'
python parse_pages.py --license CC-BY --index_path data/pages/milwaukeenns/pagelist.jsonl --source_name milwaukeenns --output_dir data/news/v0/documents --tag div --attrs '{"class": "entry-content"}'
python parse_pages.py --license CC-BY --index_path data/pages/minorityafrica/pagelist.jsonl --source_name minorityafrica --output_dir data/news/v0/documents --tag div --attrs '{"class": "post-content-container"}'
python parse_pages.py --license CC-BY --index_path data/pages/newcanadianmedia/pagelist.jsonl --source_name newcanadianmedia --output_dir data/news/v0/documents --tag div --attrs '{"class": "content-main"}'
python parse_pages.py --license CC-BY --index_path data/pages/scidev/pagelist.jsonl --source_name scidev --output_dir data/news/v0/documents --tag div --attrs '{"class": "fl-col-content fl-node-content"}'
python parse_pages.py --license CC-BY --index_path data/pages/solutionsjournalism/pagelist.jsonl --source_name solutionsjournalism --output_dir data/news/v0/documents --tag div --attrs '{"class": "sqs-html-content"}'
python parse_pages.py --license CC-BY --index_path data/pages/tasnimnews/pagelist.jsonl --source_name tasnimnews --output_dir data/news/v0/documents --tag div --attrs '{"class": "story"}'
python parse_pages.py --license CC-BY --index_path data/pages/zimfact/pagelist.jsonl --source_name zimfact --output_dir data/news/v0/documents --tag div --attrs '{"class": "entry-content"}'

# CC BY-SA
python parse_pages.py --license CC-BY-SA --index_path data/pages/educeleb/pagelist.jsonl --source_name educeleb --output_dir data/news/v0/documents
python parse_pages.py --license CC-BY-SA --index_path data/pages/libertytvradio/pagelist.jsonl --source_name libertytvradio --output_dir data/news/v0/documents --tag div --attrs '{"class": "td-post-content"}'
python parse_pages.py --license CC-BY-SA --index_path data/pages/oxpeckers/pagelist.jsonl --source_name oxpeckers --output_dir data/news/v0/documents --tag div --attrs '{"class": "post_text_inner"}'
python parse_pages.py --license CC-BY-SA --index_path data/pages/propastop/pagelist.jsonl --source_name propastop --output_dir data/news/v0/documents --tag div --attrs '{"class": "post-wrap"}'
python parse_pages.py --license CC-BY-SA --index_path data/pages/thepublicrecord/pagelist.jsonl --source_name thepublicrecord --output_dir data/news/v0/documents --tag div --attrs '{"class": "entry-content"}'

# Public Domain
python parse_pages.py --license "Public Domain" --index_path data/pages/caravanserai/pagelist.jsonl --source_name caravanserai --output_dir data/news/v0/documents --tag div --attrs '{"class": "article__content"}'

# CC NC ND
# python news/get_text.py --license CC-NC-ND --index_path data/pages/projectmultatuli/pagelist.jsonl --source_name projectmultatuli --output_dir data/news/v0/documents --tag div --attrs '{"class": "elementor-widget-container"}'
