#!/usr/bin/env sh

# CC BY
python download_pages.py --index_path data/pages/360info/pagelist.jsonl
python download_pages.py --index_path data/pages/africasacountry/pagelist.jsonl
python download_pages.py --index_path data/pages/altnews/pagelist.jsonl
python download_pages.py --index_path data/pages/balkandiskurs/pagelist.jsonl
python download_pages.py --index_path data/pages/factly/pagelist.jsonl
python download_pages.py --index_path data/pages/fides/pagelist.jsonl
python download_pages.py --index_path data/pages/freedom/pagelist.jsonl
python download_pages.py --index_path data/pages/globalvoices/pagelist.jsonl
python download_pages.py --index_path data/pages/meduza/pagelist.jsonl
python download_pages.py --index_path data/pages/mekongeye/pagelist.jsonl
python download_pages.py --index_path data/pages/milwaukeenns/pagelist.jsonl
python download_pages.py --index_path data/pages/minorityafrica/pagelist.jsonl
python download_pages.py --index_path data/pages/newcanadianmedia/pagelist.jsonl
python download_pages.py --index_path data/pages/scidev/pagelist.jsonl
python download_pages.py --index_path data/pages/solutionsjournalism/pagelist.jsonl
python download_pages.py --index_path data/pages/tasnimnews/pagelist.jsonl
python download_pages.py --index_path data/pages/zimfact/pagelist.jsonl

# CC BY-SA
python download_pages.py --index_path data/pages/educeleb/pagelist.jsonl
python download_pages.py --index_path data/pages/libertytvradio/pagelist.jsonl
python download_pages.py --index_path data/pages/oxpeckers/pagelist.jsonl
python download_pages.py --index_path data/pages/propastop/pagelist.jsonl
python download_pages.py --index_path data/pages/thepublicrecord/pagelist.jsonl

# Public Domain
python download_pages.py --index_path data/pages/caravanserai/pagelist.jsonl

# CC NC ND
# python news/get_text.py --license CC NC ND --input_dir data/pages/projectmultatuli/pagelist.jsonl
