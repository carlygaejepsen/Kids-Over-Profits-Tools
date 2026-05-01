"""
Shared state-file helpers for incremental scraping.

Each scraper writes a small JSON file (e.g. .az_state.json) tracking what it
has already successfully posted. On subsequent runs the scraper loads this
state, skips items it has already seen, and only writes new IDs back after a
successful API save — so a failed run never silently drops data.

Two state schemas are supported:

  Server-side date cursor (used by ar_scraper, since WP REST supports
  modified_after):
      {"last_run": {<slug>: <max_modified_iso>}}

  Client-side seen-IDs (used by every scraper whose source has no date
  filter):
      {"seen": {<facility_id>: [<report_id>, ...]}}
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


def load_state(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Could not read {path}: {e}; starting fresh")
        return {}


def save_state(path: Path, state: Dict) -> None:
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def seen_from_state(state: Dict) -> Dict[str, Set[str]]:
    """Convert {"seen": {fid: [...]}} JSON into {fid: set(...)} for O(1) lookup."""
    return {fid: set(ids) for fid, ids in state.get("seen", {}).items()}


def merge_new_ids(state: Dict, new_ids: Dict[str, List[str]]) -> None:
    """Merge newly-posted report IDs into state in place. Empty lists are ignored."""
    seen_dict = state.setdefault("seen", {})
    for fid, ids in new_ids.items():
        if not ids:
            continue
        existing = set(seen_dict.get(fid, []))
        seen_dict[fid] = sorted(existing | set(ids))
