import json
from typing import Any, Callable, Dict, List

import requests

DEFAULT_MAX_PAYLOAD_BYTES = 750_000
RETRYABLE_SPLIT_STATUS_CODES = {413, 500}


def _noop(_: str) -> None:
    pass


def build_payload(
    api_key: str,
    state: str,
    scraped_timestamp: str,
    facilities: List[Dict[str, Any]],
    replace: bool = False,
) -> Dict[str, Any]:
    payload = {
        "api_key": api_key,
        "state": state,
        "scraped_timestamp": scraped_timestamp,
        "facilities": facilities,
    }
    if replace:
        payload["replace"] = True
    return payload


def estimate_payload_bytes(payload: Dict[str, Any]) -> int:
    return len(json.dumps(payload).encode("utf-8"))


def chunk_facilities_by_size(
    api_key: str,
    state: str,
    scraped_timestamp: str,
    facilities: List[Dict[str, Any]],
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    replace: bool = False,
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current_batch: List[Dict[str, Any]] = []

    for facility in facilities:
        candidate_batch = current_batch + [facility]
        candidate_payload = build_payload(
            api_key=api_key,
            state=state,
            scraped_timestamp=scraped_timestamp,
            facilities=candidate_batch,
            replace=replace,
        )

        if current_batch and estimate_payload_bytes(candidate_payload) > max_payload_bytes:
            batches.append(current_batch)
            current_batch = [facility]
        else:
            current_batch = candidate_batch

    if current_batch:
        batches.append(current_batch)

    return batches


def describe_facility(facility: Dict[str, Any]) -> str:
    if not isinstance(facility, dict):
        return "unknown facility"

    facility_info = facility.get("facility_info")
    if isinstance(facility_info, dict):
        name = facility_info.get("facility_name") or facility_info.get("program_name")
        if name:
            return str(name)

    for key in ("facility_name", "program_name", "name", "id"):
        value = facility.get(key)
        if value:
            return str(value)

    return "unknown facility"


def describe_facility_details(facility: Dict[str, Any]) -> str:
    name = describe_facility(facility)
    reports = facility.get("reports") if isinstance(facility, dict) else None
    report_count = len(reports) if isinstance(reports, list) else 0
    report_ids: List[str] = []

    if isinstance(reports, list):
        for report in reports[:5]:
            if isinstance(report, dict):
                report_id = report.get("report_id")
                if report_id:
                    report_ids.append(str(report_id))

    report_suffix = ""
    if report_ids:
        report_suffix = f"; sample report_ids={', '.join(report_ids)}"

    return f"{name} ({report_count} reports{report_suffix})"


def _response_body_snippet(response: requests.Response | None) -> str:
    if response is None:
        return ""

    body = (response.text or "").strip()
    if not body:
        return ""

    body = " ".join(body.split())
    return body[:500]


def _post_batch(
    api_url: str,
    api_key: str,
    state: str,
    scraped_timestamp: str,
    facilities: List[Dict[str, Any]],
    timeout: int,
    batch_label: str,
    info: Callable[[str], None],
    error: Callable[[str], None],
    replace: bool = False,
) -> Dict[str, Any]:
    payload = build_payload(
        api_key=api_key,
        state=state,
        scraped_timestamp=scraped_timestamp,
        facilities=facilities,
        replace=replace,
    )
    payload_bytes = estimate_payload_bytes(payload)

    info(
        f"Posting batch {batch_label}: {len(facilities)} facilities "
        f"({payload_bytes:,} bytes) to {api_url}"
    )

    try:
        response = requests.post(
            api_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        body_snippet = _response_body_snippet(exc.response)
        if body_snippet:
            error(
                f"API response body for batch {batch_label} "
                f"(HTTP {status_code}): {body_snippet}"
            )

        if status_code in RETRYABLE_SPLIT_STATUS_CODES and len(facilities) > 1:
            midpoint = len(facilities) // 2
            if status_code == 413:
                info(
                    f"Batch {batch_label} exceeded the server request-size limit; "
                    "retrying as smaller batches"
                )
            else:
                info(
                    f"Batch {batch_label} hit HTTP {status_code}; "
                    "retrying as smaller batches to isolate the failing facility"
                )

            left = _post_batch(
                api_url=api_url,
                api_key=api_key,
                state=state,
                scraped_timestamp=scraped_timestamp,
                facilities=facilities[:midpoint],
                timeout=timeout,
                batch_label=f"{batch_label}a",
                info=info,
                error=error,
                replace=replace,
            )
            if not left.get("success"):
                return left

            right = _post_batch(
                api_url=api_url,
                api_key=api_key,
                state=state,
                scraped_timestamp=scraped_timestamp,
                facilities=facilities[midpoint:],
                timeout=timeout,
                batch_label=f"{batch_label}b",
                info=info,
                error=error,
                replace=False,  # only delete on the first batch
            )
            if not right.get("success"):
                return right

            return {
                "success": True,
                "facilities_saved": left.get("facilities_saved", 0)
                + right.get("facilities_saved", 0),
                "reports_saved": left.get("reports_saved", 0)
                + right.get("reports_saved", 0),
            }

        error(f"Error posting batch {batch_label} to API: {exc}")
        if status_code == 413 and len(facilities) == 1:
            error(
                "Single facility payload still exceeds the server limit: "
                f"{describe_facility_details(facilities[0])}"
            )
        if status_code == 500 and len(facilities) == 1:
            error(
                "Single facility still triggers HTTP 500: "
                f"{describe_facility_details(facilities[0])}"
            )
        return {
            "success": False,
            "error": str(exc),
            "facilities_saved": 0,
            "reports_saved": 0,
        }
    except requests.exceptions.RequestException as exc:
        error(f"Error posting batch {batch_label} to API: {exc}")
        return {
            "success": False,
            "error": str(exc),
            "facilities_saved": 0,
            "reports_saved": 0,
        }

    try:
        result = response.json()
    except ValueError:
        body_snippet = response.text[:300].strip()
        error(f"API returned non-JSON for batch {batch_label}: {body_snippet}")
        return {
            "success": False,
            "error": "non-json response",
            "facilities_saved": 0,
            "reports_saved": 0,
        }

    if result.get("success"):
        return {
            "success": True,
            "facilities_saved": int(result.get("facilities_saved", 0) or 0),
            "reports_saved": int(result.get("reports_saved", 0) or 0),
        }

    api_error = str(result.get("error", "unknown"))
    error(f"API error on batch {batch_label}: {api_error}")
    return {
        "success": False,
        "error": api_error,
        "facilities_saved": 0,
        "reports_saved": 0,
    }


def post_facilities_to_api(
    api_url: str,
    api_key: str,
    state: str,
    scraped_timestamp: str,
    facilities: List[Dict[str, Any]],
    timeout: int = 120,
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
    info: Callable[[str], None] | None = None,
    error: Callable[[str], None] | None = None,
    replace: bool = False,
) -> Dict[str, Any]:
    info = info or _noop
    error = error or _noop

    if not facilities:
        info(f"No facilities to post to {api_url}")
        return {"success": True, "facilities_saved": 0, "reports_saved": 0}

    batches = chunk_facilities_by_size(
        api_key=api_key,
        state=state,
        scraped_timestamp=scraped_timestamp,
        facilities=facilities,
        max_payload_bytes=max_payload_bytes,
        replace=replace,
    )

    if len(batches) > 1:
        info(
            f"Split {len(facilities)} facilities into {len(batches)} API requests "
            f"using a {max_payload_bytes:,}-byte payload cap"
        )

    facilities_saved = 0
    reports_saved = 0

    for index, batch in enumerate(batches, start=1):
        result = _post_batch(
            api_url=api_url,
            api_key=api_key,
            state=state,
            scraped_timestamp=scraped_timestamp,
            facilities=batch,
            timeout=timeout,
            batch_label=f"{index}/{len(batches)}",
            info=info,
            error=error,
            replace=replace and index == 1,  # only delete on the first batch
        )
        if not result.get("success"):
            return result

        facilities_saved += result.get("facilities_saved", 0)
        reports_saved += result.get("reports_saved", 0)

    info(f"API saved {facilities_saved} facilities, {reports_saved} reports")
    return {
        "success": True,
        "facilities_saved": facilities_saved,
        "reports_saved": reports_saved,
    }
