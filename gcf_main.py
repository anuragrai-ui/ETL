#!/usr/bin/env python3
"""
Google Cloud Functions entry point for the JIRA → BigQuery ETL.

This thin wrapper imports the ETL implementation from `main_bigquery.py`
and exposes an HTTP handler named `jira_data_loader` that the Functions
Framework can invoke.

Deploy with entry point: jira_data_loader
"""

import logging
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Startup logging: use stdout so Cloud Run captures it; log each step to find delays
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
    force=True,
)
_LOG = logging.getLogger(__name__)
_START = time.perf_counter()


def _startup_log(step: str) -> None:
    elapsed = round((time.perf_counter() - _START) * 1000)
    _LOG.info("[startup] %s (elapsed_ms=%d)", step, elapsed)


_startup_log("gcf_main: process started, stdlib imports done")

import functions_framework
from flask import Request

_startup_log("gcf_main: functions_framework and flask imported")

# Defer main_bigquery import until first request so the container can start
# and listen on PORT quickly (Cloud Run health check). Importing main_bigquery
# pulls in google.cloud.bigquery and a large module and can timeout otherwise.
# Slack notifier is also imported lazily in _send_slack_error_notification.

_startup_log("gcf_main: module load complete, jira_data_loader registered")


def _get_param(
    request: Request, name: str, default: Optional[Any] = None
) -> Optional[str]:
    """Fetch a parameter from JSON body or query string."""
    try:
        json_body: Dict[str, Any] = request.get_json(silent=True) or {}
    except Exception:
        json_body = {}
    args = request.args or {}
    return (json_body.get(name) if name in json_body else args.get(name, default))


def _send_slack_error_notification(
    error: Exception,
    job_details: Dict[str, Any],
    operation: str = "Cloud Function Execution"
) -> None:
    """Send error notification to Slack (lazy import to keep startup fast)."""
    try:
        from slack_notifier import SlackNotifier, create_error_context
        notifier = SlackNotifier()
        context = create_error_context(
            error=error,
            job_details=job_details,
            operation=operation
        )
        notifier.send_error_notification(context)
    except Exception as e:
        logging.debug("Slack notification skipped or failed: %s", e)


@functions_framework.http
def jira_data_loader(request: Request):
    """HTTP-triggered Cloud Function entry point.

    Request supports both JSON body and query string:
      - mode: 'full' | 'incremental' | 'backfill' (default: 'full')
      - start_date: YYYY-MM-DD
      - end_date: YYYY-MM-DD
      - max_issues: int
      - enable_deduplication: bool (default: True)
    """
    start_time = datetime.now(timezone.utc)
    job_details: Dict[str, Any] = {}
    
    try:
        mode = (_get_param(request, "mode", "full") or "full").lower()
        start_date = _get_param(request, "start_date")
        end_date = _get_param(request, "end_date")
        max_issues_raw = _get_param(request, "max_issues")
        enable_deduplication_raw = _get_param(request, "enable_deduplication", "true")

        max_issues: Optional[int] = None
        if max_issues_raw not in (None, "", "null"):
            try:
                max_issues = int(max_issues_raw)
            except Exception:
                pass

        enable_deduplication = str(enable_deduplication_raw).lower() != "false"
        
        # Build job details for error tracking
        job_details = {
            "mode": mode,
            "start_date": start_date,
            "end_date": end_date,
            "max_issues": max_issues,
            "enable_deduplication": enable_deduplication,
            "trigger": "Cloud Function",
            "start_time": start_time.isoformat()
        }

        logging.info(
            "Starting ETL via Cloud Function: mode=%s, start=%s, end=%s, max_issues=%s, dedupe=%s",
            mode,
            start_date,
            end_date,
            max_issues,
            enable_deduplication,
        )

        from main_bigquery import BigQueryJiraETL
        etl = BigQueryJiraETL()
        
        # Set job context in ETL for error tracking
        etl._set_job_context(**job_details)
        
        ok = etl.run_etl(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            max_issues=max_issues,
            enable_deduplication=enable_deduplication,
        )

        status_code = 200 if ok else 500
        return (
            {
                "status": "success" if ok else "error",
                "message": (
                    f"ETL completed in {mode} mode" if ok else "ETL process failed"
                ),
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "max_issues": max_issues,
                "enable_deduplication": enable_deduplication,
            },
            status_code,
        )

    except Exception as exc:  # pragma: no cover - defensive handler for Cloud runtime
        # Log the full traceback
        tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        logging.exception(f"Unhandled error in jira_data_loader: {exc}\n{tb}")
        
        # Send Slack notification with error details
        job_details["error_time"] = datetime.now(timezone.utc).isoformat()
        _send_slack_error_notification(
            error=exc,
            job_details=job_details,
            operation="Cloud Function jira_data_loader"
        )
        
        return (
            {
                "status": "error", 
                "message": str(exc),
                "error_type": type(exc).__name__
            }, 
            500
        )


