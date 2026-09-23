#!/usr/bin/env python3
"""
Run health monitoring and alerting for the JIRA -> BigQuery ETL.

A RunMonitor is attached to each ETL run. Instead of sending one Slack message
per error, the run records problems as it goes and sends a single report at
the end. It detects:

  * Pipeline failures and unexpected exceptions
  * Incomplete loads: fewer tickets fetched than Jira says match the query,
    runs cut short by the timebox, fetch loops aborted by API errors
  * Jira API / config changes: auth failures (401/403), rejected requests
    (400/404/410), response shape changes, tracked fields disappearing from
    the API response or changing type, missing environment variables
  * Data point loss: a tracked field's fill rate collapsing versus the last
    healthy run
  * Staleness: no successful run within STALE_AFTER_HOURS (checked at the
    start of every run and by the heartbeat endpoint)

Every run (and heartbeat) is recorded in the `jira_etl_runs` BigQuery table.
Repeated alerts with the same problem set are suppressed for
REALERT_AFTER_HOURS.
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from google.cloud import bigquery

RUN_LOG_TABLE = "jira_etl_runs"

STALE_AFTER_HOURS = float(os.environ.get("ETL_STALE_AFTER_HOURS", "3"))
REALERT_AFTER_HOURS = float(os.environ.get("ETL_REALERT_AFTER_HOURS", "6"))
# Allowed shortfall between Jira's match count and tickets fetched
COMPLETENESS_TOLERANCE_PCT = float(os.environ.get("ETL_COMPLETENESS_TOLERANCE_PCT", "1"))
COMPLETENESS_TOLERANCE_MIN = int(os.environ.get("ETL_COMPLETENESS_TOLERANCE_MIN", "5"))
# Minimum issues in a run before field presence/coverage checks apply
FIELD_CHECK_MIN_ISSUES = 20
COVERAGE_CHECK_MIN_ISSUES = 100

REQUIRED_ENV_VARS = ["JIRA_API_TOKEN", "JIRA_EMAIL", "JIRA_BASE_URL", "GCP_PROJECT_ID", "SLACK_WEBHOOK_URL"]

# Fields that feed jira_tickets columns: field id -> (label, allowed value types)
TRACKED_FIELDS: Dict[str, Any] = {
    "status": ("Status", (dict,)),
    "priority": ("Priority", (dict, type(None))),
    "assignee": ("Assignee", (dict, type(None))),
    "reporter": ("Reporter", (dict, type(None))),
    "customfield_10485": ("Customer(s)", (list, type(None))),
    "customfield_10518": ("Sub-customer", (list, dict, type(None))),
    "customfield_10010": ("Request Type", (dict, type(None))),
    "customfield_10617": ("Type of Request", (list, dict, type(None))),
    "customfield_10251": ("Sentiment", (dict, type(None))),
    "customfield_13248": ("Customer Concerns", (dict, type(None))),
    "customfield_13247": ("Ticket Categorization", (dict, type(None))),
    "customfield_10249": ("Ops Team Designation", (dict, list, type(None))),
    "customfield_10716": ("Provider NPI", (str, int, float, type(None))),
}

SEVERITY_ORDER = {"critical": 0, "warning": 1}
SEVERITY_EMOJI = {"critical": "🔴", "warning": "🟠"}


class RunMonitor:
    """Collects health signals for one ETL run and reports them."""

    def __init__(self, client: Optional[bigquery.Client], project_id: str, dataset_id: str,
                 notifier: Any = None, mode: str = "", job_details: Optional[Dict[str, Any]] = None):
        self.client = client
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.notifier = notifier
        self.mode = mode
        self.job_details = job_details or {}
        self.run_id = uuid.uuid4().hex[:12]
        self.started_at = datetime.now(timezone.utc)
        self.problems: List[Dict[str, Any]] = []
        self._problem_codes: set = set()
        self.stats: Dict[str, int] = {
            "fetched": 0, "rows_built": 0, "build_errors": 0,
            "new": 0, "updated": 0, "unchanged": 0,
            "changelog_rows": 0, "fallback_inserts": 0,
        }
        self.expected_count: Optional[int] = None
        self.limited = False       # max_issues cap reached -> skip completeness check
        self.truncated = False     # stopped early (timebox / fetch abort)
        self.errors: Dict[str, Dict[str, Any]] = {}
        self._field_present: Dict[str, int] = {f: 0 for f in TRACKED_FIELDS}
        self._field_filled: Dict[str, int] = {f: 0 for f in TRACKED_FIELDS}
        self._field_bad_type: Dict[str, Dict[str, Any]] = {}
        self._issues_observed = 0
        self.finished = False

    # ------------------------------------------------------------------ recording

    def add_problem(self, severity: str, code: str, message: str, **detail: Any) -> None:
        """Record a problem once per code (repeat calls add to its count)."""
        for p in self.problems:
            if p["code"] == code:
                p["count"] += 1
                if SEVERITY_ORDER[severity] < SEVERITY_ORDER[p["severity"]]:
                    p["severity"] = severity
                return
        self.problems.append({"severity": severity, "code": code, "message": message,
                              "detail": detail, "count": 1})
        self._problem_codes.add(code)
        log = logging.error if severity == "critical" else logging.warning
        log(f"[monitor] {severity.upper()} {code}: {message}")

    def record_error(self, operation: str, error: Exception, affected_records: Optional[int] = None) -> None:
        """Aggregate an error by operation (per-issue errors collapse to one line)."""
        key = "process_issue" if operation.startswith("process_issue_") else operation
        entry = self.errors.setdefault(key, {"count": 0, "samples": [], "error_type": type(error).__name__,
                                             "message": str(error)[:300]})
        entry["count"] += 1
        if key == "process_issue":
            self.stats["build_errors"] += 1
            issue_key = operation[len("process_issue_"):]
            if len(entry["samples"]) < 5:
                entry["samples"].append(issue_key)

    def record_fetch_error(self, error: Exception, fatal: bool, where: str = "jira_fetch") -> None:
        """Classify a Jira request failure (auth, API change, transient)."""
        status = getattr(getattr(error, "response", None), "status_code", None)
        if status in (401, 403):
            self.add_problem("critical", "JIRA_AUTH_FAILED",
                             f"Jira rejected the credentials (HTTP {status}). The API token may be expired, "
                             f"revoked, or the account lost access.", where=where)
        elif status in (400, 404, 405, 410):
            self.add_problem("critical", "JIRA_API_REJECTED",
                             f"Jira rejected the request (HTTP {status}). The API endpoint, JQL or a field "
                             f"may have changed.", where=where, error=str(error)[:300])
        else:
            self.add_problem("critical" if fatal else "warning", "JIRA_FETCH_ERROR",
                             f"Jira request failed{' and the fetch was aborted' if fatal else ' (continued)'}: "
                             f"{type(error).__name__}", where=where, status=status, error=str(error)[:300])
        if fatal:
            self.truncated = True

    def observe_issue(self, issue: Dict[str, Any]) -> None:
        """Track presence, fill rate and value types of the tracked fields."""
        fields = issue.get("fields")
        if not isinstance(fields, dict):
            self.add_problem("critical", "JIRA_RESPONSE_CHANGED",
                             "An issue came back without a 'fields' object; the Jira response format may have changed.",
                             issue_key=issue.get("key"))
            return
        self._issues_observed += 1
        for field_id, (_, allowed) in TRACKED_FIELDS.items():
            if field_id not in fields:
                continue
            self._field_present[field_id] += 1
            value = fields[field_id]
            if value not in (None, "", [], {}):
                self._field_filled[field_id] += 1
            if not isinstance(value, allowed) and field_id not in self._field_bad_type:
                self._field_bad_type[field_id] = {"type": type(value).__name__, "issue_key": issue.get("key")}

    def field_coverage(self) -> Dict[str, float]:
        n = self._issues_observed
        return {f: round(self._field_filled[f] / n, 4) for f in TRACKED_FIELDS} if n else {}

    # ------------------------------------------------------------------ checks

    def check_config(self) -> None:
        missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
        if missing:
            self.add_problem("critical", "CONFIG_MISSING",
                             f"Environment variables not set: {', '.join(missing)}", missing=missing)

    def check_jira_auth(self, http: Any, base_url: str) -> bool:
        """Cheap preflight so a bad token is reported as such, not as '0 tickets'."""
        try:
            resp = http.get(f"{base_url}/rest/api/3/myself", timeout=30)
            if resp.status_code in (401, 403):
                self.add_problem("critical", "JIRA_AUTH_FAILED",
                                 f"Jira rejected the credentials (HTTP {resp.status_code}). The API token may be "
                                 f"expired or revoked.", where="preflight")
                return False
            resp.raise_for_status()
            return True
        except Exception as e:
            self.record_fetch_error(e, fatal=False, where="preflight")
            return False

    def set_expected_count(self, http: Any, base_url: str, jql: str) -> None:
        """Ask Jira how many issues match the run's JQL (for the completeness check)."""
        jql_no_order = jql.split(" ORDER BY ")[0]
        try:
            resp = http.post(f"{base_url}/rest/api/3/search/approximate-count",
                             json={"jql": jql_no_order}, timeout=60)
            resp.raise_for_status()
            count = resp.json().get("count")
            if isinstance(count, int):
                self.expected_count = count
                logging.info(f"[monitor] Jira reports {count} issues matching: {jql_no_order}")
        except Exception as e:
            self.add_problem("warning", "COMPLETENESS_UNCHECKED",
                             f"Could not get Jira's match count, so completeness was not verified: {e}")

    def _check_completeness(self) -> None:
        if self.expected_count is None or self.limited:
            return
        shortfall = self.expected_count - self.stats["fetched"]
        tolerance = max(COMPLETENESS_TOLERANCE_MIN, int(self.expected_count * COMPLETENESS_TOLERANCE_PCT / 100))
        if shortfall > tolerance:
            self.add_problem("critical", "INCOMPLETE_FETCH",
                             f"Fetched {self.stats['fetched']:,} of {self.expected_count:,} tickets Jira reports for "
                             f"this window ({shortfall:,} missing).",
                             expected=self.expected_count, fetched=self.stats["fetched"])
        if self.stats["fetched"] and self.stats["rows_built"] < self.stats["fetched"]:
            lost = self.stats["fetched"] - self.stats["rows_built"]
            sev = "critical" if lost > max(5, self.stats["fetched"] * 0.01) else "warning"
            samples = self.errors.get("process_issue", {}).get("samples", [])
            self.add_problem(sev, "TICKETS_NOT_TRANSFORMED",
                             f"{lost:,} fetched tickets failed to transform and were not loaded"
                             f"{' (e.g. ' + ', '.join(samples) + ')' if samples else ''}.",
                             error=self.errors.get("process_issue", {}).get("message"))

    def _check_fields(self, previous_coverage: Optional[Dict[str, float]]) -> None:
        n = self._issues_observed
        if n < FIELD_CHECK_MIN_ISSUES:
            return
        for field_id, (label, _) in TRACKED_FIELDS.items():
            if self._field_present[field_id] == 0:
                self.add_problem("critical", "FIELD_MISSING_FROM_API",
                                 f"'{label}' ({field_id}) was missing from all {n} Jira responses. The field may "
                                 f"have been renamed, deleted or hidden from the API user.", field=field_id)
            elif field_id in self._field_bad_type:
                bad = self._field_bad_type[field_id]
                self.add_problem("warning", "FIELD_SHAPE_CHANGED",
                                 f"'{label}' ({field_id}) returned an unexpected type '{bad['type']}' "
                                 f"(e.g. {bad['issue_key']}). Its column may load wrong or empty values.",
                                 field=field_id)
        if previous_coverage and n >= COVERAGE_CHECK_MIN_ISSUES:
            current = self.field_coverage()
            for field_id, (label, _) in TRACKED_FIELDS.items():
                before, now = previous_coverage.get(field_id), current.get(field_id)
                if before is not None and now is not None and before >= 0.2 and now < before * 0.25:
                    self.add_problem("warning", "FIELD_COVERAGE_DROP",
                                     f"'{label}' is filled on {now:.0%} of tickets this run vs {before:.0%} on the "
                                     f"last healthy run. Data points may be getting lost.", field=field_id)

    def _check_writes(self) -> None:
        if self.stats["fallback_inserts"]:
            self.add_problem("warning", "STREAMING_BUFFER_FALLBACK",
                             f"{self.stats['fallback_inserts']:,} updates were appended as new rows because "
                             f"BigQuery's streaming buffer blocked MERGE. Expect duplicate issue_key rows.")
        for op, e in self.errors.items():
            if op == "process_issue":
                continue
            self.add_problem("critical", "ERROR_" + op.upper()[:40],
                             f"{op} failed {e['count']}x: {e['error_type']} - {e['message']}")

    # ------------------------------------------------------------------ run log

    def _table(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{RUN_LOG_TABLE}"

    def ensure_run_log_table(self) -> None:
        if not self.client:
            return
        schema = [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("mode", "STRING"),
            bigquery.SchemaField("status", "STRING", description="success | degraded | failed | heartbeat"),
            bigquery.SchemaField("started_at", "TIMESTAMP"),
            bigquery.SchemaField("finished_at", "TIMESTAMP"),
            bigquery.SchemaField("duration_seconds", "FLOAT"),
            bigquery.SchemaField("revision", "STRING"),
            bigquery.SchemaField("job_details", "JSON"),
            bigquery.SchemaField("expected_count", "INTEGER"),
            bigquery.SchemaField("stats", "JSON"),
            bigquery.SchemaField("field_coverage", "JSON"),
            bigquery.SchemaField("problems", "JSON"),
            bigquery.SchemaField("alert_signature", "STRING"),
            bigquery.SchemaField("alert_sent", "BOOLEAN"),
        ]
        try:
            self.client.create_table(bigquery.Table(self._table(), schema=schema), exists_ok=True)
        except Exception as e:
            logging.warning(f"[monitor] Could not ensure {RUN_LOG_TABLE}: {e}")

    def _query(self, sql: str) -> List[Any]:
        if not self.client:
            return []
        try:
            return list(self.client.query(sql).result())
        except Exception as e:
            logging.warning(f"[monitor] Run log query failed: {e}")
            return []

    def _previous_coverage(self) -> Optional[Dict[str, float]]:
        rows = self._query(f"""
            SELECT TO_JSON_STRING(field_coverage) AS cov FROM `{self._table()}`
            WHERE status = 'success' AND field_coverage IS NOT NULL
              AND SAFE_CAST(JSON_VALUE(stats, '$.fetched') AS INT64) >= {COVERAGE_CHECK_MIN_ISSUES}
            ORDER BY finished_at DESC LIMIT 1""")
        if rows and rows[0].cov:
            try:
                return json.loads(rows[0].cov)
            except (TypeError, ValueError):
                return None
        return None

    def check_staleness(self) -> None:
        """Alert when no run has succeeded recently (covers crashed/killed runs)."""
        rows = self._query(f"""
            SELECT MAX(finished_at) AS last_ok,
                   (SELECT COUNT(*) FROM `{self._table()}`) AS total_runs
            FROM `{self._table()}` WHERE status IN ('success', 'degraded')""")
        if not rows or not rows[0].total_runs:
            return  # no history yet
        last_ok = rows[0].last_ok
        if last_ok is None:
            self.add_problem("critical", "PIPELINE_STALE", "No ETL run has ever succeeded according to the run log.")
            return
        hours = (datetime.now(timezone.utc) - last_ok).total_seconds() / 3600
        if hours > STALE_AFTER_HOURS:
            self.add_problem("critical", "PIPELINE_STALE",
                             f"Last successful ETL run finished {hours:.1f}h ago ({last_ok:%Y-%m-%d %H:%M} UTC); "
                             f"threshold is {STALE_AFTER_HOURS:g}h. jira_tickets may be out of date.",
                             last_success=last_ok.isoformat())

    def _signature(self) -> str:
        codes = sorted(p["code"] for p in self.problems)
        return hashlib.sha1(("|".join(codes)).encode()).hexdigest()[:16] if codes else ""

    def _recently_alerted(self, signature: str) -> bool:
        if not signature:
            return False
        rows = self._query(f"""
            SELECT COUNT(*) AS n FROM `{self._table()}`
            WHERE alert_signature = '{signature}' AND alert_sent
              AND finished_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(REALERT_AFTER_HOURS * 60)} MINUTE)""")
        return bool(rows and rows[0].n)

    def _write_run_log(self, status: str, signature: str, alert_sent: bool, finished_at: datetime) -> None:
        if not self.client:
            return
        row = {
            "run_id": self.run_id,
            "mode": self.mode,
            "status": status,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": (finished_at - self.started_at).total_seconds(),
            "revision": os.environ.get("K_REVISION", "local"),
            "job_details": json.dumps(self.job_details, default=str),
            "expected_count": self.expected_count,
            "stats": json.dumps(self.stats),
            "field_coverage": json.dumps(self.field_coverage()) if self._issues_observed else None,
            "problems": json.dumps(self.problems, default=str),
            "alert_signature": signature or None,
            "alert_sent": alert_sent,
        }
        try:
            errors = self.client.insert_rows_json(self._table(), [row])
            if errors:
                logging.warning(f"[monitor] Run log insert errors: {errors}")
        except Exception as e:
            logging.warning(f"[monitor] Could not write run log: {e}")

    # ------------------------------------------------------------------ report

    def finish(self, succeeded: bool, is_heartbeat: bool = False) -> str:
        """Run end-of-run checks, write the run log, send one Slack report if needed."""
        if self.finished:
            return "done"
        self.finished = True
        if not is_heartbeat:
            self._check_completeness()
            self._check_fields(self._previous_coverage())
            self._check_writes()
            if self.truncated and not any(p["code"].startswith("JIRA_") for p in self.problems):
                self.add_problem("warning", "RUN_TRUNCATED",
                                 "The run stopped early (execution timebox); tickets after that point were not "
                                 "loaded in this run.")
        has_critical = any(p["severity"] == "critical" for p in self.problems)
        if is_heartbeat:
            status = "heartbeat"
        elif not succeeded:
            status = "failed"
        elif has_critical or self.problems:
            status = "degraded"
        else:
            status = "success"

        signature = self._signature()
        alert_sent = False
        if self.problems or status == "failed":
            if self._recently_alerted(signature):
                logging.info(f"[monitor] Same problems already alerted in the last {REALERT_AFTER_HOURS:g}h; "
                             f"not re-sending (signature {signature}).")
            else:
                alert_sent = self._send_report(status)
        finished_at = datetime.now(timezone.utc)
        self._write_run_log(status, signature, alert_sent, finished_at)
        logging.info(f"[monitor] Run {self.run_id} finished: status={status}, problems={len(self.problems)}")
        return status

    def _send_report(self, status: str) -> bool:
        if not self.notifier or not hasattr(self.notifier, "send_run_report"):
            return False
        problems = sorted(self.problems, key=lambda p: SEVERITY_ORDER[p["severity"]])
        lines = []
        for p in problems[:15]:
            count = f" (x{p['count']})" if p["count"] > 1 else ""
            lines.append(f"{SEVERITY_EMOJI[p['severity']]} *{p['code']}*{count}: {p['message']}")
        if len(problems) > 15:
            lines.append(f"…and {len(problems) - 15} more")
        expected = f" / {self.expected_count:,} expected" if self.expected_count is not None else ""
        stats_text = (f"Fetched {self.stats['fetched']:,}{expected} · new {self.stats['new']:,} · "
                      f"updated {self.stats['updated']:,} · unchanged {self.stats['unchanged']:,} · "
                      f"transform errors {self.stats['build_errors']:,}")
        duration = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        try:
            return self.notifier.send_run_report(
                status=status,
                problems_text="\n".join(lines) or "Run failed without a recorded cause; check the logs.",
                stats_text=stats_text,
                job_details={**self.job_details, "run_id": self.run_id,
                             "revision": os.environ.get("K_REVISION", "local")},
                duration_seconds=duration,
                critical=status == "failed" or any(p["severity"] == "critical" for p in problems),
            )
        except Exception as e:
            logging.error(f"[monitor] Failed to send Slack run report: {e}")
            return False


class NullMonitor(RunMonitor):
    """Used outside a monitored run; records nothing and never reports."""

    def __init__(self):
        super().__init__(client=None, project_id="", dataset_id="")

    def add_problem(self, *args: Any, **kwargs: Any) -> None:
        pass

    def finish(self, *args: Any, **kwargs: Any) -> str:
        return "unmonitored"


def run_heartbeat(client: bigquery.Client, project_id: str, dataset_id: str, notifier: Any,
                  tickets_table: str) -> Dict[str, Any]:
    """Standalone freshness check (no ETL). Alerts if runs or data are stale."""
    monitor = RunMonitor(client, project_id, dataset_id, notifier, mode="heartbeat",
                         job_details={"trigger": "heartbeat"})
    monitor.ensure_run_log_table()
    monitor.check_config()
    monitor.check_staleness()
    rows = monitor._query(f"SELECT MAX(updated) AS newest FROM `{project_id}.{dataset_id}.{tickets_table}`")
    newest = rows[0].newest if rows else None
    if newest is not None:
        hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
        if hours > STALE_AFTER_HOURS:
            monitor.add_problem("critical", "DATA_STALE",
                                f"Newest ticket update in {tickets_table} is {hours:.1f}h old "
                                f"({newest:%Y-%m-%d %H:%M} UTC); new Jira changes are not arriving.",
                                newest_update=newest.isoformat())
    status = monitor.finish(succeeded=True, is_heartbeat=True)
    return {"status": "ok" if not monitor.problems else "alert", "run_id": monitor.run_id,
            "problems": [p["code"] for p in monitor.problems], "result": status}
