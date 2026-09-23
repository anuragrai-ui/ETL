#!/usr/bin/env python3
"""
JIRA to BigQuery ETL Pipeline
Unified script for extracting JIRA data and loading into Google BigQuery
"""
import argparse
import base64
import json
import logging
import os
import sys
import time
import gc
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Generator
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.oauth2 import service_account
from dateutil import parser as dt_parser
from dotenv import load_dotenv

# Import Slack notification module (with graceful degradation)
# Import lazily to prevent container startup failures if module has issues
SLACK_NOTIFIER_AVAILABLE = False
SlackNotifier = None
ErrorContext = None
create_error_context = None

try:
    from slack_notifier import (
        SlackNotifier,
        ErrorContext,
        create_error_context
    )
    SLACK_NOTIFIER_AVAILABLE = SlackNotifier is not None
    if SLACK_NOTIFIER_AVAILABLE:
        logging.info("Slack notifier loaded successfully")
except Exception as e:
    logging.warning(f"Slack notifier not available: {e}")
    # Define placeholder classes/functions for when slack_notifier is not available
    class SlackNotifier:
        def __init__(self, *args, **kwargs): 
            self._enabled = False
        def send_error_notification(self, *args, **kwargs): 
            return False
        def send_success_notification(self, *args, **kwargs): 
            return False
        def send_warning_notification(self, *args, **kwargs): 
            return False
    
    class ErrorContext:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)
    
    def create_error_context(error, job_details, operation='', **kwargs):
        import traceback
        return ErrorContext(
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=''.join(traceback.format_exception(type(error), error, error.__traceback__)),
            job_details=job_details,
            operation=operation,
            **kwargs
        )
    
    # Even if import fails, assign the placeholder
    SlackNotifier = SlackNotifier
    ErrorContext = ErrorContext
    create_error_context = create_error_context
    SLACK_NOTIFIER_AVAILABLE = True  # Enable with fallbacks

# Load environment variables from .env file
load_dotenv()

# Configure logging
log_filename = f'jira_etl_backfill_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# BigQuery Configuration
BIGQUERY_CONFIG = {
    'project_id': os.environ.get('GCP_PROJECT_ID', 'certifyos-production-platform'),
    'dataset_id': 'Reporting',
    'location': 'US'
}

# JIRA Configuration
JIRA_CONFIG = {
    'base_url': os.environ.get('JIRA_BASE_URL', 'https://certifyos.atlassian.net'),
    'email': os.environ.get('JIRA_EMAIL', 'anurag.rai@certifyos.com'),
    'api_token': os.environ.get('JIRA_API_TOKEN', '')
}

# Table names
TICKET_TABLE = 'jira_tickets'
CHANGELOG_TABLE = 'jira_changelog'

# Ticket "Details" panel fields added after the table was first created.
# create_tables() appends any of these missing from an existing table.
DETAILS_PANEL_COLUMNS = [
    bigquery.SchemaField("request_type", "STRING", description="Service desk Request Type name (customfield_10010)"),
    bigquery.SchemaField("sentiment", "STRING", description="Sentiment (customfield_10251)"),
    bigquery.SchemaField("customer_concerns", "STRING", description="Customer Concerns (customfield_13248)"),
    bigquery.SchemaField("ticket_categorization", "STRING", description="Ticket Categorization parent value (customfield_13247)"),
    bigquery.SchemaField("ticket_categorization_detail", "STRING", description="Ticket Categorization child value (customfield_13247)"),
]

class BigQueryJiraETL:
    """BigQuery JIRA ETL Pipeline"""
    
    def __init__(self):
        """Initialize configuration, HTTP session, and BigQuery client"""
        # Config
        self.project_id = BIGQUERY_CONFIG['project_id']
        self.dataset_id = BIGQUERY_CONFIG['dataset_id']
        self.tickets_table = TICKET_TABLE
        self.changelog_table = CHANGELOG_TABLE
        
        # HTTP session first (used by some helpers)
        self.http = self._init_http_session()
        
        # BigQuery client
        self.client = self._get_bigquery_client()
        self.dataset_ref = self.client.dataset(self.dataset_id)
        
        # Slack notifier for error notifications
        self.slack_notifier = SlackNotifier()
        
        # Job context for error tracking
        self._job_context: Dict[str, Any] = {}
    
    def _set_job_context(self, **kwargs: Any) -> None:
        """Set the current job context for error reporting."""
        self._job_context = kwargs
    
    def _handle_error(
        self,
        error: Exception,
        operation: str,
        batch_number: Optional[int] = None,
        affected_records: Optional[int] = None,
        bigquery_job_id: Optional[str] = None,
        send_notification: bool = True
    ) -> ErrorContext:
        """
        Handle an error by logging and optionally sending Slack notification.
        
        Args:
            error: The exception that occurred
            operation: Description of the operation that failed
            batch_number: Optional batch number if error occurred during batch processing
            affected_records: Optional count of affected records
            bigquery_job_id: Optional BigQuery job ID
            send_notification: Whether to send Slack notification (default: True)
            
        Returns:
            ErrorContext object containing error details
        """
        context = create_error_context(
            error=error,
            job_details=self._job_context.copy(),
            operation=operation,
            batch_number=batch_number,
            affected_records=affected_records,
            bigquery_job_id=bigquery_job_id
        )
        
        # Log the error with full details
        logging.error(f"❌ Error in {operation}: {context.error_type} - {context.error_message}")
        if batch_number:
            logging.error(f"   Batch: {batch_number}")
        if affected_records:
            logging.error(f"   Affected records: {affected_records}")
        if bigquery_job_id:
            logging.error(f"   BigQuery Job ID: {bigquery_job_id}")
        
        # Send Slack notification if enabled
        if send_notification:
            try:
                self.slack_notifier.send_error_notification(context)
            except Exception as notify_error:
                logging.error(f"Failed to send Slack notification: {notify_error}")
        
        return context

    def _init_http_session(self) -> requests.Session:
        """Create a pooled HTTP session with retries for JIRA API calls"""
        session = requests.Session()
        # Configure robust retry strategy for transient errors and rate limits
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"])  # we only use GET here
        )
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=retry)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        # Default headers for all requests
        session.headers.update(self.get_jira_headers())
        session.headers.update({
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session
    
    def _get_latest_ticket_info(self) -> Dict[str, Any]:
        """Get the latest ticket information to estimate total count and track progress"""
        try:
            # Query to get the highest ticket key (more reliable than created desc)
            url = f"{JIRA_CONFIG['base_url']}/rest/api/3/search/jql"
            params = {
                'jql': 'project = TS ORDER BY key DESC',
                'fields': 'key,created',
                'startAt': 0,
                'maxResults': 1
            }
            
            response = self.http.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            issues = data.get('issues', [])
            if issues:
                latest_ticket = issues[0]
                ticket_key = latest_ticket.get('key', 'TS-31890')
                created_date = latest_ticket.get('fields', {}).get('created', '')
                
                # Extract ticket number from key (e.g., TS-31890 -> 31890)
                try:
                    ticket_number = int(ticket_key.split('-')[1]) if '-' in ticket_key else 31890
                except (ValueError, IndexError):
                    ticket_number = 31890
                
                # Estimate total tickets (tickets aren't sequential, so use a reasonable estimate)
                # Based on analysis, roughly 27% of ticket numbers exist (31890 tickets, ~8659 actual)
                estimated_total = max(int(ticket_number * 0.27), 8000)
                
                logging.debug(f"Latest ticket analysis: {ticket_key} (#{ticket_number}), created: {created_date}")
                
                return {
                    'highest_ticket': ticket_key,
                    'ticket_number': ticket_number,
                    'estimated_total': estimated_total,
                    'created_date': created_date
                }
            else:
                # Fallback if query fails
                logging.warning("Could not retrieve latest ticket info, using fallback values")
                return {
                    'highest_ticket': 'TS-100000', # Increased to 100000 to ensure future coverage
                    'ticket_number': 100000,
                    'estimated_total': 25000,
                    'created_date': 'Unknown'
                }
                
        except Exception as e:
            logging.warning(f"Error getting latest ticket info: {e}, using fallback values")
            return {
                'highest_ticket': 'TS-100000', # Increased to 100000 to ensure future coverage
                'ticket_number': 100000,
                'estimated_total': 25000,
                'created_date': 'Unknown'
            }
        
    def _get_bigquery_client(self) -> bigquery.Client:
        """Initialize BigQuery client using ADC, falling back if needed."""
        try:
            import google.auth
            credentials, default_project = google.auth.default()
            project_id = self.project_id or default_project
            client = bigquery.Client(project=project_id, credentials=credentials)
            return client
        except Exception as e:
            logging.error(f"❌ Failed to initialize BigQuery client with ADC: {e}")
            # Last resort: try without explicit credentials (let library decide)
            try:
                client = bigquery.Client(project=self.project_id)
                return client
            except Exception as e2:
                logging.error(f"❌ BigQuery client fallback also failed: {e2}")
                raise

    def _fetch_all_issues_by_key_enumeration(self, latest_ticket_num: int, fields_param: List[str], include_changelog: bool) -> Generator[Dict[str, Any], None, None]:
        """Enumerate issue keys TS-1..TS-latest and fetch existing ones.
        
        This bypasses pagination and date quirks by directly fetching each key.
        Non-existent keys will be skipped efficiently via 404 handling.
        """
        session = self.http
        base = JIRA_CONFIG['base_url']
        fetched = 0
        missing = 0
        # Allow overriding enumeration starting point via env; default to 1
        try:
            configured_start = int(os.environ.get("JIRA_ENUMERATION_START", "1"))
        except ValueError:
            configured_start = 1
        
        # Respect configured start but never exceed the latest ticket number or go below 1
        # If configured_start > latest_ticket_num, we should probably just stop or warn,
        # but adhering to the min/max logic:
        start = min(max(configured_start, 1), latest_ticket_num)
        
        logging.info(f"🔢 Enumerating keys from TS-{start} to TS-{latest_ticket_num}")
        fields = ','.join(fields_param)
        expands = 'changelog' if include_changelog else None
        
        for num in range(start, latest_ticket_num + 1):
            key = f"TS-{num}"
            try:
                # GET /rest/api/3/issue/{issueIdOrKey}
                params = {'fields': fields}
                if expands:
                    params['expand'] = expands
                resp = session.get(f"{base}/rest/api/3/issue/{key}", params=params, timeout=20)
                if resp.status_code == 404:
                    missing += 1
                    if num % 1000 == 0:
                        logging.info(f"   ... up to {key}: fetched {fetched}, missing {missing}")
                    continue
                resp.raise_for_status()
                issue = resp.json()
                # Normalize to search response shape minimally
                if 'key' not in issue and 'key' in issue.get('fields', {}):
                    issue['key'] = key
                if include_changelog and 'changelog' not in issue:
                    issue['changelog'] = {'histories': []}
                yield issue
                fetched += 1
                if fetched % 200 == 0:
                    logging.info(f"   ... enumerated to {key}: fetched {fetched}, missing {missing}")
            except requests.HTTPError as http_err:
                status = getattr(http_err.response, 'status_code', None)
                if status == 404:
                    missing += 1
                    continue
                logging.warning(f"⚠️ Error fetching {key}: {http_err}")
            except Exception as e:
                logging.warning(f"⚠️ Error on {key}: {e}")
        logging.info(f"🔚 Enumeration finished. Fetched: {fetched}, missing: {missing}")

        

    def create_tables(self) -> bool:
        """Create BigQuery tables with proper schema - skip if already exist"""
        
        # Ensure dataset exists
        try:
            dataset = self.client.get_dataset(f"{self.project_id}.{self.dataset_id}")
            logging.info(f"✅ Dataset {self.dataset_id} already exists")
        except Exception:
            logging.info(f"🛠️ Creating dataset {self.dataset_id}...")
            dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
            dataset.location = BIGQUERY_CONFIG['location']
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logging.info(f"✅ Created dataset {self.dataset_id}")
        
        table_path = f"{self.project_id}.{self.dataset_id}.{self.tickets_table}"
        changelog_path = f"{self.project_id}.{self.dataset_id}.{self.changelog_table}"
        
        # Check if tables already exist, skip creation if they do
        try:
            ticket_table = self.client.get_table(table_path)
            self.client.get_table(changelog_path)
            logging.info("✅ Tables already exist, skipping creation")
            existing = {field.name for field in ticket_table.schema}
            missing = [f for f in DETAILS_PANEL_COLUMNS if f.name not in existing]
            if missing:
                ticket_table.schema = list(ticket_table.schema) + missing
                self.client.update_table(ticket_table, ["schema"])
                logging.info(f"✅ Added columns to {self.tickets_table}: {[f.name for f in missing]}")
            return True
        except Exception:
            logging.info("🛠️ Tables not found, creating new ones...")
        
        # JIRA Tickets table schema
        ticket_schema = [
            bigquery.SchemaField("issue_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("summary", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("status_category", "STRING"),
            bigquery.SchemaField("priority", "STRING"),
            bigquery.SchemaField("issue_type", "STRING"),
            bigquery.SchemaField("created", "TIMESTAMP"),
            bigquery.SchemaField("updated", "TIMESTAMP"),
            bigquery.SchemaField("resolutiondate", "TIMESTAMP"),
            bigquery.SchemaField("issuetype_name", "STRING"),
            bigquery.SchemaField("issuetype_id", "STRING"),
            bigquery.SchemaField("resolution", "STRING"),
            bigquery.SchemaField("parent", "STRING"),
            bigquery.SchemaField("parent_id", "STRING"),
            bigquery.SchemaField("last_viewed", "TIMESTAMP"),
            
            # User fields - all STRING type as per JIRA API
            bigquery.SchemaField("assignee_account_id", "STRING"),
            bigquery.SchemaField("assignee_display_name", "STRING"),
            bigquery.SchemaField("assignee_email_address", "STRING"),
            bigquery.SchemaField("reporter_account_id", "STRING"),
            bigquery.SchemaField("reporter_display_name", "STRING"),
            bigquery.SchemaField("reporter_email_address", "STRING"),
            bigquery.SchemaField("creator_account_id", "STRING"),
            bigquery.SchemaField("creator_display_name", "STRING"),
            bigquery.SchemaField("creator_email_address", "STRING"),
            
            # Project fields
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("project_key", "STRING"),
            bigquery.SchemaField("project_name", "STRING"),
            bigquery.SchemaField("project_self", "STRING"),
            
            # Attachments and comments
            bigquery.SchemaField("attachment_count", "INTEGER"),
            bigquery.SchemaField("comment_count", "INTEGER"),
            bigquery.SchemaField("attachment", "JSON"),
            bigquery.SchemaField("comment", "JSON"),
            
            # Standard JIRA fields
            bigquery.SchemaField("labels", "JSON"),
            bigquery.SchemaField("components", "JSON"),
            bigquery.SchemaField("fix_versions", "JSON"),
            bigquery.SchemaField("affected_versions", "JSON"),
            bigquery.SchemaField("watchers", "INTEGER"),
            bigquery.SchemaField("votes", "INTEGER"),
            bigquery.SchemaField("security_level", "STRING"),
            # Complex fields as JSON to handle nested structures
            bigquery.SchemaField("customers", "JSON", description="Array of customer objects with self, value, id"),
            
            # Support Ticket specific fields - Types aligned with JIRA field definitions

            bigquery.SchemaField("date_of_first_response", "TIMESTAMP"),
            bigquery.SchemaField("regression", "STRING"),
            bigquery.SchemaField("due_date", "DATE"),
            # Ensure these fields match JIRA definitions
            bigquery.SchemaField("ts_source", "STRING", description="Source of the ticket"),  # Was showing null values
            bigquery.SchemaField("certify_workflow_adherence", "JSON", description="Workflow adherence data"),  # Was showing null values
            bigquery.SchemaField("sub_customer", "JSON", description="Array of sub-customer selections"),  # Multiselect field in JIRA
            bigquery.SchemaField("type_of_request", "JSON", description="Array of request type objects with self, value, id"),
            bigquery.SchemaField("ops_team_designation", "JSON", description="Array of ops team designation objects with self, value, id"),
            bigquery.SchemaField("certify_error_or_client_error", "JSON", description="Selections for Certify Error vs Client Error"),

            
            # Operations Ticket specific fields
            
            # Keep only workflow fields that have data
            bigquery.SchemaField("steps_in_requested", "TIMESTAMP"),
            bigquery.SchemaField("timetracking", "JSON"),
            
            # Custom field mappings with descriptive names
            bigquery.SchemaField("source", "STRING"),
            bigquery.SchemaField("work_category", "STRING"),

            bigquery.SchemaField("pod", "STRING"),
            bigquery.SchemaField("client_support_task_type", "STRING"),
            bigquery.SchemaField("client_support_escalation_field", "STRING"),

            bigquery.SchemaField("request_participants", "JSON"),

            bigquery.SchemaField("npi", "FLOAT", description="NPI (National Provider Identifier) number"),
            bigquery.SchemaField("provider_npi", "STRING", description="Provider NPI from customfield_10716"),

            bigquery.SchemaField("time_to_resolution", "STRING", description="Time to Resolution (customfield_10650)"),
            
            # Custom fields as JSON for flexibility
            bigquery.SchemaField("custom_fields", "JSON"),
            
            # Audit fields
            bigquery.SchemaField("last_updated", "TIMESTAMP"),
            
            # Additional standard JIRA fields
            bigquery.SchemaField("environment", "STRING"),
            bigquery.SchemaField("progress", "JSON"),
            bigquery.SchemaField("worklog", "JSON"),
            bigquery.SchemaField("issuelinks", "JSON"),
            bigquery.SchemaField("subtasks", "JSON"),
            bigquery.SchemaField("aggregateprogress", "JSON"),
            bigquery.SchemaField("aggregate_time_estimate", "INTEGER"),
            bigquery.SchemaField("aggregate_time_original_estimate", "INTEGER"),
            bigquery.SchemaField("aggregate_time_spent", "INTEGER"),
            bigquery.SchemaField("time_estimate", "INTEGER"),
            bigquery.SchemaField("time_original_estimate", "INTEGER"),
            bigquery.SchemaField("time_spent", "INTEGER"),
            bigquery.SchemaField("timespent", "INTEGER"),
            bigquery.SchemaField("timeestimate", "INTEGER"),
            bigquery.SchemaField("timeoriginalestimate", "INTEGER"),
            
            # Additional custom fields for common JIRA ticket data
            bigquery.SchemaField("reporter", "JSON"),
            bigquery.SchemaField("assignee", "JSON"),
            bigquery.SchemaField("creator", "JSON"),
            bigquery.SchemaField("issuetype", "JSON"),
            bigquery.SchemaField("project", "JSON"),
            *DETAILS_PANEL_COLUMNS,
        ]
        
        # Changelog table schema
        changelog_schema = [
            bigquery.SchemaField("issue_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("issue_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("changelog_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("author_display_name", "STRING"),
            bigquery.SchemaField("author_account_id", "STRING"),
            bigquery.SchemaField("author_email_address", "STRING"),
            bigquery.SchemaField("created", "TIMESTAMP"),
            bigquery.SchemaField("field", "STRING"),
            bigquery.SchemaField("field_type", "STRING"),
            bigquery.SchemaField("from_value", "STRING"),
            bigquery.SchemaField("from_id", "STRING"),
            bigquery.SchemaField("to_value", "STRING"),
            bigquery.SchemaField("to_id", "STRING"),
            bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        ]
        
        try:
            # Create tickets table
            ticket_table_ref = self.dataset_ref.table(TICKET_TABLE)
            ticket_table = bigquery.Table(ticket_table_ref, schema=ticket_schema)
            ticket_table = self.client.create_table(ticket_table, exists_ok=True)
            logging.info(f"✅ Created table {TICKET_TABLE}")
            
            # Create changelog table
            changelog_table_ref = self.dataset_ref.table(CHANGELOG_TABLE)
            changelog_table = bigquery.Table(changelog_table_ref, schema=changelog_schema)
            changelog_table = self.client.create_table(changelog_table, exists_ok=True)
            logging.info(f"✅ Created table {CHANGELOG_TABLE}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error creating BigQuery tables: {e}")
            return False
    
    def get_jira_headers(self) -> Dict[str, str]:
        """Get JIRA API headers"""
        credentials = base64.b64encode(
            f"{JIRA_CONFIG['email']}:{JIRA_CONFIG['api_token']}".encode()
        ).decode()
        return {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def fetch_jira_data(self, mode: str = "full", start_date: Optional[str] = None, 
                       end_date: Optional[str] = None, max_issues: Optional[int] = None,
                       include_changelog: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch JIRA issues from API as a memory-efficient generator.
        
        UPDATED for API changes (May 2025):
        - /rest/api/3/search endpoint removed (410), use /rest/api/3/search/jql
        - Pagination is broken: same issues repeat, total=0 always
        - Workaround: Use date-range batching to get all issues
        - Track unique issue keys to prevent duplicates
        """
        
        # Build JQL query based on mode
        jql_parts = ["project = TS"]
        
        if mode == "incremental" and start_date:
            # For incremental mode, capture both newly created AND updated tickets
            # This ensures we don't miss tickets created on the target date
            date_condition = f"(updated >= '{start_date}' OR created >= '{start_date}')"
            jql_parts.append(date_condition)
            if end_date:
                end_condition = f"(updated <= '{end_date}' OR created <= '{end_date}')"
                jql_parts.append(end_condition)
        elif mode == "backfill":
            if start_date and end_date:
                jql_parts.append(f"created >= '{start_date}' AND created <= '{end_date}'")
            elif start_date:
                jql_parts.append(f"created >= '{start_date}'")
            elif end_date:
                jql_parts.append(f"created <= '{end_date}'")
        elif mode == "full" and (start_date or end_date):
            # For full mode with date filters, capture both created AND updated tickets
            if start_date and end_date:
                date_condition = f"(updated >= '{start_date}' AND updated <= '{end_date}' OR created >= '{start_date}' AND created <= '{end_date}')"
                jql_parts.append(date_condition)
            elif start_date:
                date_condition = f"(updated >= '{start_date}' OR created >= '{start_date}')"
                jql_parts.append(date_condition)
            elif end_date:
                date_condition = f"(updated <= '{end_date}' OR created <= '{end_date}')"
                jql_parts.append(date_condition)
        
        # Set appropriate ordering based on mode
        if mode == "backfill":
            jql = " AND ".join(jql_parts) + " ORDER BY created ASC"
        elif mode == "incremental":
            # For incremental mode, order by updated desc to get newest changes first
            # This makes the pagination more stable for recent tickets
            jql = ' AND '.join(jql_parts) + ' ORDER BY updated DESC, key ASC'
        else:
            jql = ' AND '.join(jql_parts) + ' ORDER BY key ASC'
        
        logging.info(f"🔍 JQL Query: {jql}")
        
        # Dynamically get the current highest ticket number
        latest_ticket_info = self._get_latest_ticket_info()
        expected_count = latest_ticket_info['estimated_total']
        highest_ticket = latest_ticket_info['highest_ticket']
        
        logging.info(f"📊 Current highest ticket in JIRA TS project: {highest_ticket}")
        logging.info(f"📊 Estimated total tickets: {expected_count:,}")
        logging.info(f"🔍 Fetching JIRA data with mode: {mode}")
        
        # Define fields to fetch - matching working main_kranthi_improved.py configuration
        fields_param = [
            # Core JIRA fields (matching working implementation)
            "summary", "description", "status", "priority", "issuetype", 
            "created", "updated", "resolutiondate", "resolution", 
            "assignee", "reporter", "creator", "project", "labels", 
            "duedate", "attachment", "comment",
            
            # Additional standard fields that work
            "components", "fixVersions", "versions", "watches", "votes",
            "parent", "lastViewed", "security",
            
            # Time tracking fields
            "timetracking",
            
            # Custom fields with confirmed data availability (from previous successful runs)
            "customfield_10224",  # regression (100% coverage)
            "customfield_10485",  # customers
            "customfield_10518",  # sub_customer
            "customfield_10024",  # satisfaction
            "customfield_11904",  # certify_workflow_adherence
            "customfield_10165",  # pod
            "customfield_10166",  # support_category
            "customfield_10040",  # work_category
            "customfield_10065",  # start_date
            "customfield_11015",  # steps_in_requested
            "customfield_10716",  # provider_npi
            "customfield_11409",  # source
            "customfield_10461",  # ts_source
            "customfield_11833",  # certify_workflow_adherence_json
            "customfield_10617",  # type_of_request
            "customfield_10029",  # request_participants
            "customfield_10650",  # Time to Resolution
            "customfield_10249",  # ops_team_designation (array of options)
            "customfield_10999",  # ops team designation (alt/full)
            "customfield_11906",  # Certify Error or Client Error? (multi-checkbox)
            "customfield_10059",  # Time to first response (SLA)
            "customfield_11080",  # NPI (National Provider Identifier)
            "customfield_10010",  # request_type (service desk Request Type)
            "customfield_10251",  # sentiment
            "customfield_13248",  # customer_concerns
            "customfield_13247",  # ticket_categorization (cascading select)
        ]
        
        start_at = 0
        # Optimize page size based on mode and changelog requirements
        if mode == 'full' and not include_changelog:
            page_size = 100
        elif mode == 'incremental':
            # For incremental mode, use smaller pages to reduce JIRA API load
            # Daily loads typically have 50-100 tickets, so smaller pages are more stable
            page_size = 25 if include_changelog else 50
        else:
            page_size = 20 if include_changelog else 50
        total_fetched = 0
        batch_process_limit = 1000  # Process in smaller batches to avoid Cloud Run timeout
        total_available = -1  # Initialize to unknown state
        # Per-page timeout (seconds) – lower than Cloud Run request timeout to fail fast
        page_timeout = 120
        # Adaptive timing thresholds (seconds)
        slow_threshold = 45
        very_slow_threshold = 90
        
        logging.info(f"🔍 Starting JIRA data stream with JQL: {jql}")
        
        # WORKAROUND: Use date-based batching due to broken pagination
        # Get all issues by querying date ranges since pagination doesn't work
        if mode == "full":
            yield from self._fetch_all_issues_date_batched(fields_param, include_changelog, max_issues)
            return
        
        # For incremental/backfill modes, try the old approach first
        seen_issue_keys = set()
        max_pages = 1000  # Safety limit to prevent infinite loops
        page_count = 0
        
        # For incremental mode, be more aggressive about falling back to date-based approach
        # Since daily loads typically have < 200 tickets, if pagination fails early, switch fast
        consecutive_empty_pages = 0
        max_empty_pages = 1 if mode == "incremental" else 3
        # /search/jql ignores startAt and pages with nextPageToken/isLast
        next_page_token = None
        
        while page_count < max_pages:
            try:
                url = f"{JIRA_CONFIG['base_url']}/rest/api/3/search/jql"
                params = {
                    'jql': jql,
                    'fields': ','.join(fields_param),
                    'maxResults': page_size
                }
                if next_page_token:
                    params['nextPageToken'] = next_page_token
                
                # Only expand changelog if explicitly requested
                if include_changelog:
                    params['expand'] = 'changelog'
                
                logging.info(f"📄 Fetching page: startAt={start_at}, maxResults={page_size}")
                t0 = time.perf_counter()
                response = self.http.get(url, params=params, timeout=page_timeout)
                response.raise_for_status()
                data = response.json()
                elapsed = time.perf_counter() - t0
                
                issues = data.get('issues', [])
                # High-signal debug for diagnosing "0 issues" runs (safe: no secrets)
                if start_at == 0:
                    try:
                        logging.info(
                            "🛰️ JIRA search/jql response: status=%s keys=%s issues_len=%s startAt=%s maxResults=%s",
                            getattr(response, "status_code", "unknown"),
                            ",".join(sorted(list(data.keys()))),
                            len(issues) if isinstance(issues, list) else "non-list",
                            data.get("startAt"),
                            data.get("maxResults"),
                        )
                    except Exception:
                        pass
                # NOTE: Jira Cloud has had periods where `total` is unreliable/0.
                # We primarily drive off the actual `issues` array.

                if not issues:
                    # If the very first page comes back empty, do NOT assume there are no results.
                    # In practice, this can happen due to API quirks (pagination/endpoint behavior)
                    # even when the same JQL returns results in the UI.
                    if start_at == 0 and mode in ("incremental", "backfill"):
                        logging.warning(
                            "⚠️ Empty first page from JIRA API (startAt=0). Falling back to date-batched fetch. "
                            "This usually indicates an API/pagination quirk rather than truly no matching issues."
                        )
                        yield from self._fetch_remaining_issues_date_batched(
                            fields_param=fields_param,
                            include_changelog=include_changelog,
                            seen_keys=set(),
                            max_issues=max_issues,
                            start_date=start_date,
                            end_date=end_date,
                            mode=mode,
                        )
                        return

                    logging.info("📋 No more issues found, stream complete.")
                    break
                
                # Track unique issues to detect pagination loops
                new_issues = []
                for issue in issues:
                    issue_key = issue.get('key')
                    if issue_key and issue_key not in seen_issue_keys:
                        seen_issue_keys.add(issue_key)
                        new_issues.append(issue)
                
                if not new_issues:
                    consecutive_empty_pages += 1
                    logging.warning(f"⚠️ No new issues found (consecutive empty pages: {consecutive_empty_pages}/{max_empty_pages})")
                    
                    if consecutive_empty_pages >= max_empty_pages:
                        logging.warning("⚠️ Pagination loop detected - switching to date-based approach.")
                        # For incremental mode, also log how many issues we've already seen to help with debugging
                        if mode == "incremental":
                            logging.info(f"🔍 Incremental mode: Already processed {len(seen_issue_keys)} unique issues before fallback")
                        yield from self._fetch_remaining_issues_date_batched(fields_param, include_changelog, seen_issue_keys, max_issues, start_date, end_date, mode)
                        return
                else:
                    # Reset counter when we find new issues
                    consecutive_empty_pages = 0
                
                # Yield only new (unique) issues
                for issue in new_issues:
                    # Changelog is now included via expand=changelog if requested
                    if include_changelog and not issue.get('changelog'):
                        # Fallback: if expand didn't work, set empty changelog
                        issue['changelog'] = {'histories': []}
                    yield issue
                    total_fetched += 1
                    # Stop the generator if max_issues is reached
                    if max_issues and total_fetched >= max_issues:
                        logging.info(f"📋 Reached maximum issues limit: {max_issues}")
                        return
                    
                    # Implement batch processing to prevent Cloud Run timeout
                    if total_fetched % batch_process_limit == 0:
                        logging.info(f"⏱️ Batch checkpoint: {total_fetched} issues processed. Yielding control to prevent timeout.")
                        time.sleep(0.5)  # Brief pause to allow other processes
                
                # Progress tracking (don't rely on total_available since it's broken)
                logging.info(f"📊 Progress: Streamed {total_fetched} unique issues (found {len(new_issues)} new in this page)")
                
                # Jira signals the last page with isLast / no nextPageToken
                next_page_token = data.get('nextPageToken')
                if data.get('isLast') or not next_page_token:
                    logging.info("📋 Reached last page of results.")
                    break
                
                logging.info(f"📦 Received {len(issues)} issues in this page")
                logging.info(f"📊 Page progress: start_at={start_at}, total_available={total_available}, total_fetched={total_fetched}")
                
                # CRITICAL: Increment start_at for next page to avoid pagination loops
                start_at += len(issues)
                page_count += 1
                logging.info(f"📄 Next page will start at: {start_at} (page {page_count})")

                # Adapt page size based on response latency
                max_page_size = 50 if not include_changelog else 20
                min_page_size = 10 if not include_changelog else 5
                
                if elapsed > very_slow_threshold and page_size > min_page_size:
                    new_size = max(min_page_size, page_size // 2)
                    logging.info(f"🐢 Response slow ({elapsed:.1f}s). Reducing page_size {page_size} -> {new_size}")
                    page_size = new_size
                elif elapsed > slow_threshold and page_size > min_page_size + 5:
                    new_size = max(min_page_size, page_size - 5)
                    logging.info(f"🐢 Response moderately slow ({elapsed:.1f}s). Reducing page_size {page_size} -> {new_size}")
                    page_size = new_size
                elif elapsed < 5 and page_size < max_page_size:
                    new_size = min(max_page_size, page_size + 5)
                    if new_size != page_size:
                        logging.info(f"🚀 Response fast ({elapsed:.1f}s). Increasing page_size {page_size} -> {new_size}")
                        page_size = new_size
                
            except requests.exceptions.Timeout as e:
                logging.warning(f"⚠️ Request timeout occurred: {e}. Retrying with exponential backoff...")
                retry_count = getattr(self, '_retry_count', 0) + 1
                if retry_count <= 3:
                    self._retry_count = retry_count
                    wait_time = min(30, 2 ** retry_count)  # Exponential backoff, max 30s
                    logging.info(f"⏳ Waiting {wait_time}s before retry {retry_count}/3")
                    time.sleep(wait_time)
                    continue
                else:
                    logging.error(f"❌ Max retries exceeded for timeout. Stopping.")
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f"❌ Request error in JIRA stream: {e}")
                break
            except Exception as e:
                logging.error(f"❌ Unexpected error in JIRA stream: {e}")
                break
        
        # Safety check for infinite loop prevention
        if page_count >= max_pages:
            logging.warning(f"⚠️ Reached maximum page limit ({max_pages}). This may indicate a pagination issue.")

    def fetch_issue_changelog(self, issue_key: str, page_size: int = 100, max_items: int = 1000) -> List[Dict[str, Any]]:
        """Fetch full changelog for a single issue using the dedicated endpoint with pagination.

        Returns a list compatible with the 'histories' structure expected by build_changelog_rows().
        """
        if not issue_key:
            return []
        url = f"{JIRA_CONFIG['base_url']}/rest/api/3/issue/{issue_key}/changelog"
        start_at = 0
        histories: List[Dict[str, Any]] = []
        while True:
            params = {
                'startAt': start_at,
                'maxResults': page_size
            }
            resp = self.http.get(url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            # Jira returns 'values' array for changelog endpoint
            items = payload.get('values') or payload.get('histories') or []
            if not items:
                break
            histories.extend(items)
            if max_items and len(histories) >= max_items:
                histories = histories[:max_items]
                break
            if len(items) < page_size:
                break
            start_at += len(items)
            # tiny pause to be gentle with rate limits
            time.sleep(0.05)
        return histories
    
    def _fetch_all_issues_date_batched(self, fields_param: List[str], include_changelog: bool, max_issues: Optional[int]) -> Generator[Dict[str, Any], None, None]:
        """Fetch all JIRA issues using date-based batching to work around broken pagination.
        
        This method splits the query into weekly date ranges starting from March 2023 when TS tickets begin.
        """
        from datetime import datetime, timedelta
        
        logging.info("🗓️ Using weekly date-based batching workaround for broken pagination")
        
        # Get current ticket info for better progress tracking
        try:
            latest_info = self._get_latest_ticket_info()
            estimated_total = latest_info['estimated_total'] 
            highest_ticket = latest_info['highest_ticket']
            logging.info(f"🎯 Target: Fetch ~{estimated_total:,} tickets (current highest: {highest_ticket})")
        except:
            estimated_total = 8659  # Fallback
            logging.info(f"🎯 Target: Fetch ~{estimated_total:,} tickets (estimated)")
        
        # Start from March 2023 when TS tickets actually begin (based on analysis)
        start_date = datetime(2023, 3, 1)
        current_date = datetime.now()
        
        total_fetched = 0
        seen_keys = set()
        
        # Generate weekly date ranges for better efficiency
        current_batch_date = start_date
        
        # Phase 1: Date-based batching (covers most tickets efficiently)
        logging.info("📅 Phase 1: Date-based weekly batching")
        while current_batch_date <= current_date:
            # Use weekly batches (7 days) for better efficiency
            batch_start = current_batch_date
            batch_end = min(current_batch_date + timedelta(days=6), current_date)  # 7-day batches
            
            # Format dates for JQL
            start_date_str = batch_start.strftime('%Y-%m-%d')
            end_date_str = batch_end.strftime('%Y-%m-%d')
            
            # JQL for this date range - capture both created AND updated tickets
            jql = f"project = TS AND (created >= '{start_date_str}' AND created <= '{end_date_str}' OR updated >= '{start_date_str}' AND updated <= '{end_date_str}') ORDER BY key ASC"
            
            # Always log weekly progress
            should_log = True
            
            # Fetch issues for this date range
            batch_count = 0
            try:
                batch_issues = list(self._fetch_issues_for_date_range(jql, fields_param, include_changelog, max_issues - total_fetched if max_issues else None))
                
                # Filter out duplicates
                new_issues = []
                for issue in batch_issues:
                    issue_key = issue.get('key')
                    if issue_key and issue_key not in seen_keys:
                        seen_keys.add(issue_key)
                        new_issues.append(issue)
                        batch_count += 1
                
                # Yield new issues
                for issue in new_issues:
                    yield issue
                    total_fetched += 1
                    
                    if max_issues and total_fetched >= max_issues:
                        logging.info(f"🎯 Reached max_issues limit: {max_issues}")
                        return
                
                # Log progress for weekly batches with ticket-based completion estimate
                if batch_count > 0 or should_log:
                    # Calculate both time-based and ticket-based progress
                    weeks_processed = (current_batch_date - start_date).days // 7 + 1
                    total_weeks = (current_date - start_date).days // 7 + 1
                    time_progress_pct = (weeks_processed / total_weeks) * 100
                    
                    # Ticket-based progress (more meaningful)
                    ticket_progress_pct = (total_fetched / estimated_total) * 100 if estimated_total > 0 else 0
                    
                    if batch_count > 0:
                        logging.info(f"📅 Week {start_date_str} to {end_date_str}: {batch_count} new issues (total: {total_fetched:,}, {ticket_progress_pct:.1f}% of estimated tickets)")
                    elif should_log:
                        logging.info(f"📊 Progress: Week {start_date_str} to {end_date_str} - {total_fetched:,} issues so far ({ticket_progress_pct:.1f}% of estimated tickets)")
                
            except Exception as e:
                logging.warning(f"⚠️ Error fetching data for week {start_date_str} to {end_date_str}: {e}")
            
            # Move to next week
            current_batch_date += timedelta(days=7)
        
        logging.info(f"✅ Phase 1 complete: {total_fetched} issues fetched via date-based batching")
        
        # Phase 2: Comprehensive key-based scan to guarantee full coverage
        try:
            latest_info = self._get_latest_ticket_info()
            latest_ticket_num = latest_info['ticket_number']
            logging.info(f"🔍 Phase 2: Enumerating keys TS-1..TS-{latest_ticket_num} to ensure full coverage")
            
            phase2_count = 0
            for issue in self._fetch_all_issues_by_key_enumeration(latest_ticket_num, fields_param, include_changelog):
                issue_key = issue.get('key')
                if issue_key and issue_key not in seen_keys:
                    seen_keys.add(issue_key)
                    yield issue
                    total_fetched += 1
                    phase2_count += 1
                    
                    if max_issues and total_fetched >= max_issues:
                        logging.info(f"🎯 Reached max_issues limit: {max_issues}")
                        break
            
            logging.info(f"✅ Phase 2 complete: {phase2_count} additional tickets found")
        except Exception as e:
            logging.warning(f"⚠️ Phase 2 key enumeration failed: {e}")
        
        logging.info(f"🎉 Complete: {total_fetched} total issues fetched across all phases")
    
    def _fetch_remaining_issues_date_batched(self, fields_param: List[str], include_changelog: bool, seen_keys: set, max_issues: Optional[int], start_date: Optional[str] = None, end_date: Optional[str] = None, mode: str = "full") -> Generator[Dict[str, Any], None, None]:
        """Fetch remaining issues using date batching after pagination failed."""
        from datetime import datetime, timedelta
        
        logging.info(f"🔄 Switching to date-based approach. Already seen {len(seen_keys)} issues.")

        total_fetched = 0

        # For incremental mode and full mode with date filters, we need to check both updated AND created
        # For backfill mode, use created field only
        use_combined_dates = (mode == "incremental") or (mode == "full" and (start_date or end_date))
        date_field = "updated" if mode == "incremental" else "created"

        # Set date range constraints
        if start_date:
            start_constraint = f" AND {date_field} >= '{start_date}'"
        else:
            start_constraint = ""

        if end_date:
            end_constraint = f" AND {date_field} <= '{end_date}'"
        else:
            end_constraint = ""

        # Try different date ranges to find issues not yet fetched
        current_date = datetime.now()
        if end_date:
            try:
                current_date = datetime.strptime(end_date, '%Y-%m-%d')
            except:
                pass

        start_date_dt = None
        if start_date:
            try:
                start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
            except:
                pass

        # For incremental mode and full mode with date filters, use daily batches since we know there are ~1000 tickets
        # and the monthly approach is missing many
        if use_combined_dates and start_date_dt:
            # Use daily batches from start_date to current date
            current_date_only = current_date.date()
            start_date_only = start_date_dt.date()
            
            logging.info(f"🗓️ Using daily batches from {start_date_only} to {current_date_only}")
            logging.info(f"📅 Combined dates mode: capturing both CREATED and UPDATED tickets in date ranges")
            
            # Process each day individually
            current_day = start_date_only
            while current_day <= current_date_only:
                if max_issues and total_fetched >= max_issues:
                    break
                    
                day_str = current_day.strftime('%Y-%m-%d')
                next_day = current_day + timedelta(days=1)
                next_day_str = next_day.strftime('%Y-%m-%d')
                
                # For days with potentially many tickets, use 6-hour batches
                # (This helps when daily queries hit API limits of ~100 tickets)
                for hour_start in [0, 6, 12, 18]:  # 4 batches per day: 00:00, 06:00, 12:00, 18:00
                    if max_issues and total_fetched >= max_issues:
                        break
                        
                    hour_end = hour_start + 6
                    batch_start = f"{day_str} {hour_start:02d}:00"
                    batch_end = f"{day_str} {hour_end:02d}:00" if hour_end < 24 else f"{next_day_str} 00:00"
                    
                    # Query for this 6-hour batch
                    if use_combined_dates:
                        # For incremental mode, capture both created and updated in this time range
                        jql = f"project = TS AND (updated >= '{batch_start}' AND updated < '{batch_end}' OR created >= '{batch_start}' AND created < '{batch_end}') ORDER BY key ASC"
                    else:
                        jql = f"project = TS AND {date_field} >= '{batch_start}' AND {date_field} < '{batch_end}' ORDER BY key ASC"
                    
                    try:
                        batch_issues = list(self._fetch_issues_for_date_range(jql, fields_param, include_changelog, 200))
                        
                        new_issues = []
                        for issue in batch_issues:
                            issue_key = issue.get('key')
                            if issue_key and issue_key not in seen_keys:
                                seen_keys.add(issue_key)
                                new_issues.append(issue)
                                
                        for issue in new_issues:
                            yield issue
                            total_fetched += 1
                            
                            if max_issues and total_fetched >= max_issues:
                                logging.info(f"🎯 Reached max_issues limit: {max_issues}")
                                return
                                
                        if new_issues:
                            mode_desc = "(created OR updated)" if use_combined_dates else f"({date_field})"
                            logging.info(f"📅 Found {len(new_issues)} new issues {mode_desc} in {batch_start} to {batch_end}")
                            
                    except Exception as e:
                        logging.warning(f"⚠️ Error processing batch {batch_start} to {batch_end}: {e}")
                
                current_day = next_day
        else:
            # Use monthly batches for other modes (existing logic)
            months_to_check = 24
            if start_date_dt:
                # Calculate months back from current date to start_date
                months_diff = (current_date.year - start_date_dt.year) * 12 + (current_date.month - start_date_dt.month)
                months_to_check = min(24, max(1, months_diff + 1))

            for months_back in range(0, months_to_check):
                if max_issues and total_fetched >= max_issues:
                    break

                range_end = current_date - timedelta(days=30 * months_back)
                range_start = current_date - timedelta(days=30 * (months_back + 1))

                # Don't go before start_date constraint
                if start_date_dt and range_start < start_date_dt:
                    range_start = start_date_dt

                start_str = range_start.strftime('%Y-%m-%d')
                end_str = range_end.strftime('%Y-%m-%d')

                if use_combined_dates:
                    # For incremental/full mode with dates, capture both created and updated in this range
                    jql = f"project = TS AND (updated >= '{start_str}' AND updated < '{end_str}' OR created >= '{start_str}' AND created < '{end_str}') ORDER BY key ASC"
                else:
                    jql = f"project = TS AND {date_field} >= '{start_str}' AND {date_field} < '{end_str}'{start_constraint}{end_constraint} ORDER BY key ASC"
                
                try:
                    batch_issues = list(self._fetch_issues_for_date_range(jql, fields_param, include_changelog, 100))
                    
                    new_issues = []
                    for issue in batch_issues:
                        issue_key = issue.get('key')
                        if issue_key and issue_key not in seen_keys:
                            seen_keys.add(issue_key)
                            new_issues.append(issue)
                            
                    for issue in new_issues:
                        yield issue
                        total_fetched += 1
                        
                        if max_issues and total_fetched >= max_issues:
                            return
                            
                    if new_issues:
                        mode_desc = "(created OR updated)" if use_combined_dates else f"({date_field})"
                        logging.info(f"📅 Found {len(new_issues)} new issues {mode_desc} in range {start_str} to {end_str}")
                        
                except Exception as e:
                    logging.warning(f"⚠️ Error in date range {start_str} to {end_str}: {e}")
        
        logging.info(f"🔄 Date-based fallback complete: {total_fetched} additional issues")
    
    def _fetch_issues_for_date_range(self, jql: str, fields_param: List[str], include_changelog: bool, max_issues: Optional[int]) -> Generator[Dict[str, Any], None, None]:
        """Fetch issues for a specific JQL query with optimized pagination for daily ranges."""
        url = f"{JIRA_CONFIG['base_url']}/rest/api/3/search/jql"
        start_at = 0
        page_size = 100  # Use larger pages for daily ranges since they're smaller
        fetched = 0
        
        # For daily ranges, we typically expect 0-50 issues per day, so one page should be enough
        max_pages = 3  # Safety limit for daily ranges
        page_count = 0
        
        while page_count < max_pages:
            if max_issues and fetched >= max_issues:
                break
                
            params = {
                'jql': jql,
                'fields': ','.join(fields_param),
                'startAt': start_at,
                'maxResults': min(page_size, max_issues - fetched if max_issues else page_size)
            }
            
            if include_changelog:
                params['expand'] = 'changelog'
            
            try:
                response = self.http.get(url, params=params, timeout=60)  # Shorter timeout for daily batches
                response.raise_for_status()
                data = response.json()
                
                issues = data.get('issues', [])
                if page_count == 0:
                    try:
                        logging.info(
                            "🛰️ Date-range search response: status=%s issues_len=%s startAt=%s maxResults=%s",
                            getattr(response, "status_code", "unknown"),
                            len(issues) if isinstance(issues, list) else "non-list",
                            data.get("startAt"),
                            data.get("maxResults"),
                        )
                    except Exception:
                        pass
                if not issues:
                    break
                
                for issue in issues:
                    if include_changelog and not issue.get('changelog'):
                        issue['changelog'] = {'histories': []}
                    yield issue
                    fetched += 1
                    
                    if max_issues and fetched >= max_issues:
                        return
                
                # If we got fewer than requested, we're done
                if len(issues) < params['maxResults']:
                    break
                    
                start_at += len(issues)
                page_count += 1
                
                # Add a small delay to be gentle on JIRA API
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"❌ Error fetching date range batch (page {page_count + 1}): {e}")
                break
    
    def convert_to_timestamp(self, date_str: Optional[str]) -> Optional[str]:
        """Convert JIRA date string to BigQuery timestamp"""
        if date_str is None or date_str == '' or date_str == 'null':
            return None
        try:
            dt = dt_parser.parse(date_str)
            # Always convert to UTC and remove timezone info for consistent comparison
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            else:
                # If no timezone info, assume UTC
                dt = dt.replace(tzinfo=None)
            # Format for BigQuery: YYYY-MM-DD HH:MM:SS.SSSSSS
            return dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        except Exception as e:
            logging.debug(f"Error converting timestamp {date_str}: {e}")
            return None
    
    def convert_to_date(self, date_str: Optional[str]) -> Optional[str]:
        """Convert JIRA date string to BigQuery date format (YYYY-MM-DD)"""
        if date_str is None or date_str == '' or date_str == 'null':
            return None
        try:
            dt = dt_parser.parse(date_str)
            return dt.date().isoformat()
        except Exception as e:
            logging.debug(f"Error converting date {date_str}: {e}")
            return None
    
    def extract_first_response_from_comments(self, comments: Any, created: str, reporter_account_id: str) -> Optional[str]:
        """Fallback: Extract first agent response timestamp from comments when SLA field is unavailable.
        
        Args:
            comments: JIRA comments field data
            created: Ticket creation timestamp
            reporter_account_id: Account ID of the ticket reporter
            
        Returns:
            Timestamp string of first non-reporter comment after creation, or None
        """
        if not comments or not isinstance(comments, dict):
            return None
        
        comments_list = comments.get('comments', [])
        if not comments_list:
            return None
        
        try:
            # Parse ticket creation time
            ticket_created = dt_parser.parse(created)
            
            # Find first non-reporter comment after ticket creation
            for comment in sorted(comments_list, key=lambda c: c.get('created', '')):
                comment_created_str = comment.get('created')
                if not comment_created_str:
                    continue
                    
                comment_created = dt_parser.parse(comment_created_str)
                author_id = comment.get('author', {}).get('accountId', '')
                
                # Skip reporter comments and system comments, find first agent response
                if (comment_created > ticket_created and 
                    author_id != reporter_account_id and
                    author_id):  # Has author (not system)
                    return self.convert_to_timestamp(comment_created_str)
            
            return None
            
        except Exception as e:
            logging.debug(f"Error extracting first response from comments: {e}")
            return None
    
    def extract_sla_first_response_date(self, sla_field: Any, comments: Any = None, created: str = None, reporter_account_id: str = None) -> Optional[str]:
        """Extract the first response completion timestamp from a JSM SLA field with comment fallback.
        
        Handles multiple shapes JIRA Service Management may return for SLA fields
        (schema type 'sd-servicelevelagreement'):
          - simple string timestamp
          - dict with 'completedCycles' (list of cycles with 'stopTime') and/or 'ongoingCycle'
          - list of SLA dicts (rare in fields API, but handle defensively)
        
        If SLA field is empty, falls back to extracting first agent response from comments.
        
        Returns a timestamp string formatted for BigQuery or None if not available.
        """
        # Try SLA field first
        if sla_field:
            # If it's already a string timestamp
            if isinstance(sla_field, str):
                return self.convert_to_timestamp(sla_field)
        
        def _extract_stop_times(obj: Any) -> List[str]:
            times: List[str] = []
            try:
                if isinstance(obj, dict):
                    cycles = obj.get('completedCycles') or []
                    for c in cycles:
                        if isinstance(c, dict):
                            stop_time = c.get('stopTime') or c.get('stopDate')
                            if isinstance(stop_time, str):
                                times.append(stop_time)
                elif isinstance(obj, list):
                    for item in obj:
                        times.extend(_extract_stop_times(item))
            except Exception:
                    pass
            return times
        
        stop_times = _extract_stop_times(sla_field)
        if stop_times:
                # Choose the earliest stop time as the first response completion
                try:
                    parsed = sorted([dt_parser.parse(t) for t in stop_times])
                    first = parsed[0].isoformat()
                    return self.convert_to_timestamp(first)
                except Exception as e:
                    logging.debug(f"Error parsing SLA stop times {stop_times}: {e}")
        
        # Fallback to comments analysis if SLA field is empty or invalid
        if comments and created and reporter_account_id:
            fallback_result = self.extract_first_response_from_comments(comments, created, reporter_account_id)
            if fallback_result:
                logging.debug(f"Using comment fallback for first response detection")
                return fallback_result
        
        return None

    def safe_get_int(self, data: Any, key_path: str, default: int = 0) -> int:
        """Safely get integer values with proper type conversion"""
        value = self.safe_get(data, key_path, default)
        if value is None:
            return default
        try:
            return int(str(value))
        except (ValueError, TypeError):
            return default

    def safe_get(self, data: Any, key_path: str, default: Any = None) -> Any:
        """Safely get nested dictionary values using dot notation"""
        if data is None:
            return default
            
        keys = key_path.split('.')
        current = data
        
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default
            if current is None:
                return default
        
        return current
    
    def safe_get_json(self, data: Any) -> Optional[str]:
        """Safely serialize data to JSON string"""
        if data is None:
            return json.dumps([])  # Return empty array for consistency
        try:
            return json.dumps(data, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            return json.dumps([])
        
    # Handle empty dictionaries
        if isinstance(data, dict) and len(data) == 0:
            return json.dumps({})
        
        try:
            # Convert non-serializable objects to strings
            def json_serial(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return str(obj)
            
            return json.dumps(data, default=json_serial)
        except (TypeError, ValueError) as e:
            logging.debug(f"Error serializing to JSON: {e}")
            return json.dumps(str(data))

    def extract_comment_text(self, comment_body: Any) -> str:
        """Extract plain text from JIRA comment body (handles both string and ADF format)"""
        if isinstance(comment_body, str):
            return comment_body
        elif isinstance(comment_body, dict):
            text_parts = []
            content_blocks = comment_body.get("content", [])
            for block in content_blocks:
                if block.get("type") == "paragraph":
                    for span in block.get("content", []):
                        if span.get("type") == "text" and isinstance(span.get("text"), str):
                            text_parts.append(span["text"])
            return " ".join(text_parts).strip() if text_parts else ""
        return ""

    def safe_get(self, data: Any, key: str, default=None) -> Any:
        """Safely get nested dictionary values
        
        Args:
            data: The data object to extract from (dict, object, etc.)
            key: The key to get, can be dot-notated for nested access
            default: The default value to return if the key is not found
            
        Returns:
            The value at the key or the default value
        """
        if data is None:
            return default
            
        # Handle dot notation for nested keys
        if '.' in key:
            keys = key.split('.')
            result = data
            for k in keys:
                if isinstance(result, dict) and k in result:
                    result = result.get(k)
                elif hasattr(result, k):
                    result = getattr(result, k)
                else:
                    return default
            return result if result is not None else default
        
        # Handle direct key access
        if isinstance(data, dict):
            return data.get(key, default)
        return default

    def extract_custom_records(self, field_data):
        """Extract custom field records as JSON"""
        if field_data is None:
            return None
        
        if isinstance(field_data, dict):
            return json.dumps(field_data)
        elif isinstance(field_data, list):
            return json.dumps(field_data)
        else:
            return json.dumps(str(field_data))
    
    def extract_description_text(self, description_obj) -> str:
        """Extract text from JIRA description object"""
        if description_obj is None:
            return ''
        
        # Handle string descriptions (old format)
        if isinstance(description_obj, str):
            return description_obj
        
        # Handle ADF (Atlassian Document Format) descriptions
        if isinstance(description_obj, dict):
            # Try to extract plain text from ADF content
            if 'content' in description_obj:
                return self._extract_text_from_adf(description_obj['content'])
            # Fallback to string representation
            return str(description_obj)
        
        return str(description_obj) if description_obj else ''
    
    def _extract_text_from_adf(self, content) -> str:
        """Extract text from ADF (Atlassian Document Format) content"""
        if not isinstance(content, list):
            return str(content) if content else ''
        
        text_parts = []
        for item in content:
            if isinstance(item, dict):
                if item.get('type') == 'text' and 'text' in item:
                    text_parts.append(item['text'])
                elif 'content' in item:
                    # Recursive extraction for nested content
                    text_parts.append(self._extract_text_from_adf(item['content']))
        
        return ' '.join(text_parts)

    def _extract_type_of_request(self, type_field: Any) -> List[Dict[str, str]]:
        """Extract type_of_request data handling both list and dict formats"""
        if isinstance(type_field, list):
            return [{
                'self': str(self.safe_get(c, 'self', '')),
                'value': str(self.safe_get(c, 'value', '')),
                'id': str(self.safe_get(c, 'id', ''))
            } for c in type_field if isinstance(c, dict)]
        elif isinstance(type_field, dict):
            return [{
                'self': str(self.safe_get(type_field, 'self', '')),
                'value': str(self.safe_get(type_field, 'value', '')),
                'id': str(self.safe_get(type_field, 'id', ''))
            }]
        return []
    
    def safe_get(self, data: Any, key: str, default=None) -> Any:
        """Safely get nested dictionary values
        
        Args:
            data: The data object to extract from (dict, object, etc.)
            key: The key to get, can be dot-notated for nested access
            default: The default value to return if the key is not found
            
        Returns:
            The value at the key or the default value
        """
        if data is None:
            return default
            
        # Handle dot notation for nested keys
        if '.' in key:
            keys = key.split('.')
            result = data
            for k in keys:
                if isinstance(result, dict) and k in result:
                    result = result.get(k)
                elif hasattr(result, k):
                    result = getattr(result, k)
                else:
                    return default
            return result if result is not None else default
        
        # Handle direct key access
        if isinstance(data, dict):
            return data.get(key, default)
        return default

    def build_ticket_row(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Transform JIRA issue into BigQuery row format with robust field handling"""
        fields = issue.get('fields', {})
        
        # Robust field extraction with comprehensive defaults
        ticket_row = {
            'issue_id': self.safe_get(issue, 'id', ''),
            'issue_key': self.safe_get(issue, 'key', ''),
            'summary': self.safe_get(fields, 'summary', ''),
            'description': self.extract_description_text(fields.get('description')),
            'status': self.safe_get(fields, 'status.name', ''),
            'status_category': self.safe_get(fields, 'status.statusCategory.name', ''),
            'priority': self.safe_get(fields, 'priority.name', ''),
            'issue_type': self.safe_get(fields, 'issuetype.name', ''),
            'created': self.convert_to_timestamp(self.safe_get(fields, 'created')),
            'updated': self.convert_to_timestamp(self.safe_get(fields, 'updated')),
            'resolutiondate': self.convert_to_timestamp(self.safe_get(fields, 'resolutiondate')),
            'issuetype_name': self.safe_get(fields, 'issuetype.name', ''),
            'issuetype_id': self.safe_get(fields, 'issuetype.id', ''),
            'resolution': self.safe_get(fields, 'resolution.name', ''),
            'parent': self.safe_get(fields, 'parent.key', ''),
            'parent_id': self.safe_get(fields, 'parent.id', ''),
            'last_viewed': self.convert_to_timestamp(self.safe_get(fields, 'lastViewed')),
            # Explicitly handle statusCategoryChangeDate which might be available under different case in API

            # SLA: Time to first response (with comment fallback)
            'date_of_first_response': self.extract_sla_first_response_date(
                sla_field=fields.get('customfield_10059'),
                comments=fields.get('comment'),
                created=fields.get('created'),
                reporter_account_id=self.safe_get(fields, 'assignee.accountId')
            ),
            
            # User fields
            'assignee_account_id': self.safe_get(fields, 'assignee.accountId', ''),
            'assignee_display_name': self.safe_get(fields, 'assignee.displayName', ''),
            'assignee_email_address': self.safe_get(fields, 'assignee.emailAddress', ''),
            'reporter_account_id': self.safe_get(fields, 'reporter.accountId', ''),
            'reporter_display_name': self.safe_get(fields, 'reporter.displayName', ''),
            'reporter_email_address': self.safe_get(fields, 'reporter.emailAddress', ''),
            'creator_account_id': self.safe_get(fields, 'creator.accountId', ''),
            'creator_display_name': self.safe_get(fields, 'creator.displayName', ''),
            'creator_email_address': self.safe_get(fields, 'creator.emailAddress', ''),
            
            # Project fields
            'project_id': self.safe_get(fields, 'project.id', ''),
            'project_key': self.safe_get(fields, 'project.key', ''),
            'project_name': self.safe_get(fields, 'project.name', ''),
            'project_self': self.safe_get(fields, 'project.self', ''),
            
            # Attachments and comments
            'attachment_count': len(self.safe_get(fields, 'attachment') or []),
            'comment_count': len(self.safe_get(fields, 'comment.comments') or []),
            
            # Handle attachment data
            'attachment': self.safe_get_json([{
                'filename': self.safe_get(a, 'filename', ''),
                'id': self.safe_get(a, 'id', ''),
                'self': self.safe_get(a, 'self', '')
            } for a in (fields.get('attachment') or []) if isinstance(a, dict)]),
            
            # Handle comment data with text extraction
            'comment': self.safe_get_json([{
                'id': self.safe_get(c, 'id', ''),
                'author': self.safe_get(c, 'author.displayName', ''),
                'created': self.convert_to_timestamp(self.safe_get(c, 'created')),
                'body': self.extract_comment_text(self.safe_get(c, 'body')),
                'self': self.safe_get(c, 'self', '')
            } for c in (self.safe_get(fields, 'comment.comments') or []) if isinstance(c, dict)]),
            
            # Standard JIRA fields
            'labels': self.safe_get_json(fields.get('labels')),
            'components': self.safe_get_json(fields.get('components')),
            'fix_versions': self.safe_get_json(fields.get('fixVersions')),
            'affected_versions': self.safe_get_json(fields.get('versions')),
            'watchers': int(self.safe_get(fields, 'watches.watchCount', 0)),
            'votes': int(self.safe_get(fields, 'votes.votes', 0)),
            'security_level': self.safe_get(fields, 'security.name', ''),
            
            # Time tracking fields
            'timetracking': self.safe_get_json(fields.get('timetracking')),
            
            # Handle complex fields with nested structures
            'customers': self.safe_get_json([{
                'self': self.safe_get(c, 'self', ''),
                'value': self.safe_get(c, 'value', ''),
                'id': self.safe_get(c, 'id', '')
            } for c in (fields.get('customfield_10485') or []) if isinstance(c, dict)]),
            
            # Explicitly handle problematic fields mentioned by user
            'ts_source': self.extract_custom_records(self.safe_get(fields, 'customfield_10461')) or '',
            'certify_workflow_adherence': self.safe_get_json(self.safe_get(fields, 'customfield_11833') or {}),
            'sub_customer': self.extract_custom_records(self.safe_get(fields, 'customfield_10518')),
            
            # Handle complex fields with nested structures
            'type_of_request': self.safe_get_json(self._extract_type_of_request(self.safe_get(fields, 'customfield_10617'))),
            'ops_team_designation': self.safe_get_json(
                self.safe_get(fields, 'customfield_10249') or self.safe_get(fields, 'customfield_10999') or {}
            ),
            'npi': self.safe_get(fields, 'customfield_11080', None),
            'provider_npi': str(self.safe_get(fields, 'customfield_10716', '') or ''),

            # Details panel fields
            'request_type': self.safe_get(fields, 'customfield_10010.requestType.name', None),
            'sentiment': self.safe_get(fields, 'customfield_10251.name', None),
            'customer_concerns': self.safe_get(fields, 'customfield_13248.value', None),
            'ticket_categorization': self.safe_get(fields, 'customfield_13247.value', None),
            'ticket_categorization_detail': self.safe_get(fields, 'customfield_13247.child.value', None),
            
            # Audit fields: keep a single stable value equal to JIRA's updated timestamp
            'last_updated': self.convert_to_timestamp(self.safe_get(fields, 'updated')),
        }
        
        # Add all custom fields from the mapping
        # Only include custom fields that actually have data or are business-critical
        custom_field_map = {
            # Fields with confirmed data presence
            'customfield_10224': 'regression',              # Has data (100% coverage) - CONFIRMED
            'customfield_10485': 'customers',               # Has data
            'customfield_10518': 'sub_customer',            # Has data
            'customfield_11904': 'certify_workflow_adherence', # Has data (15% coverage)
            'customfield_10165': 'pod',                     # Has data
            'customfield_10040': 'work_category',           # Has data (8% coverage)
            'customfield_11015': 'steps_in_requested',      # Has data (34% coverage)
            'customfield_11409': 'source',                  # Has data (50% coverage) - TS Source
            

            
            # System fields that are standard - removed fields not in BigQuery schema
            # 'customfield_11046': 'time_in_status',  # Not in schema
            # 'customfield_11047': 'attachment_count',  # Already handled separately
            # 'customfield_11048': 'comment_count',  # Already handled separately
            # 'customfield_11838': 'status_field',  # Not in schema
            
            # Keep core business fields even if currently sparse
            'customfield_10461': 'ts_source',               # Ticket source
            'customfield_11833': 'certify_workflow_adherence_json', # Workflow data
            
            # Request and categorization fields
            'customfield_10617': 'type_of_request',
            'customfield_11906': 'certify_error_or_client_error',
            
            # Missing fields for parity with MySQL
            'customfield_10029': 'request_participants',
            'customfield_10650': 'time_to_resolution'
        }
        
        # Process all custom fields from the mapping
        custom_fields = {}
        for field_key, field_name in custom_field_map.items():
            value = fields.get(field_key)
            if value is not None:
                custom_fields[field_name] = value
        
        # Add custom fields to the existing ticket_row instead of overwriting
        for field_name, value in custom_fields.items():
            # Handle request_participants as JSON field - defined in schema as JSON type
            if field_name == 'request_participants':
                if value is None or value == '':
                    ticket_row[field_name] = json.dumps({}) # Empty JSON object for empty values
                elif isinstance(value, (dict, list)):
                    ticket_row[field_name] = json.dumps(value) # Serialize as JSON
                else:
                    ticket_row[field_name] = json.dumps({'value': str(value)}) # Wrap non-dict/list in a JSON object
                    
            # Handle time_to_resolution field
            elif field_name == 'time_to_resolution':
                if value is None or value == '':
                    ticket_row[field_name] = ''
                else:
                    ticket_row[field_name] = str(value)
                    
            # Handle other string fields
            elif field_name in ['regression', 'pod', 'source', 'work_category',
                            'client_support_task_type', 'client_support_escalation_field', 'npi']:
                # Handle string fields
                if isinstance(value, dict) and 'value' in value:
                    ticket_row[field_name] = str(value['value'])
                elif isinstance(value, list) and len(value) > 0:
                    ticket_row[field_name] = str(value[0].get('value', '')) if isinstance(value[0], dict) else str(value[0])
                else:
                    ticket_row[field_name] = str(value) if value is not None else ''

            elif field_name in ['steps_in_requested']:
                # Handle workflow step fields as timestamps - extract from quotes and convert
                if value:
                    # Remove quotes if present and convert to timestamp
                    timestamp_str = str(value).strip('"')
                    ticket_row[field_name] = self.convert_to_timestamp(timestamp_str)
                else:
                    ticket_row[field_name] = None
            else:
                # Default handling for other custom fields
                ticket_row[field_name] = self.safe_get_json(value)
        
        # Add standard JIRA fields that might be missing from the basic row
        additional_fields = {
            'due_date': self.convert_to_date(self.safe_get(fields, 'duedate')),
            # Keep only fields that are likely to have data or are business critical
        }
        
        # Merge additional fields into ticket_row
        for field_name, field_value in additional_fields.items():
            if field_name not in ticket_row:  # Only add if not already present
                ticket_row[field_name] = field_value
        
        # Do not override last_updated with load time; it should reflect JIRA's updated timestamp only
        
        # Custom field processing is already handled above - no need for additional processing
        
        # Add custom_fields JSON for any remaining fields
        ticket_row['custom_fields'] = self.safe_get_json(custom_fields)
        
        return ticket_row
    
    def build_changelog_rows(self, issue: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform JIRA changelog into BigQuery rows"""
        changelog_rows = []
        issue_id = str(issue.get('id', ''))
        issue_key = str(issue.get('key', ''))
        changelog = issue.get('changelog', {})
        histories = changelog.get('histories', [])
        
        for history in histories:
            author = history.get('author', {}) or {}
            for item in history.get('items', []):
                # Handle null values safely
                from_value = item.get('fromString')
                if from_value is None:
                    from_value = ''
                
                to_value = item.get('toString')
                if to_value is None:
                    to_value = ''
                    
                from_id = item.get('from')
                if from_id is None:
                    from_id = ''
                    
                to_id = item.get('to')
                if to_id is None:
                    to_id = ''
                
                changelog_rows.append({
                    'issue_id': issue_id,
                    'issue_key': issue_key,
                    'changelog_id': str(history.get('id', '')),
                    'author_display_name': str(self.safe_get(author, 'displayName', '')),
                    'author_account_id': str(self.safe_get(author, 'accountId', '')),
                    'author_email_address': str(self.safe_get(author, 'emailAddress', '')),
                    'created': self.convert_to_timestamp(history.get('created')),
                    'field': str(item.get('field', '')),
                    'field_type': str(item.get('fieldtype', '')),
                    'from_value': str(from_value),
                    'from_id': str(from_id),
                    'to_value': str(to_value),
                    'to_id': str(to_id),
                    'load_timestamp': datetime.now(timezone.utc).isoformat(),
                })
        
        return changelog_rows
    
    def get_existing_tickets(self, issue_keys: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get existing tickets from BigQuery by issue keys"""
        if not issue_keys:
            return {}
            
        try:
            # Create a query to get the LATEST version of existing tickets
            keys_str = "', '".join(issue_keys)
            query = f"""
            WITH LatestTickets AS (
                SELECT issue_key, issue_id, updated, last_updated,
                    ROW_NUMBER() OVER (
                        PARTITION BY issue_key 
                        ORDER BY 
                            COALESCE(last_updated, TIMESTAMP('1900-01-01')) DESC,
                            COALESCE(updated, TIMESTAMP('1900-01-01')) DESC
                    ) as rn
                FROM `{self.project_id}.{self.dataset_id}.{self.tickets_table}`
                WHERE issue_key IN ('{keys_str}')
            )
            SELECT issue_key, issue_id, updated
            FROM LatestTickets
            WHERE rn = 1
            """
            
            results = self.client.query(query).result()
            
            existing_tickets = {}
            for row in results:
                existing_tickets[row.issue_key] = {
                    'issue_id': row.issue_id,
                    'updated': row.updated
                }
            
            if existing_tickets:
                logging.info(f"🔍 Found {len(existing_tickets)} existing tickets in database")
            
            return existing_tickets
            
        except Exception as e:
            logging.error(f"❌ Error checking existing tickets: {e}")
            return {}
    
    def filter_duplicates(self, tickets: List[Dict[str, Any]], update_existing: bool = True, force_update: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Filter out duplicate tickets and optionally identify tickets to update.

        force_update: re-write every existing ticket even if its `updated`
        timestamp is unchanged (one-time repair of stale columns).
        """
        if not tickets:
            return [], []
        
        # Get issue keys from the tickets
        issue_keys = [ticket.get('issue_key') for ticket in tickets if ticket.get('issue_key')]
        
        # Get existing tickets from database
        existing_tickets = self.get_existing_tickets(issue_keys)
        
        new_tickets = []
        updated_tickets = []
        
        for ticket in tickets:
            issue_key = ticket.get('issue_key')
            if not issue_key:
                continue
                
            if issue_key in existing_tickets:
                if force_update:
                    updated_tickets.append(ticket)
                elif update_existing:
                    # Check if the ticket has been updated since last load
                    existing_data = existing_tickets[issue_key]
                    ticket_updated = ticket.get('updated')
                    existing_updated = existing_data.get('updated')
                    
                    # Compare timestamps to see if we need to update
                    if ticket_updated and existing_updated:
                        try:
                            from dateutil import parser as dt_parser
                            from datetime import timezone
                            
                            # Parse and normalize both timestamps to UTC without timezone info
                            if isinstance(ticket_updated, str):
                                ticket_updated = dt_parser.parse(ticket_updated)
                            else:
                                ticket_updated = ticket_updated
                            
                            if hasattr(existing_updated, 'replace'):
                                existing_updated = existing_updated
                            else:
                                existing_updated = dt_parser.parse(str(existing_updated))
                            
                            # Normalize both to UTC and remove timezone info for consistent comparison
                            if ticket_updated.tzinfo is not None:
                                ticket_updated = ticket_updated.astimezone(timezone.utc).replace(tzinfo=None)
                            else:
                                ticket_updated = ticket_updated.replace(tzinfo=None)
                                
                            if existing_updated.tzinfo is not None:
                                existing_updated = existing_updated.astimezone(timezone.utc).replace(tzinfo=None)
                            else:
                                existing_updated = existing_updated.replace(tzinfo=None)
                            
                            if ticket_updated > existing_updated:
                                updated_tickets.append(ticket)
                                logging.debug(f"📝 Ticket {issue_key} will be updated (newer data available)")
                            else:
                                logging.debug(f"⏭️ Skipping {issue_key} (no updates since last load)")
                        except Exception as e:
                            logging.warning(f"⚠️ Error comparing timestamps for {issue_key}: {e}. Adding to update list.")
                            updated_tickets.append(ticket)
                    else:
                        # If we can't compare timestamps, update anyway
                        updated_tickets.append(ticket)
                else:
                    logging.debug(f"⏭️ Skipping duplicate ticket: {issue_key}")
            else:
                new_tickets.append(ticket)
        
        logging.info(f"📊 Deduplication results: {len(new_tickets)} new tickets, {len(updated_tickets)} tickets to update")
        return new_tickets, updated_tickets
    
    def update_existing_tickets(self, tickets: List[Dict[str, Any]]) -> bool:
        """Update existing tickets in BigQuery with streaming buffer handling"""
        if not tickets:
            return True
            
        try:
            # First, try the MERGE approach
            return self._try_merge_update(tickets)
            
        except Exception as e:
            error_msg = str(e).lower()
            if "streaming buffer" in error_msg:
                # Use deduplicated tickets if available, otherwise fall back to original batch
                fallback_tickets = getattr(self, '_deduplicated_tickets', tickets)
                logging.warning(f"⚠️ Streaming buffer conflict detected. Using fallback strategy for {len(fallback_tickets)} deduplicated tickets")
                return self._fallback_update_strategy(fallback_tickets)
            else:
                logging.error(f"❌ Error updating existing tickets: {e}")
                return False
    
    def _try_merge_update(self, tickets: List[Dict[str, Any]]) -> bool:
        """Attempt MERGE update operation"""
        temp_table_ref = None
        try:
            # Deduplicate tickets within batch to prevent MERGE conflicts
            # Keep only the latest version of each issue_key
            deduplicated_tickets = {}
            for ticket in tickets:
                issue_key = ticket.get('issue_key')
                if not issue_key:
                    continue
                    
                # Convert updated timestamp to datetime for comparison
                ticket_updated = ticket.get('updated')
                if isinstance(ticket_updated, str):
                    try:
                        ticket_updated = datetime.fromisoformat(ticket_updated.replace('Z', '+00:00'))
                    except:
                        ticket_updated = datetime.now(timezone.utc)
                elif not isinstance(ticket_updated, datetime):
                    ticket_updated = datetime.now(timezone.utc)
                
                # Keep the ticket with the latest timestamp
                if issue_key not in deduplicated_tickets or ticket_updated > deduplicated_tickets[issue_key].get('_updated_dt', datetime.min):
                    ticket['_updated_dt'] = ticket_updated
                    deduplicated_tickets[issue_key] = ticket
            
            # Convert back to list and remove the helper field
            deduplicated_tickets_list = []
            for ticket in deduplicated_tickets.values():
                ticket.pop('_updated_dt', None)
                deduplicated_tickets_list.append(ticket)
            
            # Store deduplicated tickets for potential fallback use
            self._deduplicated_tickets = deduplicated_tickets_list
            tickets = deduplicated_tickets_list
            
            # Create a temporary table with the updated data
            temp_table_id = f"{self.tickets_table}_temp_{int(time.time())}"
            temp_table_ref = self.dataset_ref.table(temp_table_id)
            
            # Get the schema from the main table
            main_table_ref = self.dataset_ref.table(self.tickets_table)
            main_table = self.client.get_table(main_table_ref)
            
            # Create temporary table with same schema
            temp_table = bigquery.Table(temp_table_ref, schema=main_table.schema)
            try:
                temp_table = self.client.create_table(temp_table)
                logging.info(f"✅ Created temporary table: {temp_table_id}")
            except Exception as create_error:
                logging.error(f"❌ Failed to create temporary table {temp_table_id}: {create_error}")
                return False
            
            # Verify table exists before proceeding
            try:
                self.client.get_table(temp_table_ref)
            except Exception as verify_error:
                logging.error(f"❌ Temporary table {temp_table_id} does not exist after creation: {verify_error}")
                return False
            
            # Insert data into temporary table with chunking to prevent timeout
            chunk_size = 100
            for i in range(0, len(tickets), chunk_size):
                chunk = tickets[i:i+chunk_size]
                try:
                    errors = self.client.insert_rows_json(temp_table_ref, chunk)
                    if errors:
                        logging.error(f"❌ Error inserting chunk {i//chunk_size + 1} into temp table: {errors}")
                        self.client.delete_table(temp_table_ref, not_found_ok=True)
                        return False
                except Exception as insert_error:
                    logging.error(f"❌ Exception inserting chunk {i//chunk_size + 1} into temp table: {insert_error}")
                    self.client.delete_table(temp_table_ref, not_found_ok=True)
                    return False
                # Brief pause between chunks to prevent overwhelming BigQuery
                time.sleep(0.1)
            
            # Perform MERGE operation - update every column the ETL populates
            # (except the key) so fields edited after the first load, e.g.
            # customers, are refreshed. Columns the rows don't carry are left
            # untouched rather than overwritten with NULL.
            row_columns = set().union(*(ticket.keys() for ticket in tickets))
            set_clause = ",\n                ".join(
                f"`{field.name}` = source.`{field.name}`"
                for field in main_table.schema
                if field.name != "issue_key" and field.name in row_columns
            )
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{self.tickets_table}` AS target
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
            ON target.issue_key = source.issue_key
            WHEN MATCHED THEN
              UPDATE SET
                {set_clause}
            """
            
            query_job = self.client.query(merge_query)
            query_job.result()
            
            # Clean up temporary table
            self.client.delete_table(temp_table_ref)
            
            logging.info(f"✅ Updated {len(tickets)} existing tickets in BigQuery using MERGE")
            return True
            
        except Exception as e:
            # Clean up temp table if it exists
            if temp_table_ref:
                try:
                    self.client.delete_table(temp_table_ref)
                except:
                    pass
            raise e
    
    def _fallback_update_strategy(self, tickets: List[Dict[str, Any]]) -> bool:
        """Fallback strategy when MERGE fails due to streaming buffer"""
        try:
            # Strategy: Insert updated records with a newer timestamp
            # The deduplication logic in views will handle showing the latest version
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
            
            # Update the last_updated timestamp to ensure these records are newer
            for ticket in tickets:
                ticket['last_updated'] = current_time
            
            # Insert the updated tickets as new records
            success = self.insert_tickets(tickets)
            
            if success:
                logging.info(f"✅ Applied fallback strategy: inserted {len(tickets)} updated tickets as new records")
                logging.info("💡 Deduplication views will show the latest version of each ticket")
                return True
            else:
                logging.error("❌ Fallback strategy failed")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error in fallback update strategy: {e}")
            return False
    
    def insert_tickets(self, tickets: List[Dict[str, Any]], enable_deduplication: bool = False, batch_number: Optional[int] = None, force_update: bool = False) -> bool:
        """Insert tickets into BigQuery with deduplication support and error notification."""
        if not tickets:
            return True
        
        if enable_deduplication:
            # Filter out duplicates and identify tickets to update
            try:
                new_tickets, updated_tickets = self.filter_duplicates(tickets, update_existing=True, force_update=force_update)
            except Exception as e:
                self._handle_error(
                    error=e,
                    operation="filter_duplicates",
                    batch_number=batch_number,
                    affected_records=len(tickets)
                )
                return False
            
            success = True
            
            # Insert new tickets
            if new_tickets:
                try:
                    table_ref = self.dataset_ref.table(TICKET_TABLE)
                    chunk_size = 100  # Reduced chunk size to prevent BigQuery timeout
                    inserted_new = 0
                    for i in range(0, len(new_tickets), chunk_size):
                        chunk = new_tickets[i:i+chunk_size]
                        chunk_num = i // chunk_size + 1
                        try:
                            errors = self.client.insert_rows_json(table_ref, chunk)
                            if errors:
                                error_msg = f"BigQuery insert_rows_json returned errors for chunk {chunk_num}: {errors}"
                                self._handle_error(
                                    error=Exception(error_msg),
                                    operation="insert_tickets_new_chunk",
                                    batch_number=batch_number,
                                    affected_records=len(chunk)
                                )
                                success = False
                                break
                            inserted_new += len(chunk)
                        except Exception as chunk_error:
                            self._handle_error(
                                error=chunk_error,
                                operation=f"insert_tickets_new_chunk_{chunk_num}",
                                batch_number=batch_number,
                                affected_records=len(chunk)
                            )
                            success = False
                            break
                    if success:
                        logging.info(f"✅ Inserted {inserted_new} new tickets into BigQuery (in chunks of {chunk_size})")
                        
                except Exception as e:
                    self._handle_error(
                        error=e,
                        operation="insert_tickets_new",
                        batch_number=batch_number,
                        affected_records=len(new_tickets)
                    )
                    success = False
            
            # Update existing tickets
            if updated_tickets:
                try:
                    if not self.update_existing_tickets(updated_tickets):
                        success = False
                except Exception as e:
                    self._handle_error(
                        error=e,
                        operation="update_existing_tickets",
                        batch_number=batch_number,
                        affected_records=len(updated_tickets)
                    )
                    success = False
            
            # Summary
            total_processed = len(new_tickets) + len(updated_tickets)
            skipped = len(tickets) - total_processed
            if skipped > 0:
                logging.info(f"⏭️ Skipped {skipped} tickets (no changes detected)")
            
            return success
        else:
            # Original behavior without deduplication
            try:
                table_ref = self.dataset_ref.table(TICKET_TABLE)
                inserted = 0
                chunk_size = 200
                for i in range(0, len(tickets), chunk_size):
                    chunk = tickets[i:i+chunk_size]
                    chunk_num = i // chunk_size + 1
                    try:
                        errors = self.client.insert_rows_json(table_ref, chunk)
                        if errors:
                            error_msg = f"BigQuery insert_rows_json returned errors for chunk {chunk_num}: {errors}"
                            self._handle_error(
                                error=Exception(error_msg),
                                operation="insert_tickets_chunk",
                                batch_number=batch_number,
                                affected_records=len(chunk)
                            )
                            return False
                        inserted += len(chunk)
                    except Exception as chunk_error:
                        self._handle_error(
                            error=chunk_error,
                            operation=f"insert_tickets_chunk_{chunk_num}",
                            batch_number=batch_number,
                            affected_records=len(chunk)
                        )
                        return False
                logging.info(f"✅ Inserted {inserted} tickets into BigQuery (in chunks of {chunk_size})")
                return True
                    
            except Exception as e:
                self._handle_error(
                    error=e,
                    operation="insert_tickets",
                    batch_number=batch_number,
                    affected_records=len(tickets)
                )
                return False
    
    def insert_changelog_entries(self, changelog_entries: List[Dict[str, Any]], batch_number: Optional[int] = None) -> bool:
        """Insert changelog entries into BigQuery (simple append) with error notification."""
        if not changelog_entries:
            return True
        try:
            table_ref = self.dataset_ref.table(CHANGELOG_TABLE)
            inserted = 0
            chunk_size = 200
            for i in range(0, len(changelog_entries), chunk_size):
                chunk = changelog_entries[i:i+chunk_size]
                chunk_num = i // chunk_size + 1
                try:
                    errors = self.client.insert_rows_json(table_ref, chunk)
                    if errors:
                        error_msg = f"BigQuery insert_rows_json returned errors for changelog chunk {chunk_num}: {errors}"
                        self._handle_error(
                            error=Exception(error_msg),
                            operation="insert_changelog_chunk",
                            batch_number=batch_number,
                            affected_records=len(chunk)
                        )
                        return False
                    inserted += len(chunk)
                except Exception as chunk_error:
                    self._handle_error(
                        error=chunk_error,
                        operation=f"insert_changelog_chunk_{chunk_num}",
                        batch_number=batch_number,
                        affected_records=len(chunk)
                    )
                    return False
            logging.info(f"✅ Inserted {inserted} changelog entries into BigQuery (in chunks of {chunk_size})")
            return True
        except Exception as e:
            self._handle_error(
                error=e,
                operation="insert_changelog_entries",
                batch_number=batch_number,
                affected_records=len(changelog_entries)
            )
            return False

    def upsert_changelog_entries(self, changelog_entries: List[Dict[str, Any]], batch_number: Optional[int] = None) -> bool:
        """Upsert changelog entries using MERGE to avoid duplicates for scheduled runs with error notification."""
        if not changelog_entries:
            return True
        
        temp_table_ref = None
        try:
            # Create temporary table with same schema as target
            temp_table_id = f"{self.changelog_table}_temp_{int(time.time())}"
            temp_table_ref = self.dataset_ref.table(temp_table_id)
            main_table_ref = self.dataset_ref.table(self.changelog_table)
            main_table = self.client.get_table(main_table_ref)
            temp_table = bigquery.Table(temp_table_ref, schema=main_table.schema)
            self.client.create_table(temp_table)

            # Load data into temporary table
            chunk_size = 200
            for i in range(0, len(changelog_entries), chunk_size):
                chunk = changelog_entries[i:i+chunk_size]
                chunk_num = i // chunk_size + 1
                try:
                    errors = self.client.insert_rows_json(temp_table_ref, chunk)
                    if errors:
                        error_msg = f"BigQuery insert_rows_json returned errors for temp changelog chunk {chunk_num}: {errors}"
                        self._handle_error(
                            error=Exception(error_msg),
                            operation="upsert_changelog_temp_chunk",
                            batch_number=batch_number,
                            affected_records=len(chunk)
                        )
                        self.client.delete_table(temp_table_ref, not_found_ok=True)
                        return False
                except Exception as chunk_error:
                    self._handle_error(
                        error=chunk_error,
                        operation=f"upsert_changelog_temp_chunk_{chunk_num}",
                        batch_number=batch_number,
                        affected_records=len(chunk)
                    )
                    self.client.delete_table(temp_table_ref, not_found_ok=True)
                    return False

            # MERGE: match on stable identity across items in a history
            merge_query = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{self.changelog_table}` AS target
            USING `{self.project_id}.{self.dataset_id}.{temp_table_id}` AS source
            ON target.issue_key = source.issue_key
               AND target.changelog_id = source.changelog_id
               AND target.field = source.field
               AND COALESCE(target.to_value, '') = COALESCE(source.to_value, '')
               AND COALESCE(target.from_value, '') = COALESCE(source.from_value, '')
            WHEN NOT MATCHED THEN INSERT (
              issue_id, issue_key, changelog_id,
              author_display_name, author_account_id, author_email_address,
              created, field, field_type,
              from_value, from_id, to_value, to_id, load_timestamp
            ) VALUES (
              source.issue_id, source.issue_key, source.changelog_id,
              source.author_display_name, source.author_account_id, source.author_email_address,
              source.created, source.field, source.field_type,
              source.from_value, source.from_id, source.to_value, source.to_id, source.load_timestamp
            )
            """
            query_job = self.client.query(merge_query)
            query_job.result()
            
            # Get job ID if available
            job_id = query_job.job_id
            
            self.client.delete_table(temp_table_ref, not_found_ok=True)
            logging.info(f"✅ Upserted {len(changelog_entries)} changelog entries via MERGE")
            return True
        except Exception as e:
            self._handle_error(
                error=e,
                operation="upsert_changelog_entries",
                batch_number=batch_number,
                affected_records=len(changelog_entries)
            )
            try:
                if temp_table_ref:
                    self.client.delete_table(temp_table_ref, not_found_ok=True)
            except Exception:
                pass
            return False
    
    def create_customer_analysis_view(self) -> bool:
        """Create BigQuery view for customer data analysis"""
        view_query = f"""
        CREATE OR REPLACE VIEW `{BIGQUERY_CONFIG['dataset_id']}.customer_analysis` AS
        SELECT 
            issue_id,
            issue_key,
            summary,
            status,
            priority,
            created,
            updated,
            JSON_EXTRACT_SCALAR(customers, '$[0].value') as primary_customer,
            JSON_EXTRACT_SCALAR(customers, '$[0].id') as primary_customer_id,
            ARRAY(
                SELECT JSON_EXTRACT_SCALAR(customer, '$.value')
                FROM UNNEST(JSON_EXTRACT_ARRAY(customers)) as customer
            ) as customer_names,
            ARRAY(
                SELECT JSON_EXTRACT_SCALAR(customer, '$.id')
                FROM UNNEST(JSON_EXTRACT_ARRAY(customers)) as customer
            ) as customer_ids,
            ARRAY_LENGTH(JSON_EXTRACT_ARRAY(customers)) as customer_count,
            JSON_EXTRACT_SCALAR(sub_customer, '$[0].value') as primary_sub_customer,
            ARRAY_LENGTH(JSON_EXTRACT_ARRAY(sub_customer)) as sub_customer_count
        FROM `{BIGQUERY_CONFIG['dataset_id']}.{TICKET_TABLE}`
        WHERE customers IS NOT NULL OR sub_customer IS NOT NULL
        """
        
        try:
            query_job = self.client.query(view_query)
            query_job.result()
            logging.info("✅ Created customer analysis view")
            return True
        except Exception as e:
            logging.error(f"Error creating customer analysis view: {e}")
            return False

    def create_changelog_views(self) -> bool:
        """Create views splitting changelog into JIRA-native vs custom fields (Option A)."""
        dataset = BIGQUERY_CONFIG['dataset_id']
        project = BIGQUERY_CONFIG['project_id']
        changelog_fqn = f"`{project}.{dataset}.{self.changelog_table}`"

        jira_view = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_changelog_jira_events` AS
        SELECT
          issue_id,
          issue_key,
          changelog_id,
          author_display_name,
          author_account_id,
          author_email_address,
          created,
          field,
          field_type,
          from_value,
          from_id,
          to_value,
          to_id
        FROM {changelog_fqn}
        WHERE LOWER(field_type) = 'jira'
        """

        custom_view = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_changelog_custom_events` AS
        SELECT
          issue_id,
          issue_key,
          changelog_id,
          author_display_name,
          author_account_id,
          author_email_address,
          created,
          field,
          field_type,
          from_value,
          from_id,
          to_value,
          to_id
        FROM {changelog_fqn}
        WHERE LOWER(field_type) = 'custom'
        """

        try:
            self.client.query(jira_view).result()
            self.client.query(custom_view).result()
            logging.info("✅ Created changelog split views (jira/custom)")
            return True
        except Exception as e:
            logging.error(f"Error creating changelog split views: {e}")
            return False

    def create_status_views(self) -> bool:
        """Create status transitions and time-in-status views (Option C)."""
        dataset = BIGQUERY_CONFIG['dataset_id']
        project = BIGQUERY_CONFIG['project_id']
        tickets_fqn = f"`{project}.{dataset}.{self.tickets_table}`"
        changelog_fqn = f"`{project}.{dataset}.{self.changelog_table}`"

        transitions_view = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_status_transitions` AS
        SELECT
          issue_key,
          TIMESTAMP(created) AS change_at,
          from_value AS from_status,
          to_value AS to_status
        FROM {changelog_fqn}
        WHERE field = 'status'
        """

        durations_view = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_status_durations` AS
        WITH ch AS (
          SELECT issue_key, TIMESTAMP(created) AS change_at, from_value, to_value
          FROM {changelog_fqn}
          WHERE field = 'status'
        ),
        first_change AS (
          SELECT issue_key, MIN(change_at) AS first_change_at,
                 ANY_VALUE(from_value) AS first_from_value
          FROM ch
          GROUP BY issue_key
        ),
        initial AS (
          SELECT t.issue_key,
                 first_from_value AS status,
                 TIMESTAMP(t.created) AS entered_at,
                 fc.first_change_at AS left_at
          FROM {tickets_fqn} t
          JOIN first_change fc USING (issue_key)
          WHERE first_from_value IS NOT NULL AND first_from_value != ''
        ),
        sequenced AS (
          SELECT issue_key,
                 to_value AS status,
                 change_at AS entered_at,
                 LEAD(change_at) OVER (PARTITION BY issue_key ORDER BY change_at) AS left_at
          FROM ch
          WHERE to_value IS NOT NULL AND to_value != ''
        ),
        no_changes AS (
          SELECT t.issue_key,
                 t.status AS status,
                 TIMESTAMP(t.created) AS entered_at,
                 TIMESTAMP(t.resolutiondate) AS left_at
          FROM {tickets_fqn} t
          WHERE NOT EXISTS (
            SELECT 1 FROM ch WHERE ch.issue_key = t.issue_key
          )
        ),
        intervals AS (
          SELECT * FROM initial
          UNION ALL
          SELECT * FROM sequenced
          UNION ALL
          SELECT * FROM no_changes
        )
        SELECT 
          i.issue_key,
          i.status,
          i.entered_at,
          COALESCE(i.left_at, TIMESTAMP(t.resolutiondate), CURRENT_TIMESTAMP()) AS left_at,
          TIMESTAMP_DIFF(COALESCE(i.left_at, TIMESTAMP(t.resolutiondate), CURRENT_TIMESTAMP()), i.entered_at, SECOND) AS seconds_in_status
        FROM intervals i
        JOIN {tickets_fqn} t USING (issue_key)
        """

        time_in_status_view = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_time_in_status` AS
        SELECT
          issue_key,
          status,
          SUM(seconds_in_status) AS seconds_in_status,
          MIN(entered_at) AS first_entered_at,
          MAX(left_at) AS last_left_at
        FROM `{dataset}.jira_status_durations`
        GROUP BY issue_key, status
        """

        try:
            self.client.query(transitions_view).result()
            self.client.query(durations_view).result()
            self.client.query(time_in_status_view).result()
            logging.info("✅ Created status transitions and time-in-status views")
            return True
        except Exception as e:
            logging.error(f"Error creating status/time-in-status views: {e}")
            return False

    def create_latest_essentials_view(self) -> bool:
        """Create small pivot view with latest essentials (Option B)."""
        dataset = BIGQUERY_CONFIG['dataset_id']
        project = BIGQUERY_CONFIG['project_id']
        tickets_fqn = f"`{project}.{dataset}.{self.tickets_table}`"
        changelog_fqn = f"`{project}.{dataset}.{self.changelog_table}`"

        view_sql = f"""
        CREATE OR REPLACE VIEW `{dataset}.jira_latest_essentials` AS
        SELECT
          t.issue_key,
          t.status AS current_status,
          t.priority AS current_priority,
          t.assignee_display_name AS current_assignee,
          t.assignee_email_address AS current_assignee_email,
          t.updated,
          t.last_updated,
          MAX(IF(c.field = 'comment', TIMESTAMP(c.created), NULL)) AS last_comment_at,
          MAX(IF(c.field = 'assignee', TIMESTAMP(c.created), NULL)) AS last_assignee_change_at,
          MAX(IF(c.field = 'status', TIMESTAMP(c.created), NULL)) AS last_status_change_at
        FROM {tickets_fqn} t
        LEFT JOIN {changelog_fqn} c
        ON c.issue_key = t.issue_key
        GROUP BY
          t.issue_key, t.status, t.priority, t.assignee_display_name, t.assignee_email_address, t.updated, t.last_updated
        """

        try:
            self.client.query(view_sql).result()
            logging.info("✅ Created latest essentials view")
            return True
        except Exception as e:
            logging.error(f"Error creating latest essentials view: {e}")
            return False

    def deduplicate_existing_data(self) -> bool:
        """Remove duplicate tickets, keeping only the latest version of each issue_key"""
        table_fqn = f"`{self.project_id}.{self.dataset_id}.{self.tickets_table}`"
        backup_table = f"`{self.project_id}.{self.dataset_id}.{self.tickets_table}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}`"
        
        logging.info(f"🔍 Checking for duplicates in {table_fqn}")
        
        # Check current duplicate status
        duplicate_check_query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT issue_key) as unique_issues,
            COUNT(*) - COUNT(DISTINCT issue_key) as duplicate_count
        FROM {table_fqn}
        """
        
        try:
            results = self.client.query(duplicate_check_query).result()
            stats = list(results)[0]
            
            logging.info(f"📊 Current Status:")
            logging.info(f"   • Total records: {stats.total_records:,}")
            logging.info(f"   • Unique issues: {stats.unique_issues:,}")
            logging.info(f"   • Duplicate records: {stats.duplicate_count:,}")
            
            if stats.duplicate_count == 0:
                logging.info("✅ No duplicates found! Table is already clean.")
                return True
            
            # Create backup
            logging.info(f"💾 Creating backup table: {backup_table}")
            backup_query = f"""
            CREATE TABLE {backup_table} AS
            SELECT * FROM {table_fqn}
            """
            self.client.query(backup_query).result()
            logging.info("✅ Backup created successfully")
            
            # Create clean table with latest records only
            clean_table = f"`{self.project_id}.{self.dataset_id}.{self.tickets_table}_clean`"
            logging.info(f"🧹 Creating clean table: {clean_table}")
            
            create_clean_query = f"""
            CREATE OR REPLACE TABLE {clean_table} AS
            WITH latest_tickets AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY issue_key 
                        ORDER BY 
                            COALESCE(last_updated, TIMESTAMP('1900-01-01')) DESC,
                            COALESCE(updated, TIMESTAMP('1900-01-01')) DESC
                    ) as rn
                FROM {table_fqn}
            )
            SELECT * EXCEPT(rn)
            FROM latest_tickets 
            WHERE rn = 1
            """
            
            self.client.query(create_clean_query).result()
            logging.info("✅ Clean table created successfully")
            
            # Replace original table
            logging.info("🔄 Replacing original table with clean version...")
            self.client.query(f"DROP TABLE {table_fqn}").result()
            
            rename_query = f"""
            CREATE TABLE {table_fqn} AS
            SELECT * FROM {clean_table}
            """
            self.client.query(rename_query).result()
            self.client.query(f"DROP TABLE {clean_table}").result()
            
            logging.info("🎉 Deduplication completed successfully!")
            logging.info(f"📊 Removed {stats.duplicate_count:,} duplicate records")
            logging.info(f"   • Backup saved as: {backup_table}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error during deduplication: {e}")
            return False

    def _fetch_and_process_single_ticket(self, ticket_key: str, fields_param: List[str]) -> Tuple[Optional[Dict], Optional[List[Dict]]]:
        """Fetch and process a single ticket, returning (row, changelogs) or (None, None) if missing/error."""
        try:
            ticket_url = f"{JIRA_CONFIG['base_url']}/rest/api/3/issue/{ticket_key}"
            params = {
                'fields': ','.join(fields_param),
                'expand': 'changelog'
            }
            
            response = self.http.get(ticket_url, params=params, timeout=60)
            if response.status_code == 404:
                # Log the LAST few 404s to help debugging
                # We use a simple counter on the instance
                self._404_count = getattr(self, '_404_count', 0) + 1
                # Log every 100th failure and the last ones
                if self._404_count % 100 == 0:
                     logging.warning(f"⚠️ Ticket {ticket_key} returned 404 Not Found (Total 404s so far: {self._404_count}). It may be deleted or you lack permission.")
                return None, None
            
            # Check for other error codes explicitly
            if response.status_code == 401:
                logging.error(f"❌ Unauthorized (401) for {ticket_key} - Check API token.")
                return None, None
            if response.status_code == 403:
                logging.warning(f"🚫 Forbidden (403) for {ticket_key} - You do not have permission to view this ticket.")
                return None, None

            response.raise_for_status()
            ticket_data = response.json()
            
            ticket_row = self.build_ticket_row(ticket_data)
            changelog_entries = self.build_changelog_rows(ticket_data)
            
            return ticket_row, changelog_entries
            
        except Exception as e:
            if "404" not in str(e):
                logging.warning(f"⚠️ Failed to fetch ticket {ticket_key}: {e}")
            return None, None

    def backfill_missing_tickets(self):
        """Identify and backfill missing tickets using key enumeration with parallel processing"""
        logging.info("🔍 Identifying missing tickets...")
        
        # Ensure tables exist before querying
        if not self.create_tables():
            return False
            
        # Get all ticket keys from BigQuery
        bq_tickets_query = f"""
        SELECT DISTINCT issue_key 
        FROM `{self.project_id}.{self.dataset_id}.{self.tickets_table}`
        ORDER BY issue_key
        """
        bq_tickets = set()
        try:
            result = self.client.query(bq_tickets_query).result()
            for row in result:
                bq_tickets.add(row.issue_key)
        except Exception as e:
            logging.error(f"❌ Failed to get BigQuery tickets: {e}")
            return False
        
        logging.info(f"📊 Found {len(bq_tickets)} tickets in BigQuery")
        
        # Get the highest ticket number for enumeration
        latest_info = self._get_latest_ticket_info()
        highest_ticket_num = latest_info['ticket_number']
        logging.info(f"🎯 Highest JIRA ticket: TS-{highest_ticket_num}")
        
        # Identify missing tickets by key enumeration
        missing_tickets = []
        logging.info(f"🔢 Checking for missing tickets from TS-1 to TS-{highest_ticket_num}...")
        
        for num in range(1, highest_ticket_num + 1):
            ticket_key = f"TS-{num}"
            if ticket_key not in bq_tickets:
                missing_tickets.append(ticket_key)
        
        logging.info(f"🔍 Found {len(missing_tickets)} missing tickets")
        
        if missing_tickets:
            logging.info(f"⏳ Backfilling {len(missing_tickets)} missing tickets (parallel execution)...")
            backfilled_count = 0
            failed_count = 0
            
            # Get standard fields for backfill
            fields_param = [
                'summary', 'description', 'status', 'priority', 'issuetype',
                'created', 'updated', 'resolutiondate', 'resolution', 'assignee',
                'reporter', 'creator', 'project', 'labels', 'duedate', 'attachment',
                'comment', 'components', 'fixVersions', 'versions', 'watches', 'votes',
                'parent', 'lastViewed', 'security', 'timetracking'
            ] + [
                'customfield_10224', 'customfield_10485', 'customfield_10518', 'customfield_10024',
                'customfield_11904', 'customfield_10165', 'customfield_10166', 'customfield_10040',
                'customfield_10065', 'customfield_11015', 'customfield_10716', 'customfield_11409',
                'customfield_10461', 'customfield_11833', 'customfield_10617', 'customfield_10029',
                'customfield_10650', 'customfield_10249', 'customfield_10999', 'customfield_11906',
                'customfield_10059', 'customfield_11080',
                'customfield_10010', 'customfield_10251', 'customfield_13248', 'customfield_13247'
            ]
            
            batch_tickets = []
            batch_changelog_entries = []
            batch_size = 50  # Smaller batches for backfill
            
            # Soft timebox to keep Cloud Run requests within limits
            timebox_seconds = int(os.environ.get('ETL_TIMEBOX_SECONDS', '3000'))
            deadline = time.time() + timebox_seconds
            
            checked_count = 0
            total_missing = len(missing_tickets)
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            # Use thread pool for parallel fetching
            # JIRA API rate limit is usually the bottleneck, but 10 threads is typically safe
            max_workers = 10
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_key = {
                    executor.submit(self._fetch_and_process_single_ticket, key, fields_param): key 
                    for key in missing_tickets
                }
                
                for future in as_completed(future_to_key):
                    checked_count += 1
                    ticket_key = future_to_key[future]
                    
                    # Timebox check
                    if time.time() > deadline - 60:
                        logging.info("⏱️ Approaching execution timebox in backfill, stopping early.")
                        break
                    
                    try:
                        ticket_row, changelog_entries = future.result()
                        
                        if ticket_row:
                            batch_tickets.append(ticket_row)
                            logging.info(f"✅ Found ticket {ticket_key}")
                            if changelog_entries:
                                batch_changelog_entries.extend(changelog_entries)
                        else:
                            # 404 or error handled in helper
                            logging.info(f"❌ Missing/Error ticket {ticket_key}")
                            pass

                        # Process batch when full
                        if len(batch_tickets) >= batch_size:
                            if self.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication=True):
                                backfilled_count += len(batch_tickets)
                                logging.info(f"📦 Batch saved. Total backfilled: {backfilled_count}")
                            else:
                                failed_count += len(batch_tickets)
                            
                            # Reset batch
                            batch_tickets = []
                            batch_changelog_entries = []
                            
                        # Log progress periodically
                        if checked_count % 100 == 0:
                            logging.info(f"⏳ Checked {checked_count}/{total_missing} tickets... (Backfilled: {backfilled_count}, Found pending: {len(batch_tickets)})")
                            
                    except Exception as e:
                        logging.error(f"❌ Error processing future for {ticket_key}: {e}")
                        failed_count += 1
            
            # Process final batch
            if batch_tickets:
                if self.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication=True):
                    backfilled_count += len(batch_tickets)
                else:
                    failed_count += len(batch_tickets)
            
            logging.info(f"✅ Backfill completed: {backfilled_count} tickets added, {failed_count} failed/skipped")
            
            # If we processed tickets or all missing tickets were 404s (so backfilled=0 but failed=0), consider it success
            return backfilled_count > 0 or failed_count == 0
        else:
            logging.info("✅ No missing tickets found")
            return True

    def create_latest_tickets_view(self) -> bool:
        """Create a view that always shows latest version of each ticket"""
        view_name = f"`{self.project_id}.{self.dataset_id}.jira_tickets_latest`"
        table_fqn = f"`{self.project_id}.{self.dataset_id}.{self.tickets_table}`"
        
        view_query = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest_tickets AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY issue_key 
                    ORDER BY 
                        COALESCE(last_updated, TIMESTAMP('1900-01-01')) DESC,
                        COALESCE(updated, TIMESTAMP('1900-01-01')) DESC
                ) as rn
            FROM {table_fqn}
        )
        SELECT * EXCEPT(rn)
        FROM latest_tickets 
        WHERE rn = 1
        """
        
        try:
            self.client.query(view_query).result()
            logging.info(f"✅ Created view: {view_name}")
            logging.info("💡 Use this view for queries to always get the latest version of each ticket")
            return True
        except Exception as e:
            logging.error(f"❌ Error creating latest tickets view: {e}")
            return False
    
    def process_batch(self, tickets: List[Dict], changelogs: List[Dict], enable_deduplication: bool, batch_number: Optional[int] = None, force_update: bool = False) -> bool:
        """Helper function to load a single batch of data into BigQuery."""
        if not self.insert_tickets(tickets, enable_deduplication=enable_deduplication, batch_number=batch_number, force_update=force_update):
            logging.error("❌ Failed to process ticket batch.")
            return False

        if changelogs:
            if enable_deduplication:
                if not self.upsert_changelog_entries(changelogs, batch_number=batch_number):
                    logging.error("❌ Failed to upsert changelog batch.")
                    return False
            else:
                if not self.insert_changelog_entries(changelogs, batch_number=batch_number):
                    logging.error("❌ Failed to insert changelog batch.")
                    return False
        return True

    def run_etl(self, mode: str = "full", start_date: Optional[str] = None, 
            end_date: Optional[str] = None, max_issues: Optional[int] = None,
            enable_deduplication: bool = True, include_changelog: bool = True,
            force_update: bool = False) -> bool:
        """Run complete ETL pipeline with memory-efficient streaming and batching."""
        # Set job context for error tracking
        start_time = datetime.now(timezone.utc)
        self._set_job_context(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            max_issues=max_issues,
            enable_deduplication=enable_deduplication,
            include_changelog=include_changelog,
            force_update=force_update,
            start_time=start_time.isoformat(),
            project_id=self.project_id,
            dataset_id=self.dataset_id
        )
        
        try:
            logging.info(f"🚀 Starting BigQuery ETL pipeline - Mode: {mode}")
            if not self.create_tables():
                self._handle_error(
                    error=Exception("Failed to create tables"),
                    operation="create_tables"
                )
                return False

            # fetch_jira_data() is now a generator. No data is in memory yet.
            issues_generator = self.fetch_jira_data(mode, start_date, end_date, max_issues, include_changelog=include_changelog)

            batch_size = 200
            batch_tickets = []
            batch_changelog_entries = []
            total_processed = 0
            distinct_issue_keys = set()
            batch_num = 1
            # Soft timebox to keep Cloud Run requests within limits (default ~50min)
            timebox_seconds = int(os.environ.get('ETL_TIMEBOX_SECONDS', '3000'))
            deadline = time.time() + timebox_seconds

            # Loop pulls one issue at a time from the generator
            for issue in issues_generator:
                try:
                    issue_key = issue.get('key')
                    if issue_key:
                        distinct_issue_keys.add(issue_key)
                    
                    # Transform and add to the current batch
                    ticket = self.build_ticket_row(issue)
                    changelog = self.build_changelog_rows(issue)
                    batch_tickets.append(ticket)
                    batch_changelog_entries.extend(changelog)
                    total_processed += 1

                    # When the batch is full, process it
                    if len(batch_tickets) >= batch_size:
                        logging.info(f"📦 Processing batch {batch_num} with {len(batch_tickets)} tickets...")
                        if not self.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication, batch_number=batch_num, force_update=force_update):
                            # Error already handled in process_batch
                            self._handle_error(
                                error=Exception(f"Batch {batch_num} processing failed"),
                                operation="process_batch",
                                batch_number=batch_num,
                                affected_records=len(batch_tickets)
                            )
                            return False  # Stop if a batch fails

                        # Reset for the next batch to free memory
                        batch_tickets = []
                        batch_changelog_entries = []
                        batch_num += 1
                        gc.collect()  # Force garbage collection to free memory
                        time.sleep(0.5)

                    # Timebox guard: if approaching deadline, stop fetching more
                    if time.time() > deadline - 60:  # leave ~1 minute for finalization
                        logging.info("⏱️ Approaching execution timebox, stopping early to avoid Cloud Run timeout.")
                        break

                except Exception as e:
                    issue_key = issue.get('key', f'unknown-issue-{total_processed}')
                    self._handle_error(
                        error=e,
                        operation=f"process_issue_{issue_key}",
                        batch_number=batch_num,
                        affected_records=1
                    )
                    continue
            
            # Process the final, partially-filled batch
            if batch_tickets:
                logging.info(f"📦 Processing final batch {batch_num} with {len(batch_tickets)} tickets...")
                if not self.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication, batch_number=batch_num, force_update=force_update):
                    self._handle_error(
                        error=Exception(f"Final batch {batch_num} processing failed"),
                        operation="process_batch_final",
                        batch_number=batch_num,
                        affected_records=len(batch_tickets)
                    )
                    return False

            # Calculate duration
            end_time = datetime.now(timezone.utc)
            duration_seconds = (end_time - start_time).total_seconds()
            
            logging.info("🎉 BigQuery ETL completed successfully!")
            logging.info(f"📊 Total ticket versions processed: {total_processed}")
            logging.info(f"📊 Distinct tickets processed in this run: {len(distinct_issue_keys)}")
            
            # Add validation after the ETL completes
            validation_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT issue_key) as distinct_tickets
            FROM `{self.project_id}.{self.dataset_id}.{self.tickets_table}`
            """
            query_job = None
            try:
                query_job = self.client.query(validation_query)
                result = query_job.result()
                for row in result:
                    logging.info(f"✅ Validation Results:")
                    logging.info(f"   • Total rows in BigQuery: {row.total_rows:,}")
                    logging.info(f"   • Distinct tickets: {row.distinct_tickets:,}")
                    
                    # Get current expected count for validation
                    current_info = self._get_latest_ticket_info()
                    expected_total = current_info['estimated_total']
                    highest_ticket = current_info['highest_ticket']
                    
                    logging.info(f"   • Current highest ticket: {highest_ticket}")
                    completion_pct = (row.distinct_tickets / expected_total) * 100 if expected_total > 0 else 0
                    logging.info(f"   • Estimated completion: {completion_pct:.1f}% ({row.distinct_tickets:,}/{expected_total:,})")
                    
                    if row.distinct_tickets < expected_total * 0.95:  # Allow 5% margin for estimation errors
                        logging.warning(f"⚠️ BigQuery count ({row.distinct_tickets:,}) may be incomplete. Expected ~{expected_total:,} tickets.")
            except Exception as e:
                self._handle_error(
                    error=e,
                    operation="validation_query",
                    bigquery_job_id=getattr(query_job, 'job_id', None) if query_job else None
                )
            
            # Create views
            try:
                self.create_customer_analysis_view()
                self.create_changelog_views()
                self.create_status_views()
                self.create_latest_essentials_view()
                self.create_latest_tickets_view()
            except Exception as e:
                self._handle_error(
                    error=e,
                    operation="create_views"
                )
                # Don't fail the job if views creation fails
            
            # Send success notification if enabled
            try:
                stats = {
                    'total_ticket_versions': total_processed,
                    'distinct_tickets': len(distinct_issue_keys),
                    'batches_processed': batch_num,
                    'duration_seconds': duration_seconds
                }
                self.slack_notifier.send_success_notification(
                    job_details=self._job_context.copy(),
                    stats=stats,
                    duration_seconds=duration_seconds
                )
            except Exception as notify_error:
                logging.warning(f"Failed to send success notification: {notify_error}")
            
            return True
        
        except Exception as e:
            # Capture final error with full context
            end_time = datetime.now(timezone.utc)
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Initialize total_processed if not defined
            total_processed = locals().get('total_processed', 0)
            
            self._handle_error(
                error=e,
                operation="run_etl_pipeline",
                affected_records=total_processed
            )
            return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='JIRA to BigQuery ETL Pipeline')
    parser.add_argument('--mode', type=str, choices=['full', 'incremental', 'backfill', 'enumerate'], default='incremental',
                       help='ETL mode: full (all issues), incremental (recent changes), backfill (missing issues), enumerate (key enumeration only)')
    parser.add_argument('--start-date', help='Start date for incremental/backfill mode (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for incremental/backfill mode (YYYY-MM-DD)')
    parser.add_argument('--max-issues', type=int, help='Maximum number of issues to process')
    parser.add_argument('--create-tables-only', action='store_true',
                       help='Only create tables, skip data loading')
    parser.add_argument('--enable-deduplication', action='store_true', default=True,
                       help='Enable deduplication and update logic (MERGE existing tickets)')
    parser.add_argument('--disable-deduplication', action='store_true',
                       help='Disable deduplication (insert all tickets as new)')
    parser.add_argument('--deduplicate-only', action='store_true',
                       help='Only run deduplication on existing data, skip ETL')
    parser.add_argument('--include_changelog', type=lambda x: (str(x).lower() in ['true', '1', 'yes']), default=True,
                       help='Whether to include changelog data (default: True)')
    
    args = parser.parse_args()
    
    # Initialize ETL
    etl = BigQueryJiraETL()
    
    if args.deduplicate_only:
        logging.info("🧹 Running deduplication only...")
        success = etl.deduplicate_existing_data()
        if success:
            etl.create_latest_tickets_view()
        return
    
    if args.create_tables_only:
        etl.create_tables()
        etl.create_customer_analysis_view()
        # Create analytics and helper views
        etl.create_changelog_views()
        etl.create_status_views()
        etl.create_latest_essentials_view()
        etl.create_latest_tickets_view()
        return
    
    if args.mode == 'enumerate':
        logging.info("🔢 Running in enumerate mode (key enumeration only)")
        
        # Ensure tables exist
        if not etl.create_tables():
            logging.error("❌ Failed to create/verify tables. Exiting.")
            sys.exit(1)
            
        latest_info = etl._get_latest_ticket_info()
        latest_ticket_num = latest_info['ticket_number']
        
        # Initialize fields_param with default fields + custom fields
        fields_param = [
            'summary', 'description', 'status', 'priority', 'issuetype',
            'created', 'updated', 'resolutiondate', 'resolution', 'assignee',
            'reporter', 'creator', 'project', 'labels', 'duedate', 'attachment',
            'comment', 'components', 'fixVersions', 'versions', 'watches', 'votes',
            'parent', 'lastViewed', 'security', 'timetracking'
        ] + [
            'customfield_10224', 'customfield_10485', 'customfield_10518', 'customfield_10024',
            'customfield_11904', 'customfield_10165', 'customfield_10166', 'customfield_10040',
            'customfield_10065', 'customfield_11015', 'customfield_10716', 'customfield_11409',
            'customfield_10461', 'customfield_11833', 'customfield_10617', 'customfield_10029',
            'customfield_10650', 'customfield_10249', 'customfield_10999', 'customfield_11906',
            'customfield_10059', 'customfield_11080',
                'customfield_10010', 'customfield_10251', 'customfield_13248', 'customfield_13247'
        ]
        
        logging.info(f"🔍 Starting key enumeration from TS-1 to TS-{latest_ticket_num}")
        logging.info(f"📋 Include changelog: {args.include_changelog}")
        
        # Process issues using key enumeration
        batch_size = 200
        issues_processed = 0
        batch_tickets = []
        batch_changelog_entries = []
        batch_num = 1
        
        # Soft timebox to keep Cloud Run requests within limits
        timebox_seconds = int(os.environ.get('ETL_TIMEBOX_SECONDS', '3000'))
        deadline = time.time() + timebox_seconds
        
        for issue in etl._fetch_all_issues_by_key_enumeration(
            latest_ticket_num,
            fields_param,
            include_changelog=args.include_changelog
        ):
            # Timebox check
            if time.time() > deadline - 60:
                logging.info("⏱️ Approaching execution timebox in enumerate mode, stopping early.")
                break
                
            try:
                # Transform issue into ticket and changelog records
                ticket = etl.build_ticket_row(issue)
                changelog = etl.build_changelog_rows(issue)
                batch_tickets.append(ticket)
                batch_changelog_entries.extend(changelog)
                issues_processed += 1
                
                # Check max_issues limit after each issue
                if args.max_issues and issues_processed >= args.max_issues:
                    logging.info(f"🎯 Reached max_issues limit: {args.max_issues}")
                    break
                
                # When the batch is full, process it
                if len(batch_tickets) >= batch_size:
                    logging.info(f"📦 Processing batch {batch_num} with {len(batch_tickets)} tickets...")
                    if not etl.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication=True):
                        logging.error("❌ Failed to process batch, stopping enumeration")
                        break
                    
                    # Reset for the next batch
                    batch_tickets = []
                    batch_changelog_entries = []
                    batch_num += 1
                        
            except Exception as e:
                issue_key = issue.get('key', f'unknown-issue-{issues_processed}')
                logging.error(f"❌ Error processing issue {issue_key}: {e}")
                continue
        
        # Process final batch
        if batch_tickets:
            logging.info(f"📦 Processing final batch {batch_num} with {len(batch_tickets)} tickets...")
            if not etl.process_batch(batch_tickets, batch_changelog_entries, enable_deduplication=True):
                logging.error("❌ Failed to process final batch")
            else:
                logging.info(f"✅ Final batch processed successfully")
        
        logging.info(f"✅ Enumerate mode completed: {issues_processed} tickets processed")
        
        # Create views
        etl.create_customer_analysis_view()
        etl.create_changelog_views()
        etl.create_status_views()
        etl.create_latest_essentials_view()
        etl.create_latest_tickets_view()
        
        return
    
    if args.mode == 'backfill':
        logging.info("🔄 Running in backfill mode (missing tickets identification and backfill)")
        success = etl.backfill_missing_tickets()
        
        if success:
            logging.info("✅ Backfill process completed successfully")
            # Create/update views
            etl.create_customer_analysis_view()
            etl.create_changelog_views()
            etl.create_status_views()
            etl.create_latest_essentials_view()
            etl.create_latest_tickets_view()
        else:
            logging.error("❌ Backfill process failed")
            sys.exit(1)
        
        return
    
    # Run ETL
    success = etl.run_etl(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        max_issues=args.max_issues,
        enable_deduplication=args.enable_deduplication and not args.disable_deduplication
    )
    
    if success:
        logging.info("✅ ETL process completed successfully")
        # Ensure views exist after data load as well
        etl.create_changelog_views()
        etl.create_status_views()
        etl.create_latest_essentials_view()
        etl.create_latest_tickets_view()
    else:
        logging.error("❌ ETL process failed")
        sys.exit(1)



def _get_param(request, name, default=None):
    """Fetch a parameter from JSON body or query string."""
    try:
        json_body = request.get_json(silent=True) or {}
    except Exception:
        json_body = {}
    args = getattr(request, "args", None) or {}
    return json_body.get(name) if name in json_body else args.get(name, default)


def jira_data_loader(request):
    """
    HTTP-triggered entry point for Cloud Functions / Cloud Run (Functions Framework).
    Accepts both JSON body and query parameters:
      - mode: 'full' | 'incremental' | 'backfill' (default: 'incremental')
      - start_date: YYYY-MM-DD
      - end_date: YYYY-MM-DD
      - max_issues: int
      - enable_deduplication: bool (default: true)
      - force_update: bool (default: false) - rewrite existing rows even when
        Jira's `updated` is unchanged (one-time repair of stale columns)
    """
    try:
        mode = str((_get_param(request, "mode", "incremental") or "incremental")).lower()
        
        # Auto-calculate start_date for incremental mode if not provided
        # Try to get last update timestamp from BigQuery, fallback to 7 days ago
        start_date = _get_param(request, "start_date")
        if mode == "incremental" and not start_date:
            from datetime import datetime, timedelta
            try:
                # Try to get the last update timestamp from BigQuery
                etl_temp = BigQueryJiraETL()
                table_fqn = f"`{etl_temp.project_id}.{etl_temp.dataset_id}.{etl_temp.tickets_table}`"
                query = f"""
                SELECT MAX(COALESCE(last_updated, updated)) as last_update
                FROM {table_fqn}
                """
                result = list(etl_temp.client.query(query).result())
                if result and result[0].last_update:
                    # Use last update minus 1 day as safety buffer
                    last_update = result[0].last_update
                    if isinstance(last_update, str):
                        from dateutil import parser as dt_parser
                        last_update = dt_parser.parse(last_update)
                    # Subtract 1 day as safety buffer to catch any missed updates
                    start_date_dt = last_update - timedelta(days=1)
                    start_date = start_date_dt.strftime("%Y-%m-%d")
                    logging.info(f"Auto-calculated start_date from BigQuery last update ({last_update}): {start_date}")
                else:
                    # Fallback: use 7 days ago to catch any gaps
                    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                    logging.info(f"Auto-calculated start_date for incremental mode (fallback, 7 days): {start_date}")
            except Exception as e:
                # Fallback: use 7 days ago if query fails
                start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
                logging.warning(f"Could not query BigQuery for last update, using fallback (7 days): {e}")
                logging.info(f"Auto-calculated start_date for incremental mode (fallback, 7 days): {start_date}")
        
        end_date = _get_param(request, "end_date")
        max_issues_raw = _get_param(request, "max_issues")
        enable_deduplication_raw = _get_param(request, "enable_deduplication", "true")
        include_changelog_raw = _get_param(request, "include_changelog", "true")
        force_update_raw = _get_param(request, "force_update", "false")

        max_issues = None
        if max_issues_raw not in (None, "", "null"):
            try:
                max_issues = int(max_issues_raw)
            except Exception:
                max_issues = None

        enable_deduplication = str(enable_deduplication_raw).lower() != "false"
        include_changelog = str(include_changelog_raw).lower() != "false"
        force_update = str(force_update_raw).lower() == "true"

        logging.info(
            "Starting ETL via HTTP: mode=%s, start=%s, end=%s, max_issues=%s, dedupe=%s, include_changelog=%s, force_update=%s",
            mode, start_date, end_date, max_issues, enable_deduplication, include_changelog, force_update,
        )

        etl = BigQueryJiraETL()
        ok = etl.run_etl(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            max_issues=max_issues,
            enable_deduplication=enable_deduplication,
            include_changelog=include_changelog,
            force_update=force_update,
        )

        status_code = 200 if ok else 500
        return (
            {
                "status": "success" if ok else "error",
                "message": f"ETL completed in {mode} mode" if ok else "ETL process failed",
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "max_issues": max_issues,
                "enable_deduplication": enable_deduplication,
                "include_changelog": include_changelog,
                "force_update": force_update,
            },
            status_code,
        )

    except Exception as exc:
        logging.exception("Unhandled error in jira_data_loader")
        return ({"status": "error", "message": str(exc)}, 500)


if __name__ == "__main__":
    main()
