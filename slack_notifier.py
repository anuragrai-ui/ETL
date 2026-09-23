#!/usr/bin/env python3
"""
Slack Notification Module for ETL Pipeline

Sends notifications to Slack via incoming webhook for ETL job status,
including errors, warnings, and success messages.

Configuration:
    SLACK_WEBHOOK_URL: Environment variable containing the Slack webhook URL
    SLACK_NOTIFICATION_LEVEL: Controls notification verbosity (all, error, warning, none)
"""

import json
import logging
import os
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', '')
SLACK_NOTIFICATION_LEVEL = os.environ.get('SLACK_NOTIFICATION_LEVEL', 'error').lower()
SLACK_TIMEOUT_SECONDS = int(os.environ.get('SLACK_TIMEOUT_SECONDS', '10'))


@dataclass
class ErrorContext:
    """Data class to capture error context for notifications."""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    error_type: str = ''
    error_message: str = ''
    traceback: str = ''
    job_details: Dict[str, Any] = field(default_factory=dict)
    bigquery_job_id: Optional[str] = None
    affected_records: Optional[int] = None
    batch_number: Optional[int] = None
    operation: str = ''
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'timestamp': self.timestamp,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'traceback': self.traceback,
            'job_details': self.job_details,
            'bigquery_job_id': self.bigquery_job_id,
            'affected_records': self.affected_records,
            'batch_number': self.batch_number,
            'operation': self.operation,
        }


class SlackNotifier:
    """Handles sending notifications to Slack via incoming webhook."""
    
    def __init__(self, webhook_url: Optional[str] = None, notification_level: Optional[str] = None):
        """
        Initialize the Slack notifier.
        
        Args:
            webhook_url: Slack webhook URL. Falls back to SLACK_WEBHOOK_URL env var.
            notification_level: Notification level (all, error, warning, none).
                               Falls back to SLACK_NOTIFICATION_LEVEL env var.
        """
        self.webhook_url = webhook_url or SLACK_WEBHOOK_URL
        self.notification_level = notification_level or SLACK_NOTIFICATION_LEVEL
        self._enabled = bool(self.webhook_url)
        
        if not self._enabled:
            logger.warning("Slack webhook URL not configured. Notifications will be logged only.")
    
    def _should_notify(self, level: str) -> bool:
        """Check if notification should be sent based on configured level."""
        if not self._enabled:
            return False
            
        level_priority = {'none': 0, 'error': 1, 'warning': 2, 'all': 3}
        current_priority = level_priority.get(self.notification_level, 1)
        message_priority = level_priority.get(level, 1)
        
        return message_priority <= current_priority
    
    def _format_error_message(self, context: ErrorContext) -> Dict[str, Any]:
        """Format error context into a Slack message payload."""
        # Build job details text
        job_details_text = ""
        if context.job_details:
            job_details_items = []
            for key, value in context.job_details.items():
                if value is not None:
                    job_details_items.append(f"• {key.replace('_', ' ').title()}: {value}")
            job_details_text = "\n".join(job_details_items)
        
        # Build additional info
        additional_info = []
        if context.bigquery_job_id:
            additional_info.append(f"🔧 BigQuery Job ID: `{context.bigquery_job_id}`")
        if context.affected_records is not None:
            additional_info.append(f"📊 Records Affected: {context.affected_records}")
        if context.batch_number is not None:
            additional_info.append(f"📦 Batch Number: {context.batch_number}")
        if context.operation:
            additional_info.append(f"⚙️ Operation: {context.operation}")
        
        additional_text = "\n".join(additional_info) if additional_info else ""
        
        # Format traceback (truncate if too long)
        traceback_text = context.traceback
        if len(traceback_text) > 3000:
            traceback_text = traceback_text[:3000] + "\n... (truncated)"
        
        # Build Slack attachment
        attachment = {
            "color": "danger",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 ETL Pipeline Error",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Error Type:*\n`{context.error_type}`"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Timestamp:*\n{context.timestamp}"
                        }
                    ]
                }
            ]
        }
        
        # Add job details if present
        if job_details_text:
            attachment["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"📋 *Job Details:*\n{job_details_text}"
                }
            })
        
        # Add error message
        attachment["blocks"].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"❌ *Error Message:*\n```\n{context.error_message}\n```"
            }
        })
        
        # Add traceback
        if traceback_text:
            attachment["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"📝 *Traceback:*\n```\n{traceback_text}\n```"
                }
            })
        
        # Add additional info
        if additional_text:
            attachment["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": additional_text
                }
            })
        
        return {
            "attachments": [attachment]
        }
    
    def _format_success_message(
        self,
        job_details: Dict[str, Any],
        stats: Dict[str, Any],
        duration_seconds: Optional[float] = None
    ) -> Dict[str, Any]:
        """Format success notification into a Slack message payload."""
        # Build job details text
        job_details_items = []
        for key, value in job_details.items():
            if value is not None:
                job_details_items.append(f"• {key.replace('_', ' ').title()}: {value}")
        job_details_text = "\n".join(job_details_items)
        
        # Build stats text
        stats_items = []
        for key, value in stats.items():
            if value is not None:
                stats_items.append(f"• {key.replace('_', ' ').title()}: {value:,}" if isinstance(value, int) else f"• {key.replace('_', ' ').title()}: {value}")
        stats_text = "\n".join(stats_items)
        
        # Build duration text
        duration_text = ""
        if duration_seconds is not None:
            minutes = int(duration_seconds // 60)
            seconds = int(duration_seconds % 60)
            duration_text = f"• Duration: {minutes}m {seconds}s"
        
        attachment = {
            "color": "good",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ ETL Pipeline Completed Successfully",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📅 *Timestamp:* {datetime.now(timezone.utc).isoformat()}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📋 *Job Details:*\n{job_details_text}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📊 *Results:*\n{stats_text}"
                    }
                }
            ]
        }
        
        if duration_text:
            attachment["blocks"].append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"⏱️ *Timing:*\n{duration_text}"
                }
            })
        
        return {
            "attachments": [attachment]
        }
    
    def _format_warning_message(
        self,
        warning_message: str,
        job_details: Dict[str, Any],
        details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Format warning notification into a Slack message payload."""
        # Build job details text
        job_details_items = []
        for key, value in job_details.items():
            if value is not None:
                job_details_items.append(f"• {key.replace('_', ' ').title()}: {value}")
        job_details_text = "\n".join(job_details_items)
        
        attachment = {
            "color": "warning",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "⚠️ ETL Pipeline Warning",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📅 *Timestamp:* {datetime.now(timezone.utc).isoformat()}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"⚠️ *Warning:*\n{warning_message}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📋 *Job Details:*\n{job_details_text}"
                    }
                }
            ]
        }
        
        if details:
            details_items = []
            for key, value in details.items():
                if value is not None:
                    details_items.append(f"• {key.replace('_', ' ').title()}: {value}")
            if details_items:
                attachment["blocks"].append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"📝 *Additional Details:*\n" + "\n".join(details_items)
                    }
                })
        
        return {
            "attachments": [attachment]
        }
    
    def _send_webhook(self, payload: Dict[str, Any]) -> bool:
        """
        Send the webhook payload to Slack.
        
        Args:
            payload: The Slack message payload
            
        Returns:
            True if successful, False otherwise
        """
        if not self.webhook_url:
            logger.info(f"Slack notification (not sent - no webhook): {json.dumps(payload, indent=2)}")
            return True
        
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=SLACK_TIMEOUT_SECONDS,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.debug("Slack notification sent successfully")
                return True
            else:
                logger.error(f"Slack webhook returned status {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error("Slack webhook request timed out")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def send_error_notification(self, context: ErrorContext) -> bool:
        """
        Send an error notification to Slack.
        
        Args:
            context: The error context containing all error details
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if not self._should_notify('error'):
            logger.info(f"Error notification suppressed (level: {self.notification_level})")
            return True
        
        payload = self._format_error_message(context)
        logger.error(f"ETL Error: {context.error_type} - {context.error_message}")
        return self._send_webhook(payload)
    
    def send_success_notification(
        self,
        job_details: Dict[str, Any],
        stats: Dict[str, Any],
        duration_seconds: Optional[float] = None
    ) -> bool:
        """
        Send a success notification to Slack.
        
        Args:
            job_details: Details about the ETL job (mode, dates, etc.)
            stats: Statistics about the job results (records processed, etc.)
            duration_seconds: Optional duration of the job in seconds
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if not self._should_notify('all'):
            logger.debug(f"Success notification suppressed (level: {self.notification_level})")
            return True
        
        payload = self._format_success_message(job_details, stats, duration_seconds)
        return self._send_webhook(payload)
    
    def send_run_report(
        self,
        status: str,
        problems_text: str,
        stats_text: str,
        job_details: Dict[str, Any],
        duration_seconds: Optional[float] = None,
        critical: bool = True
    ) -> bool:
        """
        Send one consolidated report for an ETL run (or heartbeat) that had problems.

        Args:
            status: 'failed', 'degraded' or 'heartbeat'
            problems_text: Pre-formatted mrkdwn bullet list of problems
            stats_text: One-line run statistics
            job_details: Details about the ETL job (mode, dates, run id, revision)
            duration_seconds: Optional run duration
            critical: True if any problem is critical (sent at 'error' level,
                otherwise at 'warning' level)
        """
        if not self._should_notify('error' if critical else 'warning'):
            logger.info(f"Run report suppressed (level: {self.notification_level})")
            return False

        titles = {
            'failed': '🔴 Jira ETL run FAILED',
            'degraded': '🟠 Jira ETL run completed with problems',
            'heartbeat': '🔴 Jira ETL heartbeat alert',
        }
        details = [f"• {k.replace('_', ' ').title()}: {v}" for k, v in job_details.items()
                   if v not in (None, '')]
        if duration_seconds is not None:
            details.append(f"• Duration: {duration_seconds:.0f}s")
        project = os.environ.get('GCP_PROJECT_ID', 'certifyos-production-platform')
        service = os.environ.get('K_SERVICE', 'jiraetlpipeline')
        logs_url = f"https://console.cloud.google.com/run/detail/us-central1/{service}/logs?project={project}"

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": titles.get(status, titles['failed']), "emoji": True}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Problems:*\n{problems_text}"[:2900]}},
        ]
        if status != 'heartbeat':
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Run stats:* {stats_text}"}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": "*Job:*\n" + "\n".join(details)}})
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC · <{logs_url}|Cloud Run logs> · "
            f"run log: `Reporting.jira_etl_runs`"}]})

        payload = {"attachments": [{"color": "danger" if critical else "warning", "blocks": blocks}]}
        return self._send_webhook(payload)

    def send_warning_notification(
        self,
        warning_message: str,
        job_details: Dict[str, Any],
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send a warning notification to Slack.
        
        Args:
            warning_message: The warning message
            job_details: Details about the ETL job
            details: Optional additional details
            
        Returns:
            True if notification was sent successfully, False otherwise
        """
        if not self._should_notify('warning'):
            logger.debug(f"Warning notification suppressed (level: {self.notification_level})")
            return True
        
        payload = self._format_warning_message(warning_message, job_details, details)
        logger.warning(f"ETL Warning: {warning_message}")
        return self._send_webhook(payload)


# Convenience functions for direct use
def send_error_notification(context: ErrorContext) -> bool:
    """
    Send an error notification using the default notifier.
    
    Args:
        context: The error context
        
    Returns:
        True if successful, False otherwise
    """
    notifier = SlackNotifier()
    return notifier.send_error_notification(context)


def send_success_notification(
    job_details: Dict[str, Any],
    stats: Dict[str, Any],
    duration_seconds: Optional[float] = None
) -> bool:
    """
    Send a success notification using the default notifier.
    
    Args:
        job_details: Details about the ETL job
        stats: Statistics about the job results
        duration_seconds: Optional duration in seconds
        
    Returns:
        True if successful, False otherwise
    """
    notifier = SlackNotifier()
    return notifier.send_success_notification(job_details, stats, duration_seconds)


def send_warning_notification(
    warning_message: str,
    job_details: Dict[str, Any],
    details: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Send a warning notification using the default notifier.
    
    Args:
        warning_message: The warning message
        job_details: Details about the ETL job
        details: Optional additional details
        
    Returns:
        True if successful, False otherwise
    """
    notifier = SlackNotifier()
    return notifier.send_warning_notification(warning_message, job_details, details)


def create_error_context(
    error: Exception,
    job_details: Dict[str, Any],
    operation: str = '',
    bigquery_job_id: Optional[str] = None,
    affected_records: Optional[int] = None,
    batch_number: Optional[int] = None
) -> ErrorContext:
    """
    Create an ErrorContext from an exception and additional details.
    
    Args:
        error: The exception that occurred
        job_details: Details about the ETL job
        operation: The operation that failed
        bigquery_job_id: Optional BigQuery job ID
        affected_records: Optional count of affected records
        batch_number: Optional batch number
        
    Returns:
        An ErrorContext instance
    """
    return ErrorContext(
        error_type=type(error).__name__,
        error_message=str(error),
        traceback=''.join(traceback.format_exception(type(error), error, error.__traceback__)),
        job_details=job_details,
        operation=operation,
        bigquery_job_id=bigquery_job_id,
        affected_records=affected_records,
        batch_number=batch_number
    )


if __name__ == "__main__":
    # Test the notifier
    print("Testing Slack Notifier...")
    
    # Test error context creation
    try:
        raise ValueError("Test error for demonstration")
    except Exception as e:
        context = create_error_context(
            error=e,
            job_details={'mode': 'incremental', 'start_date': '2026-02-01'},
            operation='test_operation'
        )
        print(f"Error context created: {context.to_dict()}")
    
    # Test notifier (will only log if no webhook configured)
    notifier = SlackNotifier()
    print(f"Notifier enabled: {notifier._enabled}")
    print(f"Notification level: {notifier.notification_level}")
