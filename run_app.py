#!/usr/bin/env python3
"""
Cloud Run WSGI entrypoint: exposes the Functions Framework app as `app` so the
Python buildpack default (gunicorn -b :8080 main:app) or Procfile
(gunicorn -b :8080 run_app:app) can start the server.

This file does not import main_bigquery or gcf_main at load time; create_app()
loads gcf_main when building the app, and gcf_main keeps imports minimal so
the container can bind to PORT quickly and pass the startup probe.
"""
import os
from functions_framework import create_app

target = os.environ.get("FUNCTION_TARGET", "jira_data_loader")
source = os.environ.get("FUNCTION_SOURCE", "gcf_main.py")  # must be a file path
app = create_app(target, source, "http")
