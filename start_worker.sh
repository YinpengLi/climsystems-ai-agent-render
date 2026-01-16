#!/usr/bin/env bash
set -e
cd apps/worker
exec python worker.py
