#!/bin/bash
cd /Users/aviralgoenka/Documents/Agents/vc-sourcing-agent
exec /Users/aviralgoenka/Library/Python/3.9/bin/uvicorn app:app --host 0.0.0.0 --port 8000
