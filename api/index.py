"""
Vercel serverless entry point.
Imports the FastAPI app from dashboard.app and exposes it
for the @vercel/python runtime.
"""

import os
import sys

# Ensure the project root is on the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.app import app  # noqa: E402, F401
