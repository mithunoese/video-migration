"""
Launch the Video Migration Dashboard.

Usage:
    python run_dashboard.py              # Start on port 8000
    python run_dashboard.py --port 3000  # Start on custom port

Opens in browser automatically.
"""

import argparse
import logging
import threading
import time
import webbrowser

import uvicorn

from dotenv import load_dotenv

load_dotenv()


def open_browser(port: int):
    """Open browser after a short delay to let the server start."""
    time.sleep(1.5)
    webbrowser.open(f"http://localhost:{port}")


def main():
    parser = argparse.ArgumentParser(description="Video Migration Dashboard")
    parser.add_argument("--port", type=int, default=8000, help="Port to run on (default: 8000)")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser automatically")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print(f"\n  Video Migration Dashboard")
    print(f"  http://localhost:{args.port}")
    print(f"  Press Ctrl+C to stop\n")

    if not args.no_browser:
        threading.Thread(target=open_browser, args=(args.port,), daemon=True).start()

    uvicorn.run(
        "dashboard.app:app",
        host="0.0.0.0",
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()
