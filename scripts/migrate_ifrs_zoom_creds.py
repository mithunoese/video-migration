"""
One-time migration: copy IFRS Zoom credentials from env vars → per-project credentials table.
Run AFTER reviewing diagnostic output. Safe to re-run (upsert).

Usage:
    POSTGRES_URL=<your-url> python3 scripts/migrate_ifrs_zoom_creds.py

Or with a .env that has POSTGRES_URL:
    python3 scripts/migrate_ifrs_zoom_creds.py
"""
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Try loading .env if python-dotenv available
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass

import dashboard.db as db

PROJECT_SLUG = 'ifrs-migration'

ZOOM_KEYS = {
    'client_id':      (os.environ.get('ZOOM_CLIENT_ID', ''),       False),
    'client_secret':  (os.environ.get('ZOOM_CLIENT_SECRET', ''),   True),
    'account_id':     (os.environ.get('ZOOM_ACCOUNT_ID', ''),      False),
    'target_api':     (os.environ.get('ZOOM_TARGET_API', 'events'), False),
    'hub_id':         (os.environ.get('ZOOM_HUB_ID', ''),          False),
    'vod_channel_id': (os.environ.get('ZOOM_VOD_CHANNEL_ID', ''),  False),
}


def main():
    print("=== IFRS Zoom Credentials Migration ===\n")

    # Print env var status
    print("Env vars detected:")
    for key_name, (value, is_secret) in ZOOM_KEYS.items():
        if not value:
            print(f"  {key_name}: (not set — will skip)")
        else:
            display = '••••••••' if is_secret else (value[:4] + '••••' + value[-4:] if len(value) > 8 else '(set)')
            print(f"  {key_name}: {display}")

    print()

    # Init DB
    if not db.init():
        print("ERROR: Could not connect to database.")
        print("Set POSTGRES_URL environment variable and retry.")
        sys.exit(1)

    from dashboard.db import fetch_one, store_credential, get_all_credentials_masked
    import json

    # Show current DB state
    proj = fetch_one("SELECT id, name, slug FROM projects WHERE slug = %s", (PROJECT_SLUG,))
    if not proj:
        print(f"ERROR: project '{PROJECT_SLUG}' not found in DB")
        from dashboard.db import fetch_all
        all_proj = fetch_all("SELECT name, slug FROM projects ORDER BY name")
        print("Available projects:", [p['slug'] for p in all_proj])
        sys.exit(1)

    project_id = str(proj['id'])
    print(f"Project: {proj['name']} ({project_id})\n")

    print("Current DB zoom credentials (masked):")
    current = get_all_credentials_masked(project_id)
    print(json.dumps(current.get('zoom', {}), indent=2))
    print()

    # Confirm before writing
    answer = input("Proceed with migration? [y/N] ").strip().lower()
    if answer != 'y':
        print("Aborted.")
        sys.exit(0)

    print()
    for key_name, (value, is_secret) in ZOOM_KEYS.items():
        if not value:
            print(f"  SKIP {key_name}: (empty in env)")
            continue
        store_credential(project_id, 'zoom', key_name, value, is_secret)
        display = '••••••••' if is_secret else value
        print(f"  WROTE zoom.{key_name} = {display}")

    print("\n=== Verification (masked) ===")
    creds = get_all_credentials_masked(project_id)
    print(json.dumps(creds.get('zoom', {}), indent=2))
    print("\nDone. Zoom credentials are now in the DB for IFRS.")


if __name__ == '__main__':
    main()
