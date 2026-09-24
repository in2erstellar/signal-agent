"""Background jobs.

``python -m app.workers.runner`` runs the risk-scan, bait-check,
signal-fusion, and alert-dispatch loops. When a Telegram token is
configured it also polls for bot commands. The Helius webhook handler
lives in ``wallet_watcher`` and is mounted by the API process.
"""
