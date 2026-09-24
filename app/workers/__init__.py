"""Background jobs.

``python -m app.workers.runner`` runs the risk-scan, bait-check, and
signal-fusion loops. The Helius webhook handler lives in ``wallet_watcher``
and is mounted by the API process.
"""
