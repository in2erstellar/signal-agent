"""Background jobs.

The bait recheck worker is not here yet. It should read matured
``PendingBaitCheck`` records from ``BAIT_PENDING_STREAM`` and call
``BaitDetector.complete_pending_check``. Waiting until ``scheduled_at``
(consumer group, Celery beat, or APScheduler) is intentionally undecided.
"""
