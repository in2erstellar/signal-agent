"""ORM models for the phase-1 schema. Importing this package registers every table."""

from app.db.models.alerts import AlertDelivery
from app.db.models.bait_detection import BaitFlag
from app.db.models.funder_graph import FunderCluster, WalletFunder
from app.db.models.outcomes import LpUnlock, OutcomeCheck
from app.db.models.signals import Signal
from app.db.models.tokens import Token
from app.db.models.users import User
from app.db.models.wallets import TrackedWallet, WalletTx

__all__ = [
    "AlertDelivery",
    "BaitFlag",
    "FunderCluster",
    "LpUnlock",
    "OutcomeCheck",
    "Signal",
    "Token",
    "TrackedWallet",
    "User",
    "WalletFunder",
    "WalletTx",
]
