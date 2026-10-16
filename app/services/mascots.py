"""ASCII mascots for channel posts.

A token keeps one mascot for life: it is picked from a hash of the mint, so
the first post and every result reply show the same character. Only the
mood changes with the result. Every frame is three lines of nine columns so
text can sit to the right of it in a ``<pre>`` block.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

from app.services.channel_results import COLLAPSED

WIDTH = 9
NEUTRAL, UP, DOWN, DEAD = "neutral", "up", "down", "dead"

# name -> mood -> three lines. Keep every line at most WIDTH characters.
MASCOTS: dict[str, dict[str, tuple[str, str, str]]] = {
    "cat": {
        NEUTRAL: (" /\\_/\\", "( o.o )", " > ^ <"),
        UP: (" /\\_/\\", "( ^.^ )", " > ^ <"),
        DOWN: (" /\\_/\\", "( ;.; )", " > ^ <"),
        DEAD: (" /\\_/\\", "( x.x )", " > ^ <"),
    },
    "frog": {
        NEUTRAL: ("  @..@", " (----)", "( >__< )"),
        UP: ("  @..@", " (\\__/)", "( >__< )"),
        DOWN: ("  @..@", " (/~~\\)", "( >__< )"),
        DEAD: ("  x..x", " (----)", "( >__< )"),
    },
    "bunny": {
        NEUTRAL: (" (\\_/)", " (o.o)", ' (")(")'),
        UP: (" (\\_/)", " (^.^)", ' (")(")'),
        DOWN: (" (\\_/)", " (;.;)", ' (")(")'),
        DEAD: (" (\\_/)", " (x.x)", ' (")(")'),
    },
    "robot": {
        NEUTRAL: (" [o_o]", " /|_|\\", "  d b"),
        UP: (" [^_^]", " \\|_|/", "  d b"),
        DOWN: (" [;_;]", " /|_|\\", "  d b"),
        DEAD: (" [x_x]", " /|_|\\", "  d b"),
    },
    "ghost": {
        NEUTRAL: ("  .-.", " (o o)", " |'''|"),
        UP: ("  .-.", " (^ ^)", " |'''|"),
        DOWN: ("  .-.", " (; ;)", " |'''|"),
        DEAD: ("  .-.", " (x x)", " |'''|"),
    },
    "bear": {
        NEUTRAL: (" c(..)o", "  (--)", " /(__)\\"),
        UP: (" c(^^)o", "  (\\/)", " \\(__)/"),
        DOWN: (" c(;;)o", "  (/\\)", " /(__)\\"),
        DEAD: (" c(xx)o", "  (--)", " /(__)\\"),
    },
}
NAMES = tuple(sorted(MASCOTS))


def mascot_for(mint: str) -> str:
    digest = hashlib.sha256(mint.encode("utf-8")).digest()
    return NAMES[digest[0] % len(NAMES)]


def mood_for(change: Decimal) -> str:
    if change <= COLLAPSED:
        return DEAD
    if change > 0:
        return UP
    return DOWN


def beside(mint: str, mood: str, lines: list[str]) -> str:
    """The token's mascot with ``lines`` to its right, padded to three rows."""
    frame = MASCOTS[mascot_for(mint)][mood]
    rows = max(len(frame), len(lines))
    out = []
    for index in range(rows):
        left = frame[index] if index < len(frame) else ""
        right = lines[index] if index < len(lines) else ""
        out.append((left.ljust(WIDTH + 1) + right).rstrip())
    return "\n".join(out)
