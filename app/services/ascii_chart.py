"""Text charts for Telegram ``<pre>`` blocks.

A phone shows about 32 monospace characters, so a chart is an 8-character
axis label plus at most 20 columns. Column 0 is the reading at the signal;
every other column holds the last reading in its time bucket, and a bucket
without a reading repeats the previous one, since nothing was observed there.
Columns are filled from the bottom. The scale is linear between the real
lowest and highest readings, which the axis labels print.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

LEVELS = "▁▂▃▄▅▆▇█"
LABEL_WIDTH = 8


def format_usd(value: Decimal) -> str:
    if value >= 1_000_000:
        return f"${_one_decimal(value / Decimal(1_000_000))}M"
    if value >= 100_000:
        return f"${(value / Decimal(1000)).quantize(Decimal('1'), rounding=ROUND_HALF_UP)}k"
    if value >= 1000:
        return f"${_one_decimal(value / Decimal(1000))}k"
    return f"${value.quantize(Decimal('1'), rounding=ROUND_HALF_UP)}"


def _one_decimal(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def columns(points: list[tuple[float, Decimal]], *, span: float, width: int) -> list[Decimal]:
    """One value per column across ``[0, span]``. ``points`` are (offset, value)."""
    ordered = sorted((offset, value) for offset, value in points if 0 <= offset <= span)
    if not ordered:
        return []
    values = [ordered[0][1]]
    current = ordered[0][1]
    index = 1
    for column in range(1, width):
        bucket_end = span * column / (width - 1)
        while index < len(ordered) and ordered[index][0] <= bucket_end:
            current = ordered[index][1]
            index += 1
        values.append(current)
    return values


def render(
    points: list[tuple[float, Decimal]],
    *,
    span: float,
    x_labels: tuple[str, str, str],
    width: int = 20,
    height: int = 5,
) -> str | None:
    """Chart lines, or ``None`` with fewer than two readings."""
    if sum(1 for offset, _ in points if 0 <= offset <= span) < 2:
        return None
    values = columns(points, span=span, width=width)
    top, bottom = max(values), min(values)
    # The lowest reading keeps a one-eighth bar so a collapse stays visible.
    steps = height * len(LEVELS)
    grid = [[" "] * width for _ in range(height)]
    for column, value in enumerate(values):
        fill = steps if top == bottom else 1 + int((value - bottom) / (top - bottom) * (steps - 1))
        full, part = divmod(fill, len(LEVELS))
        for filled in range(full):
            grid[height - 1 - filled][column] = LEVELS[-1]
        if part and full < height:
            grid[height - 1 - full][column] = LEVELS[part - 1]
    lines = []
    for row_index, row in enumerate(grid):
        if row_index == 0:
            label = format_usd(top).rjust(LABEL_WIDTH - 1) + "┤"
        elif row_index == height - 1:
            label = format_usd(bottom).rjust(LABEL_WIDTH - 1) + "┤"
        else:
            label = " " * (LABEL_WIDTH - 1) + "│"
        lines.append(label + "".join(row).rstrip())
    lines.append(" " * (LABEL_WIDTH - 1) + "└" + "─" * width)
    left, middle, right = x_labels
    axis = [" "] * width
    _place(axis, 0, left)
    _place(axis, (width - len(middle)) // 2, middle)
    _place(axis, width - len(right), right)
    lines.append(" " * LABEL_WIDTH + "".join(axis).rstrip())
    return "\n".join(lines)


def _place(axis: list[str], start: int, text: str) -> None:
    for offset, char in enumerate(text):
        if 0 <= start + offset < len(axis):
            axis[start + offset] = char
