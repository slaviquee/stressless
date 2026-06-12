"""Model pricing (USD per million tokens) and cache-aware cost estimation.

Prices as of 2026-06 (platform.claude.com/docs/en/pricing). Cache reads bill at
0.1x the input price; 5-minute-TTL cache writes at 1.25x. Estimates are marked
``cost_estimated`` in the store — the SDK-reported ``total_cost_usd`` wins when
present.
"""

from __future__ import annotations

# Ordered: first prefix match wins (more specific entries first).
PRICES_PER_MTOK: list[tuple[str, float, float]] = [
    ("claude-fable-5", 10.0, 50.0),
    ("claude-opus-4-8", 5.0, 25.0),
    ("claude-opus-4-7", 5.0, 25.0),
    ("claude-opus-4-6", 5.0, 25.0),
    ("claude-opus-4-5", 5.0, 25.0),
    ("claude-opus-4-1", 15.0, 75.0),
    ("claude-opus-4-2", 15.0, 75.0),
    ("claude-opus-4-0", 15.0, 75.0),
    ("claude-opus-4-20", 15.0, 75.0),  # claude-opus-4-20250514
    ("claude-opus", 5.0, 25.0),
    ("claude-sonnet", 3.0, 15.0),
    ("claude-haiku-4-5", 1.0, 5.0),
    ("claude-haiku", 1.0, 5.0),
]

CACHE_READ_MULT = 0.1
CACHE_WRITE_MULT = 1.25


def price_for(model: str | None) -> tuple[float, float] | None:
    if not model:
        return None
    for prefix, input_price, output_price in PRICES_PER_MTOK:
        if model.startswith(prefix):
            return input_price, output_price
    for prefix, input_price, output_price in PRICES_PER_MTOK:
        if prefix in model:
            return input_price, output_price
    return None


def estimate_cost_usd(
    model: str | None,
    *,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
) -> float | None:
    prices = price_for(model)
    if prices is None:
        return None
    input_price, output_price = prices
    return (
        (input_tokens or 0) * input_price
        + (output_tokens or 0) * output_price
        + (cache_read_tokens or 0) * input_price * CACHE_READ_MULT
        + (cache_write_tokens or 0) * input_price * CACHE_WRITE_MULT
    ) / 1_000_000
