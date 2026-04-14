"""
Common lightweight utility helpers.
"""

def to_bool(value) -> bool:
    """Converts common bool-like config values into a real boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y")
    return bool(value)
