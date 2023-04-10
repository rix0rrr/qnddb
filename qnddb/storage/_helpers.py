def validate_only_sort_key(conds, sort_key):
    """Check that only the sort key is used in the given key conditions."""
    if sort_key and set(conds.keys()) - {sort_key}:
        raise RuntimeError(f"Conditions only allowed on sort key {sort_key}, got: {list(conds)}")

