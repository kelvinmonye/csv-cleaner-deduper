def normalize_columns(columns, aliases):
    normalized = []

    for col in columns:
        clean = col.strip().lower()

        found = False
        for standard, variations in aliases.items():
            if clean == standard or clean in variations:
                normalized.append(standard)
                found = True
                break

        if not found:
            normalized.append(clean.replace(" ", "_"))

    return normalized
