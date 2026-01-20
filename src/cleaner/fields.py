import re

def normalize_email(value):
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def normalize_phone(value):
    if not isinstance(value, str):
        return ""

    digits = re.sub(r"\D", "", value)
    return digits


def normalize_website(value):
    if not isinstance(value, str):
        return ""

    value = value.strip().lower()

    if value and not value.startswith(("http://", "https://")):
        value = "http://" + value

    return value.rstrip("/")
