"""Rules shared by the live seed certificate extractors."""

import re


CUSTOMS_RELEASE_STAMP = re.compile(r"\bSeed\s+Shipment\s+Release\s+From\s+Customs\b", re.I)


def certificate_fields_missing(fields, expected):
    return any(fields.get(field) is None for field in expected)


def fill_missing_certificate_fields(fields, ocr_fields):
    for field, value in ocr_fields.items():
        if value is not None and fields.get(field) is None:
            fields[field] = value
