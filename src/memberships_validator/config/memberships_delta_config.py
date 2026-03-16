from __future__ import annotations

DATASET_NAME = "Memberships"
MODE_NAME = "Delta"

EXPECTED_COLUMNS_BASELINE = [
    "membership_country_code",
    "country_code",
    "Membership_Number",
    "member_id",
    "name",
    "membershipTypeCode",
    "status",
    "created_date",
    "activation_date",
    "cancellation_date",
    "expiry_date",
    "beneficiary_1_name",
    "beneficiary_2_name",
    "beneficiary_3_name",
    "beneficiary_4_name",
    "beneficiary_5_name",
    "beneficiary_6_name",
    "beneficiary_7_name",
    "beneficiary_8_name",
    "beneficiary_9_name",
    "beneficiary_10_name",
    "beneficiary_11_name",
    "beneficiary_12_name",
    "beneficiary_13_name",
    "beneficiary_14_name",
    "beneficiary_15_name",
    "beneficiary_16_name",
    "loyaltyType",
    "cardId",
    "email",
    "phoneNumber",
    "addressLine1",
    "addressLine2",
    "city",
    "state",
    "postalCode",
    "Country_Code_Full",
    "club_code",
    "isPlatinumMember",
    "platinumEffectiveDate",
    "platinumExpirationDate",
    "isActive",
    "commercialCard",
    "Account_Number_Full",
    "Timestamp_Run",
]

EXPECTED_BASELINE_COLUMN_COUNT = 45

KEY_COLUMNS_PRIMARY = [
    "Membership_Number",
    "Account_Number_Full",
]

KEY_COLUMNS_SECONDARY = [
    "member_id",
    "cardId",
]

COUNTRY_COLUMNS = [
    "membership_country_code",
    "country_code",
    "Country_Code_Full",
]

COUNTRY_CANONICAL_CODES = [
    "AW",
    "BB",
    "CO",
    "CR",
    "DO",
    "GT",
    "HN",
    "JM",
    "NI",
    "PA",
    "SV",
    "TT",
    "VI",
]

COUNTRY_ALIASES = {
    "AW": "AW",
    "533": "AW",
    "ABW": "AW",
    "ARUBA": "AW",
    "BB": "BB",
    "052": "BB",
    "52": "BB",
    "BRB": "BB",
    "BARBADOS": "BB",
    "CO": "CO",
    "170": "CO",
    "COL": "CO",
    "COLOMBIA": "CO",
    "CR": "CR",
    "188": "CR",
    "CRI": "CR",
    "COSTARICA": "CR",
    "COSTA RICA": "CR",
    "DO": "DO",
    "214": "DO",
    "DOM": "DO",
    "DOMINICANREPUBLIC": "DO",
    "DOMINICAN REPUBLIC": "DO",
    "GT": "GT",
    "320": "GT",
    "GTM": "GT",
    "GUATEMALA": "GT",
    "HN": "HN",
    "340": "HN",
    "HND": "HN",
    "HONDURAS": "HN",
    "JM": "JM",
    "388": "JM",
    "JAM": "JM",
    "JAMAICA": "JM",
    "NI": "NI",
    "558": "NI",
    "NIC": "NI",
    "NICARAGUA": "NI",
    "PA": "PA",
    "591": "PA",
    "PAN": "PA",
    "PANAMA": "PA",
    "SV": "SV",
    "222": "SV",
    "SLV": "SV",
    "ELSALVADOR": "SV",
    "EL SALVADOR": "SV",
    "TT": "TT",
    "780": "TT",
    "TTO": "TT",
    "TRINIDADANDTOBAGO": "TT",
    "TRINIDAD AND TOBAGO": "TT",
    "VI": "VI",
    "850": "VI",
    "VIR": "VI",
    "USVI": "VI",
    "U.S. VIRGIN ISLANDS": "VI",
    "US VIRGIN ISLANDS": "VI",
    "VIRGINISLANDS": "VI",
}

DATE_COLUMNS = [
    "created_date",
    "activation_date",
    "cancellation_date",
    "expiry_date",
    "platinumEffectiveDate",
    "platinumExpirationDate",
    "Timestamp_Run",
]

BOOLEAN_LIKE_COLUMNS = [
    "isActive",
    "isPlatinumMember",
    "commercialCard",
]

TIMESTAMP_COLUMN = "Timestamp_Run"

MAX_EVIDENCE_ROWS = 1000
FAIL_ON_EMPTY_FILES = True
STRICT_SCHEMA_MODE = True
STRICT_COUNTRY_SCOPE_MODE = True

INVALID_TEXT_VALUES = {"null", "none", "nan", "nat"}