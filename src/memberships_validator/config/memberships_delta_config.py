from __future__ import annotations

DATASET_NAME = "Memberships"
MODE_NAME = "Delta"

EXPECTED_COLUMNS_BASELINE = [
    "Membership_Number",
    "Account_Number_Full",
    "club_code",
    "country_code",
    "membershipTypeCode",
    "isPlatinumMember",
    "accountStatusCode",
    "dateAccountOpened",
    "effectiveDate",
    "expiredDate",
    "platinumUpgradeProratedPrice",
    "platinumamountsavedpromoamount",
    "cardId",
    "cardStatusCode",
    "idTypeCode",
    "member_id",
    "member_prefix",
    "firstName",
    "lastName",
    "cellPhone",
    "email",
    "birthDate",
    "addressLine1",
    "addressLine2",
    "city",
    "deptStateCode",
    "autoCharge",
    "shareInfo",
    "commercialCard",
    "primary",
    "cardCount",
    "Timestamp_Run",
    "Country_Code_Full",
    "Potential_Accruals",
    "Last_Year_Shops",
    "Accruing_Balance",
    "Household_Accruing_Balance_Acumulated",
    "Redeemable_Balance",
    "Tax_Type",
    "Tax_Number",
    "Tax_Phone",
    "Tax_Name",
    "Tax_Email",
    "Tax_Address",
]

EXPECTED_BASELINE_COLUMN_COUNT = 44

ALLOWED_DOMO_EXTRA_COLUMNS = {
    "row_id",
    "source_index",
    "source_file",
    "qa_score",
    "qa_status",
    "issue_count",
    "warning_count",
    "pass_count",
    "is_duplicate_row",
    "is_country_mismatch",
    "is_key_mismatch",
    "is_timestamp_outlier",
    "notes",
    "flag_email_missing",
    "flag_email_placeholder",
    "flag_member_id_missing",
    "flag_name_pending",
    "flag_address_blank",
    "flag_account_status_non00",
    "flag_primary_issue",
    "flag_cardcount_issue",
}

KEY_COLUMNS_PRIMARY = [
    "Membership_Number",
    "Account_Number_Full",
]

KEY_COLUMNS_SECONDARY = [
    "member_id",
]

CARD_KEY_COLUMNS = [
    "Membership_Number",
    "cardId",
]

COUNTRY_COLUMNS = [
    "country_code",
    "Country_Code_Full",
]

COUNTRY_ALPHA2_CODES = {
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
}

COUNTRY_ISO3_CODES = {
    "ABW",
    "BRB",
    "COL",
    "CRI",
    "DOM",
    "GTM",
    "HND",
    "JAM",
    "NIC",
    "PAN",
    "SLV",
    "TTO",
    "VIR",
}

COUNTRY_ALPHA2_TO_ISO3 = {
    "AW": "ABW",
    "BB": "BRB",
    "CO": "COL",
    "CR": "CRI",
    "DO": "DOM",
    "GT": "GTM",
    "HN": "HND",
    "JM": "JAM",
    "NI": "NIC",
    "PA": "PAN",
    "SV": "SLV",
    "TT": "TTO",
    "VI": "VIR",
}

COUNTRY_ISO3_TO_ALPHA2 = {v: k for k, v in COUNTRY_ALPHA2_TO_ISO3.items()}

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
    "dateAccountOpened",
    "effectiveDate",
    "expiredDate",
    "birthDate",
    "Timestamp_Run",
]

NUMERIC_COLUMNS = [
    "platinumUpgradeProratedPrice",
    "platinumamountsavedpromoamount",
    "Potential_Accruals",
    "Last_Year_Shops",
    "Accruing_Balance",
    "Household_Accruing_Balance_Acumulated",
    "Redeemable_Balance",
    "cardCount",
]

INTEGER_COLUMNS = [
    "cardCount",
]

BOOLEAN_LIKE_COLUMNS = [
    "isPlatinumMember",
    "autoCharge",
    "shareInfo",
    "commercialCard",
    "primary",
]

TIMESTAMP_COLUMN = "Timestamp_Run"

REQUIRED_NOT_BLANK_FIELDS = [
    "Membership_Number",
    "Account_Number_Full",
    "accountStatusCode",
    "effectiveDate",
    "expiredDate",
    "primary",
    "cardCount",
    "cardId",
    "isPlatinumMember",
    "cardStatusCode",
    "Country_Code_Full",
    "country_code",
    "club_code",
    "membershipTypeCode",
    "member_id",
    "Timestamp_Run",
]

DISALLOWED_SPECIAL_CHARACTERS = {"\\"}

ACCOUNT_STATUS_ALLOWED = {
    "00",
    "59",
    "62",
    "64",
}

CARD_STATUS_ALLOWED = {
    "00",
    "56",
    "59",
    "62",
    "64",
}

ACTIVE_ACCOUNT_STATUS_CODES = {"00"}
ACTIVE_CARD_STATUS_CODES = {"00", "0"}

PRIMARY_ALLOWED_VALUES = {"Y", "N"}
PLATINUM_ALLOWED_VALUES = {"0", "1"}

CLUB_TO_COUNTRY_ALPHA2 = {
    "6301": "GT",
    "6303": "GT",
    "6304": "GT",
    "6305": "GT",
    "6306": "GT",
    "6307": "GT",
    "6308": "GT",
    "8001": "TT",
    "8002": "TT",
    "8003": "TT",
    "8004": "TT",
    "8101": "VI",
    "8501": "BB",
    "8701": "JM",
    "8702": "JM",
}

DOWNGRADE_MEMBERSHIP_TYPE_CODES = {"DI"}
UPGRADE_MEMBERSHIP_TYPE_CODES = {"DI"}

UPGRADE_PRICE_COLUMNS = [
    "platinumUpgradeProratedPrice",
    "platinumamountsavedpromoamount",
]

TRANSITION_VALUE_COLUMNS = [
    "membershipTypeCode",
    "isPlatinumMember",
    "accountStatusCode",
    "effectiveDate",
    "expiredDate",
    "Accruing_Balance",
    "Redeemable_Balance",
    "Tax_Type",
    "Tax_Number",
]

MAX_EVIDENCE_ROWS = 1000
FAIL_ON_EMPTY_FILES = True
INVALID_TEXT_VALUES = {"null", "none", "nan", "nat"}

DELTA_WINDOW_MINUTES = 5

BOTH_FILES_ARE_CONSOLIDATED = True
CONSOLIDATED_COMPARISON_WINDOW_MINUTES = 60
CONSOLIDATED_SNAPSHOT_TOLERANCE_MINUTES = 60

DELTA_ALLOW_ROW_COUNT_DIFFERENCE = False
DELTA_ALLOW_KEY_SET_DIFFERENCE = False
DELTA_ALLOW_COUNTRY_COUNT_DIFFERENCE = False
DELTA_ALLOW_TIMESTAMP_RANGE_DIFFERENCE = False
DELTA_SKIP_FULL_MULTISET_RECONCILIATION = True

MIN_COMMON_PRIMARY_KEYS_FOR_STRICT_COMPARISON = 1
ALLOW_WARNING_WHEN_NOT_COMPARABLE = False

DOMO_IS_CONSOLIDATED = True
DOMO_CONSOLIDATED_WINDOW_MINUTES = 60