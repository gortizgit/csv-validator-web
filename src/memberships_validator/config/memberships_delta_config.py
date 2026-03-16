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