from __future__ import annotations

PRICE_COUNTRIES = ["AW", "BB", "CO", "CR", "DO", "GT", "HN", "JM", "NI", "PA", "SV", "TT", "VI"]

KEY_COLUMN = "Parent_Item_Code"
DATASET_NAME = "Prices"

EXPECTED_BASELINE_COLUMN_COUNT = 79

EXPECTED_COLUMNS_BASELINE = [
    "Parent_Item_Code",
    "Cost_Center_TT",
    "POS_Sign_Price_TT",
    "Sell_Price_Effective_Date_TT",
    "Sell_Price_Expired_Date_TT",
    "Currency_Code_TT",
    "Cost_Center_GT",
    "POS_Sign_Price_GT",
    "Sell_Price_Effective_Date_GT",
    "Sell_Price_Expired_Date_GT",
    "Currency_Code_GT",
    "Cost_Center_BB",
    "POS_Sign_Price_BB",
    "Sell_Price_Effective_Date_BB",
    "Sell_Price_Expired_Date_BB",
    "Currency_Code_BB",
    "Cost_Center_SV",
    "POS_Sign_Price_SV",
    "Sell_Price_Effective_Date_SV",
    "Sell_Price_Expired_Date_SV",
    "Currency_Code_SV",
    "Cost_Center_HN",
    "POS_Sign_Price_HN",
    "Sell_Price_Effective_Date_HN",
    "Sell_Price_Expired_Date_HN",
    "Currency_Code_HN",
    "Cost_Center_CR",
    "POS_Sign_Price_CR",
    "Sell_Price_Effective_Date_CR",
    "Sell_Price_Expired_Date_CR",
    "Currency_Code_CR",
    "Cost_Center_AW",
    "POS_Sign_Price_AW",
    "Sell_Price_Effective_Date_AW",
    "Sell_Price_Expired_Date_AW",
    "Currency_Code_AW",
    "Cost_Center_VI",
    "POS_Sign_Price_VI",
    "Sell_Price_Effective_Date_VI",
    "Sell_Price_Expired_Date_VI",
    "Currency_Code_VI",
    "Cost_Center_JM",
    "POS_Sign_Price_JM",
    "Sell_Price_Effective_Date_JM",
    "Sell_Price_Expired_Date_JM",
    "Currency_Code_JM",
    "Cost_Center_NI",
    "POS_Sign_Price_NI",
    "Sell_Price_Effective_Date_NI",
    "Sell_Price_Expired_Date_NI",
    "Currency_Code_NI",
    "Cost_Center_PA",
    "POS_Sign_Price_PA",
    "Sell_Price_Effective_Date_PA",
    "Sell_Price_Expired_Date_PA",
    "Currency_Code_PA",
    "Cost_Center_CO",
    "POS_Sign_Price_CO",
    "Sell_Price_Effective_Date_CO",
    "Sell_Price_Expired_Date_CO",
    "Currency_Code_CO",
    "Cost_Center_DO",
    "POS_Sign_Price_DO",
    "Sell_Price_Effective_Date_DO",
    "Sell_Price_Expired_Date_DO",
    "Currency_Code_DO",
    "Country_Code_AW",
    "Country_Code_BB",
    "Country_Code_CO",
    "Country_Code_CR",
    "Country_Code_DO",
    "Country_Code_GT",
    "Country_Code_HN",
    "Country_Code_JM",
    "Country_Code_NI",
    "Country_Code_PA",
    "Country_Code_SV",
    "Country_Code_TT",
    "Country_Code_VI",
]

SNAPSHOT_CANDIDATE_COLUMNS = [
    "Business_Date",
    "business_date",
    "Run_Timestamp",
    "run_timestamp",
    "Timestamp_run",
    "timestamp_run",
    "Approved_Comparison_Window",
    "approved_comparison_window",
    "Snapshot_Date",
    "snapshot_date",
]

COUNTRY_FIELD_TEMPLATES = {
    "cost_center": "Cost_Center_{country}",
    "pos_sign_price": "POS_Sign_Price_{country}",
    "effective_date": "Sell_Price_Effective_Date_{country}",
    "expired_date": "Sell_Price_Expired_Date_{country}",
    "currency_code": "Currency_Code_{country}",
    "country_code": "Country_Code_{country}",
}

COUNTRY_COMPARE_FIELDS = [
    "Cost_Center_{country}",
    "POS_Sign_Price_{country}",
    "Sell_Price_Effective_Date_{country}",
    "Sell_Price_Expired_Date_{country}",
    "Currency_Code_{country}",
    "Country_Code_{country}",
]

STRUCTURED_FIELDS = {
    "Cost_Center": "cost_center",
    "POS_Sign_Price": "price",
    "Sell_Price_Effective_Date": "date",
    "Sell_Price_Expired_Date": "date",
}

STRUCTURED_FIELD_PREFIXES = [
    "Cost_Center_",
    "POS_Sign_Price_",
    "Sell_Price_Effective_Date_",
    "Sell_Price_Expired_Date_",
]

CODE_FIELD_PREFIXES = [
    "Currency_Code_",
    "Country_Code_",
]

MAX_EVIDENCE_ROWS = 200000

STRICT_SCHEMA_MODE = True
STRICT_BLANK_ROW_LEVEL_MODE = True
STRICT_SNAPSHOT_MODE = True
STRICT_STRUCTURE_BOTH_SIDES_MODE = False