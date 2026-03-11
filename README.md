# Prices Validator Base

This project validates the **Prices** dataset from **Domo vs Informatica** using a strict reconciliation model.

## What this version validates

- Business snapshot / approved comparison window (manual input or shared metadata columns)
- Full 79-column Prices baseline schema
- Exact header text and no unexpected columns
- Exact column order
- Exact record count
- `Parent_Item_Code` population, set equality, and duplicate behavior
- `Parent_Item_Code` string preservation
- Blank / null-like / whitespace behavior
- Country-level raw equality for:
  - `Cost_Center_XX`
  - `POS_Sign_Price_XX`
  - `Sell_Price_Effective_Date_XX`
  - `Sell_Price_Expired_Date_XX`
  - `Currency_Code_XX`
  - `Country_Code_XX`
- Structured token validation for `COUNTRY~STORE:PRICE` and `COUNTRY~STORE:YYYY-MM-DD`
- Cross-field store alignment by country
- Overall reconciliation decision

## Run from CLI

```bash
python main.py --domo .\domo_prices.csv --informatica .\informatica_prices.csv --out .\reports\run_001
```

Optional snapshot values:

```bash
python main.py --domo .\domo_prices.csv --informatica .\informatica_prices.csv --domo-snapshot "2026-03-09 14:00" --informatica-snapshot "2026-03-09 14:00" --out .\reports\run_001
```

## Run UI

```bash
python -m streamlit run app.py
```

## Output

The validator produces:

- `summary.md`
- `check_results.csv`
- `validation_report.xlsx`
- evidence CSV files for mismatches and structural issues
