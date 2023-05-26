For more information about the API (params, columns), see
the [Revenue Reporting API](https://dash.applovin.com/documentation/mediation/reporting-api/max-ad-revenue).

# Revenue Reporting API

## Get report

```python
from applovin_report import RevenueReport

report = RevenueReport(api_key="your_api_key")

_columns = [
    "day",
    "package_name",
    "platform",
    "country",
    "application",
    "max_ad_unit_test",
    "max_ad_unit_id",
    "network",
    "network_placement",
    'ad_format',
    "attempts",
    "responses",
    "fill_rate",
    "impressions",
    "estimated_revenue",
    "ecpm",
]

result = report.get_report(
    start_date="2023-05-23",
    end_date="2023-05-23",
    columns=_columns,
    filter_package_name="com.jura.car.crashes.simulator", )

print(result)
```

## Get report in batch

```python
from applovin_report import RevenueReport
import pandas as pd

report = RevenueReport(api_key="your_api_key")

_columns = [
    "day",
    "package_name",
    "platform",
    "country",
    "application",
    "max_ad_unit_test",
    "max_ad_unit_id",
    "network",  # bigquery
    "network_placement",
    'ad_format',  # big_query
    "attempts",
    "responses",
    "fill_rate",
    "impressions",
    "estimated_revenue",
    "ecpm",
]

df = pd.DataFrame(columns=_columns)
for df_result in report.get_report_batch(
    start_date="2023-05-23",
    end_date="2023-05-25",
    columns=_columns,
    batch_size=10000,
    filter_package_name="com.cooking.games.fever.food.city.craze.dream",
):
    # Concat for full data  or process every batch
    df = pd.concat([df, df_result], ignore_index=True)
print(df)
```
