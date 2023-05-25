import time
import traceback
from ctypes import Union
from datetime import datetime, timedelta

import pandas as pd
import requests


class RevenueReport:
    ENDPOINT = "https://r.applovin.com/maxReport"

    def __init__(self, api_key: Union[str, list[str]]):
        """
        Args:
            api_key: str or list[str]: API key(s) to use for the report

        Returns:
            None

        Doc Author:
            minhpc@ikameglobal.com
        """
        self.api_key = api_key

    def get_report(
        self,
        start_date: str = None,
        end_date: str = None,
        columns: list[str] = None,
        limit: int = 100000,
        max_retries: int = 3,
        retry_interval: int = 30,
        **kwargs,
    ):
        """
        Retrieve a report from the MAX Revenue Report API.


        Args:
            start_date: str: YYYY-MM-DD, within the last 45 days
            end_date: str: YYYY-MM-DD, within the last 45 days
            columns: list[str]: List of columns to include in the report
            limit: int: Set the number of rows to return
            max_retries: int: Set the number of retries
            retry_interval: int: Set the number of seconds to wait between retries
            **kwargs: dict: Additional parameters to pass to the API

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the report data

        Doc Author:
            minhpc@ikameglobal.com
        """
        if not start_date or not end_date:
            start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        params = {
            "api_key": self.api_key,
            "start": start_date,
            "end": end_date,
            "columns": ",".join(columns),
            "format": "json",
            "limit": limit,
            **kwargs,
        }

        for i in range(max_retries + 1):
            response = requests.get(url=RevenueReport.ENDPOINT, params=params)

            if response.status_code == 200:
                return pd.DataFrame(response.json()["results"])
            else:
                print(f"Retrying... ({i + 1}/{max_retries})")
                time.sleep(retry_interval)

        print(traceback.format_exc())
        raise Exception(f"Error: {response.status_code}")

    def get_report_batch(
        self,
        start_date: str = None,
        end_date: str = None,
        columns: list[str] = None,
        batch_size: int = 100000,
        max_retries: int = 3,
        retry_interval: int = 30,
        **kwargs,
    ):

        """
        Retrieve a report from the MAX Revenue Report API in batches.

        Args:
            start_date: str: YYYY-MM-DD, within the last 45 days
            end_date: str: YYYY-MM-DD, within the last 45 days
            columns: list[str]: List of columns to include in the report
            batch_size: int: Number of rows to return per batch
            max_retries: int: Number of retries
            retry_interval: int: Number of seconds to wait between retries
            **kwargs: dict: Additional parameters to pass to the API

        Returns:
            A generator that yields a pandas DataFrame containing the report data

        Doc Author:
            minhpc@ikameglobal.com
        """
        if not start_date or not end_date:
            start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        offset = 0
        has_next_batch = True
        while has_next_batch:
            params = {
                "api_key": self.api_key,
                "start": start_date,
                "end": end_date,
                "columns": ",".join(columns),
                "format": "json",
                "offset": offset,
                "limit": batch_size,
                **kwargs,
            }

            response = None
            for i in range(max_retries + 1):
                response = requests.get(url=RevenueReport.ENDPOINT, params=params)
                if response.status_code == 200:
                    break
                print(f"Retrying... ({i + 1}/{max_retries})")
                time.sleep(retry_interval)
            if response.status_code != 200:
                print(traceback.format_exc())
                raise Exception(
                    f"Error: {response.status_code}"
                    f"Last offset: {offset}"
                    f"Batch size: {batch_size}"
                )

            results = response.json()["results"]
            has_next_batch = len(results) == batch_size
            offset += batch_size
            yield pd.DataFrame(results)


if __name__ == "__main__":
    service = RevenueReport()
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
        # 'ad_format',  # big_query
        "attempts",
        "responses",
        "fill_rate",
        "impressions",
        "estimated_revenue",
        "ecpm",
    ]

    total_len = 0
    df: pd.DataFrame = None
    start_time = time.time()
    for df_result in service.get_report_batch(
        start_date="2023-05-14",
        end_date="2023-05-16",
        columns=_columns,
        batch_size=100000,
        filter_package_name="com.jura.car.crashes.simulator",
    ):
        # Concat to df
        if df is None:
            df = df_result
        else:
            df = pd.concat([df, df_result], ignore_index=True)

        # Print progress
        total_len += len(df_result)
        print(f"Progress: {total_len} rows, {time.time() - start_time:.2f} seconds")
        # Average speed
        print(
            f"Average speed: {total_len / (time.time() - start_time):.2f} rows/second"
        )

    # In day column, convert YYYY-MM-DD to YYYYMMDD
    df["day"] = df["day"].apply(lambda x: x.replace("-", ""))

    # Write to csv
    # df.to_csv('/home/dawn/applovin.csv', index=False, header=True)

    platforms = df["platform"].unique().tolist()
    platforms = [platform.upper() for platform in platforms]

    dates = df["day"].unique().tolist()
    print(dates)
    print(platforms)
