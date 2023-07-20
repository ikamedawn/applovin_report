import logging
import pandas as pd
import requests
from pandas import DataFrame

from applovin_report.utils.datetime_utils import day_ago
from applovin_report.utils.logging_utils import logging_basic_config
from requests.adapters import HTTPAdapter, Retry

logging_basic_config()
STATUS_RETRIES = (500, 502, 503, 504)


class UserAdRevenueReport:
    """
    Detailed documentation for this API can be found at:
        [User Ad Revenue Report API](https://dash.applovin.com/documentation/mediation/reporting-api/user-ad-revenue)
    """

    ENDPOINT = "https://r.applovin.com/max/userAdRevenueReport"

    def __init__(self, api_key: str | list[str],
                 status_retries: list[int] = STATUS_RETRIES,
                 max_retries=5, retry_delay=1):
        """
        Args:
            api_key: API key(s) to use for the report
            status_retries: A set of HTTP status codes that we should force a retry on
            max_retries: Total number of retries to allow
            retry_delay: Num of seconds sleep between attempts

        Returns:
            None

        Doc Author:
            mungvt@ikameglobal.com
        """
        self.api_key = api_key
        self.session = requests.Session()
        retries = Retry(total=max_retries, backoff_factor=retry_delay, status_forcelist=status_retries)
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def get_report(
        self,
        date: str = day_ago(1),
        aggregated: bool = True,
        application: str = "",
        store_id: str = "",
        platform: str = "",
        **kwargs,
    ) -> DataFrame:
        """
        Retrieve a report from the MAX Revenue Report API.


        Args:
            date: YYYY-MM-DD, within the last 45 days
            aggregated: Whether the data should be aggregated (per user) or not (per impression), defaults to true
            application: Application package name for Android
            store_id: Application store id for IOS
            platform: ios or android
            **kwargs: Additional parameters to pass to the API

        Returns:
            A pandas DataFrame containing the report data.

        Doc Author:
            mungvt@ikameglobal.com
        """

        params = {
            "api_key": self.api_key,
            "date": date,
            "aggregated": aggregated,
            "platform": platform,
            **kwargs,
        }
        app_id = "no_platform_app_id"
        if platform == "android":
            params["application"] = application
            app_id = application
        elif platform == "ios":
            params["store_id"] = store_id
            app_id = store_id
        else:
            logging.warning(f"Wrong platform: {platform}!")
        response = self.session.get(url=self.ENDPOINT, params=params)
        if response.status_code == 404:
            logging.warning(response.text + '. Skipped it.')
            return pd.DataFrame()
        else:
            result = pd.read_csv(response.json()['ad_revenue_report_url'], dtype='object')
            logging.info(f'Collected successful ad revenue report of App id: {app_id}, Platform: {platform}.')
            return result
