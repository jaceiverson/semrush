"""
https://www.semrush.com/api-analytics/
"""
import os
import requests as r
import pandas as pd
import urllib
from typing import Optional, Union

from dotenv import load_dotenv

load_dotenv()

# TODO add api_key value, keeping it removed to not call the API


class SEMRush:
    def __init__(self, key: Optional[str] = None) -> None:
        self.key = os.environ["SEMRUSH_API_KEY"] if key is None else key
        self.base_url = "https://api.semrush.com/"

    @staticmethod
    def make_table(response: r.Response) -> pd.DataFrame:
        """
        takes in a response object and returns a table
        """
        data = response.text.strip().split("\r\n")
        # not working
        # df=pd.read_csv(resp.text,sep=';')
        return pd.DataFrame(
            [x.split(";") for x in data][1:], columns=data[0].split(";")
        )

    def _api_pull(
        self, endpoint: str, output_as_df: bool = False
    ) -> Union[r.Response, pd.DataFrame]:
        """
        Takes in a URL (that is a SEMrush API endpoint)
        Gets the response, and returns a pd dataframe object of the result
        """
        resp = r.get(self.base_url + endpoint)
        return self.make_table(resp) if output_as_df else resp

    def check_api_units(self):
        """returns the number of api units we have remaining"""
        units_url = f"http://www.semrush.com/users/countapiunits.html?key={self.key}"

    def domain_rank_history(
        self,
        url: str,
        limit: int = 10,
        columns: str = "Or,Xn, FKn, FPn",
        sort: str = "dt_desc",
        database: str = "us",
    ) -> Union[r.Response, pd.DataFrame]:
        endpoint = (
            "?type=domain_rank_history"
            f"&key={self.key}"
            f"&display_limit={limit}"
            f"&export_columns={columns}"
            f"&domain={url}"
            f"&database={database}"
            f"&display_sort={sort}"
        )

        return self._api_pull(endpoint)

    def keywords(
        self,
        url: str,
        limit: int = 20,
        columns: str = "Ph,Po,Pp,Pd,Nq,Cp,Ur,Tr,Tc,Co,Nr,Td",
        sort: str = "tr_desc",
        database: str = "us",
    ) -> Union[r.Response, pd.DataFrame]:
        """
        domain_organic

        &display_filter=%2B%7CPh%7CCo%7Cseo
        """
        endpoint = (
            "?type=domain_organic"
            f"&key={self.key}"
            f"&display_limit={limit}"
            f"&export_columns={columns}"
            f"&domain={url}"
            f"&display_sort={sort}"
            f"&database={database}"
        )
        return self._api_pull(endpoint)

    def url_organic(
        self,
        url: str,
        limit: int = 20,
        columns: str = "Ph,Po,Nq,Cp,Co,Tr,Tc,Nr,Td",
        database: str = "us",
    ) -> Union[r.Response, pd.DataFrame]:
        """
        URL ORGANIC SEARCH KEYWORDS
        url_organic

        takes a URL and makes a API call to get the KW list, defaults to 20 here
        Can pull up to 10,000 KW through the API
        """
        url_kw_endpoint = (
            "?type=url_organic"
            f"&key={self.key}"
            f"&display_limit={limit}"
            f"&export_columns={columns}"
            f"&url={url}"
            f"&database={database}"
        )

        return self._api_pull(url_kw_endpoint)

    def get_competitors_by_url(
        self,
        url: str,
        limit: int = 10,
        columns: str = "Dn,Cr,Np,Or,Ot,Oc,Ad",
        database: str = "us",
    ) -> Union[r.Response, pd.DataFrame]:
        """
        COMPETITORS IN ORGANIC SEARCH
        domain_organic_organic

        takes in a url (normally your start_url)
        and then grabs the top 10 competitors (by deafult, number can be changed)

        """
        url_comp_endpoint = (
            "?type=domain_organic_organic"
            f"&key={self.key}"
            f"&display_limit={limit}"
            f"&export_columns={columns}"
            f"&domain={url}"
            f"&database={database}"
        )

        return self._api_pull(url_comp_endpoint)

    def get_kw_serp(
        self,
        kw: str,
        limit: int = 10,
        columns: str = "Dn,Ur,Fk,Fp",
        database: str = "us",
    ) -> Union[r.Response, pd.DataFrame]:
        """
        This grabs the top {limit} SERP results based on a kw
        """
        url_endpoint = (
            "?type=phrase_organic"
            f"&key={self.key}"
            f"&phrase={kw}"
            f"&export_columns={columns}"
            f"&database={database}"
            f"&display_limit={limit}"
        )

        return self._api_pull(url_endpoint)

    def phrase_these(
        self,
        kw_list: list[str],
        columns: str = "Ph,Nq,Cp,Kd",
        database: str = "us",
    ) -> pd.DataFrame:
        """
        calls the endpoint to batch get keywords and output given the columns
        columns should be passed in as a string, commas separating each letter code
        the entire list of codes is found here: https://developer.semrush.com/api/v3/analytics/basic-docs/#columns
        """

        def chunks(l: list, n: int):
            """creates a small list of lists with each segment being n in length"""
            n = max(1, n)
            return (l[i : i + n] for i in range(0, len(l), n))

        keywords = pd.DataFrame()
        for x in chunks(kw_list, 100):
            url_endpoint = (
                "?type=phrase_these"
                f"&key={self.key}"
                f"&phrase={';'.join(x)}"
                f"&export_columns={columns}"
                f"&database={database}"
            )

            keywords = pd.concat(
                [keywords, self._api_pull(url_endpoint, output_as_df=True)]
            )

        return keywords

    def domain_pages(
        self,
        url: str,
        d_filter: Optional[str] = None,
        d_limit: int = 10,
        database: str = "us",
        columns: str = "Ur,Pc,Tg,Tr",
        sort: str = "tg_desc",
    ) -> Union[r.Response, pd.DataFrame]:
        """
        db is the database
        use 'us' or us desktop and 'mobile-us' or us mobile

        report_date str format 'YYYYMM15'

        """
        url_endpoint = (
            f"?type=domain_organic_unique"
            f"&key={self.key}"
            f"&display_filter={d_filter}"
            f"&display_limit={d_limit}"
            f"&export_columns={columns}"
            f"&domain={url}"
            f"&display_sort={sort}"
            f"&database={database}"
        )

        return self._api_pull(url_endpoint)

    def related_keywords(
        self,
        keyword: str,
        limit: int = 10,
        columns: str = "Ph,Nq,Nr,Rr",
        sort: str = "ng_desc",
        database: str = "us",
    ) -> pd.DataFrame:
        url_endpoint = (
            f"?type=phrase_related"
            f"&key={self.key}"
            f"&phrase={keyword}"
            f"&export_columns={columns}"
            f"&database={database}"
            f"&display_limit={limit}"
            f"&display_sort={sort}"
        )
        return self._api_pull(url_endpoint, output_as_df=True)


def exclude_from_url(excludes: list[str]) -> str:
    """
    <sign>|<field>|<operation>|<value>
    -|Ur|Co|mortgage
    -|Ur|Co|mortgage
    """

    filter_str = ""

    for x in excludes:
        temp_str = "-|Ur|Co|" + x

        if filter_str != "":
            filter_str += "|" + urllib.parse.quote_plus(temp_str)
        else:
            filter_str += urllib.parse.quote_plus(temp_str)

    return filter_str


def remove_branded(df: pd.DataFrame, branded_kw_list: list[str]) -> pd.DataFrame:
    """
    this will get rid of any KW that are in your branded list
    TODO
    needs to be built out to include the base domain and include a
    list of supplied words to be excluded
    """
    return df[df["Keyword"].str.isin(branded_kw_list)]
