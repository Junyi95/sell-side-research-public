import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, TypeVar, Union
from urllib.parse import quote

import aiofiles
import aiohttp
import httpx
import pandas as pd
import requests
import ujson as json
from pandas.tseries.offsets import BDay

pd.options.mode.chained_assignment = None

JSONTypeVar = TypeVar("JSONTypeVar", dict, list, str, int, float, bool, type(None))
JSON = Union[Dict[str, JSONTypeVar], List[JSONTypeVar], str, int, float, bool, None]


def bofa_analyst_lookups(
    analyst_codes: List[str],
    cookie_2165: str,
    encrypted_session_query: str,
) -> Dict[str, str] | None:
    cookies = {
        "GZIP": 1,
        r"2165%5F0": cookie_2165,
        "2165_0": cookie_2165,
    }
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Dnt": "1",
        "Host": "www.gwm.ml.wallst.com",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Cookie": "; ".join([f"{key}={value}" for key, value in cookies.items()]),
    }

    def name_formater(last: str, first: str):
        return f"{last}, {first}"

    def gwm_ml_url_formatter(codes: List[str]):
        delimiter = "|"
        analyst_codes_str = delimiter.join(codes)
        encoded_string = quote(analyst_codes_str)
        return f"https://www.gwm.ml.wallst.com/stockresearch/api/research/1.0/sql/analysts{encrypted_session_query}&analystCodes={encoded_string}"

    if len(analyst_codes) < 32:
        url = gwm_ml_url_formatter(analyst_codes)
        res = requests.get(url=url, headers=headers)
        if res.ok:
            json_data = res.json()["data"]["analysts"]
            return {
                analyst["analystCode"]: name_formater(
                    analyst["lastName"], analyst["firstName"]
                )
                for analyst in json_data
            }
        return None

    async def fetch(session: aiohttp.ClientSession, curr_analyst_codes):
        try:
            curr_url = gwm_ml_url_formatter(curr_analyst_codes)
            async with session.get(curr_url) as response:
                if response.status != 200:
                    raise Exception(
                        f"Bad Status in BofA Analyst Lookup: {response.status}"
                    )
                json = await response.json()
                json_data = json["data"]["analysts"]
                return {
                    analyst["analystCode"]: name_formater(
                        analyst["lastName"], analyst["firstName"]
                    )
                    for analyst in json_data
                }
        except Exception as e:
            print(e)
            return None

    async def get_promises(session: aiohttp.ClientSession):
        def chunk_list(lst, chunk_size):
            for i in range(0, len(lst), chunk_size):
                yield lst[i : i + chunk_size]

        nested_lists = list(chunk_list(analyst_codes, 31))
        return await asyncio.gather(*[fetch(session, codes) for codes in nested_lists])

    async def run_fetch_all() -> List[Dict[str, str]]:
        async with aiohttp.ClientSession(headers=headers) as session:
            all_data = await get_promises(session)
            return all_data

    list_of_dicts = asyncio.run(run_fetch_all())
    return {k: v for d in list_of_dicts if d is not None for k, v in d.items()}


def build_bofa_search_payload(
    datetime_from: datetime,
    datetime_to: datetime,
    row_count: int = 10000,
    search_string: str = "",
):
    return {
        "queries": [
            {
                "rowCount": row_count,
                "firstRow": 0,
                "arguments": [
                    {
                        "field": "MLExternal",
                        "conditions": [{"operator": "EqualTo", "values": ["TRUE"]}],
                    },
                    {
                        "field": "DocumentDate",
                        "conditions": [
                            {
                                "operator": "GreaterThanEqualTo",
                                "values": [datetime_from.strftime("%Y-%m-%d %H:%M:%S")],
                            },
                            {
                                "operator": "LessThanEqualTo",
                                "values": [datetime_to.strftime("%Y-%m-%d %H:%M:%S")],
                            },
                        ],
                    },
                    {
                        "field": "FreeText",
                        "conditions": [
                            {"operator": "EqualTo", "values": [search_string]}
                        ],
                    },
                ],
            },
        ]
    }


def better_search_bofa_global_research(
    cookie_2165: str,
    encrypted_session_query: str = None,
    search_string: str = "",
    datetime_from=datetime.now() - BDay(5),
    datetime_to=datetime.now(),
    run_analyst_lookup=False,
    xlsx_path: str = None,
    just_today=False,
    verbose=False,
) -> pd.DataFrame:
    headers = {
        "Accept": "undefined",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        # "Content-Length": "1131",
        "Content-Type": "application/json",
        "Dnt": "1",
        "Host": "www.gwm.ml.wallst.com",
        "Origin": "https://olui2.fs.ml.com",
        "Referer": "https://olui2.fs.ml.com/",
        "Sec-Ch-Ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    cookies = {
        "2165%5F0": cookie_2165,
    }

    difference = datetime_to - datetime_from
    multiple_payloads = difference.days > 21

    def agg_payloads(
        businessdays: int = 15, row_count: int = 2500
    ):  # 15 business days for 3 weeks
        current_date = datetime_from
        business_dates = [current_date]
        while current_date < datetime_to:
            current_date += BDay(businessdays)
            if current_date < datetime_to:
                business_dates.append(current_date)
            else:
                business_dates.append(datetime_to)
                break

        payloads = []
        for i in range(len(business_dates) - 1):
            payload = build_bofa_search_payload(
                datetime_from=business_dates[i],
                datetime_to=business_dates[i + 1],
                search_string=search_string,
                row_count=row_count,
            )
            payloads.append(payload)

        return payloads

    if multiple_payloads:
        payloads = agg_payloads()
        print(f"Will Fetch {len(payloads)} payloads") if verbose else None
        print(payloads) if verbose else None
    elif just_today:
        payload = build_bofa_search_payload(
            datetime_from=(datetime.now() - BDay(1)), datetime_to=datetime.now()
        )
    else:
        payload = build_bofa_search_payload(
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            search_string=search_string,
        )

    url = "https://www.gwm.ml.wallst.com/stockresearch/api/dm/1.0/queries/commonReports?YYY32_NblkchvtwfIBVldm+Dp+oLE0JmurNGdXOqtz+Xq0B9GIGonayPS4WTDi4GhmXB+YOFfkExecXbdt6lFLwsKKVgM4vr425oxPrd4Lx+kuh+Y=HTTP/1.1"

    def handle_search_results(json_data: JSON, run_lookup: bool) -> pd.DataFrame:
        documents = json_data["documents"]
        df = pd.DataFrame(documents)

        if run_lookup:
            if not encrypted_session_query:
                print("Include your encrypted_session_query token")
            else:
                split_codes = df["analysts"].str.split("|")
                analyst_codes = [
                    code for sublist in split_codes for code in sublist if code
                ]
                lookup_dict = bofa_analyst_lookups(
                    analyst_codes, cookie_2165, encrypted_session_query
                )
                if lookup_dict is not None and bool(lookup_dict):

                    def map_codes_to_names(codes_str):
                        codes = codes_str.split("|")
                        names = [lookup_dict.get(code, code) for code in codes if code]
                        return "|".join(names)

                    df["analysts"] = df["analysts"].apply(map_codes_to_names)
                else:
                    print("BofA Analyst Lookup Failed")

        df["reportLink"] = df["documentKey"].apply(
            lambda x: rf'https://olui2.fs.ml.com/MDWSODUtility/PdfLoader.aspx?src=%2Fnet%2Futil%2FGetPdfFile%3Fdockey%3D6208-{str(x).split("-")[1]}-1%26segment%3DDIRECT'
        )
        return df

    if not multiple_payloads:
        res = requests.post(
            url=url, data=json.dumps(payload), headers=headers, cookies=cookies
        )
        if res.ok:
            try:
                json_data = res.json()["data"][0]
                print("Search Result Matches: ", json_data["matches"])
                if json_data["matches"] == 0:
                    return pd.DataFrame()
            except Exception as e:
                print(e)
                print(res.json())
                return pd.DataFrame()

            df = handle_search_results(json_data, run_analyst_lookup)
            if xlsx_path:
                df.to_excel(xlsx_path, index=False)

            return df

        print(f"Bad Status in BofA Global Research Search: {res.status_code}")
        return pd.DataFrame()

    async def fetch(client: httpx.AsyncClient, payload: JSON, key: int = None):
        try:
            payload = json.dumps(payload)
            print(f"Fetching Payload {key} - {payload}") if verbose else None
            response = await client.post(url, data=payload, follow_redirects=True)
            if response.status_code != 200:
                raise Exception(
                    f"Bad Status in BofA Global Research Search: {response.status}"
                )
            json_obj = response.json()
            json_data = json_obj["data"][0]
            print("Search Result Matches: ", json_data["matches"])
            if json_data["matches"] == 0:
                return pd.DataFrame()

            return handle_search_results(json_data, run_analyst_lookup)

        except Exception as e:
            print(e)
            return pd.DataFrame()

    timeout = httpx.Timeout(10)

    async def run_fetch_all():
        async with httpx.AsyncClient(
            cookies=cookies, headers=headers, timeout=timeout
        ) as client:
            tasks = [
                fetch(client, payload, index) for index, payload in enumerate(payloads)
            ]
            return await asyncio.gather(*tasks)

    dfs = asyncio.run(run_fetch_all())
    big_df = pd.concat(dfs, ignore_index=True)
    big_df = big_df.drop_duplicates(subset="documentKey", keep="first")
    if xlsx_path:
        big_df.to_excel(xlsx_path, index=False)
    
    return big_df


def bofa_download_reports(
    df: pd.DataFrame,  # assuming that df has 'reportLink', 'documentDate', 'headline' cols
    cookies: Dict[str, str],
    # base_ml_cookies: Dict[str, str],
    # session_cookies: Dict[str, str],
    base_path: str = os.getcwd() + r"\reports",
    verbose=False,
):
    df["documentDate"] = pd.to_datetime(df["documentDate"]).dt.strftime("%Y-%m-%d")
    df["headline"] = df["headline"].apply(
        lambda x: re.sub(r"\s+", "_", re.sub(r'[<>:"/\\|?*]', "", x))
    )
    df = df[["documentDate", "headline", "reportLink"]]

    @dataclass
    class DocumentInfo:
        documentDate: str
        headline: str
        reportLink: str

    documents_dict: List[DocumentInfo] = [
        DocumentInfo(**d) for d in df.to_dict("records")
    ]
    for doc_info in documents_dict:
        full_path = os.path.join(base_path, doc_info.documentDate)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
            print(f"Creating {full_path}") if verbose else None

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,application/pdf",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Dnt": "1",
        "Referer": "https://olui2.fs.ml.com/RIResearchReportsUI/BofAMLSearch.aspx",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    }

    async def fetch(client: httpx.AsyncClient, doc_info: DocumentInfo):
        try:
            file_path = os.path.join(
                base_path, doc_info.documentDate, f"{doc_info.headline}.pdf"
            )
            if os.path.isfile(file_path):
                print(f"{file_path} already exists - exiting") if verbose else None
                return

            response = await client.get(doc_info.reportLink, follow_redirects=True)
            if response.status_code != 200:
                print(
                    f"Bad Status in BofA Global Research Report Download: {response.status_code} - {doc_info.documentDate} - {doc_info.headline} - {doc_info.reportLink}"
                )
                return

            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type:
                if verbose:
                    print(
                        f"Content is HTML, not downloading - {doc_info.documentDate} - {doc_info.headline}"
                    )
                    print(response.content)
                return

            content_start = (
                response.content[:100].decode("utf-8", errors="ignore").lower()
            )
            if "<!doctype html>" in content_start or "<html" in content_start:
                if verbose:
                    print(
                        f"Content appears to be HTML by inspection, not downloading - {doc_info.documentDate} - {doc_info.headline}"
                    )
                return

            chunk_size = 1024 * 1024
            async with aiofiles.open(file_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size):
                    await f.write(chunk)
                (
                    print(
                        f"BofA Global Research Report Downloaded - {doc_info.documentDate} - {doc_info.headline}"
                    )
                    if verbose
                    else None
                )

            print("Fetch Finished") if verbose else None

        except Exception as e:
            print(e)

    async def run_fetch_all():
        async with httpx.AsyncClient(cookies=cookies, headers=headers) as client:
            tasks = [fetch(client, doc_info) for doc_info in documents_dict]
            await asyncio.gather(*tasks)

    asyncio.run(run_fetch_all())
