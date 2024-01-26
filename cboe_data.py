"""
Retrieves delayed quotes for option chain data from CBOE's API
    
It reuses the same code from OpenBB 
(https://github.com/OpenBB-finance/OpenBBTerminal)
"""

from datetime import datetime
import random
import requests
from typing import Tuple
import pandas as pd


TICKER_EXCEPTIONS: list[str] = ["NDX", "RUT"]


def get_user_agent() -> str:
    """Get a not very random user agent."""
    user_agent_strings = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:82.1) Gecko/20100101 Firefox/82.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:83.0) Gecko/20100101 Firefox/83.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:84.0) Gecko/20100101 Firefox/84.0",
    ]

    return random.choice(user_agent_strings)  # nosec # noqa: S311


# Write an abstract helper to make requests from a url with potential headers and params
def request(
    url: str, method: str = "get", timeout: int = 5, **kwargs
) -> requests.Response:
    """Abstract helper to make requests from a url with potential headers and params.

    Parameters
    ----------
    url : str
        Url to make the request to
    method : str
        HTTP method to use.  Choose from:
        delete, get, head, patch, post, put, by default "get"
    timeout : int
        How many seconds to wait for the server to send data

    Returns
    -------
    requests.Response
        Request response object

    Raises
    ------
    ValueError
        If invalid method is passed
    """
    method = method.lower()
    if method not in ["delete", "get", "head", "patch", "post", "put"]:
        raise ValueError(f"Invalid method: {method}")
    # We want to add a user agent to the request, so check if there are any headers
    # If there are headers, check if there is a user agent, if not add one.
    # Some requests seem to work only with a specific user agent, so we want to be able to override it.
    headers = kwargs.pop("headers", {})
    timeout = timeout 

    if "User-Agent" not in headers:
        headers["User-Agent"] = get_user_agent()
    func = getattr(requests, method)
    return func(
        url,
        headers=headers,
        timeout=timeout,
        **kwargs,
    )


def get_cboe_directory() -> pd.DataFrame:
    """Gets the US Listings Directory for the CBOE.

    Returns
    -------
    pd.DataFrame: CBOE_DIRECTORY
        DataFrame of the CBOE listings directory

    Examples
    -------
    >>> from openbb_terminal.stocks.options import cboe_model
    >>> CBOE_DIRECTORY = cboe_model.get_cboe_directory()
    """
    try:
        CBOE_DIRECTORY: pd.DataFrame = pd.read_csv(
            "https://www.cboe.com/us/options/symboldir/equity_index_options/?download=csv"
        )
        CBOE_DIRECTORY = CBOE_DIRECTORY.rename(
            columns={
                " Stock Symbol": "Symbol",
                " DPM Name": "DPM Name",
                " Post/Station": "Post/Station",
            }
        ).set_index("Symbol")

        return CBOE_DIRECTORY

    except requests.exceptions.HTTPError:
        return pd.DataFrame()
    
    
def get_cboe_index_directory() -> pd.DataFrame:
    """Gets the US Listings Directory for the CBOE

    Returns
    -------
    pd.DataFrame: CBOE_INDEXES

    Examples
    -------
    >>> from openb_terminal.stocks.options import cboe_model
    >>> CBOE_INDEXES = cboe_model.get_cboe_index_directory()
    """

    try:
        CBOE_INDEXES: pd.DataFrame = pd.DataFrame(
            pd.read_json(
                "https://cdn.cboe.com/api/global/us_indices/definitions/all_indices.json"
            )
        )

        CBOE_INDEXES = CBOE_INDEXES.rename(
            columns={
                "calc_end_time": "Close Time",
                "calc_start_time": "Open Time",
                "currency": "Currency",
                "description": "Description",
                "display": "Display",
                "featured": "Featured",
                "featured_order": "Featured Order",
                "index_symbol": "Ticker",
                "mkt_data_delay": "Data Delay",
                "name": "Name",
                "tick_days": "Tick Days",
                "tick_frequency": "Frequency",
                "tick_period": "Period",
                "time_zone": "Time Zone",
            },
        )

        indices_order: list[str] = [
            "Ticker",
            "Name",
            "Description",
            "Currency",
            "Tick Days",
            "Frequency",
            "Period",
            "Time Zone",
        ]

        CBOE_INDEXES = pd.DataFrame(CBOE_INDEXES, columns=indices_order).set_index(
            "Ticker"
        )

        return CBOE_INDEXES

    except requests.exceptions.HTTPError:
        return pd.DataFrame()


# Gets the list of indexes for parsing the ticker symbol properly.
INDEXES = get_cboe_index_directory().index.tolist()
SYMBOLS = get_cboe_directory()


def get_ticker_info(symbol: str) -> Tuple[pd.DataFrame, list[str]]:
    """Gets basic info for the symbol and expiration dates

    Parameters
    ----------
    symbol: str
        The ticker to lookup

    Returns
    -------
    Tuple: [pd.DataFrame, pd.Series]
        ticker_details
        ticker_expirations

    Examples
    --------
    >>> from openbb_terminal.stocks.options import cboe_model
    >>> ticker_details,ticker_expirations = cboe_model.get_ticker_info('AAPL')
    >>> vix_details,vix_expirations = cboe_model.get_ticker_info('VIX')
    """

    stock = "stock"
    index = "index"
    symbol = symbol.upper()
    new_ticker: str = ""
    ticker_details = pd.DataFrame()
    ticker_expirations: list = []
    try:
        if symbol in TICKER_EXCEPTIONS:
            new_ticker = "^" + symbol
        elif symbol not in INDEXES:
            new_ticker = symbol

        elif symbol in INDEXES:
            new_ticker = "^" + symbol

            # Gets the data to return, and if none returns empty Tuple #

        symbol_info_url = (
            "https://www.cboe.com/education/tools/trade-optimizer/symbol-info/?symbol="
            f"{new_ticker}"
        )

        symbol_info = request(symbol_info_url)
        symbol_info_json = symbol_info.json()
        symbol_info_json = pd.Series(symbol_info.json())

        if symbol_info_json.success is False:
            ticker_details = pd.DataFrame()
            ticker_expirations = []
            print("No data found for the symbol: " f"{symbol}" "")
        else:
            symbol_details = pd.Series(symbol_info_json["details"])
            symbol_details = pd.DataFrame(symbol_details).transpose()
            symbol_details = symbol_details.reset_index()
            ticker_expirations = symbol_info_json["expirations"]

            # Cleans columns depending on if the security type is a stock or an index

            type_ = symbol_details.security_type

            if stock[0] in type_[0]:
                stock_details = symbol_details
                ticker_details = pd.DataFrame(stock_details).rename(
                    columns={
                        "current_price": "price",
                        "bid_size": "bidSize",
                        "ask_size": "askSize",
                        "iv30": "ivThirty",
                        "prev_day_close": "previousClose",
                        "price_change": "change",
                        "price_change_percent": "changePercent",
                        "iv30_change": "ivThirtyChange",
                        "iv30_percent_change": "ivThirtyChangePercent",
                        "last_trade_time": "lastTradeTimestamp",
                        "exchange_id": "exchangeID",
                        "tick": "tick",
                        "security_type": "type",
                    }
                )
                details_columns = [
                    "symbol",
                    "type",
                    "tick",
                    "bid",
                    "bidSize",
                    "askSize",
                    "ask",
                    "price",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "previousClose",
                    "change",
                    "changePercent",
                    "ivThirty",
                    "ivThirtyChange",
                    "ivThirtyChangePercent",
                    "lastTradeTimestamp",
                ]
                ticker_details = (
                    pd.DataFrame(ticker_details, columns=details_columns)
                    .set_index(keys="symbol")
                    .dropna(axis=1)
                    .transpose()
                )

            if index[0] in type_[0]:
                index_details = symbol_details
                ticker_details = pd.DataFrame(index_details).rename(
                    columns={
                        "symbol": "symbol",
                        "security_type": "type",
                        "current_price": "price",
                        "price_change": "change",
                        "price_change_percent": "changePercent",
                        "prev_day_close": "previousClose",
                        "iv30": "ivThirty",
                        "iv30_change": "ivThirtyChange",
                        "iv30_change_percent": "ivThirtyChangePercent",
                        "last_trade_time": "lastTradeTimestamp",
                    }
                )

                index_columns = [
                    "symbol",
                    "type",
                    "tick",
                    "price",
                    "open",
                    "high",
                    "low",
                    "close",
                    "previousClose",
                    "change",
                    "changePercent",
                    "ivThirty",
                    "ivThirtyChange",
                    "ivThirtyChangePercent",
                    "lastTradeTimestamp",
                ]

                ticker_details = (
                    pd.DataFrame(ticker_details, columns=index_columns)
                    .set_index(keys="symbol")
                    .dropna(axis=1)
                    .transpose()
                ).rename(columns={f"{new_ticker}": f"{symbol}"})

    except requests.exceptions.HTTPError:
        print("There was an error with the request'\n")
        ticker_details = pd.DataFrame()
        ticker_expirations = list()
        return ticker_details, ticker_expirations

    return ticker_details, ticker_expirations

def get_ticker_iv(symbol: str) -> pd.DataFrame:
    """Gets annualized high/low historical and implied volatility over 30/60/90 day windows.

    Parameters
    ----------
    symbol: str
        The loaded ticker

    Returns
    -------
    pd.DataFrame: ticker_iv

    Examples
    --------
    >>> from openbb_terminal.stocks.options import cboe_model
    >>> ticker_iv = cboe_model.get_ticker_iv('AAPL')
    >>> ndx_iv = cboe_model.get_ticker_iv('NDX')
    """

    # Checks ticker to determine if ticker is an index or an exception that requires modifying the request's URLs
    try:
        if symbol in TICKER_EXCEPTIONS:
            quotes_iv_url = (
                "https://cdn.cboe.com/api/global/delayed_quotes/historical_data/_"
                f"{symbol}.json"
            )
        elif symbol not in INDEXES:
            quotes_iv_url = (
                "https://cdn.cboe.com/api/global/delayed_quotes/historical_data/"
                f"{symbol}.json"
            )

        elif symbol in INDEXES:
            quotes_iv_url = (
                "https://cdn.cboe.com/api/global/delayed_quotes/historical_data/_"
                f"{symbol}.json"
            )
        h_iv = request(quotes_iv_url)

        if h_iv.status_code != 200:
            print("No data found for the symbol: " f"{symbol}" "")
            return pd.DataFrame()

        data = h_iv.json()
        h_data = pd.DataFrame(data)[2:-1]["data"].rename(f"{symbol}")
        h_data.rename(
            {
                "hv30_annual_high": "hvThirtyOneYearHigh",
                "hv30_annual_low": "hvThirtyOneYearLow",
                "hv60_annual_high": "hvSixtyOneYearHigh",
                "hv60_annual_low": "hvsixtyOneYearLow",
                "hv90_annual_high": "hvNinetyOneYearHigh",
                "hv90_annual_low": "hvNinetyOneYearLow",
                "iv30_annual_high": "ivThirtyOneYearHigh",
                "iv30_annual_low": "ivThirtyOneYearLow",
                "iv60_annual_high": "ivSixtyOneYearHigh",
                "iv60_annual_low": "ivSixtyOneYearLow",
                "iv90_annual_high": "ivNinetyOneYearHigh",
                "iv90_annual_low": "ivNinetyOneYearLow",
            },
            inplace=True,
        )

        iv_order = [
            "ivThirtyOneYearHigh",
            "hvThirtyOneYearHigh",
            "ivThirtyOneYearLow",
            "hvThirtyOneYearLow",
            "ivSixtyOneYearHigh",
            "hvSixtyOneYearHigh",
            "ivSixtyOneYearLow",
            "hvsixtyOneYearLow",
            "ivNinetyOneYearHigh",
            "hvNinetyOneYearHigh",
            "ivNinetyOneYearLow",
            "hvNinetyOneYearLow",
        ]

        ticker_iv = pd.DataFrame(h_data).transpose()
    except requests.exceptions.HTTPError:
        print("There was an error with the request'\n")

    return pd.DataFrame(ticker_iv, columns=iv_order).transpose()

def get_quotes(symbol: str) -> pd.DataFrame:
    """Gets the complete options chains for a ticker.

    Parameters
    ----------
    symbol: str
        The ticker get options data for

    Returns
    -------
    pd.DataFrame
        DataFrame with all active options contracts for the underlying symbol.

    Examples
    --------
    >>> from openbb_terminal.stocks.options import cboe_model
    >>> xsp = cboe_model.OptionsChains().get_chains('XSP')
    >>> xsp_chains = xsp.chains
    """
    # Checks ticker to determine if ticker is an index or an exception that requires modifying the request's URLs.

    try:
        if symbol in TICKER_EXCEPTIONS:
            quotes_url = (
                "https://cdn.cboe.com/api/global/delayed_quotes/options/_"
                f"{symbol}"
                ".json"
            )
        else:
            if symbol not in INDEXES:
                quotes_url = (
                    "https://cdn.cboe.com/api/global/delayed_quotes/options/"
                    f"{symbol}"
                    ".json"
                )
            if symbol in INDEXES:
                quotes_url = (
                    "https://cdn.cboe.com/api/global/delayed_quotes/options/_"
                    f"{symbol}"
                    ".json"
                )

        r = request(quotes_url)
        if r.status_code != 200:
            print("No data found for the symbol: " f"{symbol}" "")
            return pd.DataFrame()

        r_json = r.json()
        data = pd.DataFrame(r_json["data"])
        options = pd.Series(data.options, index=data.index)
        options_columns = list(options[0])
        options_data = list(options[:])
        options_df = pd.DataFrame(options_data, columns=options_columns)

        options_df = options_df.rename(
            columns={
                "option": "contractSymbol",
                "bid_size": "bidSize",
                "ask_size": "askSize",
                "iv": "impliedVolatility",
                "open_interest": "openInterest",
                "theo": "theoretical",
                "last_trade_price": "lastTradePrice",
                "last_trade_time": "lastTradeTimestamp",
                "percent_change": "changePercent",
                "prev_day_close": "previousClose",
            }
        )

        # Pareses the option symbols into columns for expiration, strike, and optionType

        option_df_index = options_df["contractSymbol"].str.extractall(
            r"^(?P<Ticker>\D*)(?P<expiration>\d*)(?P<optionType>\D*)(?P<strike>\d*)"
        )
        option_df_index = option_df_index.reset_index().drop(
            columns=["match", "level_0"]
        )
        option_df_index.optionType = option_df_index.optionType.str.replace(
            "C", "call"
        ).str.replace("P", "put")
        option_df_index.strike = [ele.lstrip("0") for ele in option_df_index.strike]
        option_df_index.strike = pd.Series(option_df_index.strike).astype(float)
        option_df_index.strike = option_df_index.strike * (1 / 1000)
        option_df_index.strike = option_df_index.strike.to_list()
        option_df_index.expiration = [
            ele.lstrip("1") for ele in option_df_index.expiration
        ]
        option_df_index.expiration = pd.DatetimeIndex(
            option_df_index.expiration, yearfirst=True
        ).astype(str)
        option_df_index = option_df_index.drop(columns=["Ticker"])

        # Joins the parsed symbol into the dataframe.

        quotes = option_df_index.join(options_df)

        now = datetime.now()
        temp = pd.DatetimeIndex(quotes.expiration)
        temp_ = (temp - now).days + 1
        quotes["dte"] = temp_

        quotes = quotes.set_index(
            keys=["expiration", "strike", "optionType"]
        ).sort_index()
        quotes["openInterest"] = quotes["openInterest"].astype(int)
        quotes["volume"] = quotes["volume"].astype(int)
        quotes["bidSize"] = quotes["bidSize"].astype(int)
        quotes["askSize"] = quotes["askSize"].astype(int)
        quotes["previousClose"] = round(quotes["previousClose"], 2)
        quotes["changePercent"] = round(quotes["changePercent"], 2)

    except requests.exceptions.HTTPError:
        print("There was an error with the request'\n")
        return pd.DataFrame()

    return quotes.reset_index()