import pandas as pd


def mtd(prices: pd.Series) -> float | None:
    """
    Month-to-date return: first available price of the current month → latest price.
    Returns None if fewer than 2 data points exist in the current month.
    """
    if prices.empty:
        return None
    latest = prices.index.max()
    window = prices[prices.index >= latest.replace(day=1)]
    if len(window) < 2:
        return None
    return float(window.iloc[-1] / window.iloc[0] - 1)


def ytd(prices: pd.Series) -> float | None:
    """
    Year-to-date return: last price of previous year-end → latest price.
    Falls back to first available price of the current year if prior year data
    is not present.
    Returns None if a meaningful base cannot be found.
    """
    if prices.empty:
        return None
    latest = prices.index.max()

    prev_year = prices[prices.index <= pd.Timestamp(latest.year - 1, 12, 31)]
    if not prev_year.empty:
        base = prev_year.iloc[-1]
    else:
        this_year = prices[prices.index.year == latest.year]
        if len(this_year) < 2:
            return None
        base = this_year.iloc[0]

    return float(prices.iloc[-1] / base - 1)


def monthly_returns(prices: pd.Series) -> pd.Series:
    """
    Return for each completed calendar month in the data.

    Uses the last available price of each month as the reference.
    The current (incomplete) month is excluded.
    Index: month-end dates. Values: simple returns (e.g. 0.03 = 3%).
    """
    if prices.empty:
        return pd.Series(dtype=float)

    monthly_last = prices.resample("ME").last()
    returns = monthly_last.pct_change().dropna()

    # Drop the current (incomplete) month
    latest = prices.index.max()
    returns = returns[returns.index.to_period("M") < latest.to_period("M")]

    return returns


def compute_all(prices: pd.Series) -> dict:
    """
    Compute MTD, YTD, and per-month returns for a single price series.

    Returns:
        {
            "mtd": float | None,
            "ytd": float | None,
            "monthly": {"YYYY-MM-DD": float, ...}   # month-end date → return
        }
    """
    monthly = monthly_returns(prices)
    return {
        "mtd": mtd(prices),
        "ytd": ytd(prices),
        "monthly": {str(ts.date()): round(v, 6) for ts, v in monthly.items()},
    }
