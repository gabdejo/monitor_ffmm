import pandas as pd


def rebase(prices: pd.Series, base: float = 100.0) -> pd.Series:
    """
    Normalize a price series so its first observation equals `base`.

    Args:
        prices: DatetimeIndex Series of prices.
        base:   starting index value (default 100).
    """
    if prices.empty:
        return prices
    return prices / prices.iloc[0] * base


def align_and_rebase(
    series: dict[str, pd.Series],
    start_date: pd.Timestamp | None = None,
    end_date:   pd.Timestamp | None = None,
    base: float = 100.0,
) -> pd.DataFrame:
    """
    Align multiple price series to a common date window and rebase all to `base`.

    Intended for building the wealth-curve chart where funds and their ETF
    benchmarks must start at the same index value on the same date.

    Args:
        series:     mapping of label → price Series (DatetimeIndex, float values).
        start_date: common anchor date. Defaults to the latest of all series'
                    first dates so every series has data from day one.
        end_date:   cutoff date (inclusive). Defaults to latest available.
        base:       index value at start_date (default 100).

    Returns:
        DataFrame — index = dates, one column per label, values = rebased index.
    """
    if not series:
        return pd.DataFrame()

    if start_date is None:
        start_date = max(s.index.min() for s in series.values())

    df = pd.DataFrame(
        {label: s[s.index >= start_date] for label, s in series.items()}
    )

    if end_date is not None:
        df = df[df.index <= end_date]

    df = df[df.index.dayofweek < 5]  # keep Mon–Fri only
    df = df.ffill()                  # fill ETF gaps on days markets were closed
    df = df.dropna()                 # drop leading rows where any series has no data yet

    return df.div(df.iloc[0]) * base
