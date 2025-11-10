import pandas as pd
import numpy as np

def compute_avg_percentile(columns: list[str], df: pd.DataFrame, group_by_champ: bool = False) -> pd.DataFrame:
    """
    Computes average and percentile statistics (Q1, Q2, Q3) for given columns, grouped by role or champion.
    Returns: pd.DataFrame
    """
    if group_by_champ:
        group_cols = ["championName", "individualPosition", "win"]
    else:
        group_cols = ["individualPosition", "win"]

    agg_dict = {}
    for col in columns:
        agg_dict[col] = [
            ("avg_" + col, lambda x: round(x.mean(), 4)),
            ("Q1_" + col, lambda x: round(np.percentile(x, 25), 4)),
            ("Q2_" + col, lambda x: round(np.percentile(x, 50), 4)),
            ("Q3_" + col, lambda x: round(np.percentile(x, 75), 4))
        ]

    named_aggs = {}
    for col, funcs in agg_dict.items():
        for new_col_name, func in funcs:
            named_aggs[new_col_name] = (col, func)

    result = (
        df.groupby(group_cols)
        .agg(**named_aggs)
        .reset_index()
        .sort_values(by=["individualPosition", "win"], ascending=[False, False])
    )

    return result


def compute_cols_per_minutes(columns: list[str], df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates per-minute values for the specified columns based on game duration.
    Returns: pd.DataFrame
    """
    df = df.copy()

    for col_name in columns:
        df[col_name] = round(df[col_name] / (df["gameDuration"] / 60), 3)

    return df


def split_multi_col_to_save_format(columns: list[str], df: pd.DataFrame, group_by_champ: bool = False) -> pd.DataFrame:
    """
    Transforms a DataFrame with aggregated statistics into a long format suitable for saving.
    Returns: pd.DataFrame
    """
    df = df.copy()

    if group_by_champ:
        id_cols = ["championName", "individualPosition", "win"]
    else:
        id_cols = ["individualPosition", "win"]

    value_vars = []
    for col_name in columns:
        for stat in ["avg", "Q1", "Q2", "Q3"]:
            value_vars.append(f"{stat}_{col_name}")

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=value_vars,
        var_name="stat_col",
        value_name="value"
    )

    df_long[["stat", "column_stats"]] = df_long["stat_col"].str.extract(r"^(avg|Q1|Q2|Q3)_(.+)$")

    df_final = (
        df_long
        .pivot_table(
            index=id_cols + ["column_stats"],
            columns="stat",
            values="value",
            aggfunc="first"
        )
        .reset_index()
    )

    df_final = df_final.rename(columns={
        "avg": "AVG",
        "Q1": "Q1",
        "Q2": "Q2",
        "Q3": "Q3"
    })

    return df_final



def filter_player_by_playrate(df: pd.DataFrame, playrate_col: str, threshold_percent: int = 7) -> pd.DataFrame:
    """
    Filters player data to keep only rows where playrate for a given column exceeds the threshold percentage.
    Returns: pd.DataFrame
    """
    df = df.copy()

    player_champ_counts = (
        df.groupby(["puuid", playrate_col])
          .size()
          .reset_index(name="count")
    )

    player_champ_counts["total_player_games"] = (
        player_champ_counts.groupby("puuid")["count"].transform("sum")
    )

    player_champ_counts["percent_games"] = (
        player_champ_counts["count"] / player_champ_counts["total_player_games"] * 100
    )

    valid_combinations = player_champ_counts[
        player_champ_counts["percent_games"] >= threshold_percent
    ][["puuid", playrate_col]]

    df_filtered = df.merge(valid_combinations, on=["puuid", playrate_col], how="inner")

    return df_filtered


def compute_multi_kill(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes average multi-kill statistics (double, triple, quadra, penta) by champion and position.
    Returns: pd.DataFrame
    """
    kill_type = ['doubleKills', 'tripleKills', 'quadraKills', 'pentaKills']

    result = (
        df
        .groupby(['championName', 'individualPosition', 'win'])[kill_type]
        .mean()
        .reset_index()
        .sort_values(['championName', 'individualPosition', 'win'], ascending=[False, False, False])
    )

    result[kill_type] = result[kill_type].round(4)

    return result

def compute_win_rate_by_champ(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates win rate and total games played per champion and position.
    Returns: pd.DataFrame
    """
    df = df.copy()
    winrate_df = (
        df.groupby(['championName', 'individualPosition'], as_index=False)
        .agg(
            win_rate=('win', 'mean'),  # moyenne des True/False
            total_games=('win', 'count')  # nombre total de parties
        )
    )

    winrate_df['win_rate'] = winrate_df['win_rate'] * 100
    return winrate_df

def surrender_analyses(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Analyzes surrender behavior, including early surrenders and distribution over time.
    Returns: tuple(pd.DataFrame of surrender distribution, dict of summary stats)
    """
    ff_df = df[df['gameEndedInSurrender'] == True].copy()

    ff_df['ff15'] = np.where(
        (ff_df['gameDuration'] >= 1) & (ff_df['gameDuration'] < 1200),
        True, False
    )

    ff_df =ff_df[ff_df['gameDuration'] >= 900]

    ff_df['minute_bins'] = np.floor(ff_df['gameDuration'] / 60).astype(int)

    total_game = df['gameId'].nunique()
    total_ff = len(ff_df)

    total_15ff = ff_df['ff15'].sum()  # True = 1, False = 0
    total_not_15ff = total_ff - total_15ff

    ff_dict = {
        'total_game': total_game,
        'total_ff': total_ff,
        'total_15ff': total_15ff,
        'total_not_15ff': total_not_15ff
    }

    minute_bins_df = (
        ff_df.groupby('minute_bins', as_index=False)
        .size()
        .rename(columns={'size': 'count'})
    )

    minute_bins_df['count'] = round((minute_bins_df['count'] / total_ff) * 100, 2)

    minute_bins_df = minute_bins_df.sort_values('minute_bins').reset_index(drop=True)

    return minute_bins_df, ff_dict


def compute_spell_per_champ_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates the total spell casts per champion across all games.
    Returns: pd.DataFrame
    """
    df = df.copy()

    return df.groupby('championName').agg({
        'spell1Casts': 'sum',
        'spell2Casts': 'sum',
        'spell3Casts': 'sum',
        'spell4Casts': 'sum',
    }).reset_index()

def compute_total_ping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'totalPings' column summing all ping-related columns for each game entry.
    Returns: pd.DataFrame
    """
    df = df.copy()
    df['totalPings'] = (
        df['allInPings'] +
        df['assistMePings'] +
        df['commandPings'] +
        df['enemyMissingPings'] +
        df['enemyVisionPings'] +
        df['holdPings'] +
        df['getBackPings'] +
        df['needVisionPings'] +
        df['onMyWayPings'] +
        df['pushPings'] +
        df['basicPings'] +
        df['visionClearedPings']
    )

    return df