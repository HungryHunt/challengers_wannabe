import pandas as pd

def format_top_champions(df: pd.DataFrame) -> dict[str, dict[str, list[dict]]]:
    """
    Formats the top champions with lane, win rate, and total games into a nested dictionary structure.
    Returns: dict with 'keyHighlights' containing a list of top champions
    """
    top_champions = [
        {
            "champion": row["championName"],
            "lane": row["individualPosition"] if row["individualPosition"] != "UTILITY" else "SUPPORT",
            "winRate": float(row["win_rate"]),
            "totalGames": int(row["total_games"])
        }
        for _, row in df.iterrows()
    ]

    return {
        "keyHighlights": {
            "topChampions": top_champions
        }
    }


def transform_df_to_body(df: pd.DataFrame, group_by_cols: list[str], stats_col_mapping: dict[str, str], ref_prefix: str) -> list[dict]:
    """
    Transforms a grouped DataFrame into a structured list of dictionaries comparing player stats to reference stats.
    Returns: list[dict]
    """
    column_stats_value = []
    grouped = df.groupby(group_by_cols)
    for key, group in grouped:
        if len(group_by_cols) == 3:
            champion, lane, _ = key
        else:
            champion, lane = key

        obj = {
            "champion": champion,
            "lane": lane.lower() if lane != 'UTILITY' else 'support' ,
            "win": {},
            "loose": {}
        }
        has_nan = False

        for _, row in group.iterrows():
            if row.isna().any():
                has_nan = True
                continue

            stats = {
                "playerStats": {
                    key : row[value] for key, value in stats_col_mapping.items()
                },
                "challengerStats": {
                    key : round(row[f'{ref_prefix}{value}'], 2) for key, value in stats_col_mapping.items()
                }
            }

            if row['win']:
                obj['win'] = stats
            else:
                obj['loose'] = stats

        if obj["win"] == {}:
            del obj['win']
        if obj["loose"] == {}:
            del obj['loose']

        if not has_nan:
            column_stats_value.append(obj)

    return column_stats_value


def format_pings(df: pd.DataFrame) -> dict[str, list[dict]]:
    """
    Formats ping-related statistics per champion and lane into a dictionary for output.
    Returns: dict with each ping type mapped to structured records
    """
    to_keep = [
        "totalPings",
        "allInPingsPerMins",
        "assistMePingsPerMins",
        "commandPingsPerMins",
        "enemyMissingPingsPerMins",
        "enemyVisionPingsPerMins",
        "holdPingsPerMins",
        "getBackPingsPerMins",
        "needVisionPingsPerMins",
        "onMyWayPingsPerMins",
        "pushPingsPerMins",
        "basicPingsPerMins",
        "visionClearedPingsPerMins"
    ]
    group_by_cols = ['championName', 'individualPosition', 'column_stats']
    col_mapping = {
        'average': 'AVG',
        'q1': 'Q1',
        'median': 'Q2',
        'q3': 'Q3',
    }

    pings = {}

    for value in to_keep:
        filtered_df = df[df['column_stats'] == value]
        if filtered_df.empty:
            continue
        pings[value] = transform_df_to_body(filtered_df, group_by_cols, col_mapping, "ref_")

    return pings


def format_kda(df: pd.DataFrame) -> dict[str, list[dict]]:
    """
    Formats KDA-related statistics per champion and lane into a dictionary for output.
    Returns: dict with KDA metrics mapped to structured records
    """
    to_keep = [
        "kda",
        "kills",
        "deaths",
        "assists",
    ]
    group_by_cols = ['championName', 'individualPosition', 'column_stats']
    col_mapping = {
        'average': 'AVG',
        'q1': 'Q1',
        'median': 'Q2',
        'q3': 'Q3',
    }

    kda = {}

    for value in to_keep:
        filtered_df = df[df['column_stats'] == value]
        if filtered_df.empty:
            continue
        kda[value] = transform_df_to_body(filtered_df, group_by_cols, col_mapping, "ref_")

    return kda


def format_damages(df: pd.DataFrame) -> dict[str, list[dict]]:
    """
    Formats damage-related statistics per champion and lane, renaming keys for clarity.  
    Returns: dict mapping damage types to structured records
    """
    to_keep = {
        "damageDealtToObjectivesPerMins" : "damageDealtToObjectives",
        "damageDealtToTurretsPerMins" : "damageDealtToTowers",
        "magicDamageDealtToChampionsPerMins" : "magicalDamageToChampions",
        "physicalDamageDealtToChampionsPerMins" : "physicalDamageToChampions",
        "totalDamageDealtToChampionsPerMins" : "totalDamageToChampions"
    }
    group_by_cols = ['championName', 'individualPosition', 'column_stats']
    col_mapping = {
        'average': 'AVG',
        'q1': 'Q1',
        'median': 'Q2',
        'q3': 'Q3',
    }

    damages = {}

    for value in to_keep.keys():
        filtered_df = df[df['column_stats'] == value]
        if filtered_df.empty:
            continue
        damages[value] = transform_df_to_body(filtered_df, group_by_cols, col_mapping, "ref_")

    damages_renamed = {to_keep.get(k, k): v for k, v in damages.items()}

    return damages_renamed


def format_multi_kill(df: pd.DataFrame) -> list[dict]:
    """
    Formats multi-kill statistics per champion and lane into a list of structured records.
    Returns: list[dict]
    """
    group_by_cols = ['championName', 'individualPosition']
    col_mapping = {
        'doubleKills': 'doubleKills',
        'tripleKills': 'tripleKills',
        'quadraKills': 'quadraKills',
        'pentaKills': 'pentaKills',
    }
    return transform_df_to_body(df, group_by_cols, col_mapping, "ref_")


def format_duration(df: pd.DataFrame, duration_df: pd.DataFrame) -> dict[str, dict]:
    """
    Formats game duration statistics for both player and challenger into a dictionary.
    Returns: dict with 'player_duration' and 'chall_duration' lists
    """
    df = df.copy()
    df['lane'] = df['individualPosition'].str.lower()
    df['lane'] = df['lane'].replace('utility', 'support')

    df_formatted = df[['lane', 'win', 'Q1', 'Q2', 'Q3', 'AVG']].rename(
        columns={'Q1': 'q1', 'Q2': 'median', 'Q3': 'q3', 'AVG': 'average'}
    )

    player_duration = df_formatted.to_dict(orient='records')
    chall_duration = duration_df.to_dict(orient='records')[0]

    return {
        'player_duration': player_duration,
        'chall_duration': chall_duration,
    }


def transform_row_to_string(row: pd.Series) -> str:
    """
    Transforms a row of player and challenger stats into a formatted string representation.
    Returns: str
    """
    player_values = f"{row['AVG']}, {row['Q1']}, {row['Q2']}, {row['Q3']}"

    challenger_values = f"{row['ref_AVG']}, {row['ref_Q1']}, {row['ref_Q2']}, {row['ref_Q3']}"

    result = (f"[{row['column_stats']}: championName{{{row['championName']}}}, "
              f"individualPosition{{{row['individualPosition']}}}, "
              f"win{{{row['win']}}}, "
              f"player{{{player_values}}}, "
              f"challenger{{{challenger_values}}}]")

    return result


def format_ff(ff_df: pd.DataFrame, ff_dict: dict, chall_ff: pd.DataFrame, chall_ff_stats: pd.DataFrame) -> dict[str, dict]:
    """
    Formats surrender (forfeit) statistics for both player and challenger into a structured dictionary.  
    Returns: dict with 'playerStats' and 'challengerStats'
    """
    chall_ff_stats_dict = chall_ff_stats.to_dict(orient='records')[0]
    return {
        'playerStats': {
            "totalGames": ff_dict['total_game'],
            "totalGamesSurrendered": ff_dict['total_ff'],
            "percentageGamesSurrendered": round((ff_dict['total_ff'] / ff_dict['total_game']) * 100, 2),
            "percentageSurrenders15To20": round((ff_dict['total_15ff'] / ff_dict['total_ff']) * 100, 2),
            "percentageSurrendersAfter20": round((ff_dict['total_not_15ff'] / ff_dict['total_ff']) * 100, 2),
            "SurrenderCurve": ff_df.values.tolist()
        },
        'challengerStats': {
            "percentageGamesSurrendered": chall_ff_stats_dict['percents_ff'],
            "percentageSurrenders15To20": chall_ff_stats_dict['percents_pre_20_ff'],
            "percentageSurrendersAfter20": chall_ff_stats_dict['percents_post_20_ff'],
            "SurrenderCurve": chall_ff.values.tolist()
        }
    }


def format_tips_from_bedrock(tips_str: str) -> dict[str, str]:
    """
    Parses a multi-line string of tips into a dictionary with sequential keys.
    Returns: dict mapping 'tips1', 'tips2', etc. to each tip
    """
    result = {}
    tips = 1
    for line in tips_str.split("\n"):
        if line is None or len(line) == 0:
            continue
        result[f'tips{tips}'] = line.replace('*', '')[1:]
        tips += 1
    return result