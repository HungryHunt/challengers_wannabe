import boto3
import csv
import json
import logging
import numpy as np
import pandas as pd
import urllib3


from datetime import datetime
from botocore.exceptions import NoCredentialsError
from format_match_api_response import generate_player_line, PLAYER_LINE_SCHEMA
from format_df_to_body import *
from dataframe_computing import *
from ratelimit import limits, sleep_and_retry


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_routing_value(region: str) -> str:
    """Map platform region to routing value for Riot ID API"""
    routing_map = {
        'na1': 'americas',
        'br1': 'americas',
        'la1': 'americas',
        'la2': 'americas',
        'euw1': 'europe',
        'eun1': 'europe',
        'tr1': 'europe',
        'ru': 'europe',
        'kr': 'asia',
        'jp1': 'asia',
        'oc1': 'sea',
        'ph2': 'sea',
        'sg2': 'sea',
        'th2': 'sea',
        'tw2': 'sea',
        'vn2': 'sea'
    }
    return routing_map.get(region, 'europe')

class UnauthorizedError(Exception):
    def __init__(self, message):
        super().__init__(message)


@sleep_and_retry
@limits(calls=99, period=120)
def send_get_api_request(url: str, request_dict: dict) -> tuple[dict, int]:
    """
    Sends a GET request to the given URL and returns the decoded JSON response with the HTTP status code.
    Returns: tuple[dict, int]
    """

    logger.info(f"[INFO] - {datetime.now()} : request sent to {url}")
    api_response = request_dict['http'].request('GET', url, headers=request_dict['headers'])
    api_response_decoded = json.loads(api_response.data.decode("utf-8"))

    if api_response.status == 401:
        raise UnauthorizedError(f'Auth token is Forbidden : {api_response.status} - {api_response_decoded["status"]["message"]}')

    return api_response_decoded, api_response.status



def get_account_puuid_from_name_and_tag(game_name: str, tag_line: str, server: str, request_dict: dict) -> str:
    """
    Retrieves the player's unique Riot PUUID using their Riot ID and tag line.
    Returns: str
    """
    account_url = f"https://{get_routing_value(server)}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
    response, status_code = send_get_api_request(account_url, request_dict)

    if status_code == 404:
        raise Exception(f'[GET ACCOUNT] - No results found for player with riot id {game_name}#{tag_line}"')
    elif status_code != 200:
        raise Exception(f'[GET ACCOUNT] - Status code {status_code} - {response}')

    return response['puuid']


def get_current_ranked_info(puuid: str, server: str, request_dict: dict) -> dict | None:
    """
    Fetches the player's ranked information (tier, division, LP, wins, losses) or returns None if not ranked.
    Returns: dict | None
    """
    league_url = f"https://{server}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
    response, status_code = send_get_api_request(league_url, request_dict)

    if status_code == 404:
        raise Exception(f'[GET RANK] - No results found for player with puuid {puuid}"')
    elif status_code != 200:
        raise Exception(f'[GET RANK] - Status code {status_code} - {response}')

    solo_queue_rank = None
    for mode_rank in response:
        if mode_rank['queueType'] == 'RANKED_SOLO_5x5':
            solo_queue_rank = mode_rank

    if solo_queue_rank is None:
        return None

    return {
        'tier': solo_queue_rank['tier'],
        'division':solo_queue_rank['rank'],
        'lp':solo_queue_rank['leaguePoints'],
        'wins':solo_queue_rank['wins'],
        'losses':solo_queue_rank['losses']
    }


def send_players_data_to_bedrock_for_advices(df: pd.DataFrame, tier: str, aws_session: boto3.Session) -> str | None:
    """
    Sends player data to an AWS Bedrock flow to receive performance advice as text.
    Returns: str | None
    """
    division = f"The playere division is {tier}.\n"
    query = df.apply(transform_row_to_string, axis=1)
    query = str(query.tolist())[1:-1].replace("'", "")
    query = division + query

    bedrock = aws_session.client('bedrock-agent-runtime')

    bedrock_agent = aws_session.client('bedrock-agent')

    response = bedrock_agent.list_flows(maxResults=100)
    flow_id = response.get('flowSummaries', [])[0]['id']
    alias_response = bedrock_agent.list_flow_aliases(flowIdentifier=flow_id)
    highest_version = -1
    alias_id = None
    for alias in alias_response['flowAliasSummaries']:
        if alias['routingConfiguration'][0]['flowVersion'] == "DRAFT":
            continue
        elif int(alias['routingConfiguration'][0]['flowVersion']) > highest_version:
            highest_version = alias['routingConfiguration'][0]['flowVersion']
            alias_id = alias['id']

    response = bedrock.invoke_flow(
        flowIdentifier=flow_id,
        flowAliasIdentifier=alias_id,
        inputs=[
            {
                "nodeName": "FlowInputNode",
                "nodeOutputName": "document",
                "content": {
                    "document": query
                }
            }
        ]
    )
    output_event = {}
    for stream_event in response["responseStream"]:
        if "flowOutputEvent" in stream_event:
            output_event = stream_event["flowOutputEvent"]
        elif "flowErrorEvent" in stream_event:
            return None
        elif "flowCompletionEvent" in stream_event:
            logger.info("Flow terminé :", stream_event["flowCompletionEvent"])

    return output_event.get('content', {}).get('document')


def retrieve_api_key(aws_session: boto3.Session) -> str :
    """
    Retrieves the Riot API key stored in AWS SSM Parameter Store or raise Exception if unavailable.
    Returns: str
    """
    try:
        ssm = aws_session.client('ssm')
        response = ssm.get_parameter(
            Name='riot_API_key'
        )
        api_key = response['Parameter']['Value']
        return api_key

    except NoCredentialsError:
        logger.error("[GET PARAMETER] - Riot API Key not found in SSM.")
        raise Exception("[GET PARAMETER] - Riot API Key not found in SSM.")

    except Exception as e:
        logger.error(f"[GET PARAMETER] - Unexpected error raised : {e}")
        raise e


def get_player_year_history(puuid: str, request_dict: dict) -> list[dict]:
    """
    Retrieves the player's yearly match history and compiles a list of match summaries.
    Returns: list[dict]
    """
    match_history_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime=1736409600&start=[PAGE]&count=100"
    match_replay_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/[MATCH_ID]"

    match_ids_decoded = None
    page = 0

    games_recap = []

    while match_ids_decoded != []:
        try:
            match_ids_decoded, status_code = send_get_api_request(
                match_history_url.replace("[PAGE]", str(page)),
                request_dict)

            if status_code != 200:
                raise Exception(f'[GET ACCOUNT] - Status code {status_code} - {match_ids_decoded}')

            for match_id in match_ids_decoded:
                match_history_decoded, match_status_code = send_get_api_request(
                    match_replay_url.replace("[MATCH_ID]", str(match_id)),
                    request_dict)

                if match_status_code == 404:
                    logger.info(f'[GET MATCHES] - Data not found for match \"{match_id}\", and puuid :{puuid} "')
                    continue
                elif match_status_code != 200:
                    logger.error(f'[GET MATCHES] - Status code error for match \"{match_id}\", and puuid :{puuid} "')

                try:
                    if match_history_decoded['info']['queueId'] in [420, 400]:
                        player_match_history = generate_player_line(match_history_decoded, puuid)
                        games_recap.append(player_match_history)
                except Exception as e:
                    logger.error(f'[GET MATCHES] - Code Error - {e}')
                    continue

            page += 100

        except UnauthorizedError as u:
            raise UnauthorizedError(u)
        except Exception as e:
            raise e

    return games_recap


def cast_dataframe_to_dict(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Casts DataFrame columns to the provided schema, including boolean normalization.
    Returns: pandas.DataFrame
    """
    df = df.copy()
    bool_cols = [c for c, t in schema.items() if t == bool]
    df[bool_cols] = df[bool_cols].apply(
        lambda col: col.str.strip().str.lower().map({'true': True, 'false': False}))
    return df.astype(schema)


def convert_csv_to_df(path: str, schema: dict) -> pd.DataFrame:
    """
    Converts a CSV file into a DataFrame following the specified schema.
    Returns: pandas.DataFrame
    """
    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        referential = [line for line in reader]

    referential_df = pd.DataFrame(referential, columns=schema.keys())
    return cast_dataframe_to_dict(referential_df, schema)

def get_referential_dataset() -> pd.DataFrame:
    """
    Loads and returns the referential dataset containing champion performance percentiles.
    Returns: pandas.DataFrame
    """
    schema = {
        'ref_championName': 'object',
        'ref_individualPosition': 'object',
        'ref_win': bool,
        'ref_column_stats': 'object',
        'ref_AVG': 'float64',
        'ref_Q1': 'float64',
        'ref_Q2': 'float64',
        'ref_Q3': 'float64'
    }
    return convert_csv_to_df("data/average_percentiles.csv", schema)


def get_duration_referential_dataset() -> pd.DataFrame:
    """
    Loads and returns the referential dataset for game duration statistics.
    Returns: pandas.DataFrame
    """
    schema = {
        'average':'float64',
        'q1':'float64',
        'median':'float64',
        'q3':'float64'
    }

    return convert_csv_to_df("data/duration.csv", schema)


def get_ff_mins_referential_dataset() -> pd.DataFrame:
    """
    Loads and returns the referential dataset for forfeit counts per minute.
    Returns: pandas.DataFrame
    """
    schema = {
        'minute_bins': 'int64',
        'count': 'float64'
    }

    return convert_csv_to_df("data/ff_per_mins.csv", schema)


def get_ff_stats_referential_dataset() -> pd.DataFrame:
    """
    Loads and returns the referential dataset containing overall forfeit statistics.
    Returns: pandas.DataFrame
    """
    schema = {
        'percents_ff': 'float64',
        'percents_pre_20_ff': 'float64',
        'percents_post_20_ff': 'float64'
    }
    return convert_csv_to_df("data/ff_stats.csv", schema)


def get_kill_referential_dataset() -> pd.DataFrame:
    """
    Loads and returns the referential dataset containing average multi-kill statistics.
    Returns: pandas.DataFrame
    """
    schema = {
        'ref_championName': 'object',
        'ref_individualPosition': 'object',
        'ref_win': bool,
        'ref_doubleKills' : 'float64',
        'ref_tripleKills' : 'float64',
        'ref_quadraKills' : 'float64',
        'ref_pentaKills' : 'float64'
    }

    return convert_csv_to_df("data/multi_kills.csv", schema)


def analyze_game_history(game_history: list[dict]) -> dict[str, object]:
    """
    Analyzes a player's full game history to produce detailed gameplay statistics and performance summaries.
    Returns: dict[str, object]
    """
    classic_cols = [
        "kda",
        'kills',
        'deaths',
        'assists',
        "physicalDamageDealtToChampions",
        "magicDamageDealtToChampions",
        "totalDamageDealtToChampions",
        'wardsPlaced',
        'wardsKilled',
        'visionWardsBoughtInGame',
        'visionScore'
    ]

    col_per_mins = [
        "physicalDamageDealtToChampions",
        "magicDamageDealtToChampions",
        "totalDamageDealtToChampions",
        "damageDealtToTurrets",
        "damageDealtToObjectives",
        "allInPings",
        "assistMePings",
        "commandPings",
        "enemyMissingPings",
        "enemyVisionPings",
        "holdPings",
        "getBackPings",
        "needVisionPings",
        "onMyWayPings",
        "pushPings",
        "basicPings",
        "visionClearedPings"
    ]

    player_df = pd.DataFrame(game_history, columns=PLAYER_LINE_SCHEMA.keys())
    player_df = cast_dataframe_to_dict(player_df, PLAYER_LINE_SCHEMA)

    referential_df = get_referential_dataset()
    global_referential_df = referential_df[referential_df['ref_championName'] == 'GLOBAL']\
        .rename(columns={
            'ref_championName': 'global_championName',
            'ref_individualPosition': 'global_individualPosition',
            'ref_win': 'global_win',
            'ref_column_stats': 'global_column_stats',
            'ref_Q1': 'global_Q1',
            'ref_Q2': 'global_Q2',
            'ref_Q3': 'global_Q3',
            'ref_AVG': 'global_AVG'
    })
    kill_referential_df = get_kill_referential_dataset()


    player_df['kda'] = np.where(
        player_df['deaths'] != 0,
        (player_df['kills'] + player_df['assists']) / player_df['deaths'],
        player_df['kills'] + player_df['assists']
    )
    player_df = compute_total_ping(player_df)

    ranked_games = player_df[player_df["queueId"] == 420]
    draft_games = player_df[player_df["queueId"] == 400] # TO DO

    if len(ranked_games) == 0:
        # Edge case for the moment cause I might not have time to implement draft games
        ranked_games = draft_games


    champ_filtered_ranked_games = filter_player_by_playrate(ranked_games, 'championName')

    stats_df = compute_stats_from_df(champ_filtered_ranked_games, classic_cols, col_per_mins)
    multi_kill_df = compute_multi_kill(champ_filtered_ranked_games)

    duration_df = compute_game_duration_df(ranked_games)
    ff_df, surrender_dict = surrender_analyses(ranked_games)
    stats_enriched_df = merge_stats_df(stats_df, referential_df, global_referential_df)
    multi_kill_enriched_df = merge_multi_kill_df(multi_kill_df, kill_referential_df)

    stats_highlights = compute_player_highlights(stats_enriched_df, ['Q1', 'Q2', 'Q3', 'AVG'], "ref_")

    win_rate_df = compute_win_rate_by_champ(champ_filtered_ranked_games)

    spells_cols = ['spell1Casts', 'spell2Casts', 'spell3Casts', 'spell4Casts', 'summoner1Casts', 'summoner2Casts']
    spells_casted = player_df[spells_cols].sum(axis=0).to_dict()

    return {
        'durations': duration_df,
        'ff': ff_df,
        'surrender_stat': surrender_dict,
        'player_stats': stats_highlights,
        'multi_kill_stats': multi_kill_enriched_df,
        'win_rate' : win_rate_df,
        'spells': spells_casted
    }


def compute_player_highlights(df: pd.DataFrame, cols: list[str], prefix: str) -> pd.DataFrame:
    """
    Identifies player performances significantly above or below reference percentiles.
    Returns: pandas.DataFrame
    """
    df = df.copy()
    df_clean = df.dropna()

    ahead = (df_clean[cols].values > df_clean[[f'{prefix}{col}' for col in cols]].values).all(axis=1)
    below = (df_clean[cols].values < df_clean[[f'{prefix}{col}' for col in cols]].values).all(axis=1)
    return df_clean[ahead | below]


def merge_multi_kill_df(df: pd.DataFrame, referential_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges player multi-kill data with referential statistics for comparative analysis.
    Returns: pandas.DataFrame
    """
    result = pd.merge(
        df,
        referential_df,
        left_on=['championName', 'individualPosition', 'win'],
        right_on=['ref_championName', 'ref_individualPosition', 'ref_win'],
        how='left'
    )

    return result[['championName', 'individualPosition', 'win',
                   'doubleKills', 'tripleKills', 'quadraKills', 'pentaKills',
                   'ref_doubleKills', 'ref_tripleKills', 'ref_quadraKills', 'ref_pentaKills',]]


def merge_stats_df(df: pd.DataFrame, referential_df: pd.DataFrame, global_referential_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges player performance statistics with both champion-specific and global referential datasets.
    Returns: pandas.DataFrame
    """
    result = pd.merge(
        df,
        referential_df,
        left_on=['championName', 'individualPosition', 'win', 'column_stats'],
        right_on=['ref_championName', 'ref_individualPosition', 'ref_win', 'ref_column_stats'],
        how='left'
    )

    result = pd.merge(
        result,
        global_referential_df,
        left_on=['individualPosition', 'win', 'column_stats'],
        right_on=['global_individualPosition', 'global_win', 'global_column_stats'],
        how='left'
    )

    return result[['championName', 'individualPosition', 'win', 'column_stats',
                     'Q1', 'Q2', 'Q3', 'AVG',
                     'ref_Q1', 'ref_Q2', 'ref_Q3', 'ref_AVG',
                     'global_Q1', 'global_Q2', 'global_Q3', 'global_AVG']]


def compute_stats_from_df(df: pd.DataFrame, classic_cols: list[str], col_per_mins: list[str]) -> pd.DataFrame:
    """
    Computes aggregated performance metrics per champion and per minute from raw match data.
    Returns: pandas.DataFrame
    """
    df_copy = df.copy()
    per_game_df = compute_avg_percentile(columns=classic_cols, df=df_copy, group_by_champ=True)
    per_game_df = split_multi_col_to_save_format(classic_cols, per_game_df, group_by_champ=True)

    per_min_cols_rename = {col_name: f'{col_name}PerMins' for col_name in col_per_mins}

    per_min_df = compute_cols_per_minutes(col_per_mins, df_copy).rename(columns=per_min_cols_rename)
    per_min_df = compute_avg_percentile(columns=per_min_cols_rename.values(), df=per_min_df, group_by_champ=True)
    per_min_df = split_multi_col_to_save_format(per_min_cols_rename.values(), per_min_df, group_by_champ=True)

    return pd.concat([per_game_df, per_min_df])


def compute_game_duration_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates average game duration statistics and percentiles based on filtered ranked matches.
    Returns: pandas.DataFrame
    """
    df_copy = df.copy()

    lane_filtered_ranked_games = filter_player_by_playrate(df_copy, 'individualPosition', threshold_percent=20)
    lane_filtered_ranked_games['gameDuration'] = round((lane_filtered_ranked_games["gameDuration"] / 60), 3)
    duration_df = compute_avg_percentile(['gameDuration'], lane_filtered_ranked_games)

    return split_multi_col_to_save_format(['gameDuration'], duration_df)


def prepare_data_for_response(stats_dict: dict[str, object], player_name: str, player_tag: str) -> dict[str, object]:
    """
    Formats analyzed player data into a structured response with key performance highlights.
    Returns: dict[str, object]
    """
    key_highlights = format_top_champions(stats_dict['win_rate'])
    pings = format_pings(stats_dict['player_stats'])
    kda = format_kda(stats_dict['player_stats'])
    damage = format_damages(stats_dict['player_stats'])
    multi_kills = format_multi_kill(stats_dict['multi_kill_stats'])
    duration = format_duration(stats_dict['durations'], get_duration_referential_dataset())
    surrenders = format_ff(
        stats_dict['ff'],
        stats_dict['surrender_stat'],
        get_ff_mins_referential_dataset(),
        get_ff_stats_referential_dataset()
    )

    return {
        "username": player_name,
        "tag": player_tag,
        "keyHighlights": key_highlights,
        "pings": pings,
        "kda": kda,
        "damage": damage,
        "multiKills": multi_kills,
        "gameDuration": duration,
        "surrenders": surrenders
    }

def lambda_handler(event: dict, context: object) -> dict[str, object]:
    """
    Main AWS Lambda entry point that processes player data requests, performs analysis, and returns a JSON response.
    Returns: dict[str, object]
    """
    params = event.get('queryStringParameters', {})

    player_name = params.get('username', None)
    player_tag = params.get('tag', None)
    server = params.get('region', None)

    if (player_name is None or
            player_tag is None or
            server is None):
        return {
            'statusCode': 400,
            'body': {'Parameters missing' : f'Mandatory parameters are missing.\nPlease provide "username" and "tag"\n{params}'}
        }

    # TO REMOVE, only here for POC
    if player_name not in ["Happy Hunt", "Hungry Hunt"]:
        return  {
            'statusCode': 400,
            'body': {'Unsupported' : f'This lambda is in POC phase, only few users are accepted.\nPlease provide "username" and "tag"\n{params}'}
        }

    session = boto3.Session(region_name="us-east-1")

    api_key = retrieve_api_key(session)

    request_object = {
        'http': urllib3.PoolManager(),
        'headers': {'X-Riot-Token': api_key}
    }
    account_puuid = get_account_puuid_from_name_and_tag(player_name, player_tag, server, request_object)
    player_info = get_current_ranked_info(account_puuid, server, request_object)



    if False:
        if player_info is None:
            legger.info("[RETRIEVE RANKED GAMES] - No ranked game, we switch to draft")

        player_year_games = get_player_year_history(account_puuid, request_object)

    # TO REMOVE, only here for POC
    with open(f"./poc_games/{player_name.replace(' ', '_')}.csv", newline='', encoding='utf-8') as csvfile:
        lecteur = csv.reader(csvfile)
        player_year_games = [ligne for ligne in lecteur]


    player_analysis = analyze_game_history(player_year_games)

    bedrock_advices = send_players_data_to_bedrock_for_advices(player_analysis['player_stats'], player_info['tier'], session)
    tips_body = format_tips_from_bedrock(bedrock_advices)

    body = {}
    try:
        body = prepare_data_for_response(player_analysis, player_name, player_tag)
        body['spells_pressed'] = player_analysis['spells']
        body['tips'] = tips_body
    except Exception as e:
        logger.error(e)
        logging.error(f"Failed to interact with AWS Bedrock.\n{e}")

    print(json.dumps(body, indent=4))

    return {
        'statusCode': 200,
        'body': json.dumps(body)
    }



#if __name__ == "__main__":
#    pd.set_option('display.max_rows', None)
#    pd.set_option('display.max_columns', None)
#    pd.set_option('display.width', None)
#    pd.set_option('display.max_colwidth', None)
#
#    event = {
#        "queryStringParameters": {
#            "username": "Hungry Hunt",
#            "tag": "EUW",
#            "region": "euw1"
#        }
#    }
#    result = lambda_handler(event, None)
