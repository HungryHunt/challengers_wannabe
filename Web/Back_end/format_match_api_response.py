PLAYER_LINE_SCHEMA = {
    'gameCreation': str,
    'gameDuration': int,
    'gameId': int,
    'gameVersion': str,
    'platformId': str,
    'queueId': int,
    'team_1_teamId': int,
    'team_1_win': bool,
    'team_1_total_tower_killed': int,
    'team_1_first_tower': bool,
    'team_1_atakhan': bool,
    'team_1_total_baron_killed': int,
    'team_1_total_dragon_killed': int,
    'team_1_total_grubs_killed': int,
    'team_1_total_herald_killed': int,
    'team_2_teamId': int,
    'team_2_win': bool,
    'team_2_total_tower_killed': int,
    'team_2_first_tower': bool,
    'team_2_atakhan': bool,
    'team_2_total_baron_killed': int,
    'team_2_total_dragon_killed': int,
    'team_2_total_grubs_killed': int,
    'team_2_total_herald_killed': int,
    'puuid': str,
    'riotIdGameName': str,
    'riotIdTagline': str,
    'summonerId': str,
    'summonerLevel': int,
    'teamId': int,
    'participantId': int,
    'win': bool,
    'allInPings': int,
    'assistMePings': int,
    'commandPings': int,
    'enemyMissingPings': int,
    'enemyVisionPings': int,
    'holdPings': int,
    'getBackPings': int,
    'needVisionPings': int,
    'onMyWayPings': int,
    'pushPings': int,
    'basicPings': int,
    'visionClearedPings': int,
    'champExperience': int,
    'champLevel': int,
    'championId': int,
    'championName': str,
    'kills': int,
    'deaths': int,
    'assists': int,
    'individualPosition': str,
    'lane': str,
    'neutralMinionsKilled': int,
    'damageDealtToBuildings': int,
    'damageDealtToObjectives': int,
    'damageDealtToTurrets': int,
    'turretKills': int,
    'inhibitorKills': int,
    'wardsPlaced': int,
    'wardsKilled': int,
    'visionWardsBoughtInGame': int,
    'visionScore': int,
    'teamEarlySurrendered': bool,
    'gameEndedInSurrender': bool,
    'gameEndedInEarlySurrender': bool,
    'doubleKills': int,
    'tripleKills': int,
    'quadraKills': int,
    'pentaKills': int,
    'spell1Casts': int,
    'spell2Casts': int,
    'spell3Casts': int,
    'spell4Casts': int,
    'summoner1Id': int,
    'summoner2Id': int,
    'summoner1Casts': int,
    'summoner2Casts': int,
    'physicalDamageDealtToChampions': int,
    'magicDamageDealtToChampions': int,
    'totalDamageDealtToChampions': int,
    'dragonKills': int,
    'totalAllyJungleMinionsKilled': int,
    'totalEnemyJungleMinionsKilled': int,
    'totalMinionsKilled': int
}


def get_general_info(api_response: dict) -> list[str]:
    """
    Extracts general match information such as game ID, duration, version, platform, and queue ID.
    Returns: list of strings
    """
    return [
            str(api_response["info"]['gameCreation']),
            str(api_response["info"]['gameDuration']),
            str(api_response["info"]['gameId']),
            str(api_response["info"]['gameVersion']),
            str(api_response["info"]['platformId']) ,
            str(api_response["info"]['queueId'])
    ]


def get_partiticpants(api_response: dict) -> list[str]:
    """
    Extracts the participant PUUIDs from a match metadata object.
    Returns: list of strings
    """
    return [
            str(api_response["metadata"]['participants'][0]),
            str(api_response["metadata"]['participants'][1]),
            str(api_response["metadata"]['participants'][2]),
            str(api_response["metadata"]['participants'][3]),
            str(api_response["metadata"]['participants'][4]),
            str(api_response["metadata"]['participants'][5]),
            str(api_response["metadata"]['participants'][6]),
            str(api_response["metadata"]['participants'][7]),
            str(api_response["metadata"]['participants'][8]),
            str(api_response["metadata"]['participants'][9])
    ]


def teams_general_info(api_response: dict) -> list[str]:
    """
    Extracts summarized team-level statistics from match info, including objectives and wins.
    Returns: list of strings
    """
    teams = []
    for team in api_response["info"]['teams']:
        team_info = [
                str(team['teamId']),
                str(team['win']),
                str(team['objectives']['tower']['kills']),
                str(team['objectives']['tower']['first']),
                str(team['objectives']['atakhan']['first']),
                str(team['objectives']['baron']['kills']),
                str(team['objectives']['dragon']['kills']),
                str(team['objectives']['horde']['kills']),
                str(team['objectives']['riftHerald']['kills']),
        ]

        teams += team_info

    return teams


def get_player_info(participant_api_response: dict) -> list[str]:
    """
    Extracts detailed statistics for a single participant from the match API response.
    Returns: list of strings
    """
    return [
            str(participant_api_response['puuid']),
            str(participant_api_response['riotIdGameName']),
            str(participant_api_response['riotIdTagline']),
            str(participant_api_response['summonerId']),
            str(participant_api_response['summonerLevel']),
            str(participant_api_response['teamId']),
            str(participant_api_response['participantId']),
            str(participant_api_response['win']),
            str(participant_api_response['allInPings']),
            str(participant_api_response['assistMePings']),
            str(participant_api_response['commandPings']),
            str(participant_api_response['enemyMissingPings']),
            str(participant_api_response['enemyVisionPings']),
            str(participant_api_response['holdPings']),
            str(participant_api_response['getBackPings']),
            str(participant_api_response['needVisionPings']),
            str(participant_api_response['onMyWayPings']),
            str(participant_api_response['pushPings']),
            str(participant_api_response['basicPings']),
            str(participant_api_response['visionClearedPings']),
            str(participant_api_response['champExperience']),
            str(participant_api_response['champLevel']),
            str(participant_api_response['championId']),
            str(participant_api_response['championName']),
            str(participant_api_response['kills']),
            str(participant_api_response['deaths']),
            str(participant_api_response['assists']),
            str(participant_api_response['individualPosition']),
            str(participant_api_response['lane']),
            str(participant_api_response['neutralMinionsKilled']),
            str(participant_api_response['damageDealtToBuildings']),
            str(participant_api_response['damageDealtToObjectives']),
            str(participant_api_response['damageDealtToTurrets']),
            str(participant_api_response['turretKills']),
            str(participant_api_response['inhibitorKills']),
            str(participant_api_response['wardsPlaced']),
            str(participant_api_response['wardsKilled']),
            str(participant_api_response['visionWardsBoughtInGame']),
            str(participant_api_response['visionScore']),
            str(participant_api_response['teamEarlySurrendered']),
            str(participant_api_response['gameEndedInSurrender']),
            str(participant_api_response['gameEndedInEarlySurrender']),
            str(participant_api_response['doubleKills']),
            str(participant_api_response['tripleKills']),
            str(participant_api_response['quadraKills']),
            str(participant_api_response['pentaKills']),
            str(participant_api_response['spell1Casts']),
            str(participant_api_response['spell2Casts']),
            str(participant_api_response['spell3Casts']),
            str(participant_api_response['spell4Casts']),
            str(participant_api_response['summoner1Id']),
            str(participant_api_response['summoner2Id']),
            str(participant_api_response['summoner1Casts']),
            str(participant_api_response['summoner2Casts']),
            str(participant_api_response['physicalDamageDealtToChampions']),
            str(participant_api_response['magicDamageDealtToChampions']),
            str(participant_api_response['totalDamageDealtToChampions']),
            str(participant_api_response['dragonKills']),
            str(participant_api_response['totalAllyJungleMinionsKilled']),
            str(participant_api_response['totalEnemyJungleMinionsKilled']),
            str(participant_api_response['totalMinionsKilled'])
        ]


def generate_csv_line_from_match_api_response(api_response: dict) -> str:
    """
    Generates a semicolon-separated string summarizing all participants, match info, teams, and player stats.
    Returns: str
    """
    participants_puuids = get_partiticpants(api_response)
    general_info = get_general_info(api_response)
    teams = teams_general_info(api_response)

    players_summary = []
    for player in api_response["info"]['participants']:
        player_info = get_player_info(player)
        players_summary += player_info

    return ";".join(participants_puuids + general_info + teams + players_summary)


def generate_player_line(api_response: dict, player_puuid: str) -> list[str]:
    """
    Generates a detailed line of match data for a specific player, including general match info, teams, and player stats.
    Returns: list of strings
    """
    general_info = get_general_info(api_response)
    teams = teams_general_info(api_response)

    players_summary = []
    for player in api_response["info"]['participants']:
        if player_puuid == player["puuid"]:
            players_summary = get_player_info(player)

    return general_info + teams + players_summary


