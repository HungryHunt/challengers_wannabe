import os

import urllib3
import json

from format_match_api_response import generate_csv_line_from_match_api_response
from datetime import datetime
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry
from urllib3 import PoolManager
from pybloom_live import BloomFilter


# API link : https://developer.riotgames.com/apis
# Test link : https://riftrewind.devpost.com/
# Rate Limits :
#   - 20 requests every 1 seconds(s)
#   - 100 requests every 2 minutes(s)


class UnauthorizedError(Exception):
    def __init__(self, message):
        super().__init__(message)


def get_routing_value(region):
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
    return routing_map.get(region, 'americas')



@sleep_and_retry
@limits(calls=99, period=120)
def send_get_api_request(url: str, http_header: dict, http_object: PoolManager):
    api_response = http_object.request('GET', url, headers=http_header)
    api_response_decoded = json.loads(api_response.data.decode("utf-8"))

    if api_response.status == 404:
        raise Exception({
            'statusCode': 404,
            'body': json.dumps({'error': 'Riot ID not found. Check spelling and region.'})
        })
    elif api_response.status == 401:
        raise UnauthorizedError(f'Auth token is Forbidden : {api_response.status} - {api_response_decoded["status"]["message"]}')
    elif api_response.status != 200:
        raise Exception({
            'statusCode': api_response.status,
            'body': json.dumps({
                'error': f'Failed to fetch account: {api_response.status} - {api_response_decoded["status"]["message"]}'})
        })

    print(f"[INFO] - {datetime.now()} : request sent to {url}")

    return api_response_decoded


def append_line_to_file(line, file_path):
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(str(line) + "\n")


def write_checkpoint(file_name, checkpoint, open_mode):
    if type(checkpoint) == str:
        with open(file_name, open_mode, encoding="utf-8") as f:
            f.write(str(checkpoint) + "\n")
    elif type(checkpoint) == list:
        with open(file_name, open_mode, encoding="utf-8") as f:
            for line in checkpoint:
                f.write(str(line) + "\n")



def get_players_in_elo():
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    http = urllib3.PoolManager()
    headers = {'X-Riot-Token': api_key}
    reload = True
    players_puuid = []

    tier = "DIAMOND"
    division = "I"
    page = 1
    elo_url = f"https://euw1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}"

    elo_response_decoded = None
    total_player_in_elo = 0

    while elo_response_decoded != []:
        elo_response_decoded = send_get_api_request(elo_url, headers, http)

        total_player_in_elo += len(elo_response_decoded)

        if os.path.exists(f"players/{tier.lower()}_puuid.txt"):
            open_mode = "a"
        else:
            open_mode = "w"

        with open(f"players/{tier.lower()}_puuid.txt", open_mode, encoding="utf-8") as f:
            for player in elo_response_decoded:
                f.write(player["puuid"] + ";" + str(player["leaguePoints"]) + ";" + str(player["wins"]) + ";" + str(
                    player["losses"]) + "\n")

        print(f"Page {page} fetched.")
        page += 1
        elo_url = f"https://euw1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}"

    print(f"Total {tier.lower()} : {total_player_in_elo}")


def get_player_in_high_elo():
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    http = urllib3.PoolManager()
    headers = {'X-Riot-Token': api_key}
    reload = True

    #files_to_process = ["chall_puuid.txt", "grandmaster_puuid.txt", "master_puuid.txt", "diamand_puuid.txt"]
    files_to_process = ["chall_puuid.txt"]

    bloom_filter = BloomFilter(capacity=10_000_000)
    file_iterator = 189

    if reload:
        with open("./checkpoints/all_matchs_ids.txt", "r", encoding="utf-8") as f:
            for line in f:
                bloom_filter.add(line.strip())

    match_history_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/[PUUID]/ids?startTime=1736409600&start=[PAGE]&count=100"
    match_replay_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/[MATCH_ID]"

    for file in files_to_process:
        players_puuid = []
        with open(f"./players/{file}", "r", encoding="utf-8") as player_file:
            for line in player_file:
                players_puuid.append(line.split(";")[0])

        for player in players_puuid:
            write_checkpoint("./checkpoints/players.txt", f"{file};{player}", open_mode="w")
            print(f"[INFO] - {datetime.now()} : Checkpoint created at ./checkpoints/players.txt")

            match_history = []

            match_ids_decoded = None
            page = 0
            while match_ids_decoded != [] and page < 10:
                try:
                    match_ids_decoded = send_get_api_request(
                        match_history_url.replace("[PUUID]", player).replace("[PAGE]", str(page)),
                        headers,
                        http)

                    match_history.extend(match_ids_decoded)
                    page += 100
                    write_checkpoint("./checkpoints/players_match.txt", f"{player};{page}", open_mode="w")
                except UnauthorizedError as u:
                    raise UnauthorizedError(u)
                except Exception as e:
                    if len(match_history) == 0:
                        continue

            write_checkpoint("./checkpoints/match_history.txt", match_history, open_mode="w")
            with open("./checkpoints/all_matchs_ids.txt", "a", encoding="utf-8") as match_id_file:
                for match_id in match_history:
                    match_id_file.write(str(match_id) + "\n")
            print(f"[INFO] - {datetime.now()} : Checkpoint created at ./checkpoints/match_history.txt")

            line_iterator = 0
            match_group = []
            match_iterator = 0
            for match_id in match_history:
                if match_id in bloom_filter:
                    print(f"[INFO] - {datetime.now()} : Match already processed {match_id}")
                    continue
                bloom_filter.add(match_id)

                try:
                    match_decoded = send_get_api_request(
                        match_replay_url.replace("[MATCH_ID]", match_id),
                        headers,
                        http)

                    match_summary = generate_csv_line_from_match_api_response(match_decoded)
                    match_group.append(match_summary)
                    match_iterator += 1
                    if match_iterator >= 40:
                        break
                except UnauthorizedError as u:
                    raise UnauthorizedError(u)
                except Exception as e:
                    print(f"[INFO] - {datetime.now()} : Error found for match {match_id}")
                    with open("./checkpoints/errors.txt", "a", encoding="utf-8") as error_file:
                        error_file.write(str(match_id) + "\n")
                    continue

                if line_iterator >= 240:
                    with open(f"./matchs/{file.split("_")[0]}_match_{file_iterator}.csv", "w",
                              encoding="utf-8") as match_files:
                        for line in match_group:
                            match_files.write(line + "\n")
                    line_iterator = 0
                    file_iterator += 1
                    match_group = []
                    print(
                        f"[INFO] - {datetime.now()} : Match file created at ./matchs/{file.split("_")[0]}_match_{file_iterator}.csv")
                else:
                    line_iterator += 1

            if len(match_group) > 0:
                with open(f"./matchs/{file.split("_")[0]}_match_{file_iterator}.csv", "w",
                          encoding="utf-8") as match_files:
                    for line in match_group:
                        match_files.write(line + "\n")

            print(f"[INFO] - {datetime.now()} : Ended retrieving players game {player}")
        print(f"[INFO] - {datetime.now()} : Treatment ended for file {file}")



def get_otps_uuid():
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    http = urllib3.PoolManager()
    headers = {'X-Riot-Token': api_key}

    puuid_url = "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/[PLAYER_NAME]/[TAG]"

    players = []
    with open("./players/best_otps_euw.txt", "r", encoding="utf-16") as otp_file:
        for line in otp_file:
            players.append(line.strip())

    players_puuid = []
    for player in players:
        info_splited = player.split(";")
        champion = info_splited[0]
        name = info_splited[1]
        tag = info_splited[2]

        try:
            match_ids_decoded = send_get_api_request(
                puuid_url.replace("[PLAYER_NAME]", name).replace("[TAG]", tag),
                headers,
                http)

            players_puuid.append(f"{champion};{match_ids_decoded['puuid']}")
        except UnauthorizedError as u:
            raise UnauthorizedError(u)
        except Exception as e:
            print(f"[ERROR] - {datetime.now()} : Player {name}#{tag} faced an issue for champion {champion}")
            continue

    with open("./players/otps_puuid.txt", "w", encoding="utf-16") as otp_puuid_file:
        for player in players_puuid:
            otp_puuid_file.write(player + "\n")



def get_otps_game():
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    http = urllib3.PoolManager()
    headers = {'X-Riot-Token': api_key}

    bloom_filter = BloomFilter(capacity=1_000_000)
    file_iterator = 0

    match_history_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/[PUUID]/ids?startTime=1736409600&start=0&count=100"
    match_replay_url = f"https://europe.api.riotgames.com/lol/match/v5/matches/[MATCH_ID]"

    players_puuid = []
    with open("./players/otps_puuid.txt", "r", encoding="utf-16") as otp_file:
        for line in otp_file:
            players_puuid.append(line.strip())

    for player in players_puuid:
        info_splited = player.split(";")
        champion = info_splited[0]
        puuid = info_splited[1]

        write_checkpoint("./checkpoints/otps_players.txt", f"{champion};{puuid}", open_mode="w")
        print(f"[INFO] - {datetime.now()} : Checkpoint created at ./checkpoints/otps_players.txt")

        match_history = []

        try:
            print(match_history_url.replace("[PUUID]", puuid))
            match_ids_decoded = send_get_api_request(
                match_history_url.replace("[PUUID]", puuid),
                headers,
                http)

            match_history.extend(match_ids_decoded)
            write_checkpoint("./checkpoints/otps_players_match.txt", f"{player}", open_mode="w")
        except UnauthorizedError as u:
            raise UnauthorizedError(u)
        except Exception as e:
            if len(match_history) == 0:
                continue

        write_checkpoint("./checkpoints/otps_match_history.txt", match_history, open_mode="w")
        print(f"[INFO] - {datetime.now()} : Checkpoint created at ./checkpoints/otps_match_history.txt")

        match_group = []

        with open("./checkpoints/otps_all_matchs_ids.txt", "a", encoding="utf-8") as match_id_file:
            for match_id in match_history:
                match_id_file.write(str(match_id) + "\n")

        for match_id in match_history:
            if match_id in bloom_filter:
                print(f"[INFO] - {datetime.now()} : Match already processed {match_id}")
                continue
            bloom_filter.add(match_id)

            try:
                match_decoded = send_get_api_request(
                    match_replay_url.replace("[MATCH_ID]", match_id),
                    headers,
                    http)

                match_summary = generate_csv_line_from_match_api_response(match_decoded)
                match_group.append(match_summary)
            except UnauthorizedError as u:
                raise UnauthorizedError(u)
            except Exception as e:
                print(f"[INFO] - {datetime.now()} : Error found for match {match_id}")
                with open("./checkpoints/errors.txt", "a", encoding="utf-8") as error_file:
                    error_file.write(str(match_id) + "\n")
                continue

        with open(f"./otp_matchs/{champion.replace(' ', '').replace('.', '').replace('\'', '').lower()}_match_{file_iterator}.csv", "w",
                  encoding="utf-8") as match_files:
            for line in match_group:
                match_files.write(line + "\n")
        file_iterator += 1
        print(f"[INFO] - {datetime.now()} : Match file created at ./otp_matchs/{champion.replace(' ', '').replace('.', '').replace('\'', '').lower()}_match_{file_iterator}.csv")


        print(f"[INFO] - {datetime.now()} : Ended retrieving players game {champion}")





if __name__ == '__main__':
    get_player_in_high_elo()

