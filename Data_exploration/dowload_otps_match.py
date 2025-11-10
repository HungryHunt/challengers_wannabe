import os
import requests
import urllib3
import json
import time

from bs4 import BeautifulSoup
from datetime import datetime




if __name__ == '__main__':
    all_champs = []
    with open("./champions.txt", "r") as f:
        for line in f:
            all_champs.append(line.strip())
    url = "https://www.onetricks.gg/fr/champions/ranking/[CHAMPION]"

    for champion in all_champs:
        otps = []
        response = requests.get(url.replace("[CHAMPION]", champion.replace(' ', '').replace('.', '').replace('\'', '')))

        if response.status_code == 200:
            try:
                soup = BeautifulSoup(response.text, "html.parser")
                table = soup.find("table", class_="utils_tablePlayers__lh0ZH")
                rows = table.find_all("tr")
                i = 0
                for row in rows:
                    if i == 3:
                        break
                    try:
                        cols = row.find_all("td")
                        values = [col.text.strip() for col in cols]
                        if "EUW1" in values[4]:
                            name_tag_split = values[4].split("   ")
                            name = name_tag_split[0]
                            tag = name_tag_split[1].split(" ")[0].replace("#", "")
                            otps.append(f"{champion};{name};{tag}")
                            i += 1
                    except Exception as e:
                        continue
                if otps:
                    with open(f"./players/best_otps_euw.txt", "a", encoding='utf-16') as f:
                        for player_name in otps:
                            f.write(str(player_name) + "\n")
                    print(f"[INFO] - {datetime.now()} : {champion} otps found")
            except Exception as e:
                print(f"[ERROR] - {datetime.now()} : {champion} have found an error\n{e}")

        else:
            print(f"[WARNING] - {datetime.now()} : Error while retrieving info for {champion.upper()}")

        time.sleep(1)


