from bs4 import BeautifulSoup
import pandas as pd
import requests
import concurrent.futures

MAX_THREADS = 30

def get_game_list():
    df = pd.read_csv("https://raw.githubusercontent.com/petebrown/update-player-stats/main/data/players_df.csv")

    df = df[(df.goals_for > 0) | (df.goals_against > 0)]

    df.sb_game_id = df.sb_game_id.str.replace("tpg", "")

    df["url"] = df.apply(lambda x: f"https://www.soccerbase.com/matches/additional_information.sd?id_game={x.sb_game_id}", axis=1)

    games = df[["url", "venue"]].drop_duplicates().to_dict("records")

    return games

def get_goals(game_dict):
    url = game_dict["url"]
    venue = game_dict["venue"]
    game_id = url.split("=")[1]

    r = requests.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')

    target_teams = ["for", "against"]
    team_order = ["teamA", "teamB"]

    game_goals = []

    for target_team in target_teams:
        try:
            if target_team == "for":
                if venue == "H":
                    side = team_order[0]
                elif venue == "A":
                    side = team_order[1]
                else:
                    next
            elif target_team == "against":
                if venue == "H":
                    side = team_order[1]
                elif venue == "A":
                    side = team_order[0]
                else:
                    next

            goals = doc.select(f'.matchInfo .goalscorers .{side} p span')

            for goal in goals:
                player_id = goal.find("a")["href"].split("=")[1]
                player_text = goal.text
                player_name = player_text.split("(")[0].strip()
                goal_details = player_text.split("(")[-1].replace(")","").strip()
                minutes = goal_details.split(",")
                
                for minute in minutes:
                    if "pen" in minute:
                        penalty = 1
                        minute = minute.replace("pen ", "")
                    else:
                        penalty = 0

                    if "og" in minute:
                        own_goal = 1
                        minute = minute.replace("og ", "")
                    else:
                        own_goal = 0

                    if "s/o" in minute:
                        next
                    else:
                        record = {
                            "game_id": int(game_id),
                            "player_id": int(player_id),
                            "player_name": player_name,
                            "minute": int(minute.strip()),
                            "penalty": penalty,
                            "own_goal": own_goal,
                            "goal_type": target_team,
                            "goal_details": goal_details,
                        }
                        game_goals.append(record)
        except:
            print(f"Failed on {url}")
            next
    return game_goals

def async_scraping(scrape_function, urls):
    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results

def main():
    games_dict = get_game_list()

    goals = async_scraping(get_goals, games_dict)
    goals = list(goals)
    goals = [goal for sublist in goals for goal in sublist]

    df = pd.DataFrame(goals)
    manual_goals = pd.read_csv("manual_goals.csv")

    df = pd.concat([df, manual_goals]).drop_duplicates(ignore_index=True)
    return df

df = main()
df.to_csv("./data/goals.csv", index=False)