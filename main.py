import requests
import pandas as pd
import time, json, os, logging, threading
from datetime import datetime, timedelta, timezone
from nba_api.stats.static import players
from nba_api.stats.endpoints import PlayerGameLog
from flask import Flask, jsonify
from flask_cors import CORS

# ---------------- CONFIG ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, origins=[
    "https://dashboard.thebettinginsider.com",
    "http://localhost:3000",
    "http://localhost:3001"
])

API_KEY = os.getenv("ODDS_API_KEY", "d8ba5d45eca27e710d7ef2680d8cb452")
CACHE_FILE = "nba_cache.json"
CACHE_TTL_HOURS = 24
BATCH_SIZE = 100
SLEEP_BETWEEN_REQUESTS = 0.4
SLEEP_BETWEEN_BATCHES = 60
ET = timezone(timedelta(hours=-5))

latest_props_data = {"last_updated": None, "props": [], "summary": {}, "error": None}
data_lock = threading.Lock()

# ---------------- CACHE ----------------
def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            cache = json.load(f)
        cutoff = (datetime.now(ET) - timedelta(hours=CACHE_TTL_HOURS)).timestamp()
        return {k: v for k, v in cache.items() if v.get("timestamp", 0) > cutoff}
    except Exception:
        return {}

def save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        logger.warning(f"Failed to save cache: {e}")

# ---------------- HELPERS ----------------
def format_game_time(game_time_str):
    dt = datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
    dt_et = dt.astimezone(ET)
    return dt_et.strftime("%a %m/%d %I:%M%p ET")

def get_upcoming_games_filter():
    now_et = datetime.now(ET)
    def should_include_game(game_time_str):
        dt = datetime.fromisoformat(game_time_str.replace("Z", "+00:00"))
        dt_et = dt.astimezone(ET)
        return (dt_et.date() - now_et.date()).days <= 2
    return should_include_game

# ---------------- NBA LOGIC ----------------
def fetch_nba_props():
    global latest_props_data
    try:
        logger.info("Starting full NBA props update (SEASON CONSISTENCY)...")
        cache = load_cache()

        # 1. Get events
        events_url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={API_KEY}"
        resp = requests.get(events_url, timeout=10)
        resp.raise_for_status()
        events = resp.json()

        game_filter = get_upcoming_games_filter()
        events_to_check = [ev for ev in events if game_filter(ev["commence_time"])]
        if not events_to_check:
            with data_lock:
                latest_props_data.update({
                    "last_updated": datetime.now(ET).isoformat(),
                    "props": [],
                    "summary": {"total_games": 0, "total_props": 0},
                    "error": "No NBA games found"
                })
            return

        # 2. Markets
        markets = ",".join(["player_points", "player_rebounds", "player_assists", "player_threes"])

        # 3. Gather all props
        props = []
        for ev in events_to_check:
            event_id = ev["id"]
            home, away = ev["home_team"], ev["away_team"]
            game_time = format_game_time(ev["commence_time"])

            odds_url = (
                f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event_id}/odds"
                f"?regions=us&oddsFormat=american&markets={markets}&apiKey={API_KEY}"
            )
            odds_resp = requests.get(odds_url, timeout=10)
            odds_resp.raise_for_status()
            game_data = odds_resp.json()

            for bookmaker in game_data.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        player = outcome.get("description")
                        side = outcome.get("name")
                        line = outcome.get("point")
                        odds = outcome.get("price")
                        if odds is not None and -600 <= odds <= -150:
                            props.append({
                                "game": f"{away} @ {home}",
                                "game_time": game_time,
                                "market": market["key"],
                                "player": player,
                                "side": side,
                                "line": line,
                                "odds": odds,
                                "bookmaker": bookmaker.get("key"),
                                "bookmaker_title": bookmaker.get("title")
                            })

        logger.info(f"Pulled {len(props)} props in odds range.")

        # 4. Stat mapping
        market_to_stat = {
            "player_points": "PTS",
            "player_rebounds": "REB",
            "player_assists": "AST",
            "player_threes": "FG3M"
        }

        def get_player_logs(player_name):
            """Load from cache or nba_api"""
            if player_name in cache:
                return pd.DataFrame(cache[player_name]["data"])
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            try:
                match = [p for p in players.get_players() if p['full_name'].lower() == player_name.lower()]
                if not match:
                    return pd.DataFrame()
                pid = match[0]['id']
                gamelog = PlayerGameLog(player_id=pid, season='2024-25', season_type_all_star='Regular Season')
                df = gamelog.get_data_frames()[0]
                cache[player_name] = {
                    "data": df[['GAME_DATE','PTS','REB','AST','FG3M']].to_dict(orient='records'),
                    "timestamp": datetime.now(ET).timestamp()
                }
                return df
            except Exception as e:
                logger.warning(f"Failed logs for {player_name}: {e}")
                return pd.DataFrame()

        # 5. Check props in batches
        prop_groups, total_checked = {}, 0
        total_batches = (len(props) // BATCH_SIZE) + 1

        for b in range(total_batches):
            batch = props[b*BATCH_SIZE:(b+1)*BATCH_SIZE]
            if not batch:
                break
            logger.info(f"Processing batch {b+1}/{total_batches} ({len(batch)} props)")
            for p in batch:
                total_checked += 1
                stat_col = market_to_stat.get(p["market"])
                if not stat_col:
                    continue
                df = get_player_logs(p["player"])
                if df.empty or len(df) < 4:
                    continue
                vals = df[stat_col].tolist()
                all_hit = all((v > p["line"]) if p["side"] == "Over" else (v < p["line"]) for v in vals)
                if all_hit:
                    prop_key = (p["player"], p["market"], p["line"], p["side"], p["game"])
                    if prop_key not in prop_groups:
                        avg_val = sum(vals)/len(vals)
                        prop_groups[prop_key] = {
                            "game": p["game"],
                            "game_time": p["game_time"],
                            "market": p["market"].replace('_',' ').title(),
                            "player": p["player"],
                            "side": p["side"],
                            "line": float(p["line"]),
                            "bookmakers": [],
                            "season_avg": round(float(avg_val),1),
                            "recent_values": [float(v) for v in vals],
                            "games_played": len(vals)
                        }
                    prop_groups[prop_key]["bookmakers"].append({
                        "name": p["bookmaker"],
                        "title": p["bookmaker_title"],
                        "odds": int(p["odds"])
                    })
            save_cache(cache)
            if b < total_batches-1:
                logger.info(f"Batch {b+1} complete. Sleeping {SLEEP_BETWEEN_BATCHES}s...")
                time.sleep(SLEEP_BETWEEN_BATCHES)

        qualifying = []
        for prop_data in prop_groups.values():
            seen=set(); uniq=[]
            for bm in prop_data["bookmakers"]:
                key=(bm["name"], bm["odds"])
                if key not in seen:
                    seen.add(key); uniq.append(bm)
            prop_data["bookmakers"]=sorted(uniq,key=lambda x:x["odds"],reverse=True)
            qualifying.append(prop_data)

        with data_lock:
            latest_props_data.update({
                "last_updated": datetime.now(ET).isoformat(),
                "props": qualifying,
                "summary": {
                    "total_props_checked": total_checked,
                    "total_consistent": len(qualifying),
                    "odds_range": "-600 to -150",
                    "cache_size": len(cache),
                    "batches_processed": total_batches,
                    "mode": "FULL SEASON CONSISTENCY"
                },
                "error": None
            })

        logger.info(f"NBA FULL update complete! Checked {total_checked} props; Found {len(qualifying)} consistent.")
        save_cache(cache)

    except Exception as e:
        logger.error(f"NBA props error: {e}")
        with data_lock:
            latest_props_data["error"] = str(e)
            latest_props_data["last_updated"] = datetime.now(ET).isoformat()

# ---------------- FLASK ROUTES ----------------
@app.route('/')
def index():
    with data_lock:
        data = latest_props_data.copy()
    if data["last_updated"]:
        dt=datetime.fromisoformat(data["last_updated"])
        data["last_updated_formatted"]=dt.strftime("%I:%M %p ET")
    else:
        data["last_updated_formatted"]="Never"
    if data.get("props"):
        by_game={}
        for prop in data["props"]:
            by_game.setdefault(prop["game"],[]).append(prop)
        data["props_by_game"]=by_game
    return jsonify(data)

@app.route('/props')
def get_props(): 
    return index()

@app.route('/health')
def health():
    return jsonify({
        "status":"healthy",
        "last_updated":latest_props_data.get("last_updated"),
        "props_count":len(latest_props_data.get("props",[]))
    })

# ---------------- MAIN ----------------
if __name__ == '__main__':
    # Start data fetch in background thread so Flask can boot immediately
    threading.Thread(target=fetch_nba_props, daemon=True).start()

    port = int(os.getenv('PORT', 5000))
    logger.info(f"Starting Flask server on port {port}...")
    app.run(host='0.0.0.0', port=port, debug=False)
