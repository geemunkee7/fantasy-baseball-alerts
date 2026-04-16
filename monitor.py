import os
import requests
import feedparser
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ============================================================
# CONFIGURATION - pulled from GitHub Secrets automatically
# ============================================================
PUSHOVER_USER = os.environ.get('PUSHOVER_USER_KEY', '')
PUSHOVER_TOKEN = os.environ.get('PUSHOVER_API_TOKEN', '')
LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID', '')
YAHOO_CLIENT_ID = os.environ.get('YAHOO_CLIENT_ID', '')
YAHOO_CLIENT_SECRET = os.environ.get('YAHOO_CLIENT_SECRET', '')

ROTOWIRE_MLB_RSS = "https://www.rotowire.com/baseball/rss.xml"
LOOKBACK_MINUTES = 11  # slightly over 10 to avoid gaps between runs

# ============================================================
# SEND PUSHOVER ALERT TO YOUR IPHONE
# ============================================================
def send_pushover(title, message, priority=1):
    try:
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": title,
                "message": message,
                "priority": priority,
                "sound": "siren"
            }
        )
        print(f"Alert sent ({response.status_code}): {title}")
    except Exception as e:
        print(f"Pushover error: {e}")

# ============================================================
# GET RECENT NEWS FROM ROTOWIRE RSS
# ============================================================
def get_recent_news():
    try:
        feed = feedparser.parse(ROTOWIRE_MLB_RSS)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
        recent = []
        for entry in feed.entries:
            try:
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                if published >= cutoff:
                    title = entry.get('title', '')
                    summary = entry.get('summary', title)
                    player_name = title.split(':')[0].strip() if ':' in title else title.strip()
                    recent.append({
                        'title': title,
                        'summary': summary,
                        'published': published,
                        'player_name': player_name
                    })
            except Exception:
                continue
        print(f"Found {len(recent)} news items in last {LOOKBACK_MINUTES} minutes")
        return recent
    except Exception as e:
        print(f"RSS fetch error: {e}")
        return []

# ============================================================
# GET ALL ROSTERED PLAYERS FROM YOUR YAHOO LEAGUE
# ============================================================
def get_taken_players():
    try:
        from yfpy.query import YahooFantasySportsQuery
        query = YahooFantasySportsQuery(
            league_id=LEAGUE_ID,
            game_code="mlb",
            yahoo_consumer_key=YAHOO_CLIENT_ID,
            yahoo_consumer_secret=YAHOO_CLIENT_SECRET,
            env_file_location=Path("."),
            env_var_fallback=True,
            save_token_data_to_env_file=True
        )
        taken = set()
        for team_id in range(1, 13):
            try:
                roster = query.get_team_roster_player_info_by_team_id(team_id)
                if roster:
                    for player in roster:
                        try:
                            name = player.name.full
                        except Exception:
                            try:
                                name = str(player.name)
                            except Exception:
                                name = None
                        if name:
                            taken.add(name.lower().strip())
            except Exception as e:
                print(f"Could not get roster for team {team_id}: {e}")
                continue
        print(f"Found {len(taken)} rostered players in your league")
        return taken
    except Exception as e:
        print(f"Yahoo connection error: {e}")
        return set()

# ============================================================
# DETERMINE ALERT TYPE BASED ON NEWS CONTENT
# ============================================================
def get_alert_type(item):
    text = (item['title'] + ' ' + item['summary']).lower()
    if any(w in text for w in ['promoted', 'called up', 'recalled', 'call-up']):
        return "🚀 CALLUP ALERT"
    elif any(w in text for w in ['closer', 'save opportunity', 'saves role', 'closing']):
        return "💾 CLOSER ALERT"
    elif any(w in text for w in ['activated', 'reinstated', 'returns from il', 'comes off il']):
        return "✅ IL RETURN ALERT"
    elif any(w in text for w in ['placed on il', 'injured list', 'day-to-day', 'disabled list']):
        return "🚑 INJURY — CHECK REPLACEMENT"
    elif any(w in text for w in ['starting', 'start tuesday', 'start wednesday',
                                  'start thursday', 'start friday', 'start saturday',
                                  'start sunday', 'start monday']):
        return "⚾ STARTER ALERT"
    else:
        return "📰 PLAYER NEWS"

# ============================================================
# MAIN
# ============================================================
def main():
    print(f"\n{'='*50}")
    print(f"Monitor run: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*50}")

    news = get_recent_news()
    if not news:
        print("No recent news. Exiting.")
        return

    taken = get_taken_players()
    if not taken:
        print("WARNING: Could not load league rosters - sending all news as alerts")

    alerts_sent = 0
    for item in news:
        player = item['player_name']
        player_lower = player.lower().strip()

        if not player_lower:
            continue

        is_available = player_lower not in taken

        if is_available or not taken:
            alert_type = get_alert_type(item)
            title = f"{alert_type}: {player}"
            message = f"{item['summary']}\n\n✅ AVAILABLE in your league — grab them now!"
            send_pushover(title, message, priority=1)
            alerts_sent += 1
        else:
            print(f"Skipping {player} — already rostered")

    print(f"\nDone. {alerts_sent} alert(s) sent.")

if __name__ == "__main__":
    main()
