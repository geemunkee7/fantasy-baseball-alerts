import os
import re
import requests
import feedparser
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATION
# ============================================================
PUSHOVER_USER = os.environ.get('PUSHOVER_USER_KEY', '')
PUSHOVER_TOKEN = os.environ.get('PUSHOVER_API_TOKEN', '')
LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID', '')
YAHOO_CLIENT_ID = os.environ.get('YAHOO_CLIENT_ID', '')
YAHOO_CLIENT_SECRET = os.environ.get('YAHOO_CLIENT_SECRET', '')
MY_TEAM_ID = 10

ET_TZ = ZoneInfo("America/New_York")
LOOKBACK_MINUTES = 11

# ============================================================
# TOP 15 SS WATCHLIST
# ============================================================
TOP_15_SS = [
    "Gunnar Henderson", "Bobby Witt Jr.", "Trea Turner",
    "Francisco Lindor", "Corey Seager", "CJ Abrams",
    "Anthony Volpe", "Elly De La Cruz", "Jeremy Pena",
    "Willy Adames", "JP Crawford", "Carlos Correa",
    "Ezequiel Tovar", "Dansby Swanson", "Jackson Holliday"
]
MY_SS = ["gunnar henderson", "trea turner"]

# ============================================================
# NEWS SOURCES
# ============================================================
TIER1_SOURCES = [
    {"name": "Rotowire",         "url": "https://www.rotowire.com/baseball/rss.xml",       "type": "fantasy"},
    {"name": "MLB Trade Rumors", "url": "https://www.mlbtraderumors.com/feed",              "type": "transactions"},
    {"name": "ESPN MLB",         "url": "https://www.espn.com/espn/rss/mlb/news",           "type": "news"},
    {"name": "MLB.com Official", "url": "https://www.mlb.com/feeds/news/rss.xml",           "type": "news"},
    {"name": "MiLB Official",    "url": "https://www.milb.com/feeds/news/rss.xml",          "type": "prospects"},
]
TIER2_SOURCES = [
    {"name": "r/fantasybaseball", "url": "https://www.reddit.com/r/fantasybaseball/new/.rss", "type": "reddit"},
    {"name": "r/baseball",        "url": "https://www.reddit.com/r/baseball/new/.rss",        "type": "reddit"},
]

FANTASY_KEYWORDS = [
    'promoted', 'called up', 'recalled', 'call-up', 'debut',
    'closer', 'saves', 'save opportunity', 'ninth inning', 'closing role',
    'injured', 'placed on il', 'injured list', 'day-to-day',
    'activated', 'reinstated', 'returns from il', 'comes off il',
    'starting lineup', 'leadoff', 'batting first',
    'designated for assignment', 'dfa', 'released',
    'suspension', 'optioned', 'demoted', 'scratched',
    'trade', 'acquired', 'signed'
]
HIGH_PRIORITY_KEYWORDS = [
    'promoted', 'called up', 'recalled', 'call-up', 'debut',
    'closer', 'save opportunity', 'closing role',
    'activated', 'reinstated', 'returns from il'
]
SS_INJURY_KEYWORDS = [
    'injured', 'il', 'injured list', 'day-to-day', 'placed on',
    'disabled', 'hamstring', 'oblique', 'knee', 'wrist', 'shoulder',
    'elbow', 'back', 'thumb', 'ankle', 'concussion', 'surgery', 'fracture'
]

# ============================================================
# PUSHOVER
# ============================================================
def send_pushover(title, message, priority=1):
    try:
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": PUSHOVER_TOKEN,
                "user": PUSHOVER_USER,
                "title": title[:100],
                "message": message[:1024],
                "priority": priority,
                "sound": "siren"
            },
            timeout=10
        )
        print(f"  Alert sent ({response.status_code}): {title}")
    except Exception as e:
        print(f"  Pushover error: {e}")

def strip_html(text):
    return re.sub('<[^<]+?>', '', str(text)).strip()

# ============================================================
# YAHOO
# ============================================================
def get_yahoo_query():
    from yfpy.query import YahooFantasySportsQuery
    return YahooFantasySportsQuery(
        league_id=LEAGUE_ID,
        game_code="mlb",
        yahoo_consumer_key=YAHOO_CLIENT_ID,
        yahoo_consumer_secret=YAHOO_CLIENT_SECRET,
        env_file_location=Path("."),
        env_var_fallback=True,
        save_token_data_to_env_file=True
    )

def get_all_rosters():
    """Returns (taken_set, my_roster_list). One Yahoo call loads everything."""
    try:
        query = get_yahoo_query()
        today = date.today()
        taken = set()
        my_roster = []
        for team_id in range(1, 13):
            try:
                roster = query.get_team_roster_player_info_by_date(team_id, today)
                if not roster:
                    continue
                for player in roster:
                    try:
                        name = player.name.full
                    except Exception:
                        try:
                            name = str(player.name)
                        except Exception:
                            name = None
                    if not name:
                        continue
                    taken.add(name.lower().strip())
                    if team_id == MY_TEAM_ID:
                        try:
                            my_roster.append({
                                'name': name,
                                'position': player.primary_position,
                                'pct_owned': float(
                                    getattr(player.percent_owned, 'value', 0) or 0
                                ),
                                'is_undroppable': int(
                                    getattr(player, 'is_undroppable', 0) or 0
                                ),
                                'status': str(getattr(player, 'status', '') or ''),
                                'selected_position': (
                                    player.selected_position.position
                                    if hasattr(player, 'selected_position') else ''
                                )
                            })
                        except Exception:
                            pass
            except Exception as e:
                print(f"  Team {team_id} error: {e}")
        print(f"  {len(taken)} rostered, {len(my_roster)} on my team")
        return taken, my_roster
    except Exception as e:
        print(f"  Yahoo error: {e}")
        return set(), []

def get_drop_candidates(my_roster, count=3):
    """Weakest droppable pitchers sorted by % owned ascending."""
    candidates = [
        p for p in my_roster
        if not p['is_undroppable']
        and 'IL' not in p['status']
        and p['position'] in ['SP', 'RP', 'P']
    ]
    candidates.sort(key=lambda x: x['pct_owned'])
    return candidates[:count]

# ============================================================
# MLB STATS API
# ============================================================
def get_probable_pitchers(start_date, end_date):
    """Dict: name -> {count, id, dates}"""
    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&startDate={start_date}&endDate={end_date}"
            f"&gameType=R&hydrate=probablePitcher"
        )
        data = requests.get(url, timeout=15).json()
        pitchers = {}
        for day in data.get('dates', []):
            for game in day.get('games', []):
                for side in ['home', 'away']:
                    p = game.get('teams', {}).get(side, {}).get('probablePitcher', {})
                    if p and p.get('fullName'):
                        n = p['fullName']
                        if n not in pitchers:
                            pitchers[n] = {'count': 0, 'id': p.get('id', 0), 'dates': []}
                        pitchers[n]['count'] += 1
                        pitchers[n]['dates'].append(day.get('date', ''))
        return pitchers
    except Exception as e:
        print(f"  Schedule API error: {e}")
        return {}

def get_pitcher_stats(player_id):
    """2026 season ERA, WHIP, K, IP, K/BB."""
    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
            f"?stats=season&group=pitching&season=2026"
        )
        data = requests.get(url, timeout=5).json()
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                s = split.get('stat', {})
                try:
                    return {
                        'era':  float(s.get('era',  '99.99') or '99.99'),
                        'whip': float(s.get('whip', '9.99')  or '9.99'),
                        'k':    int(s.get('strikeOuts', 0)    or 0),
                        'ip':   float(s.get('inningsPitched', '0') or '0'),
                        'kbb':  float(s.get('strikeoutWalkRatio', '0') or '0'),
                    }
                except Exception:
                    pass
    except Exception:
        pass
    return {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0, 'kbb': 0.0}

def score_pitcher(stats):
    """Higher = better. Weights K, K/BB, ERA, WHIP for your league."""
    if stats.get('ip', 0) < 5:
        return -999
    return (
        stats.get('k',   0)   * 0.5
        + stats.get('kbb', 0) * 10
        - stats.get('era', 5) * 5
        - stats.get('whip', 1.4) * 20
    )

# ============================================================
# ALERT 1: FRIDAY — 2-START PITCHERS NEXT WEEK
# ============================================================
def send_two_start_alert(taken, my_roster):
    print("Running Friday 2-start alert...")
    today = datetime.now(ET_TZ).date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    next_mon = today + timedelta(days=days_ahead)
    next_sun = next_mon + timedelta(days=6)

    all_starters = get_probable_pitchers(next_mon, next_sun)
    two_starters = {n: i for n, i in all_starters.items() if i['count'] >= 2}

    if not two_starters:
        send_pushover(
            "⚾ 2-START ALERT",
            f"No confirmed 2-starters yet for {next_mon}. "
            f"Probables may not be posted. Check back Saturday.",
            priority=0
        )
        return

    available = {}
    for name, info in two_starters.items():
        if name.lower().strip() not in taken:
            info['stats'] = get_pitcher_stats(info['id'])
            available[name] = info

    if not available:
        send_pushover("⚾ 2-START ALERT", "All 2-start pitchers are already owned.", priority=0)
        return

    ranked = sorted(
        available.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:5]

    drops = get_drop_candidates(my_roster, count=3)
    drop_str = (
        ' | '.join(f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops)
        or "No obvious drops — use bench spot"
    )

    lines = [f"📅 {next_mon} — {next_sun}\n"]
    for name, info in ranked:
        s = info['stats']
        dates = ', '.join(d[5:] for d in info['dates'][:2])
        stat_line = (
            f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f}"
            if s['ip'] >= 5 else "No 2026 stats yet"
        )
        lines.append(f"• {name} — starts {dates}\n  {stat_line}")

    lines.append(f"\n💀 Weakest drops on your staff:\n{drop_str}")
    send_pushover("⚾ 2-START SP TARGETS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT 2: THU-SUN EVERY 12 HOURS — STREAMING PITCHERS
# ============================================================
def send_streaming_alert(taken, my_roster):
    print("Running streaming pitcher alert...")
    today = datetime.now(ET_TZ).date()
    end_of_week = today + timedelta(days=(6 - today.weekday()))

    all_starters = get_probable_pitchers(today, end_of_week)

    available = {}
    for name, info in all_starters.items():
        if name.lower().strip() not in taken:
            s = get_pitcher_stats(info['id'])
            info['stats'] = s
            if s['ip'] >= 5 and (s['era'] < 4.50 or s['kbb'] > 2.0):
                available[name] = info

    if not available:
        print("  No quality streaming options found")
        return

    ranked = sorted(
        available.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:5]

    drops = get_drop_candidates(my_roster, count=2)
    drop_str = (
        ' | '.join(f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops)
        or "No obvious drops"
    )

    lines = [f"📅 Streaming through {end_of_week}\n"]
    for name, info in ranked:
        s = info['stats']
        starts = info['count']
        dates = ', '.join(d[5:] for d in info['dates'][:2])
        lines.append(
            f"• {name} ({starts} start{'s' if starts > 1 else ''}, {dates})\n"
            f"  ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f}"
        )

    lines.append(f"\n💀 Consider dropping:\n{drop_str}")
    send_pushover("🌊 STREAMING SP OPTIONS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT 3: DAILY 8AM ET — TOP 15 SS INJURY WATCHLIST
# ============================================================
def check_ss_injury_watchlist():
    print("Checking SS injury watchlist (last 24h)...")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
    alerts = []
    seen = set()

    for source in TIER1_SOURCES:
        try:
            feed = feedparser.parse(source['url'], request_headers=headers)
            for entry in feed.entries:
                try:
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                        if pub < cutoff:
                            continue
                    title   = strip_html(entry.get('title', ''))
                    summary = strip_html(entry.get('summary', title))
                    text    = (title + ' ' + summary).lower()
                    for ss in TOP_15_SS:
                        if ss.lower() in text and ss.lower() not in seen:
                            if any(kw in text for kw in SS_INJURY_KEYWORDS):
                                seen.add(ss.lower())
                                alerts.append({
                                    'player':  ss,
                                    'is_mine': ss.lower() in MY_SS,
                                    'summary': summary[:250],
                                    'source':  source['name']
                                })
                except Exception:
                    continue
        except Exception:
            continue

    print(f"  {len(alerts)} SS injury alert(s)")
    for alert in alerts:
        is_mine = alert['is_mine']
        send_pushover(
            f"{'🚨' if is_mine else '👀'} SS INJURY: {alert['player']}"
            f"{' ← YOUR PLAYER!' if is_mine else ''}",
            f"{alert['summary']}\n\nSource: {alert['source']}",
            priority=1 if is_mine else 0
        )

# ============================================================
# RSS BREAKING NEWS
# ============================================================
def fetch_feed(source):
    try:
        headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
        feed = feedparser.parse(source["url"], request_headers=headers)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
        items = []
        for entry in feed.entries:
            try:
                pub = (
                    datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    if hasattr(entry, 'published_parsed') and entry.published_parsed
                    else datetime.now(timezone.utc)
                )
                if pub < cutoff:
                    continue
                title   = strip_html(entry.get('title', ''))
                summary = strip_html(entry.get('summary', entry.get('description', title)))
                summary = summary[:300] + '...' if len(summary) > 300 else summary
                player_name = (
                    title.split(':')[0].strip()
                    if ':' in title and source["type"] != "reddit"
                    else None
                )
                items.append({
                    'source': source["name"], 'type': source["type"],
                    'title': title, 'summary': summary,
                    'published': pub, 'player_name': player_name
                })
            except Exception:
                continue
        print(f"  {source['name']}: {len(items)} new items")
        return items
    except Exception as e:
        print(f"  {source['name']} error: {e}")
        return []

def should_check_reddit():
    m = datetime.now(timezone.utc).minute
    return m < 11 or 30 <= m < 41

def get_all_news():
    items = []
    print("Checking Tier 1 sources...")
    for s in TIER1_SOURCES:
        items.extend(fetch_feed(s))
    if should_check_reddit():
        print("Checking Reddit (Tier 2)...")
        for s in TIER2_SOURCES:
            items.extend(fetch_feed(s))
    else:
        print("Skipping Reddit this run")
    print(f"Total: {len(items)} raw items")
    return items

def is_fantasy_relevant(item):
    text = (item['title'] + ' ' + item['summary']).lower()
    return any(kw in text for kw in FANTASY_KEYWORDS)

def get_alert_details(item):
    text = (item['title'] + ' ' + item['summary']).lower()
    if any(w in text for w in ['promoted', 'called up', 'recalled', 'call-up', 'debut']):
        return "🚀 CALLUP", 1
    if any(w in text for w in ['closer', 'save opportunity', 'saves role', 'closing role', 'ninth inning']):
        return "💾 CLOSER ROLE", 1
    if any(w in text for w in ['activated', 'reinstated', 'returns from il', 'comes off il', 'off the il']):
        return "✅ IL RETURN", 1
    if any(w in text for w in ['placed on il', 'injured list', 'day-to-day', 'goes on il']):
        return "🚑 INJURY", 1
    if any(w in text for w in ['designated for assignment', 'dfa']):
        return "🔄 DFA", 0
    if any(w in text for w in ['trade', 'acquired']):
        return "🔁 TRADE", 0
    return "📰 NEWS", 0

# ============================================================
# MAIN
# ============================================================
def main():
    now_utc   = datetime.now(timezone.utc)
    now_et    = datetime.now(ET_TZ)
    hour_et   = now_et.hour
    minute_et = now_et.minute
    weekday   = now_et.weekday()  # 0=Mon 1=Tue 2=Wed 3=Thu 4=Fri 5=Sat 6=Sun

    print(f"\n{'='*50}")
    print(f"Run: {now_utc.strftime('%Y-%m-%d %H:%M UTC')} | "
          f"{now_et.strftime('%H:%M ET %A')}")
    print(f"{'='*50}")

    # 8am window fires once daily
    daily_window = (hour_et == 8 and minute_et < 10)

    # 8am OR 8pm window fires twice daily
    twice_daily_window = (hour_et in [8, 20] and minute_et < 10)

    # Rosters loaded lazily — only when a feature needs them
    taken, my_roster = None, None

    # ── DAILY 8AM: SS INJURY WATCHLIST ─────────────────────────
    if daily_window:
        print("\n--- DAILY SS INJURY WATCHLIST ---")
        check_ss_injury_watchlist()

    # ── FRIDAY 8AM: 2-START PITCHER ALERT ──────────────────────
    if weekday == 4 and daily_window:
        print("\n--- FRIDAY 2-START PITCHER ALERT ---")
        taken, my_roster = get_all_rosters()
        send_two_start_alert(taken, my_roster)

    # ── THU-SUN EVERY 12 HOURS: STREAMING PITCHER ALERT ────────
    # Thu 8am, Thu 8pm, Fri 8am, Fri 8pm, Sat 8am, Sat 8pm, Sun 8am
    is_streaming_window = (
        twice_daily_window
        and (
            weekday in [3, 4, 5]
            or (weekday == 6 and hour_et == 8)
        )
    )
    if is_streaming_window:
        print("\n--- STREAMING PITCHER ALERT (Thu-Sun) ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        send_streaming_alert(taken, my_roster)

    # ── EVERY 10 MIN: BREAKING NEWS ─────────────────────────────
    print("\n--- BREAKING NEWS CHECK ---")
    news     = get_all_news()
    relevant = [i for i in news if is_fantasy_relevant(i)]
    print(f"Fantasy-relevant: {len(relevant)}")

    if not relevant:
        print("Nothing fantasy-relevant. Done.")
        return

    if taken is None:
        taken, my_roster = get_all_rosters()

    alerted, sent = set(), 0

    for item in relevant:
        player    = item['player_name']
        is_reddit = item['type'] == 'reddit'

        if is_reddit:
            text = (item['title'] + ' ' + item['summary']).lower()
            if any(kw in text for kw in HIGH_PRIORITY_KEYWORDS):
                at, pr = get_alert_details(item)
                send_pushover(
                    f"{at} [{item['source']}]",
                    f"{item['title']}\n\n{item['summary'][:200]}",
                    pr
                )
                sent += 1
            continue

        if not player:
            continue

        pl = player.lower().strip()
        if pl in alerted:
            print(f"  Skip {player} — already alerted this run")
            continue
        if pl in taken:
            print(f"  Skip {player} — rostered")
            continue

        at, pr = get_alert_details(item)
        send_pushover(
            f"{at}: {player} [{item['source']}]",
            f"{item['summary']}\n\n✅ AVAILABLE — act now!",
            pr
        )
        alerted.add(pl)
        sent += 1

    print(f"\nDone. {sent} alert(s) sent.")

if __name__ == "__main__":
    main()
