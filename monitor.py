import os
import re
import json
import unicodedata
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
PUSHOVER_USER       = os.environ.get('PUSHOVER_USER_KEY', '')
PUSHOVER_TOKEN      = os.environ.get('PUSHOVER_API_TOKEN', '')
LEAGUE_ID           = os.environ.get('YAHOO_LEAGUE_ID', '')
YAHOO_CLIENT_ID     = os.environ.get('YAHOO_CLIENT_ID', '')
YAHOO_CLIENT_SECRET = os.environ.get('YAHOO_CLIENT_SECRET', '')
MY_TEAM_ID          = 10
ET_TZ               = ZoneInfo("America/New_York")
MIN_EXPECTED_ROSTERED = 100

PROBABLES_FILE      = '/tmp/morning_probables.json'
SITTING_ALERTS_FILE = '/tmp/sitting_alerts.json'
SEEN_ALERTS_FILE    = '/tmp/seen_alerts.json'

# ============================================================
# CONSTANTS
# ============================================================
TOP_15_SS = [
    "Gunnar Henderson", "Bobby Witt Jr.", "Trea Turner",
    "Francisco Lindor", "Corey Seager", "CJ Abrams",
    "Anthony Volpe", "Elly De La Cruz", "Jeremy Pena",
    "Willy Adames", "JP Crawford", "Carlos Correa",
    "Ezequiel Tovar", "Dansby Swanson", "Jackson Holliday"
]
MY_SS            = ["gunnar henderson", "trea turner"]
STRONG_POSITIONS = {'SS', '1B', 'OF'}
COLON_FORMAT_SOURCES = {'Rotowire', 'MLB Trade Rumors'}

# Sources that publish transaction news in reliable formats
TRANSACTION_SOURCES = {'Rotowire', 'MLB Trade Rumors'}

# Sources that mix feature articles with news — require transaction verb
FEATURE_RISK_SOURCES = {'ESPN MLB', 'MLB.com Official', 'MiLB Official',
                         'r/fantasybaseball', 'r/baseball'}

# Explicit transaction verbs — required for non-Rotowire sources
TRANSACTION_VERBS = [
    'placed on', 'activated', 'reinstated', 'recalled', 'promoted',
    'called up', 'designated for assignment', 'dfa', 'outrighted',
    'traded', 'acquired', 'signed', 'released', 'optioned', 'demoted',
    'suspended', 'transferred', 'selected', 'claimed', 'purchased'
]

NON_PLAYER_PREFIXES = {
    'mlb', 'nfl', 'nba', 'nhl', 'report', 'breaking', 'update',
    'fantasy', 'rotowire', 'espn', 'video', 'watch', 'photos',
    'power rankings', 'week in review', 'trade deadline', 'opening day',
    'spring training', 'trade rumors', 'injury report', 'minor leagues',
    'milb', 'prospects', 'roster moves', 'transactions', 'waiver wire',
    'free agency', 'offseason', 'playoffs', 'world series', 'all-star',
    'draft', 'podcast', 'analysis', 'preview', 'recap', 'highlights',
    'morning report', 'daily notes', 'sources', 'exclusive'
}

TEAM_NAME_MAP = {
    'BAL': 'Baltimore Orioles',    'BOS': 'Boston Red Sox',
    'NYY': 'New York Yankees',     'TB':  'Tampa Bay Rays',
    'TOR': 'Toronto Blue Jays',    'CWS': 'Chicago White Sox',
    'CLE': 'Cleveland Guardians',  'DET': 'Detroit Tigers',
    'KC':  'Kansas City Royals',   'MIN': 'Minnesota Twins',
    'HOU': 'Houston Astros',       'LAA': 'Los Angeles Angels',
    'ATH': 'Athletics',            'SEA': 'Seattle Mariners',
    'TEX': 'Texas Rangers',        'ATL': 'Atlanta Braves',
    'MIA': 'Miami Marlins',        'NYM': 'New York Mets',
    'PHI': 'Philadelphia Phillies','WSH': 'Washington Nationals',
    'CHC': 'Chicago Cubs',         'CIN': 'Cincinnati Reds',
    'MIL': 'Milwaukee Brewers',    'PIT': 'Pittsburgh Pirates',
    'STL': 'St. Louis Cardinals',  'AZ':  'Arizona Diamondbacks',
    'COL': 'Colorado Rockies',     'LAD': 'Los Angeles Dodgers',
    'SD':  'San Diego Padres',     'SF':  'San Francisco Giants',
}

SS_INJURY_KEYWORDS = [
    'injured', 'il', 'injured list', 'day-to-day', 'placed on',
    'disabled', 'hamstring', 'oblique', 'knee', 'wrist', 'shoulder',
    'elbow', 'back', 'thumb', 'ankle', 'concussion', 'surgery', 'fracture'
]

CLOSER_KEYWORDS = [
    'closer', 'closing role', 'save opportunity', 'saves role',
    'ninth inning', 'closing duties', 'holds the closer',
    'closing games', 'save situation'
]

ACTION_KEYWORDS = [
    'called up', 'promoted', 'recalled', 'call-up',
    'closer', 'closing role', 'save opportunity', 'ninth inning',
    'activated', 'reinstated', 'returns from il', 'comes off il',
    'placed on il', 'injured list', 'day-to-day', 'goes on il',
    'designated for assignment', 'dfa', 'outrighted',
    'trade', 'acquired', 'traded', 'signed', 'released',
    'starting lineup', 'leadoff', 'everyday', 'regular',
    'optioned', 'demoted', 'scratched', 'suspended'
]

# Non-events — returns from these lists are never actionable
NON_EVENT_LIST_KEYWORDS = [
    'paternity', 'bereavement', 'family medical',
    'paternity list', 'bereavement list'
]

# "Debut" is only actionable when forward-looking, not in recaps
DEBUT_FORWARD_SIGNALS = [
    'set to debut', 'will debut', 'expected to debut',
    'scheduled to debut', 'could debut', 'may debut',
    'first career', 'first major league', 'first mlb'
]

TOP_PROSPECTS = {
    "jackson holliday", "wyatt langford", "jackson chourio",
    "evan carter", "junior caminero", "cole young",
    "colson montgomery", "noah schultz", "charlie condon",
    "walker jenkins", "bryce eldridge", "spencer jones",
    "konnor griffin", "rhett lowder", "chase burns",
    "andrew painter", "cade horton", "jackson merrill",
    "james wood", "dylan crews", "paul skenes",
    "xavier isaac", "kyle manzardo", "travis bazzana",
    "hagen smith", "jac caglianone", "brayden taylor",
    "noble meyer", "braden montgomery", "max clark",
    "arjun nimmala", "pete crow-armstrong", "owen caissie",
    "matt shaw", "christian scott", "brandon sproat",
    "tyler black", "sal stewart", "jacob gonzalez",
    "tanner bibee", "gavin williams", "bo naylor",
    "jasson dominguez", "anthony volpe", "eury perez",
    "elly de la cruz", "noelvi marte", "rece hinds",
    "cam collier", "gavin stone", "emmet sheehan",
    "taj bradley", "colton cowser", "heston kjerstad",
    "chase davis", "hurston waldrep", "joey loperfido",
    "coby mayo", "yainer diaz", "matt mclain",
    "jacob berry", "cam smith", "theo hardy",
    "aidan miller", "jurrangelo cijntje", "nolan schanuel",
    "enmanuel valdez", "ben brown", "hayden wesneski",
    "jose cuas", "everson pereira", "oswald peraza",
    "peyton burdick", "jake burger", "landon knack",
    "james outman", "michael busch", "ryan pepiot",
    "josh lowe", "randy arozarena", "kyle stowers",
    "jackson chourio", "sal frelick", "joey wiemer",
    "adley rutschman", "jordan westburg", "chayce mcdermott",
    "grayson rodriguez", "dean kremer", "coleman crow"
}

# ============================================================
# NEWS SOURCES
# ============================================================
TIER1_SOURCES = [
    {"name": "Rotowire",         "url": "https://www.rotowire.com/baseball/rss.xml",         "type": "fantasy"},
    {"name": "MLB Trade Rumors", "url": "https://www.mlbtraderumors.com/feed",                "type": "transactions"},
    {"name": "ESPN MLB",         "url": "https://www.espn.com/espn/rss/mlb/news",             "type": "news"},
    {"name": "MLB.com Official", "url": "https://www.mlb.com/feeds/news/rss.xml",             "type": "news"},
    {"name": "MiLB Official",    "url": "https://www.milb.com/feeds/news/rss.xml",            "type": "prospects"},
]

TIER2_SOURCES = [
    {"name": "r/fantasybaseball", "url": "https://www.reddit.com/r/fantasybaseball/new/.rss", "type": "reddit"},
    {"name": "r/baseball",        "url": "https://www.reddit.com/r/baseball/new/.rss",        "type": "reddit"},
]

TIER3_SOURCES = [
    {"name": "MLB-orioles",    "url": "https://www.mlb.com/orioles/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-redsox",     "url": "https://www.mlb.com/red-sox/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-yankees",    "url": "https://www.mlb.com/yankees/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-rays",       "url": "https://www.mlb.com/rays/feeds/news/rss.xml",       "type": "team"},
    {"name": "MLB-bluejays",   "url": "https://www.mlb.com/blue-jays/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-whitesox",   "url": "https://www.mlb.com/white-sox/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-guardians",  "url": "https://www.mlb.com/guardians/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-tigers",     "url": "https://www.mlb.com/tigers/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-royals",     "url": "https://www.mlb.com/royals/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-twins",      "url": "https://www.mlb.com/twins/feeds/news/rss.xml",      "type": "team"},
    {"name": "MLB-astros",     "url": "https://www.mlb.com/astros/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-angels",     "url": "https://www.mlb.com/angels/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-athletics",  "url": "https://www.mlb.com/athletics/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-mariners",   "url": "https://www.mlb.com/mariners/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-rangers",    "url": "https://www.mlb.com/rangers/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-braves",     "url": "https://www.mlb.com/braves/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-marlins",    "url": "https://www.mlb.com/marlins/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-mets",       "url": "https://www.mlb.com/mets/feeds/news/rss.xml",       "type": "team"},
    {"name": "MLB-phillies",   "url": "https://www.mlb.com/phillies/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-nationals",  "url": "https://www.mlb.com/nationals/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-cubs",       "url": "https://www.mlb.com/cubs/feeds/news/rss.xml",       "type": "team"},
    {"name": "MLB-reds",       "url": "https://www.mlb.com/reds/feeds/news/rss.xml",       "type": "team"},
    {"name": "MLB-brewers",    "url": "https://www.mlb.com/brewers/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-pirates",    "url": "https://www.mlb.com/pirates/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-cardinals",  "url": "https://www.mlb.com/cardinals/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-dbacks",     "url": "https://www.mlb.com/d-backs/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-rockies",    "url": "https://www.mlb.com/rockies/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-dodgers",    "url": "https://www.mlb.com/dodgers/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-padres",     "url": "https://www.mlb.com/padres/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-giants",     "url": "https://www.mlb.com/giants/feeds/news/rss.xml",     "type": "team"},
]

# ============================================================
# NAME NORMALIZATION
# ============================================================
def normalize_name(name):
    if not name:
        return ''
    name = re.sub(r'\s*\(.*?\)', '', name).strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return ' '.join(name.lower().split())

def looks_like_player_name(text):
    if not text:
        return False
    text = text.strip()
    if text.lower() in NON_PLAYER_PREFIXES:
        return False
    if any(text.lower().startswith(p) for p in NON_PLAYER_PREFIXES):
        return False
    words = text.split()
    if not (2 <= len(words) <= 4):
        return False
    suffixes = {'jr.', 'sr.', 'ii', 'iii', 'iv'}
    for word in words:
        if word.lower() in suffixes:
            continue
        if not word[0].isupper():
            return False
    non_name_words = {
        'mlb', 'nfl', 'nba', 'nhl', 'espn', 'the', 'for', 'and',
        'power', 'rankings', 'trade', 'deadline', 'spring', 'training',
        'opening', 'day', 'world', 'series', 'all-star', 'free', 'agency',
        'report', 'update', 'breaking', 'fantasy', 'baseball', 'weekly',
        'daily', 'morning', 'sources', 'video', 'watch', 'review', 'week'
    }
    for word in words:
        if word.lower() in non_name_words:
            return False
    return True

# ============================================================
# TRANSACTION ARTICLE FILTER
# Non-Rotowire sources must contain an explicit transaction verb
# to be considered actionable. This eliminates feature articles,
# recaps, and opinion pieces that contain fantasy keywords
# but describe no actual roster move.
# ============================================================
def is_transaction_article(item):
    """
    Returns True if this article describes an actual roster transaction.
    For trusted sources (Rotowire, MLB Trade Rumors) — always True.
    For all other sources — requires an explicit transaction verb.
    """
    source = item.get('source', '')
    if source in TRANSACTION_SOURCES:
        return True

    text = (item['title'] + ' ' + item['summary']).lower()

    # Must contain at least one explicit transaction verb
    if not any(verb in text for verb in TRANSACTION_VERBS):
        print(f"  Skipping [{source}] — no transaction verb found")
        return False

    # Must NOT be a past-tense recap of something that already happened
    # "debut" is only actionable when forward-looking
    if 'debut' in text:
        if not any(signal in text for signal in DEBUT_FORWARD_SIGNALS):
            # "debut" appears but no forward signal — it's a recap
            print(f"  Skipping [{source}] — debut in recap context")
            return False

    return True

# ============================================================
# FANTASY RELEVANCE FILTER
# All alert types pass through this — no exceptions
# ============================================================
def is_fantasy_relevant(player_name, text):
    norm = normalize_name(player_name)

    # Always relevant: top prospects
    if norm in TOP_PROSPECTS:
        return True

    # Always relevant: closer/saves situation
    if any(w in text for w in CLOSER_KEYWORDS):
        return True

    # Always relevant: explicit everyday/starting role
    everyday_words = [
        'everyday', 'regular', 'starting', 'full-time',
        'every day', 'leadoff', 'cleanup', 'lineup'
    ]
    if any(w in text for w in everyday_words):
        return True

    # Suppress: paternity/bereavement returns
    if any(w in text for w in NON_EVENT_LIST_KEYWORDS):
        return False

    # Suppress: explicit low-value signals
    low_value  = ['utility', 'bench', 'depth', 'non-roster', 'september']
    high_value = ['prospect', 'top', 'ranked', 'first call',
                  'role', 'opportunity', 'closer', 'save', 'replace']
    low_count  = sum(1 for w in low_value  if w in text)
    high_count = sum(1 for w in high_value if w in text)
    if low_count > high_count and low_count >= 2:
        return False

    return True

# ============================================================
# PUSHOVER
# ============================================================
def send_pushover(title, message, priority=1):
    try:
        response = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token":    PUSHOVER_TOKEN,
                "user":     PUSHOVER_USER,
                "title":    title[:100],
                "message":  message[:1024],
                "priority": priority,
                "sound":    "siren"
            },
            timeout=10
        )
        print(f"  Alert sent ({response.status_code}): {title}")
    except Exception as e:
        print(f"  Pushover error: {e}")

def strip_html(text):
    return re.sub('<[^<]+?>', '', str(text)).strip()

# ============================================================
# STATE PERSISTENCE
# ============================================================
def load_morning_probables():
    try:
        if not Path(PROBABLES_FILE).exists():
            return {}
        with open(PROBABLES_FILE, 'r') as f:
            data = json.load(f)
        if data.get('date') != date.today().isoformat():
            return {}
        return data.get('probables', {})
    except Exception as e:
        print(f"  Could not load morning probables: {e}")
        return {}

def save_morning_probables(probables):
    try:
        data = {'date': date.today().isoformat(), 'probables': probables}
        with open(PROBABLES_FILE, 'w') as f:
            json.dump(data, f)
        print(f"  Saved {len(probables)} morning probables")
    except Exception as e:
        print(f"  Could not save morning probables: {e}")

def load_sitting_alerts():
    try:
        if not Path(SITTING_ALERTS_FILE).exists():
            return {}
        with open(SITTING_ALERTS_FILE, 'r') as f:
            data = json.load(f)
        if data.get('date') != date.today().isoformat():
            return {}
        return data.get('alerted', {})
    except Exception:
        return {}

def save_sitting_alerts(alerted):
    try:
        data = {'date': date.today().isoformat(), 'alerted': alerted}
        with open(SITTING_ALERTS_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"  Could not save sitting alerts: {e}")

def load_seen_alerts():
    try:
        if not Path(SEEN_ALERTS_FILE).exists():
            return {}
        with open(SEEN_ALERTS_FILE, 'r') as f:
            data = json.load(f)
        cutoff = datetime.now(timezone.utc).timestamp() - (4 * 3600)
        return {k: v for k, v in data.items() if v > cutoff}
    except Exception:
        return {}

def save_seen_alerts(seen):
    try:
        with open(SEEN_ALERTS_FILE, 'w') as f:
            json.dump(seen, f)
    except Exception as e:
        print(f"  Could not save seen alerts: {e}")

def mark_alert_seen(player_name, alert_type, seen_dict):
    key = f"{normalize_name(player_name)}:{alert_type}"
    seen_dict[key] = datetime.now(timezone.utc).timestamp()

def is_alert_seen(player_name, alert_type, seen_dict):
    key = f"{normalize_name(player_name)}:{alert_type}"
    return key in seen_dict

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
    try:
        query     = get_yahoo_query()
        today     = date.today()
        taken     = set()
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
                    taken.add(normalize_name(name))
                    if team_id == MY_TEAM_ID:
                        try:
                            my_roster.append({
                                'name':              name,
                                'name_normalized':   normalize_name(name),
                                'position':          player.primary_position,
                                'pct_owned':         float(getattr(player.percent_owned, 'value', 0) or 0),
                                'is_undroppable':    int(getattr(player, 'is_undroppable', 0) or 0),
                                'status':            str(getattr(player, 'status', '') or ''),
                                'selected_position': (
                                    player.selected_position.position
                                    if hasattr(player, 'selected_position') else ''),
                                'team_abbr':         str(getattr(player, 'editorial_team_abbr', '') or ''),
                            })
                        except Exception:
                            pass
            except Exception as e:
                print(f"  Team {team_id} error: {e}")

        if len(taken) < MIN_EXPECTED_ROSTERED:
            print(f"  ⚠️ Only {len(taken)} players — Yahoo may have failed")
            send_pushover(
                "⚠️ SYSTEM WARNING",
                f"Yahoo returned only {len(taken)} players. Alerts suppressed.",
                priority=0
            )
            return None, None

        print(f"  {len(taken)} rostered, {len(my_roster)} on my team")
        return taken, my_roster

    except Exception as e:
        print(f"  Yahoo error: {e}")
        send_pushover(
            "⚠️ SYSTEM WARNING",
            f"Yahoo connection failed: {str(e)[:200]}.",
            priority=0
        )
        return None, None

def get_drop_candidates(my_roster, count=3):
    candidates = [
        p for p in my_roster
        if not p['is_undroppable']
        and 'IL' not in p['status']
        and p['position'] in ['SP', 'RP', 'P']
    ]
    candidates.sort(key=lambda x: x['pct_owned'])
    return candidates[:count]

def get_weak_positions(my_roster):
    weak   = []
    by_pos = {}
    for p in my_roster:
        pos = p['position']
        if pos not in by_pos:
            by_pos[pos] = []
        by_pos[pos].append(p)
    for pos, players in by_pos.items():
        if pos in STRONG_POSITIONS or pos in ['BN', 'Util', 'IL']:
            continue
        for p in players:
            if 'IL' in (p['status'] or '') or p['pct_owned'] < 65:
                if pos not in weak:
                    weak.append(pos)
    return weak

# ============================================================
# MLB STATS API
# ============================================================
def get_todays_schedule():
    try:
        today_str = date.today().strftime('%Y-%m-%d')
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&date={today_str}&gameType=R"
            f"&hydrate=probablePitcher,lineups,status"
        )
        data  = requests.get(url, timeout=15).json()
        games = []
        for day in data.get('dates', []):
            for game in day.get('games', []):
                teams   = game.get('teams', {})
                home    = teams.get('home', {})
                away    = teams.get('away', {})
                lineups = game.get('lineups', {})
                games.append({
                    'home_team':     home.get('team', {}).get('name', ''),
                    'away_team':     away.get('team', {}).get('name', ''),
                    'home_probable': (home.get('probablePitcher') or {}).get('fullName', ''),
                    'away_probable': (away.get('probablePitcher') or {}).get('fullName', ''),
                    'status':        game.get('status', {}).get('detailedState', ''),
                    'game_time_utc': game.get('gameDate', ''),
                    'home_lineup':   [p.get('fullName', '') for p in lineups.get('homePlayers', [])],
                    'away_lineup':   [p.get('fullName', '') for p in lineups.get('awayPlayers', [])],
                })
        print(f"  Schedule: {len(games)} games today")
        return games
    except Exception as e:
        print(f"  Schedule API error: {e}")
        return []

def get_games_in_progress(games):
    active = set()
    for game in games:
        if game['status'] in ['In Progress', 'Final', 'Game Over',
                               'Manager challenge', 'Delay', 'Rain Delay']:
            active.add(game['home_team'])
            active.add(game['away_team'])
    return active

def game_starts_soon(game, hours=3):
    try:
        game_time = game.get('game_time_utc', '')
        if not game_time:
            return True
        game_dt = datetime.strptime(
            game_time[:19], '%Y-%m-%dT%H:%M:%S'
        ).replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        hours_until = (game_dt - now_utc).total_seconds() / 3600
        return -1 <= hours_until <= hours
    except Exception:
        return True

def get_team_batting_stats():
    try:
        url  = ("https://statsapi.mlb.com/api/v1/teams/stats"
                "?season=2026&group=hitting&stats=season&sportId=1")
        data = requests.get(url, timeout=10).json()
        team_ops = {}
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                team_name = split.get('team', {}).get('name', '')
                ops_val   = split.get('stat', {}).get('ops', '') or ''
                try:
                    team_ops[team_name] = float(ops_val)
                except (ValueError, TypeError):
                    pass
        print(f"  Team batting stats: {len(team_ops)} teams")
        return team_ops
    except Exception as e:
        print(f"  Team stats API error: {e}")
        return {}

def get_probable_pitchers_with_matchups(start_date, end_date, team_ops):
    try:
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&startDate={start_date}&endDate={end_date}"
            f"&gameType=R&hydrate=probablePitcher"
        )
        data     = requests.get(url, timeout=15).json()
        pitchers = {}
        for day in data.get('dates', []):
            for game in day.get('games', []):
                game_date = day.get('date', '')
                for side, opp_side in [('home', 'away'), ('away', 'home')]:
                    p = game.get('teams', {}).get(side, {}).get('probablePitcher', {})
                    opp_team = (game.get('teams', {})
                                    .get(opp_side, {})
                                    .get('team', {})
                                    .get('name', ''))
                    if p and p.get('fullName'):
                        n       = p['fullName']
                        pid     = p.get('id', 0)
                        opp_ops = team_ops.get(opp_team, 0.720)
                        if n not in pitchers:
                            pitchers[n] = {
                                'count': 0, 'id': pid,
                                'dates': [], 'opponents': [], 'opp_ops': []
                            }
                        pitchers[n]['count'] += 1
                        pitchers[n]['dates'].append(game_date)
                        pitchers[n]['opponents'].append(opp_team)
                        pitchers[n]['opp_ops'].append(opp_ops)
        return pitchers
    except Exception as e:
        print(f"  Probable pitchers API error: {e}")
        return {}

def get_pitcher_stats_blended(player_id):
    today        = date.today()
    season_start = date(today.year, 3, 20)
    days_in      = (today - season_start).days

    if days_in < 26:
        w_prior, w_curr = 0.80, 0.20
    elif days_in < 57:
        w_prior, w_curr = 0.60, 0.40
    elif days_in < 103:
        w_prior, w_curr = 0.35, 0.65
    else:
        w_prior, w_curr = 0.10, 0.90

    def fetch_stats(season):
        try:
            url  = (f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                    f"?stats=season&group=pitching&season={season}")
            data = requests.get(url, timeout=5).json()
            for sg in data.get('stats', []):
                for split in sg.get('splits', []):
                    s = split.get('stat', {})
                    try:
                        ip = float(s.get('inningsPitched', '0') or '0')
                        gs = int(s.get('gamesStarted', 0) or 0)
                        return {
                            'era':          float(s.get('era',  '99.99') or '99.99'),
                            'whip':         float(s.get('whip', '9.99')  or '9.99'),
                            'k':            int(s.get('strikeOuts', 0)    or 0),
                            'ip':           ip,
                            'gs':           gs,
                            'ip_per_start': round(ip / gs, 1) if gs > 0 else 0,
                            'kbb':          float(s.get('strikeoutWalkRatio', '0') or '0'),
                        }
                    except Exception:
                        pass
        except Exception:
            pass
        return None

    curr  = fetch_stats(today.year)
    prior = fetch_stats(today.year - 1)

    if prior is None and curr is not None:
        curr['blend_note'] = 'rookie/no prior stats'
        return curr
    if curr is None and prior is not None:
        prior['blend_note'] = 'no current stats yet'
        return prior
    if curr is None and prior is None:
        return {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0,
                'gs': 0, 'ip_per_start': 0, 'kbb': 0.0}

    ip_per_start = prior.get('ip_per_start', 0) or curr.get('ip_per_start', 0)

    return {
        'era':          round(prior['era']  * w_prior + curr['era']  * w_curr, 2),
        'whip':         round(prior['whip'] * w_prior + curr['whip'] * w_curr, 2),
        'kbb':          round(prior['kbb']  * w_prior + curr['kbb']  * w_curr, 2),
        'k':            curr['k'],
        'ip':           curr['ip'],
        'gs':           curr.get('gs', 0),
        'ip_per_start': ip_per_start,
        'blend_note':   f"{int(w_prior*100)}% prior / {int(w_curr*100)}% current"
    }

def is_opener(stats):
    ip_per_start = stats.get('ip_per_start', 0)
    gs           = stats.get('gs', 0)
    ip           = stats.get('ip', 0)
    if gs < 2:
        if gs > 0 and ip > 0:
            return (ip / gs) < 3.0
        return False
    return ip_per_start < 3.0

def passes_spot_start_gate(stats, opp_ops):
    if stats.get('ip', 0) < 5:
        return False
    if is_opener(stats):
        return False
    era  = stats.get('era',  99)
    whip = stats.get('whip',  9)
    kbb  = stats.get('kbb',   0)
    if opp_ops <= 0.690:
        return era < 4.50 and whip < 1.35 and kbb > 1.8
    elif opp_ops <= 0.730:
        return era < 4.00 and whip < 1.25 and kbb > 2.2
    else:
        return era < 3.50 and whip < 1.15 and kbb > 2.8

def passes_quality_gate(stats, strict=True):
    if is_opener(stats):
        return False
    if strict:
        return (
            stats.get('ip', 0)   >= 10
            and stats.get('era',  99) < 4.00
            and stats.get('whip',  9) < 1.30
            and stats.get('kbb',   0) > 2.0
        )
    else:
        return (
            stats.get('ip', 0)   >= 5
            and stats.get('era',  99) < 4.50
            and stats.get('whip',  9) < 1.35
            and stats.get('kbb',   0) > 1.8
        )

def score_pitcher(stats):
    if stats.get('ip', 0) < 5:
        return -999
    if is_opener(stats):
        return -999
    return (
        stats.get('k',   0) * 0.5
        + stats.get('kbb', 0) * 10
        - stats.get('era', 5) * 5
        - stats.get('whip', 1.4) * 20
    )

def matchup_label(opp_ops):
    if opp_ops   <= 0.680: return '✅ Great'
    elif opp_ops <= 0.720: return '✅ Good'
    elif opp_ops <= 0.750: return '⚠️ Neutral'
    else:                  return '❌ Tough'

# ============================================================
# ACTIONABILITY FILTER
# ============================================================
def extract_player_name(item):
    source  = item.get('source', '')
    title   = item.get('title', '')
    summary = item.get('summary', '')

    if source in COLON_FORMAT_SOURCES and ':' in title:
        candidate = title.split(':')[0].strip()
        if looks_like_player_name(candidate):
            return candidate

    full_text  = title + ' ' + summary
    pattern    = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b'
    candidates = re.findall(pattern, full_text)
    for candidate in candidates:
        if looks_like_player_name(candidate):
            return candidate
    return None

def find_named_replacements(text, taken):
    results = []
    pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b'
    candidates = re.findall(pattern, text)
    role_context_words = [
        'closer', 'closing', 'ninth', 'saves', 'replace',
        'fill', 'step in', 'takeover', 'role', 'inherit'
    ]
    for candidate in candidates:
        if not looks_like_player_name(candidate):
            continue
        idx = text.lower().find(candidate.lower())
        if idx == -1:
            continue
        surrounding = text[max(0, idx-100):idx+100].lower()
        if any(w in surrounding for w in role_context_words):
            norm = normalize_name(candidate)
            results.append((candidate, norm not in taken))
    return results

def get_actionability(item, taken):
    if item['type'] == 'reddit':
        return False, '', 0, None, {}

    # CRITICAL: Non-Rotowire sources must be transaction articles
    if not is_transaction_article(item):
        return False, '', 0, None, {}

    text   = (item['title'] + ' ' + item['summary']).lower()
    player = extract_player_name(item)

    if not player:
        return False, '', 0, None, {}

    player_normalized = normalize_name(player)
    if player_normalized in taken:
        return False, '', 0, None, {}

    # All alert types pass through fantasy relevance — no exceptions
    if not is_fantasy_relevant(player, text):
        print(f"  Skipping {player} — not fantasy relevant")
        return False, '', 0, None, {}

    if not any(kw in text for kw in ACTION_KEYWORDS):
        return False, '', 0, None, {}

    extra = {}

    # ── CALLUP ────────────────────────────────────────────────
    if any(w in text for w in ['called up', 'promoted', 'recalled', 'call-up']):
        return True, '🚀 CALLUP', 1, player, extra

    # ── CLOSER ROLE ───────────────────────────────────────────
    if any(w in text for w in CLOSER_KEYWORDS):
        return True, '💾 CLOSER ROLE', 1, player, extra

    # ── IL RETURN — requires role signal, no paternity/bereavement
    if any(w in text for w in ['activated', 'reinstated', 'returns from il',
                                'comes off il', 'off the il', 'cleared to return']):
        role_signals = [
            'everyday', 'regular', 'starting', 'lineup', 'closer',
            'cleanup', 'leadoff', 'ace', 'rotation', 'saves',
            'full-time', 'impact', 'key', 'star', 'elite'
        ]
        if any(w in text for w in role_signals):
            return True, '✅ IL RETURN', 1, player, extra
        print(f"  Skipping {player} IL return — no role signal")
        return False, '', 0, None, {}

    # ── INJURY OPPORTUNITY ────────────────────────────────────
    if any(w in text for w in ['placed on il', 'injured list',
                                'day-to-day', 'goes on il', 'to the il']):
        full_text        = item['title'] + ' ' + item['summary']
        is_closer_injury = any(w in text for w in CLOSER_KEYWORDS)

        if is_closer_injury:
            replacements           = find_named_replacements(full_text, taken)
            available_replacements = [r for r in replacements if r[1]]
            owned_replacements     = [r for r in replacements if not r[1]]
            extra['is_closer_injury']       = True
            extra['available_replacements'] = available_replacements
            extra['owned_replacements']     = owned_replacements
            if available_replacements:
                return True, '💾 SAVES OPP', 1, player, extra
            else:
                extra['watch_mode'] = True
                return True, '💾 SAVES WATCH', 0, player, extra

        opp_words = ['start', 'lineup', 'replac', 'fill', 'opportunit',
                     'role', 'regular', 'everyday', 'every day',
                     'platoon', 'takeover']
        if any(w in text for w in opp_words):
            return True, '🚑 INJURY OPP', 1, player, extra
        return False, '', 0, None, {}

    # ── DFA → CALLUP ──────────────────────────────────────────
    if any(w in text for w in ['designated for assignment', 'dfa', 'outrighted']):
        callup_words = ['prospect', 'called up', 'promoted', 'minor league',
                        'aaa', 'triple-a', 'recall', 'top prospect']
        if any(w in text for w in callup_words):
            return True, '🔄 DFA→CALLUP OPP', 1, player, extra
        return False, '', 0, None, {}

    # ── TRADE OPPORTUNITY ─────────────────────────────────────
    if any(w in text for w in ['trade', 'acquired', 'traded']):
        role_words = ['everyday', 'starting', 'regular', 'lineup',
                      'closer', 'opportunit', 'full-time', 'every day']
        if any(w in text for w in role_words):
            return True, '🔁 TRADE OPP', 0, player, extra
        return False, '', 0, None, {}

    return False, '', 0, None, {}

def build_alert_message(alert_type, player, summary, source, extra):
    if alert_type == '💾 SAVES OPP':
        available = extra.get('available_replacements', [])
        if available:
            grab_names = ', '.join(r[0] for r in available[:2])
            return (
                f"{player} (closer) placed on IL.\n\n"
                f"🎯 Grab NOW — {grab_names} available in your league "
                f"and may inherit saves!\n\nSource: {source}"
            )
        return (
            f"{player} (closer) placed on IL.\n\n"
            f"⚠️ Saves situation open — check bullpen free agents "
            f"in Yahoo!\n\nSource: {source}"
        )

    if alert_type == '💾 SAVES WATCH':
        owned = extra.get('owned_replacements', [])
        if owned:
            owned_names = ', '.join(r[0] for r in owned[:2])
            return (
                f"{player} (closer) placed on IL.\n\n"
                f"👀 MONITOR: {owned_names} mentioned as replacement "
                f"but already owned. Watch for role clarification.\n\n"
                f"Source: {source}"
            )
        return (
            f"{player} (closer) placed on IL.\n\n"
            f"👀 MONITOR: No replacement named yet. Watch for "
            f"saves role announcement.\n\nSource: {source}"
        )

    if alert_type == '🚑 INJURY OPP':
        return (
            f"{summary}\n\n"
            f"✅ {player} is AVAILABLE — role opportunity may exist. "
            f"Check Yahoo.\n\nSource: {source}"
        )

    return f"{summary}\n\n✅ AVAILABLE — act now!\n\nSource: {source}"

# ============================================================
# ALERT: SPOT START (Tue-Sat 9am)
# ============================================================
def send_spot_start_alert(taken, my_roster, games=None):
    print("Running spot start alert...")
    today     = datetime.now(ET_TZ).date()
    look_from = today + timedelta(days=1)
    look_to   = today + timedelta(days=3)

    team_ops     = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(look_from, look_to, team_ops)

    available = {}
    for name, info in all_starters.items():
        if normalize_name(name) in taken:
            continue
        stats = get_pitcher_stats_blended(info['id'])
        info['stats'] = stats
        if is_opener(stats):
            continue
        opp_ops_list = info.get('opp_ops', [0.720])
        if not any(passes_spot_start_gate(stats, ops) for ops in opp_ops_list):
            continue
        available[name] = info

    if not available:
        print("  No quality spot starts found")
        return

    ranked = sorted(
        available.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:3]

    drops    = get_drop_candidates(my_roster, count=2)
    drop_str = ' | '.join(
        f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops
    ) or "No obvious drops"

    lines = [f"📅 Spot starts {look_from} – {look_to}:\n"]
    for name, info in ranked:
        s         = info['stats']
        opponents = info.get('opponents', [])
        opp_ops   = info.get('opp_ops', [])
        dates     = info.get('dates', [])
        blend     = s.get('blend_note', '')
        stat_line = (
            f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | K/BB {s['kbb']:.1f}"
            if s['ip'] >= 5 else "Limited stats"
        )
        matchups = ', '.join(
            f"{d[5:]} vs {opp} {matchup_label(ops)}"
            for d, opp, ops in zip(dates[:3], opponents[:3], opp_ops[:3])
        )
        lines.append(
            f"• {name}\n"
            f"  {stat_line} ({blend})\n"
            f"  {matchups}"
        )
    lines.append(f"\n💀 Consider dropping:\n{drop_str}")
    send_pushover("🎯 SPOT START OPTIONS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: 2-START PITCHERS
# ============================================================
def send_two_start_alert(taken, my_roster, preliminary=False):
    label = "Friday preliminary" if preliminary else "Saturday full"
    print(f"Running {label} 2-start alert...")
    today      = datetime.now(ET_TZ).date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    next_mon   = today + timedelta(days=days_ahead)
    next_sun   = next_mon + timedelta(days=6)

    team_ops     = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(next_mon, next_sun, team_ops)
    two_starters = {n: i for n, i in all_starters.items() if i['count'] >= 2}

    if not two_starters:
        if not preliminary:
            send_pushover(
                "⚾ 2-START ALERT",
                f"No confirmed 2-starters yet for {next_mon}. Check back Sunday.",
                priority=0
            )
        return

    quality_options = {}
    for name, info in two_starters.items():
        if normalize_name(name) in taken:
            continue
        stats = get_pitcher_stats_blended(info['id'])
        info['stats'] = stats
        if is_opener(stats):
            continue
        if not passes_quality_gate(stats, strict=True):
            continue
        if min(info.get('opp_ops', [0.720, 0.720])) > 0.750:
            continue
        quality_options[name] = info

    if not quality_options:
        if not preliminary:
            send_pushover(
                "⚾ 2-START ALERT",
                f"No available 2-starters cleared quality + matchup "
                f"filters for {next_mon}.",
                priority=0
            )
        return

    ranked = sorted(
        quality_options.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:3]

    drops    = get_drop_candidates(my_roster, count=3)
    drop_str = ' | '.join(
        f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops
    ) or "No obvious drops"

    prefix = "📋 EARLY LOOK — " if preliminary else ""
    lines  = [f"{prefix}📅 Week of {next_mon}:\n"]
    for name, info in ranked:
        s         = info['stats']
        dates     = info.get('dates', [])
        opponents = info.get('opponents', [])
        opp_ops   = info.get('opp_ops', [])
        blend     = s.get('blend_note', '')
        stat_line = (
            f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f} ({blend})"
            if s['ip'] >= 5 else "Limited stats"
        )
        start_lines = []
        for i, (d, opp, ops) in enumerate(
                zip(dates[:2], opponents[:2], opp_ops[:2])):
            start_lines.append(
                f"  Start {i+1}: {d[5:]} vs {opp} {matchup_label(ops)}"
            )
        lines.append(f"• {name}\n  {stat_line}\n" + '\n'.join(start_lines))

    if not preliminary:
        lines.append(f"\n💀 Potential drops:\n{drop_str}")

    title = "⚾ 2-START EARLY LOOK" if preliminary else "⚾ 2-START SP TARGETS"
    send_pushover(title, '\n'.join(lines), priority=0)

# ============================================================
# ALERT: STREAMING PITCHERS
# ============================================================
def send_streaming_alert(taken, my_roster, games=None):
    print("Running streaming pitcher alert...")
    today        = datetime.now(ET_TZ).date()
    end_of_week  = today + timedelta(days=(6 - today.weekday()))
    team_ops     = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(today, end_of_week, team_ops)
    active_teams = get_games_in_progress(games) if games else set()

    available = {}
    for name, info in all_starters.items():
        if normalize_name(name) in taken:
            continue

        pitcher_team_active = False
        if games and info.get('dates') and info['dates'][0] == today.isoformat():
            for game in games:
                home_prob = normalize_name(game.get('home_probable', ''))
                away_prob = normalize_name(game.get('away_probable', ''))
                norm_name = normalize_name(name)
                if home_prob == norm_name and game['home_team'] in active_teams:
                    pitcher_team_active = True
                    break
                if away_prob == norm_name and game['away_team'] in active_teams:
                    pitcher_team_active = True
                    break

        if pitcher_team_active:
            print(f"  {name} skipped — game already in progress")
            continue

        s = get_pitcher_stats_blended(info['id'])
        info['stats'] = s
        if is_opener(s):
            continue
        if not passes_quality_gate(s, strict=False):
            continue
        if min(info.get('opp_ops', [0.720])) > 0.750:
            continue
        available[name] = info

    if not available:
        print("  No quality streaming options")
        return

    ranked = sorted(
        available.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:3]

    drops    = get_drop_candidates(my_roster, count=2)
    drop_str = ' | '.join(
        f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops
    ) or "No obvious drops"

    lines = [f"📅 Streaming through {end_of_week}:\n"]
    for name, info in ranked:
        s         = info['stats']
        starts    = info['count']
        opponents = info.get('opponents', [])
        opp_ops   = info.get('opp_ops', [])
        opp_str   = ', '.join(
            f"{opp} {matchup_label(ops)}"
            for opp, ops in zip(opponents[:2], opp_ops[:2])
        )
        lines.append(
            f"• {name} ({starts} start{'s' if starts > 1 else ''})\n"
            f"  ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f}\n"
            f"  vs {opp_str}"
        )
    lines.append(f"\n💀 Consider dropping:\n{drop_str}")
    send_pushover("🌊 STREAMING SP OPTIONS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: WIRE DIGEST (Mon/Tue/Fri 8:50am)
# ============================================================
def send_wire_digest(taken, my_roster):
    print("Running wire digest...")
    weak_positions = get_weak_positions(my_roster)
    print(f"  Weak positions: {weak_positions}")
    if not weak_positions:
        print("  No weak positions — skipping")
        return
    try:
        query           = get_yahoo_query()
        recommendations = []
        seen_names      = set()
        for pos in ['RP'] + [p for p in weak_positions if p != 'RP']:
            try:
                players = query.get_league_players(
                    player_count=15, position_filter=pos
                )
                if not players:
                    continue
                available = []
                for player in players:
                    try:
                        name = player.name.full
                        pct  = float(
                            getattr(player.percent_owned, 'value', 0) or 0
                        )
                        if normalize_name(name) in taken:
                            continue
                        available.append({'name': name, 'pct': pct, 'pos': pos})
                    except Exception:
                        continue
                available.sort(key=lambda x: x['pct'], reverse=True)
                for p in available:
                    if p['name'] not in seen_names and len(recommendations) < 3:
                        seen_names.add(p['name'])
                        recommendations.append(p)
                        break
            except Exception as e:
                print(f"  {pos} fetch error: {e}")

        if not recommendations:
            print("  No recommendations found")
            return

        drops    = get_drop_candidates(my_roster, count=3)
        drop_str = ' | '.join(
            f"{p['name']} ({p['pct_owned']:.0f}%)" for p in drops
        ) or "No obvious drops"

        weak_str = ', '.join(weak_positions)
        lines    = [f"📋 Your weak spots: {weak_str}\n"]
        for i, r in enumerate(recommendations, 1):
            lines.append(
                f"{i}. {r['name']} ({r['pos']}, {r['pct']:.0f}% owned)"
            )
        lines.append(f"\n💀 Potential drops:\n{drop_str}")
        lines.append("\n📱 Check Yahoo for full stats before acting.")
        send_pushover("📋 WIRE DIGEST", '\n'.join(lines), priority=0)

    except Exception as e:
        print(f"  Wire digest error: {e}")

# ============================================================
# ALERT: PITCHER SCRATCHED
# ============================================================
def store_morning_probables(games):
    probables = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']:
            continue
        if game['home_probable']:
            probables[game['home_team']] = game['home_probable']
        if game['away_probable']:
            probables[game['away_team']] = game['away_probable']
    save_morning_probables(probables)
    print(f"  Stored {len(probables)} morning probables")

def check_pitcher_scratched(my_roster, games):
    print("Checking pitcher scratches...")
    morning_probables = load_morning_probables()
    if not morning_probables:
        print("  No morning probables stored yet — skipping")
        return

    current_probables = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']:
            continue
        if game['home_probable']:
            current_probables[game['home_team']] = game['home_probable']
        if game['away_probable']:
            current_probables[game['away_team']] = game['away_probable']

    my_sps = [
        p for p in my_roster
        if p['position'] == 'SP'
        and 'IL' not in (p['status'] or '')
        and p['selected_position'] not in ['BN', 'IL']
    ]

    for sp in my_sps:
        team_name = TEAM_NAME_MAP.get(sp['team_abbr'], '')
        if not team_name:
            continue
        morning_starter = morning_probables.get(team_name, '')
        if normalize_name(morning_starter) != normalize_name(sp['name']):
            continue
        current_starter = current_probables.get(team_name, '')
        if not current_starter:
            continue
        if normalize_name(current_starter) != normalize_name(sp['name']):
            send_pushover(
                f"🚫 SCRATCH: {sp['name']}",
                f"{sp['name']} was this morning's probable for "
                f"{team_name} but has been replaced.\n"
                f"Now starting: {current_starter}\n\n"
                f"⚠️ Swap in a bench SP or grab a streamer!",
                priority=1
            )

# ============================================================
# ALERT: BATTER SITTING + POSTPONED
# ============================================================
def check_lineups_and_weather(my_roster, games):
    print("Checking lineups and postponements...")
    sitting_alerted = load_sitting_alerts()
    my_hitters = [
        p for p in my_roster
        if p['position'] not in ['SP', 'RP', 'P']
        and 'IL' not in (p['status'] or '')
        and p['selected_position'] not in ['BN', 'IL']
    ]
    newly_alerted = dict(sitting_alerted)

    for game in games:
        home_team     = game['home_team']
        away_team     = game['away_team']
        status        = game['status']
        all_lineup    = game['home_lineup'] + game['away_lineup']
        lineup_posted = len(all_lineup) > 0

        for hitter in my_hitters:
            team_name = TEAM_NAME_MAP.get(hitter['team_abbr'], '')
            if not team_name or team_name not in (home_team, away_team):
                continue
            player_key = normalize_name(hitter['name'])

            if status in ['Postponed', 'Suspended']:
                if player_key not in sitting_alerted:
                    send_pushover(
                        f"🌧️ POSTPONED: {hitter['name']}",
                        f"{away_team} @ {home_team} has been "
                        f"{status.lower()}.\n"
                        f"{hitter['name']} will not play today.\n\n"
                        f"⚠️ Swap in a bench hitter!",
                        priority=1
                    )
                    newly_alerted[player_key] = 'postponed'
                continue

            if not game_starts_soon(game, hours=3):
                print(
                    f"  Skipping lineup check for "
                    f"{hitter['name']} — game not soon"
                )
                continue

            if lineup_posted and status not in [
                    'Final', 'Game Over', 'In Progress']:
                if player_key in sitting_alerted:
                    continue
                in_lineup = any(
                    normalize_name(hitter['name']) in normalize_name(lp)
                    or normalize_name(lp) in normalize_name(hitter['name'])
                    for lp in all_lineup
                )
                if not in_lineup:
                    send_pushover(
                        f"🪑 SITTING: {hitter['name']}",
                        f"{hitter['name']} is NOT in today's lineup "
                        f"for {team_name}.\n\n"
                        f"⚠️ Swap in a bench hitter before lock!",
                        priority=1
                    )
                    newly_alerted[player_key] = 'sitting'

    save_sitting_alerts(newly_alerted)

# ============================================================
# SS INJURY DETECTION
# ============================================================
def is_ss_injury_news(item):
    text = (item['title'] + ' ' + item['summary']).lower()
    if not any(kw in text for kw in SS_INJURY_KEYWORDS):
        return False, None, False
    for ss in TOP_15_SS:
        if normalize_name(ss) in normalize_name(
                item['title'] + ' ' + item['summary']):
            return True, ss, normalize_name(ss) in MY_SS
    return False, None, False

# ============================================================
# RSS FEED FETCHING
# ============================================================
def fetch_feed(source, lookback_minutes=15):
    try:
        headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
        feed    = feedparser.parse(source["url"], request_headers=headers)
        cutoff  = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        items   = []
        for entry in feed.entries:
            try:
                pub = (
                    datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    if hasattr(entry, 'published_parsed')
                    and entry.published_parsed
                    else datetime.now(timezone.utc)
                )
                if pub < cutoff:
                    continue
                title   = strip_html(entry.get('title', ''))
                summary = strip_html(
                    entry.get('summary', entry.get('description', title))
                )
                summary = (
                    summary[:300] + '...' if len(summary) > 300 else summary
                )
                items.append({
                    'source':    source["name"],
                    'type':      source["type"],
                    'title':     title,
                    'summary':   summary,
                    'published': pub,
                })
            except Exception:
                continue
        if items:
            print(f"  {source['name']}: {len(items)} items")
        return items
    except Exception as e:
        print(f"  {source['name']} error: {e}")
        return []

def should_check_reddit():
    m = datetime.now(timezone.utc).minute
    return m < 16 or 30 <= m < 46

def get_all_news(lookback_minutes=15):
    items = []
    print("Checking Tier 1 sources...")
    for s in TIER1_SOURCES:
        items.extend(fetch_feed(s, lookback_minutes))

    if should_check_reddit():
        print("Checking Reddit (Tier 2)...")
        for s in TIER2_SOURCES:
            items.extend(fetch_feed(s, lookback_minutes))
    else:
        print("Skipping Reddit this run")

    print("Checking Tier 3 (30 MLB team feeds)...")
    tier3_count = 0
    for s in TIER3_SOURCES:
        new_items = fetch_feed(s, lookback_minutes)
        items.extend(new_items)
        tier3_count += len(new_items)
    print(f"  Tier 3 total: {tier3_count} items")

    print(f"Total: {len(items)} raw items")
    return items

# ============================================================
# NEWS PROCESSOR
# ============================================================
def process_news_alerts(news, taken, is_digest=False):
    actionable      = []
    alerted_players = set()
    alerted_ss      = set()
    seen_alerts     = load_seen_alerts()

    for item in news:
        ss_hit, ss_name, is_mine = is_ss_injury_news(item)
        if ss_hit and normalize_name(ss_name) not in alerted_ss:
            if is_alert_seen(ss_name, 'SS_INJURY', seen_alerts):
                print(f"  Skipping {ss_name} SS — already alerted recently")
                continue
            alerted_ss.add(normalize_name(ss_name))
            if not is_digest:
                send_pushover(
                    f"{'🚨' if is_mine else '👀'} SS INJURY: {ss_name}"
                    f"{' ← YOUR PLAYER!' if is_mine else ''}",
                    f"{item['summary'][:250]}\n\nSource: {item['source']}",
                    priority=1 if is_mine else 0
                )
                mark_alert_seen(ss_name, 'SS_INJURY', seen_alerts)
            else:
                actionable.append({
                    'alert_type': f"{'🚨' if is_mine else '👀'} SS INJURY",
                    'priority':   1 if is_mine else 0,
                    'player':     ss_name,
                    'summary':    item['summary'][:150],
                    'source':     item['source'],
                    'extra':      {}
                })
            continue

        is_actionable, alert_type, priority, player, extra = \
            get_actionability(item, taken)
        if not is_actionable:
            continue

        player_norm = normalize_name(player or '')
        if player_norm in alerted_players:
            continue

        if is_alert_seen(player, alert_type, seen_alerts):
            print(f"  Skipping {player} {alert_type} — alerted recently")
            continue

        alerted_players.add(player_norm)
        actionable.append({
            'alert_type': alert_type,
            'priority':   priority,
            'player':     player,
            'summary':    item['summary'],
            'source':     item['source'],
            'extra':      extra
        })

    if not actionable:
        save_seen_alerts(seen_alerts)
        return 0

    if is_digest:
        lines = [
            f"🌅 OVERNIGHT "
            f"({len(actionable)} item"
            f"{'s' if len(actionable) > 1 else ''}):\n"
        ]
        for a in actionable:
            lines.append(
                f"{a['alert_type']}: {a['player']}\n"
                f"{a['summary'][:150]}\n"
            )
        max_priority = max(a['priority'] for a in actionable)
        send_pushover(
            "🌅 OVERNIGHT DIGEST", '\n'.join(lines), priority=max_priority
        )
    else:
        for a in actionable:
            msg = build_alert_message(
                a['alert_type'], a['player'],
                a['summary'], a['source'], a['extra']
            )
            send_pushover(
                f"{a['alert_type']}: {a['player']} [{a['source']}]",
                msg, a['priority']
            )
            mark_alert_seen(a['player'], a['alert_type'], seen_alerts)

    save_seen_alerts(seen_alerts)
    return len(actionable)

# ============================================================
# MAIN
# ============================================================
def main():
    now_utc   = datetime.now(timezone.utc)
    now_et    = datetime.now(ET_TZ)
    hour_et   = now_et.hour
    minute_et = now_et.minute
    weekday   = now_et.weekday()

    print(f"\n{'='*50}")
    print(f"Run: {now_utc.strftime('%Y-%m-%d %H:%M UTC')} | "
          f"{now_et.strftime('%H:%M ET %A')}")
    print(f"{'='*50}")

    in_sleep = (
        hour_et >= 23
        or hour_et < 6
        or (hour_et == 6 and minute_et < 30)
    )

    overnight_digest_window  = (hour_et == 6  and 30 <= minute_et < 45)
    morning_probables_window = (hour_et == 8  and minute_et < 15)
    two_start_friday_pm      = (weekday == 4  and hour_et == 20 and minute_et < 15)
    two_start_saturday       = (weekday == 5  and hour_et == 8  and minute_et < 15)

    spot_start_window = (
        hour_et == 9 and minute_et < 15
        and weekday in [1, 2, 3, 4, 5]
    )

    streaming_window = (
        hour_et in [8, 20] and minute_et < 15
        and (
            (weekday == 2 and hour_et == 8)  or
            (weekday == 3)                   or
            (weekday == 4 and hour_et == 8)  or
            (weekday == 5 and hour_et == 8)  or
            (weekday == 6 and hour_et == 8)
        )
    )

    digest_window = (
        hour_et == 8 and 50 <= minute_et < 60
        and weekday in [0, 1, 4]
    )

    pitcher_scratch_window = (
        (hour_et == 8  and 30 <= minute_et < 45) or
        (hour_et == 11 and minute_et < 15)        or
        (hour_et == 14 and minute_et < 15)        or
        (hour_et == 16 and 30 <= minute_et < 45)
    )

    lineup_weather_window = (
        (hour_et == 10 and 30 <= minute_et < 45) or
        (hour_et == 13 and 30 <= minute_et < 45) or
        (hour_et == 16 and 30 <= minute_et < 45)
    )

    taken, my_roster, games = None, None, None

    if overnight_digest_window:
        print("\n--- OVERNIGHT DIGEST ---")
        overnight_news = get_all_news(lookback_minutes=450)
        taken, my_roster = get_all_rosters()
        if taken is None:
            print("  Yahoo failed — skipping")
        else:
            sent = process_news_alerts(overnight_news, taken, is_digest=True)
            if sent == 0:
                print("  Nothing actionable overnight")

    if morning_probables_window:
        print("\n--- STORING MORNING PROBABLES ---")
        if games is None:
            games = get_todays_schedule()
        store_morning_probables(games)

    if two_start_friday_pm:
        print("\n--- FRIDAY PRELIMINARY 2-START ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_two_start_alert(taken, my_roster, preliminary=True)

    if two_start_saturday:
        print("\n--- SATURDAY 2-START ALERT ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_two_start_alert(taken, my_roster, preliminary=False)

    if spot_start_window:
        print("\n--- SPOT START ALERT ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_spot_start_alert(taken, my_roster, games)

    if streaming_window:
        print("\n--- STREAMING PITCHER ALERT ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if games is None:
            games = get_todays_schedule()
        if taken is not None:
            send_streaming_alert(taken, my_roster, games)

    if digest_window:
        print("\n--- WIRE DIGEST ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_wire_digest(taken, my_roster)

    if pitcher_scratch_window:
        print("\n--- PITCHER SCRATCH CHECK ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            if games is None:
                games = get_todays_schedule()
            check_pitcher_scratched(my_roster, games)

    if lineup_weather_window:
        print("\n--- LINEUP + WEATHER CHECK ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            if games is None:
                games = get_todays_schedule()
            check_lineups_and_weather(my_roster, games)

    if not in_sleep and not overnight_digest_window:
        print("\n--- BREAKING NEWS CHECK ---")
        news = get_all_news(lookback_minutes=15)
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            sent = process_news_alerts(news, taken, is_digest=False)
            print(f"  {sent} alert(s) sent")
    elif in_sleep:
        print("\n[Sleep window — alerts resume at 6:30am ET]")

    print("\nDone.")

if __name__ == "__main__":
    main()
