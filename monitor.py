import os
import re
import json
import html
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
SEASON_START          = date(2026, 3, 20)

# ── Persistent state files ──────────────────────────────────
PROBABLES_FILE         = '/tmp/morning_probables.json'
SITTING_ALERTS_FILE    = '/tmp/sitting_alerts.json'
SEEN_ALERTS_FILE       = '/tmp/seen_alerts.json'
TRANSACTIONS_FILE      = '/tmp/league_transactions.json'
MATCHUP_CACHE_FILE     = '/tmp/matchup_cache.json'
CLOSERMONKEY_CACHE     = '/tmp/closermonkey_cache.json'
YAHOO_PLAYER_CACHE     = '/tmp/yahoo_player_cache.json'
SLEEP_QUEUE_FILE       = '/tmp/sleep_queue.json'
LEAGUEMATE_FILE        = '/tmp/leaguemate_profiles.json'
TRADE_HISTORY_FILE     = '/tmp/trade_proposals.json'
POS_ELIGIBILITY_FILE   = '/tmp/pos_eligibility_alerts.json'
SCRATCH_ALERTED_FILE   = '/tmp/scratch_alerted.json'

# ============================================================
# CONSTANTS
# ============================================================
MY_CLOGGED_POSITIONS = {'SS', 'OF'}
MY_UNDROPPABLE = {
    "gunnar henderson", "trea turner", "matt olson",
    "shohei ohtani", "nico hoerner"
}
MY_IL_SLOTS = 3  # Number of IL slots on my roster

TOP_15_SS = [
    "Gunnar Henderson", "Bobby Witt Jr.", "Trea Turner",
    "Francisco Lindor", "Corey Seager", "CJ Abrams",
    "Anthony Volpe", "Elly De La Cruz", "Jeremy Pena",
    "Willy Adames", "JP Crawford", "Carlos Correa",
    "Ezequiel Tovar", "Dansby Swanson", "Jackson Holliday"
]

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

CLOSER_KEYWORDS = [
    'closer', 'closing role', 'save opportunity', 'saves role',
    'ninth inning', 'closing duties', 'closing games', 'save situation',
    'closing out', 'closer role'
]

INJURY_KEYWORDS = [
    'placed on il', 'injured list', 'day-to-day', 'goes on il',
    'to the il', 'on the il', 'fracture', 'surgery', 'torn',
    'strain', 'sprain', 'concussion', 'oblique', 'hamstring',
    'disabled list', 'missed time', 'out indefinitely', 'out for'
]

MINOR_INJURY_KEYWORDS = [
    'paternity', 'bereavement', 'family medical', 'paternity list',
    'bereavement list', 'rest day', 'maintenance day'
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
    "sal frelick", "joey wiemer", "adley rutschman",
    "jordan westburg", "chayce mcdermott", "grayson rodriguez",
    "dean kremer", "connor prielipp", "bryce eldridge",
}

INVALID_NAME_WORDS = {
    'on', 'to', 'from', 'with', 'after', 'before', 'the', 'and', 'or',
    'in', 'at', 'by', 'for', 'of', 'injured', 'list', 'il', 'right',
    'left', 'elbow', 'knee', 'shoulder', 'wrist', 'hamstring', 'back',
    'thumb', 'ankle', 'concussion', 'surgery', 'fracture', 'sign',
    'place', 'trade', 'acquire', 'release', 'option', 'demote',
    'recall', 'promote', 'activate', 'reinstate', 'suspend',
    'designate', 'assign', 'claim', 'select', 'transfer', 'loose',
    'tight', 'sore', 'strained', 'sprained', 'bullpen', 'rotation',
    'lineup', 'roster', 'manager', 'coach', 'team', 'club', 'spring'
}

MINOR_LEAGUE_TEAMS = {
    'sugar land', 'salt lake', 'round rock', 'las vegas', 'el paso',
    'oklahoma city', 'iowa cubs', 'lehigh valley', 'durham bulls',
    'charlotte knights', 'columbus clippers', 'buffalo bisons',
    'scranton wilkes', 'pawtucket', 'norfolk tides', 'toledo mud',
    'louisville bats', 'gwinnett stripers', 'memphis redbirds',
    'nashville sounds', 'new orleans', 'reno aces', 'tacoma rainiers',
    'sacramento river', 'albuquerque isotopes', 'st paul saints',
    'worcester red sox', 'jacksonville jumbo', 'pensacola blue wahoos'
}

ACTION_VERBS = {
    'sign', 'place', 'trade', 'acquire', 'release', 'option',
    'demote', 'recall', 'promote', 'activate', 'reinstate',
    'suspend', 'designate', 'assign', 'claim', 'select', 'transfer',
    'waive', 'cut', 'drop', 'add', 'purchase', 'outrighted'
}

KNOWN_MEDIA_NAMES = {
    'eric karabell', 'francys romero', 'jeff passan', 'ken rosenthal',
    'jon heyman', 'bob nightengale', 'buster olney', 'tim kurkjian',
    'mark feinsand', 'joel sherman', 'george king', 'mike puma',
    'anthony castrovince', 'matt kelly', 'sarah langs', 'david adler',
    'jay paris', 'jim bowden', 'kiley mcdaniel', 'keith law',
    'sam miller', 'ben lindbergh', 'eno sarris', 'travis sawchik',
    'derrick goold', 'jesse rogers', 'jordan bastian', 'jane lee',
    'alex speier', 'peter gammons', 'jayson stark', 'pedro gomez',
    'eduardo perez', 'jessica mendoza', 'david schoenfield',
    'bradford doolittle', 'david laurila', 'c trent rosecrans',
    'nick cafardo', 'gordon edes', 'amie just', 'anne rogers',
    'scott miller', 'randy miller', 'danny knobler', 'michael silverman',
    'chad jennings', 'bryan hoch', 'marly rivera', 'enrique rojas',
    'jerry crasnick', 'jim callis', 'jonathan mayo', 'john manuel',
    'mike axisa', 'r j anderson', 'dayn perry', 'matt snyder',
    'mike oz', 'pat ragazzo', 'scott pianowski', 'andy behrens',
    'brad evans', 'liz loza', 'dalton del don', 'nando di fino',
    'derek vermilya', 'fred zinkie', 'greg jewett', 'clay link',
}

# ── News sources ─────────────────────────────────────────────
TIER1_SOURCES = [
    {"name": "Rotowire",         "url": "https://www.rotowire.com/baseball/rss.xml",               "type": "fantasy"},
    {"name": "MLB Trade Rumors", "url": "https://www.mlbtraderumors.com/feed",                     "type": "transactions"},
    {"name": "ESPN MLB",         "url": "https://www.espn.com/espn/rss/mlb/news",                  "type": "news"},
    {"name": "MLB.com Official", "url": "https://www.mlb.com/feeds/news/rss.xml",                  "type": "news"},
    {"name": "MiLB Official",    "url": "https://www.milb.com/feeds/news/rss.xml",                 "type": "prospects"},
    {"name": "CBS Sports MLB",   "url": "https://www.cbssports.com/rss/headlines/mlb",             "type": "news"},
    {"name": "CBS Fantasy MLB",  "url": "https://www.cbssports.com/rss/headlines/fantasy/baseball","type": "fantasy"},
    {"name": "RotoBaller",       "url": "https://www.rotoballer.com/feed",                         "type": "fantasy"},
    {"name": "Pitcher List",     "url": "https://www.pitcherlist.com/feed",                        "type": "fantasy"},
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
# UTILITY FUNCTIONS
# ============================================================
def normalize_name(name):
    if not name:
        return ''
    name = re.sub(r'\s*\(.*?\)', '', name).strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return ' '.join(name.lower().split())

def clean_text(text):
    if not text:
        return ''
    text = html.unescape(text)
    text = re.sub('<[^<]+?>', '', text)
    return text.strip()

def get_current_week():
    days_in = (date.today() - SEASON_START).days
    return max(1, (days_in // 7) + 1)

def get_season_blend():
    days_in = (date.today() - SEASON_START).days
    if days_in < 26:
        return 0.80, 0.20
    elif days_in < 57:
        return 0.60, 0.40
    elif days_in < 103:
        return 0.35, 0.65
    else:
        return 0.10, 0.90

def monday_of_week(d=None):
    if d is None:
        d = date.today()
    return d - timedelta(days=d.weekday())

def sunday_of_week(d=None):
    return monday_of_week(d) + timedelta(days=6)

def days_left_in_week():
    return max(0, 6 - date.today().weekday())

def format_date(d):
    if isinstance(d, str):
        try:
            d = datetime.strptime(d, '%Y-%m-%d').date()
        except Exception:
            return d
    return d.strftime('%a %-m/%-d')

def matchup_label(opp_ops):
    if opp_ops   <= 0.680: return '✅ Great'
    elif opp_ops <= 0.710: return '✅ Good'
    elif opp_ops <= 0.740: return '⚠️ Neutral'
    else:                  return '❌ Tough'

def is_high_quality_matchup(opp_ops):
    return opp_ops <= 0.710

def _estimate_days_out(text):
    """Estimate days out from injury language. Returns int or None."""
    if 'tommy john' in text:                       return 365
    if 'season-ending' in text:                    return 180
    if '60-day' in text:                           return 60
    if '6-8 weeks' in text:                        return 49
    if '4-6 weeks' in text:                        return 35
    if '2-4 weeks' in text:                        return 21
    if '15-day' in text:                           return 15
    if '1-2 weeks' in text:                        return 10
    if '10-day' in text:                           return 10
    if 'week to week' in text:                     return 14
    if 'day-to-day' in text or 'dtd' in text:      return 3
    return None

# ============================================================
# PUSHOVER
# ============================================================
def send_pushover(title, message, priority=0):
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        print(f"  [PUSHOVER SKIPPED] {title}: {message[:80]}")
        return
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

# ============================================================
# STATE PERSISTENCE
# ============================================================
def _load_json(path, default):
    try:
        if Path(path).exists():
            with open(path, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return default

def _save_json(path, data):
    try:
        with open(path, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"  Save error {path}: {e}")

def load_seen_alerts():
    data   = _load_json(SEEN_ALERTS_FILE, {})
    cutoff = datetime.now(timezone.utc).timestamp() - (8 * 3600)
    return {k: v for k, v in data.items() if isinstance(v, (int, float)) and v > cutoff}

def save_seen_alerts(seen):
    _save_json(SEEN_ALERTS_FILE, seen)

def is_alert_seen(key, seen):
    return key in seen

def mark_alert_seen(key, seen):
    seen[key] = datetime.now(timezone.utc).timestamp()

def load_sleep_queue():
    data   = _load_json(SLEEP_QUEUE_FILE, [])
    cutoff = datetime.now(timezone.utc).timestamp() - (12 * 3600)
    return [x for x in data if x.get('ts', 0) > cutoff]

def save_sleep_queue(queue):
    _save_json(SLEEP_QUEUE_FILE, queue)

def load_sitting_alerts():
    data = _load_json(SITTING_ALERTS_FILE, {})
    if data.get('date') != date.today().isoformat():
        return {}
    return data.get('alerted', {})

def save_sitting_alerts(alerted):
    _save_json(SITTING_ALERTS_FILE, {'date': date.today().isoformat(), 'alerted': alerted})

def load_morning_probables():
    data = _load_json(PROBABLES_FILE, {})
    if data.get('date') != date.today().isoformat():
        return {}
    return data.get('probables', {})

def save_morning_probables(probables):
    _save_json(PROBABLES_FILE, {'date': date.today().isoformat(), 'probables': probables})

def load_scratch_alerted():
    data = _load_json(SCRATCH_ALERTED_FILE, {})
    if data.get('date') != date.today().isoformat():
        return {}
    return data.get('alerted', {})

def save_scratch_alerted(alerted):
    _save_json(SCRATCH_ALERTED_FILE, {'date': date.today().isoformat(), 'alerted': alerted})

def load_transactions():
    data   = _load_json(TRANSACTIONS_FILE, [])
    cutoff = datetime.now(timezone.utc).timestamp() - (90 * 86400)
    return [t for t in data if t.get('timestamp', 0) > cutoff]

def save_transactions(transactions):
    _save_json(TRANSACTIONS_FILE, transactions)

def load_leaguemate_profiles():
    return _load_json(LEAGUEMATE_FILE, {})

def save_leaguemate_profiles(profiles):
    _save_json(LEAGUEMATE_FILE, profiles)

def load_trade_history():
    return _load_json(TRADE_HISTORY_FILE, [])

def save_trade_history(history):
    _save_json(TRADE_HISTORY_FILE, history)

def load_pos_eligibility_alerts():
    data = _load_json(POS_ELIGIBILITY_FILE, {})
    week = get_current_week()
    if data.get('week') != week:
        return {}
    return data.get('alerted', {})

def save_pos_eligibility_alerts(alerted):
    _save_json(POS_ELIGIBILITY_FILE, {'week': get_current_week(), 'alerted': alerted})

# ============================================================
# YAHOO API
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
    """Returns (taken_set, my_roster_list, all_rosters_dict). None on failure."""
    try:
        query       = get_yahoo_query()
        today       = date.today()
        taken       = set()
        my_roster   = []
        all_rosters = {}

        for team_id in range(1, 13):
            try:
                roster = query.get_team_roster_player_info_by_date(team_id, today)
                if not roster:
                    continue
                team_players = []
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
                    norm = normalize_name(name)
                    taken.add(norm)
                    pdata = {
                        'name':              name,
                        'name_normalized':   norm,
                        'position':          str(getattr(player, 'primary_position', '') or ''),
                        'pct_owned':         float(getattr(player.percent_owned, 'value', 0) or 0) if hasattr(player, 'percent_owned') else 0.0,
                        'is_undroppable':    int(getattr(player, 'is_undroppable', 0) or 0),
                        'status':            str(getattr(player, 'status', '') or ''),
                        'injury_note':       str(getattr(player, 'injury_note', '') or ''),
                        'selected_position': (player.selected_position.position if hasattr(player, 'selected_position') else ''),
                        'team_abbr':         str(getattr(player, 'editorial_team_abbr', '') or ''),
                        'player_id':         str(getattr(player, 'player_id', '') or ''),
                        'eligible_positions': [],
                    }
                    try:
                        ep = player.eligible_positions
                        if ep:
                            pdata['eligible_positions'] = [
                                str(getattr(p, 'position', p)) for p in
                                (ep if isinstance(ep, list) else [ep])
                            ]
                    except Exception:
                        pass
                    team_players.append(pdata)
                    if team_id == MY_TEAM_ID:
                        my_roster.append(pdata)
                all_rosters[team_id] = team_players
            except Exception as e:
                print(f"  Team {team_id} error: {e}")

        if len(taken) < MIN_EXPECTED_ROSTERED:
            print(f"  ⚠️ Only {len(taken)} players — Yahoo may have failed")
            send_pushover("⚠️ SYSTEM WARNING",
                          f"Yahoo returned only {len(taken)} players. Alerts suppressed.",
                          priority=0)
            return None, None, None

        print(f"  {len(taken)} rostered, {len(my_roster)} on my team")
        return taken, my_roster, all_rosters

    except Exception as e:
        print(f"  Yahoo error: {e}")
        send_pushover("⚠️ SYSTEM WARNING", f"Yahoo connection failed: {str(e)[:200]}.", priority=0)
        return None, None, None

def validate_player_in_yahoo(player_name, taken=None):
    """
    Validate player exists in MLB database.
    Returns (canonical_name, is_available) or (None, False).
    """
    norm = normalize_name(player_name)

    # Quick check: in taken set means exists (just not available)
    if taken and norm in taken:
        return player_name, False

    try:
        url    = f"https://statsapi.mlb.com/api/v1/people/search?names={requests.utils.quote(player_name)}&sportId=1"
        data   = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        if not people:
            return None, False
        canonical      = people[0].get('fullName', player_name)
        canonical_norm = normalize_name(canonical)
        is_available   = taken is None or canonical_norm not in taken
        return canonical, is_available
    except Exception:
        return player_name, (taken is None or norm not in taken)

def get_league_free_agents(position=None, count=25):
    try:
        query   = get_yahoo_query()
        players = query.get_league_players(
            player_count=count,
            position_filter=position,
            status_filter='FA'
        )
        result = []
        for p in (players or []):
            try:
                name = p.name.full
                pct  = float(getattr(p.percent_owned, 'value', 0) or 0)
                pos  = str(getattr(p, 'primary_position', '') or '')
                pid  = str(getattr(p, 'player_id', '') or '')
                result.append({'name': name, 'pct_owned': pct, 'position': pos, 'player_id': pid})
            except Exception:
                continue
        return result
    except Exception as e:
        print(f"  Free agent fetch error: {e}")
        return []

def count_my_il_slots_used(my_roster):
    """Return number of players currently in my IL slots."""
    return sum(1 for p in my_roster if p.get('selected_position') == 'IL')

def get_worst_il_stash(my_roster):
    """Return the lowest-value player currently in my IL slots."""
    il_players = [p for p in my_roster if p.get('selected_position') == 'IL']
    if not il_players:
        return None
    return min(il_players, key=lambda p: p['pct_owned'])

# ============================================================
# MLB STATS API
# ============================================================
def get_team_batting_stats():
    try:
        url  = ("https://statsapi.mlb.com/api/v1/teams/stats"
                "?season=2026&group=hitting&stats=season&sportId=1")
        data = requests.get(url, timeout=10).json()
        result = {}
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                name    = split.get('team', {}).get('name', '')
                ops_val = split.get('stat', {}).get('ops', '') or ''
                try:
                    result[name] = float(ops_val)
                except (ValueError, TypeError):
                    pass
        print(f"  Team batting stats: {len(result)} teams")
        return result
    except Exception as e:
        print(f"  Team stats error: {e}")
        return {}

def get_schedule(start_date, end_date, hydrate='probablePitcher'):
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/schedule"
                f"?sportId=1&startDate={start_date}&endDate={end_date}"
                f"&gameType=R&hydrate={hydrate}")
        data = requests.get(url, timeout=15).json()
        return data
    except Exception as e:
        print(f"  Schedule fetch error: {e}")
        return {}

def get_todays_schedule():
    try:
        today_str = date.today().strftime('%Y-%m-%d')
        data  = get_schedule(today_str, today_str, 'probablePitcher,lineups,status')
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
                    'home_team_id':  home.get('team', {}).get('id', 0),
                    'away_team_id':  away.get('team', {}).get('id', 0),
                })
        print(f"  Schedule: {len(games)} games today")
        return games
    except Exception as e:
        print(f"  Schedule error: {e}")
        return []

def get_probable_pitchers(start_date, end_date, team_ops):
    try:
        data     = get_schedule(str(start_date), str(end_date))
        pitchers = {}
        for day in data.get('dates', []):
            for game in day.get('games', []):
                game_date = day.get('date', '')
                for side, opp_side in [('home', 'away'), ('away', 'home')]:
                    p        = game.get('teams', {}).get(side, {}).get('probablePitcher', {})
                    opp_team = game.get('teams', {}).get(opp_side, {}).get('team', {}).get('name', '')
                    my_team  = game.get('teams', {}).get(side,     {}).get('team', {}).get('name', '')
                    if p and p.get('fullName'):
                        n       = p['fullName']
                        pid     = p.get('id', 0)
                        opp_ops = team_ops.get(opp_team, 0.720)
                        if n not in pitchers:
                            pitchers[n] = {
                                'count': 0, 'id': pid,
                                'dates': [], 'opponents': [],
                                'opp_ops': [], 'team': my_team
                            }
                        pitchers[n]['count'] += 1
                        pitchers[n]['dates'].append(game_date)
                        pitchers[n]['opponents'].append(opp_team)
                        pitchers[n]['opp_ops'].append(opp_ops)
        return pitchers
    except Exception as e:
        print(f"  Probable pitchers error: {e}")
        return {}

def get_pitcher_stats(player_id, season=None):
    if season is None:
        season = date.today().year
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
                        'wins':         int(s.get('wins', 0) or 0),
                    }
                except Exception:
                    pass
    except Exception:
        pass
    return None

def get_pitcher_stats_blended(player_id):
    w_prior, w_curr = get_season_blend()
    curr  = get_pitcher_stats(player_id, date.today().year)
    prior = get_pitcher_stats(player_id, date.today().year - 1)
    empty = {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0,
             'gs': 0, 'ip_per_start': 0, 'kbb': 0.0, 'wins': 0}
    if prior is None and curr is not None:
        curr['blend_note'] = 'rookie/no prior stats'
        return curr
    if curr is None and prior is not None:
        prior['blend_note'] = 'no current stats yet'
        return prior
    if curr is None and prior is None:
        return {**empty, 'blend_note': 'no stats'}
    ip_per_start = prior.get('ip_per_start', 0) or curr.get('ip_per_start', 0)
    return {
        'era':          round(prior['era']  * w_prior + curr['era']  * w_curr, 2),
        'whip':         round(prior['whip'] * w_prior + curr['whip'] * w_curr, 2),
        'kbb':          round(prior['kbb']  * w_prior + curr['kbb']  * w_curr, 2),
        'k':            curr['k'],
        'ip':           curr['ip'],
        'gs':           curr.get('gs', 0),
        'ip_per_start': ip_per_start,
        'wins':         curr.get('wins', 0),
        'blend_note':   f"{int(w_prior*100)}% prior / {int(w_curr*100)}% current"
    }

def get_player_id_from_name(name):
    try:
        url    = f"https://statsapi.mlb.com/api/v1/people/search?names={requests.utils.quote(name)}&sportId=1"
        data   = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        if people:
            return people[0].get('id')
    except Exception:
        pass
    return None

def get_hitter_stats(player_id, season=None):
    if season is None:
        season = date.today().year
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                f"?stats=season&group=hitting&season={season}")
        data = requests.get(url, timeout=5).json()
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                s = split.get('stat', {})
                try:
                    return {
                        'avg':  float(s.get('avg', '.000')  or '.000'),
                        'ops':  float(s.get('ops', '.000')  or '.000'),
                        'hr':   int(s.get('homeRuns', 0)    or 0),
                        'rbi':  int(s.get('rbi', 0)         or 0),
                        'sb':   int(s.get('stolenBases', 0) or 0),
                        'pa':   int(s.get('plateAppearances', 0) or 0),
                    }
                except Exception:
                    pass
    except Exception:
        pass
    return None

def is_opener(stats):
    if not stats:
        return False
    ip_per_start = stats.get('ip_per_start', 0)
    gs           = stats.get('gs', 0)
    ip           = stats.get('ip', 0)
    if gs < 2:
        return (ip / gs) < 3.0 if gs > 0 and ip > 0 else False
    return ip_per_start < 3.0

def is_high_quality_sp(stats, opp_ops):
    if not stats or is_opener(stats):
        return False
    _, w_curr = get_season_blend()
    min_ip = max(5, int(w_curr * 50))
    if stats.get('ip', 0) < min_ip:
        return False
    era  = stats.get('era',  99)
    whip = stats.get('whip', 9)
    kbb  = stats.get('kbb',  0)
    if opp_ops <= 0.690:
        return era < 4.50 and whip < 1.35 and kbb > 1.8
    elif opp_ops <= 0.730:
        return era < 4.10 and whip < 1.28 and kbb > 2.0
    else:
        return era < 3.60 and whip < 1.18 and kbb > 2.5

def sp_long_term_value(p, stats):
    if p['is_undroppable'] or p['name_normalized'] in MY_UNDROPPABLE:
        return True
    if p['pct_owned'] >= 60:
        return True
    if stats and stats.get('era', 99) < 3.80 and stats.get('ip', 0) >= 20:
        return True
    return False

def score_sp(stats, opp_ops=None):
    if not stats or stats.get('ip', 0) < 5 or is_opener(stats):
        return -999
    s = (
        stats.get('k', 0) * 0.4
        + stats.get('kbb', 0) * 8
        - stats.get('era', 5) * 4
        - stats.get('whip', 1.4) * 15
    )
    if opp_ops is not None:
        s -= (opp_ops - 0.700) * 30
    return s

# ============================================================
# CLOSERMONKEY
# ============================================================
def fetch_closermonkey():
    try:
        cached = _load_json(CLOSERMONKEY_CACHE, {})
        age    = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
        if age < 14400 and cached.get('data'):
            return cached['data']
    except Exception:
        pass
    try:
        response = requests.get(
            'https://www.closermonkey.com',
            headers={"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"},
            timeout=15
        )
        text         = response.text
        depth_charts = {}
        current_team = None
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', text, re.DOTALL | re.IGNORECASE)
        for row in rows:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL | re.IGNORECASE)
            cells = [clean_text(c).strip() for c in cells if clean_text(c).strip()]
            if not cells:
                continue
            for abbr, full_name in TEAM_NAME_MAP.items():
                if any(abbr in c or full_name in c for c in cells):
                    current_team = full_name
                    break
            if current_team:
                for cell in cells:
                    if looks_like_player_name(cell):
                        norm = normalize_name(cell)
                        team_words = {
                            'angels', 'astros', 'athletics', 'blue jays', 'braves',
                            'brewers', 'cardinals', 'cubs', 'diamondbacks', 'dodgers',
                            'giants', 'guardians', 'mariners', 'marlins', 'mets',
                            'nationals', 'orioles', 'padres', 'phillies', 'pirates',
                            'rangers', 'rays', 'red sox', 'reds', 'rockies', 'royals',
                            'tigers', 'twins', 'white sox', 'yankees'
                        }
                        if any(t in norm for t in team_words):
                            continue
                        if current_team not in depth_charts:
                            depth_charts[current_team] = []
                        if norm not in depth_charts[current_team]:
                            depth_charts[current_team].append(norm)
        closer_lookup = {}
        for team, pitchers in depth_charts.items():
            if pitchers:
                closer_lookup[pitchers[0]] = team
        data = {'depth_charts': depth_charts, 'closer_lookup': closer_lookup}
        _save_json(CLOSERMONKEY_CACHE, {
            'ts': datetime.now(timezone.utc).timestamp(),
            'data': data
        })
        print(f"  Closermonkey: {len(closer_lookup)} closers loaded")
        return data
    except Exception as e:
        print(f"  Closermonkey error: {e}")
        return {}

def get_all_closers():
    return set(fetch_closermonkey().get('closer_lookup', {}).keys())

def get_closer_team(player_norm):
    return fetch_closermonkey().get('closer_lookup', {}).get(player_norm)

def get_closer_candidates(team_name, taken, limit=3):
    """Return available Closermonkey candidates with fantasy relevance validation."""
    chart  = fetch_closermonkey().get('depth_charts', {}).get(team_name, [])
    result = []
    fa     = get_league_free_agents(position='RP', count=30)
    fa_map = {normalize_name(p['name']): p for p in fa}

    for norm in chart[1:limit+3]:
        if norm in taken:
            continue
        pid = get_player_id_from_name(norm.title())
        if pid:
            stats        = get_pitcher_stats_blended(pid)
            owned_enough = False
            fa_player    = fa_map.get(norm)
            if fa_player:
                pct = fa_player['pct_owned']
                if (pct >= 15
                        or norm in TOP_PROSPECTS
                        or (stats and stats.get('era', 99) < 3.80 and stats.get('ip', 0) >= 10)):
                    owned_enough = True
            if not owned_enough:
                continue
        result.append(norm.title())
        if len(result) >= limit:
            break
    return result

# ============================================================
# NAME EXTRACTION
# ============================================================
def looks_like_player_name(text):
    if not text:
        return False
    text  = text.strip()
    words = text.split()
    if not (2 <= len(words) <= 4):
        return False
    suffixes = {'jr.', 'sr.', 'ii', 'iii', 'iv'}
    for word in words:
        if word.lower() in suffixes:
            continue
        if not word[0].isupper():
            return False
    non_name = {
        'mlb', 'nfl', 'nba', 'nhl', 'espn', 'the', 'for', 'and', 'or',
        'power', 'rankings', 'trade', 'deadline', 'spring', 'training',
        'opening', 'day', 'world', 'series', 'all-star', 'free', 'agency',
        'report', 'update', 'breaking', 'fantasy', 'baseball', 'weekly',
        'daily', 'morning', 'sources', 'video', 'watch', 'review', 'week',
        'angels', 'orioles', 'yankees', 'rays', 'red', 'sox', 'blue', 'jays',
        'white', 'guardians', 'tigers', 'royals', 'twins', 'astros',
        'athletics', 'mariners', 'rangers', 'braves', 'marlins', 'mets',
        'phillies', 'nationals', 'cubs', 'reds', 'brewers', 'pirates',
        'cardinals', 'diamondbacks', 'rockies', 'dodgers', 'padres', 'giants',
        'rotowire', 'cbssports', 'rotoballer', 'pitcherlist',
    }
    for word in words:
        if word.lower() in non_name:
            return False
    return True

def extract_player_name(title, summary, source=''):
    COLON_SOURCES = {'Rotowire', 'MLB Trade Rumors'}
    if source in COLON_SOURCES and ':' in title:
        candidate = title.split(':')[0].strip()
        if looks_like_player_name(candidate):
            words = candidate.lower().split()
            if not any(w in INVALID_NAME_WORDS for w in words):
                if not any(w in ACTION_VERBS for w in words):
                    if candidate.lower() not in MINOR_LEAGUE_TEAMS:
                        return candidate
    full_text  = clean_text(title + ' ' + summary)
    pattern    = r'\b([A-Z][a-z\']+(?:\s+[A-Z][a-z\']+){1,3})\b'
    candidates = re.findall(pattern, full_text)
    for candidate in candidates:
        if not looks_like_player_name(candidate):
            continue
        words = candidate.lower().split()
        if any(w in INVALID_NAME_WORDS for w in words):
            continue
        if any(w in ACTION_VERBS for w in words):
            continue
        if candidate.lower() in MINOR_LEAGUE_TEAMS:
            continue
        return candidate
    return None

# ============================================================
# MATCHUP DATA
# ============================================================
def get_matchup_data():
    try:
        cached = _load_json(MATCHUP_CACHE_FILE, {})
        age    = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
        if age < 1800 and cached.get('data'):
            print("  Using cached matchup data")
            return cached['data']
    except Exception:
        pass
    try:
        query       = get_yahoo_query()
        week        = get_current_week()
        opp_team_id = None
        try:
            matchups = query.get_team_matchups(MY_TEAM_ID)
            for m in (matchups if isinstance(matchups, list) else [matchups]):
                teams = getattr(m, 'teams', []) or []
                for team in (teams if isinstance(teams, list) else [teams]):
                    tid = int(getattr(team, 'team_id', 0) or 0)
                    if tid != MY_TEAM_ID:
                        opp_team_id = tid
                        break
                if opp_team_id:
                    break
        except Exception as e:
            print(f"  Matchup opponent lookup error: {e}")

        def parse_stats(raw):
            result = {}
            try:
                team_stats = getattr(raw, 'team_stats', None) or raw
                stats      = getattr(team_stats, 'stats', None)
                if stats is None:
                    return result
                stat_list = getattr(stats, 'stat', None) or stats
                if not isinstance(stat_list, list):
                    stat_list = [stat_list]
                id_map = {
                    '60': 'R', '7': 'H', '12': 'HR', '13': 'RBI',
                    '16': 'SB', '3': 'AVG', '55': 'OPS', '28': 'W',
                    '32': 'SV', '27': 'K', '26': 'ERA', '29': 'WHIP', '72': 'KBB'
                }
                for s in stat_list:
                    sid = str(getattr(s, 'stat_id', '') or '')
                    val = getattr(s, 'value', None)
                    if val is not None and sid in id_map:
                        try:
                            result[id_map[sid]] = float(val)
                        except (ValueError, TypeError):
                            pass
            except Exception as e:
                print(f"  Stats parse error: {e}")
            return result

        my_stats  = {}
        opp_stats = {}
        try:
            my_raw   = query.get_team_stats_by_week(MY_TEAM_ID, week)
            my_stats = parse_stats(my_raw)
        except Exception as e:
            print(f"  My stats error: {e}")
        if opp_team_id:
            try:
                opp_raw   = query.get_team_stats_by_week(opp_team_id, week)
                opp_stats = parse_stats(opp_raw)
            except Exception as e:
                print(f"  Opp stats error: {e}")

        data = {
            'my_stats':    my_stats,
            'opp_stats':   opp_stats,
            'opp_team_id': opp_team_id,
            'week':        week,
        }
        _save_json(MATCHUP_CACHE_FILE, {
            'ts': datetime.now(timezone.utc).timestamp(),
            'data': data
        })
        print(f"  Matchup data: {len(my_stats)} my cats, {len(opp_stats)} opp cats")
        return data
    except Exception as e:
        print(f"  Matchup data error: {e}")
        return None

# ============================================================
# LEAGUEMATE TRANSACTION TRACKING
# ============================================================
def sync_league_transactions():
    try:
        query        = get_yahoo_query()
        transactions = load_transactions()
        existing_ids = {t.get('id') for t in transactions}
        league_trans = query.get_league_transactions()
        if not league_trans:
            return
        now_ts    = datetime.now(timezone.utc).timestamp()
        new_count = 0
        for trans in (league_trans if isinstance(league_trans, list) else [league_trans]):
            try:
                trans_id   = str(getattr(trans, 'transaction_id', '') or '')
                trans_type = str(getattr(trans, 'type', '') or '').lower()
                timestamp  = float(getattr(trans, 'timestamp', now_ts) or now_ts)
                if trans_id in existing_ids:
                    continue
                players     = getattr(trans, 'players', None) or {}
                player_info = []
                try:
                    player_list = (players if isinstance(players, list)
                                   else getattr(players, 'player', []) or [])
                    if not isinstance(player_list, list):
                        player_list = [player_list]
                    for pl in player_list:
                        pname = ''
                        try:
                            pname = pl.name.full
                        except Exception:
                            pass
                        tdata     = getattr(pl, 'transaction_data', None)
                        dest_team = str(getattr(tdata, 'destination_team_key', '') or '')
                        src_team  = str(getattr(tdata, 'source_team_key', '') or '')
                        ptype     = str(getattr(tdata, 'type', '') or '').lower()
                        pid       = str(getattr(pl, 'player_id', '') or '')
                        # Try multiple position attribute paths
                        pos = ''
                        try:
                            pos = str(pl.primary_position or '')
                        except Exception:
                            pass
                        if not pos:
                            try:
                                pos = str(pl.eligible_positions.position or '')
                            except Exception:
                                pass
                        if not pos and pid:
                            try:
                                url  = f"https://statsapi.mlb.com/api/v1/people/{pid}?hydrate=currentTeam"
                                pdat = requests.get(url, timeout=3).json()
                                pos  = pdat.get('people', [{}])[0].get('primaryPosition', {}).get('abbreviation', '')
                            except Exception:
                                pass
                        player_info.append({
                            'name': pname, 'player_id': pid,
                            'position': pos, 'type': ptype,
                            'dest_team': dest_team, 'src_team': src_team
                        })
                except Exception:
                    pass
                transactions.append({
                    'id':        trans_id,
                    'type':      trans_type,
                    'timestamp': timestamp,
                    'logged_at': now_ts,
                    'players':   player_info,
                })
                new_count += 1
            except Exception:
                continue
        if new_count > 0:
            save_transactions(transactions)
            print(f"  Logged {new_count} new transactions ({len(transactions)} total)")
        _build_leaguemate_profiles(transactions)
    except Exception as e:
        print(f"  Transaction sync error: {e}")

def _build_leaguemate_profiles(transactions):
    profiles = {}
    for t in transactions:
        if 'add' not in t.get('type', ''):
            continue
        for p in t.get('players', []):
            if p.get('type') != 'add':
                continue
            team_key = p.get('dest_team', '')
            if not team_key:
                continue
            if team_key not in profiles:
                profiles[team_key] = {
                    'adds': [],
                    'avg_response_hours': {},
                    'categories': {'closer': [], 'prospect': [], 'streamer': []}
                }
            profiles[team_key]['adds'].append({
                'player':    p['name'],
                'position':  p['position'],
                'timestamp': t['timestamp'],
            })
    for team_key, profile in profiles.items():
        adds = profile['adds']
        if adds:
            profile['total_adds'] = len(adds)
            recent = [a for a in adds
                      if a['timestamp'] > datetime.now(timezone.utc).timestamp() - 30 * 86400]
            profile['adds_last_30d'] = len(recent)
    save_leaguemate_profiles(profiles)
    print(f"  Built profiles for {len(profiles)} teams")

def get_waiver_drops_to_review(taken, my_roster):
    transactions = load_transactions()
    cutoff       = datetime.now(timezone.utc).timestamp() - 86400
    recent_drops = []
    for t in transactions:
        if t.get('timestamp', 0) < cutoff:
            continue
        if 'drop' not in t.get('type', ''):
            continue
        for p in t.get('players', []):
            if p.get('type') not in ('drop', 'release'):
                continue
            name = p.get('name', '')
            if not name:
                continue
            norm = normalize_name(name)
            if norm in {normalize_name(r['name']) for r in my_roster}:
                continue
            recent_drops.append({
                'name':      name,
                'position':  p.get('position', ''),
                'player_id': p.get('player_id', ''),
                'timestamp': t['timestamp'],
                'notes':     t.get('type', ''),
            })
    return recent_drops

# ============================================================
# DROP CANDIDATE LOGIC
# ============================================================
def get_weak_positions(my_roster):
    weak   = []
    strong = {'SS', '1B', 'OF'}
    by_pos = {}
    for p in my_roster:
        pos = p['position']
        if pos not in by_pos:
            by_pos[pos] = []
        by_pos[pos].append(p)
    for pos, players in by_pos.items():
        if pos in strong or pos in ['BN', 'Util', 'IL']:
            continue
        for p in players:
            if 'IL' in (p['status'] or '') or p['pct_owned'] < 60:
                if pos not in weak:
                    weak.append(pos)
    return weak

def find_best_drop(my_roster, team_ops, protect_closer=True, prefer_position=None):
    today       = datetime.now(ET_TZ).date()
    end_of_week = sunday_of_week(today)
    week_starts = get_probable_pitchers(today, end_of_week, team_ops)
    rp_players  = [p for p in my_roster if p['position'] == 'RP']
    only_closer = len(rp_players) == 1
    candidates  = []
    for p in my_roster:
        if p['is_undroppable'] or p['name_normalized'] in MY_UNDROPPABLE:
            continue
        if 'IL' in (p['status'] or ''):
            continue
        if protect_closer and only_closer and p['position'] == 'RP':
            continue
        score = 0
        if p['position'] in ['SP', 'RP', 'P']:
            player_id = get_player_id_from_name(p['name'])
            stats     = get_pitcher_stats_blended(player_id) if player_id else None
            has_start = normalize_name(p['name']) in {normalize_name(k) for k in week_starts}
            if sp_long_term_value(p, stats):
                if not has_start:
                    score += 30
                elif has_start:
                    remaining_ops = max(
                        (week_starts.get(k, {}).get('opp_ops', [0.720])
                         for k in week_starts if normalize_name(k) == normalize_name(p['name'])),
                        default=[0.720]
                    )
                    worst_ops = max(remaining_ops) if remaining_ops else 0.720
                    if worst_ops > 0.750 and p['pct_owned'] < 45:
                        score += 20
                    else:
                        continue
            else:
                score += 50
                if has_start:
                    remaining_ops = [0.720]
                    for k in week_starts:
                        if normalize_name(k) == normalize_name(p['name']):
                            remaining_ops = week_starts[k].get('opp_ops', [0.720])
                    worst_ops = max(remaining_ops) if remaining_ops else 0.720
                    if worst_ops <= 0.720:
                        score -= 25
        else:
            if p['pct_owned'] < 20 and p['position'] not in ['C', '1B', '2B', '3B', 'SS']:
                score += 40
            elif p['pct_owned'] < 10:
                score += 30
            else:
                continue
        if prefer_position and p['position'] == prefer_position:
            score += 15
        score -= p['pct_owned'] * 0.3
        candidates.append((score, p))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def _get_compatible_positions(pos):
    compat = {
        'C':  ['C', 'Util', 'BN'],
        '1B': ['1B', 'Util', 'BN'],
        '2B': ['2B', 'Util', 'BN'],
        '3B': ['3B', 'Util', 'BN'],
        'SS': ['SS', 'Util', 'BN'],
        'OF': ['OF', 'Util', 'BN'],
        'SP': ['SP', 'P', 'BN'],
        'RP': ['RP', 'P', 'BN'],
    }
    return compat.get(pos, ['Util', 'BN'])

# ============================================================
# RSS FEED FETCHING
# ============================================================
def fetch_feed(source, lookback_minutes=20):
    try:
        headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
        feed    = feedparser.parse(source["url"], request_headers=headers)
        cutoff  = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        items   = []
        for entry in feed.entries:
            try:
                pub = (
                    datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    if hasattr(entry, 'published_parsed') and entry.published_parsed
                    else datetime.now(timezone.utc)
                )
                if pub < cutoff:
                    continue
                title   = clean_text(entry.get('title', ''))
                summary = clean_text(entry.get('summary', entry.get('description', title)))
                summary = summary[:400] + '...' if len(summary) > 400 else summary
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

def get_all_news(lookback_minutes=20):
    items = []
    print("Checking Tier 1 sources...")
    for s in TIER1_SOURCES:
        items.extend(fetch_feed(s, lookback_minutes))
    m = datetime.now(timezone.utc).minute
    if m < 16 or 30 <= m < 46:
        print("Checking Reddit (Tier 2)...")
        for s in TIER2_SOURCES:
            items.extend(fetch_feed(s, lookback_minutes))
    else:
        print("Skipping Reddit this run")
    print("Checking Tier 3 (30 MLB team feeds)...")
    t3 = 0
    for s in TIER3_SOURCES:
        new = fetch_feed(s, lookback_minutes)
        items.extend(new)
        t3 += len(new)
    print(f"  Tier 3 total: {t3} items")
    print(f"Total: {len(items)} raw items")
    return items

# ============================================================
# BREAKING NEWS PROCESSING
# ============================================================
def awake_hours():
    now = datetime.now(ET_TZ)
    return 6 <= now.hour < 22 or (now.hour == 6 and now.minute >= 30)

def process_breaking_news(news, taken, my_roster, team_ops):
    seen        = load_seen_alerts()
    sleep_queue = load_sleep_queue() if not awake_hours() else []
    alerts_sent = 0

    for item in news:
        title   = item['title']
        summary = item['summary']
        source  = item['source']
        text    = (title + ' ' + summary).lower()

        player = extract_player_name(title, summary, source)
        if not player:
            continue

        # Suppress known media/writer names
        if normalize_name(player) in KNOWN_MEDIA_NAMES:
            continue

        canonical, is_available = validate_player_in_yahoo(player, taken)
        if canonical is None:
            continue

        player_norm = normalize_name(canonical)

        # ── SS INJURY WATCHLIST ─────────────────────────────────
        for ss in TOP_15_SS:
            if normalize_name(ss) == player_norm:
                if any(kw in text for kw in INJURY_KEYWORDS):
                    if any(kw in text for kw in MINOR_INJURY_KEYWORDS):
                        break
                    key = f"ss_injury:{player_norm}"
                    if is_alert_seen(key, seen):
                        break
                    is_mine = player_norm in [normalize_name(s) for s in
                              ["Gunnar Henderson", "Trea Turner"]]
                    if not is_mine:
                        available_ss = [
                            p for p in get_league_free_agents(position='SS', count=10)
                            if p['pct_owned'] >= 20
                               or normalize_name(p['name']) in TOP_PROSPECTS
                        ]
                        if not available_ss:
                            break
                    title_str = f"{'🚨' if is_mine else '👀'} SS INJURY: {canonical}"
                    if is_mine:
                        title_str += " ← YOUR PLAYER"
                        msg = (
                            f"{canonical} — {_extract_injury_detail(text)}\n\n"
                            f"⚠️ YOUR SS IS HURT. Check IL status immediately.\n\n"
                            f"Source: {source}"
                        )
                    else:
                        best       = available_ss[0] if available_ss else None
                        pickup_str = (f"\n🎯 Available SS to add: {best['name']} "
                                      f"({best['pct_owned']:.0f}% owned)" if best else "")
                        msg = (
                            f"{canonical} — {_extract_injury_detail(text)}"
                            f"{pickup_str}\n\nSource: {source}"
                        )
                    _fire_or_queue(title_str, msg, priority=1 if is_mine else 0,
                                   seen=seen, key=key, sleep_queue=sleep_queue,
                                   queue_category='ss_injury')
                    alerts_sent += 1
                break

        # Skip minor injuries
        if any(kw in text for kw in MINOR_INJURY_KEYWORDS):
            continue

        # ── BREAKING NEWS - CLOSER ──────────────────────────────
        all_closers         = get_all_closers()
        is_confirmed_closer = player_norm in all_closers
        closer_role_change  = any(kw in text for kw in CLOSER_KEYWORDS)
        role_loss           = any(w in text for w in [
            'optioned', 'demoted', 'placed on il', 'injured list',
            'released', 'suspended'
        ])

        if is_confirmed_closer and any(kw in text for kw in INJURY_KEYWORDS):
            key = f"saves_opp:{player_norm}"
            if not is_alert_seen(key, seen):
                closer_team = get_closer_team(player_norm)
                candidates  = get_closer_candidates(closer_team, taken) if closer_team else []
                if candidates:
                    drop_cand = find_best_drop(my_roster, team_ops)
                    drop_str  = (f"\n\n💀 Consider dropping: {drop_cand['name']} "
                                 f"({drop_cand['pct_owned']:.0f}%)" if drop_cand else "")
                    grab_str  = ', '.join(candidates[:2])
                    msg = (
                        f"⚡ {canonical} (closer) placed on IL.\n\n"
                        f"🎯 GRAB NOW: {grab_str} — available and may inherit saves!\n"
                        f"Team: {closer_team or 'unknown'}{drop_str}\n\n"
                        f"Source: {source}"
                    )
                    _fire_or_queue(
                        f"💾 SAVES OPP: {canonical} on IL", msg,
                        priority=1, seen=seen, key=key,
                        sleep_queue=sleep_queue, queue_category='saves'
                    )
                    alerts_sent += 1
                else:
                    key_watch = f"saves_watch:{player_norm}"
                    if not is_alert_seen(key_watch, seen):
                        msg = (
                            f"👀 {canonical} (closer) placed on IL.\n\n"
                            f"No clear available backup yet for {closer_team or 'their team'}. "
                            f"Watch for role announcement.\n\nSource: {source}"
                        )
                        _fire_or_queue(
                            f"💾 SAVES WATCH: {canonical} on IL", msg,
                            priority=0, seen=seen, key=key_watch,
                            sleep_queue=sleep_queue, queue_category='saves'
                        )
                        alerts_sent += 1

        elif closer_role_change and not role_loss and is_available:
            # Gate: suppress if injury language present AND player is out long-term
            if any(kw in text for kw in INJURY_KEYWORDS):
                days_out = _estimate_days_out(text)
                # Allow if returning imminently (<= 21 days) — stash candidate
                if days_out is None or days_out > 21:
                    pass  # Injured and out long-term — don't fire closer role alert
                else:
                    # Short-term IL, high value — check if worth stashing
                    key = f"closer_role:{player_norm}"
                    if not is_alert_seen(key, seen):
                        il_used  = count_my_il_slots_used(my_roster)
                        has_slot = il_used < MY_IL_SLOTS
                        worst_il = get_worst_il_stash(my_roster)
                        can_bump = (worst_il and worst_il['pct_owned'] < 30
                                    and (il_used >= MY_IL_SLOTS))
                        if has_slot or can_bump:
                            slot_str = "Open IL slot available" if has_slot else \
                                       f"Bump {worst_il['name']} from IL slot"
                            msg = (
                                f"💾 {canonical} returning soon from IL — closer when healthy!\n\n"
                                f"🎯 Stash now: {slot_str}\n"
                                f"Expected return: ~{days_out} days\n\nSource: {source}"
                            )
                            _fire_or_queue(
                                f"💾 CLOSER STASH: {canonical}", msg,
                                priority=1, seen=seen, key=key,
                                sleep_queue=sleep_queue, queue_category='saves'
                            )
                            alerts_sent += 1
            else:
                # No injury language — clean closer role takeover
                key = f"closer_role:{player_norm}"
                if not is_alert_seen(key, seen):
                    drop_cand = find_best_drop(my_roster, team_ops)
                    drop_str  = (f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)"
                                 if drop_cand else "")
                    msg = (
                        f"⚡ {canonical} taking over closing role — saves opportunity!\n\n"
                        f"Available in your league. Add now before leaguemates react.{drop_str}\n\n"
                        f"Source: {source}"
                    )
                    _fire_or_queue(
                        f"💾 CLOSER ROLE: {canonical}", msg,
                        priority=1, seen=seen, key=key,
                        sleep_queue=sleep_queue, queue_category='saves'
                    )
                    alerts_sent += 1

        # ── BREAKING NEWS - INJURY ──────────────────────────────
        elif any(kw in text for kw in INJURY_KEYWORDS) and not is_confirmed_closer:
            key = f"injury_opp:{player_norm}"
            if is_alert_seen(key, seen):
                continue
            if not _check_position_relevance(text, my_roster):
                continue
            has_role_opportunity = any(w in text for w in [
                'replace', 'fill', 'opportunity', 'role', 'regular',
                'everyday', 'every day', 'platoon', 'takeover', 'lineup',
                'starting', 'start', 'called up', 'promoted', 'recalled',
                'rotation spot', 'spot start', 'filling in'
            ])
            if not has_role_opportunity:
                continue
            backup = _find_relevant_backup(text, taken, my_roster, team_ops)
            if backup is None:
                continue
            injury_detail = _extract_injury_detail(text)
            drop_cand     = find_best_drop(my_roster, team_ops)
            drop_str      = (f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)"
                             if drop_cand else "")
            stat_str   = backup.get('stat_str', '')
            reason_str = backup.get('reason', '')
            msg = (
                f"🚑 {canonical} — {injury_detail}\n\n"
                f"🎯 Add: {backup['name']} ({backup['pct_owned']:.0f}% owned)"
                f"{' — ' + stat_str if stat_str else ''}\n"
                f"{reason_str}{drop_str}\n\nSource: {source}"
            )
            _fire_or_queue(
                f"🚑 INJURY OPP: {canonical}", msg,
                priority=1, seen=seen, key=key,
                sleep_queue=sleep_queue, queue_category='injury'
            )
            alerts_sent += 1

        # ── BREAKING NEWS - TOP PROSPECT ────────────────────────
        elif player_norm in TOP_PROSPECTS:
            callup_signals = [
                'called up', 'promoted', 'recalled', 'selected', 'debut',
                'call-up', 'joining', 'arrives', 'expected to start',
                'set to make', 'will start', 'roster spot'
            ]
            if not any(s in text for s in callup_signals):
                continue
            if not is_available:
                continue
            key = f"prospect:{player_norm}"
            if is_alert_seen(key, seen):
                continue
            drop_cand = find_best_drop(my_roster, team_ops)
            drop_str  = (f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)"
                         if drop_cand else "")
            msg = (
                f"⚡ {canonical} called up to the majors!\n\n"
                f"Top prospect — expected significant playing time. "
                f"Add before leaguemates react.{drop_str}\n\nSource: {source}"
            )
            _fire_or_queue(
                f"🔮 TOP PROSPECT: {canonical}", msg,
                priority=1, seen=seen, key=key,
                sleep_queue=sleep_queue, queue_category='prospect'
            )
            alerts_sent += 1

    save_seen_alerts(seen)
    if not awake_hours():
        save_sleep_queue(sleep_queue)
    print(f"  Breaking news: {alerts_sent} alert(s)")
    return alerts_sent

def _fire_or_queue(title, message, priority, seen, key, sleep_queue, queue_category):
    mark_alert_seen(key, seen)
    if awake_hours():
        send_pushover(title, message, priority)
    else:
        sleep_queue.append({
            'title':    title,
            'message':  message,
            'priority': priority,
            'category': queue_category,
            'ts':       datetime.now(timezone.utc).timestamp(),
        })

def _extract_injury_detail(text):
    injuries = [
        'hamstring', 'oblique', 'elbow', 'shoulder', 'knee', 'wrist',
        'back', 'thumb', 'ankle', 'hip', 'quad', 'calf', 'groin',
        'forearm', 'bicep', 'tricep', 'finger', 'hand', 'rib',
        'concussion', 'surgery', 'fracture', 'torn'
    ]
    timelines = [
        '10-day', '15-day', '60-day', '1-2 weeks', '2-4 weeks',
        '4-6 weeks', '6-8 weeks', 'season-ending', 'indefinitely',
        'out for', 'expected back', 'return timeline', 'out at least'
    ]
    found_injury   = next((i for i in injuries   if i in text), 'injury')
    found_timeline = next((t for t in timelines  if t in text), 'timeline TBD')
    return f"{found_injury} — {found_timeline}"

def _find_relevant_backup(text, taken, my_roster, team_ops):
    """
    Find a specific, fantasy-relevant backup created by an injury.
    Returns dict with name/pct_owned/stat_str/reason, or None.
    """
    pos_signals = {
        'SP': ['pitcher', 'starter', 'right-hander', 'left-hander', 'righty', 'lefty', 'ace', 'rotation'],
        'RP': ['reliever', 'closer', 'bullpen'],
        'C':  ['catcher'],
        '1B': ['first base', 'first baseman'],
        '2B': ['second base', 'second baseman'],
        '3B': ['third base', 'third baseman'],
        'SS': ['shortstop'],
        'OF': ['outfielder', 'outfield', 'center field', 'left field', 'right field'],
    }
    injured_pos = None
    for pos, signals in pos_signals.items():
        if any(s in text for s in signals):
            injured_pos = pos
            break
    if injured_pos in MY_CLOGGED_POSITIONS:
        return None
    candidates = get_league_free_agents(position=injured_pos, count=20) if injured_pos else []
    if not candidates:
        candidates = get_league_free_agents(count=20)
    today         = date.today()
    week_sun      = sunday_of_week(today)
    week_starters = {}
    if injured_pos == 'SP':
        week_starters = get_probable_pitchers(today, week_sun, team_ops)

    for player in candidates:
        name      = player['name']
        norm      = normalize_name(name)
        pct_owned = player['pct_owned']
        pos       = player.get('position', injured_pos or '')
        canonical_name, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical_name is None:
            continue
        reason   = ''
        stat_str = ''
        passes   = False

        # Check 1: Meaningful ownership
        if pct_owned >= 25:
            passes = True
            reason = f"Established fantasy asset ({pct_owned:.0f}% owned)"

        # Check 2: Top prospect
        if not passes and norm in TOP_PROSPECTS:
            passes = True
            reason = "Top MLB prospect — injury may accelerate path to majors"

        # Check 3: Platoon → everyday
        if not passes and any(w in text for w in [
            'everyday', 'every day', 'full-time', 'regular', 'starting role',
            'starts every', 'penciled in', 'lineup daily'
        ]):
            pid = get_player_id_from_name(name)
            if pid:
                stats = get_hitter_stats(pid)
                if stats and stats.get('pa', 0) >= 30 and stats.get('ops', 0) >= 0.700:
                    passes   = True
                    reason   = "Platoon role becoming everyday opportunity"
                    stat_str = f"OPS {stats['ops']:.3f} | {stats['pa']} PA"

        # Check 4: Reliever → spot starter (must pass quality threshold)
        if not passes and pos == 'RP' and injured_pos == 'SP':
            pid = get_player_id_from_name(name)
            if pid:
                stats = get_pitcher_stats_blended(pid)
                if (stats and stats.get('era', 99) < 4.00
                        and stats.get('whip', 9) < 1.30
                        and stats.get('kbb', 0) > 2.0):
                    for sp_name, sp_info in week_starters.items():
                        if normalize_name(sp_name) == norm:
                            opp_ops = sp_info['opp_ops'][0] if sp_info['opp_ops'] else 0.720
                            if is_high_quality_matchup(opp_ops):
                                passes   = True
                                reason   = (f"Quality reliever getting spot start vs "
                                            f"{sp_info['opponents'][0] if sp_info['opponents'] else 'weak offense'}")
                                stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                            break

        # Check 5: Callup acceleration
        if not passes and any(w in text for w in [
            'called up', 'promoted', 'recalled', 'expected to join',
            'being considered', 'performing well at aaa', 'option running out'
        ]):
            if norm in TOP_PROSPECTS or pct_owned >= 10:
                passes = True
                reason = "Callup accelerated by injury — may get immediate role"

        # Check 6: Prior season starter production
        if not passes:
            pid = get_player_id_from_name(name)
            if pid:
                if pos in ['SP', 'P']:
                    prior = get_pitcher_stats(pid, date.today().year - 1)
                    if prior and prior.get('ip', 0) >= 100 and prior.get('era', 99) < 4.20:
                        passes   = True
                        reason   = f"Proven starter — {prior['ip']:.0f} IP last season"
                        stat_str = f"Prior ERA {prior['era']:.2f} | WHIP {prior['whip']:.2f}"
                else:
                    prior = get_hitter_stats(pid, date.today().year - 1)
                    if prior and prior.get('pa', 0) >= 300 and prior.get('ops', 0) >= 0.740:
                        passes   = True
                        reason   = f"Proven hitter — {prior['pa']} PA last season"
                        stat_str = f"Prior OPS {prior['ops']:.3f} | HR {prior['hr']}"

        if not passes:
            continue

        if not stat_str:
            pid = get_player_id_from_name(name)
            if pid:
                if pos in ['SP', 'RP', 'P']:
                    s = get_pitcher_stats_blended(pid)
                    if s and s.get('ip', 0) >= 5:
                        stat_str = f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | K/BB {s['kbb']:.1f}"
                else:
                    s = get_hitter_stats(pid)
                    if s and s.get('pa', 0) >= 20:
                        stat_str = f"OPS {s['ops']:.3f} | AVG {s['avg']:.3f} | HR {s['hr']}"

        return {
            'name':      canonical_name,
            'pct_owned': pct_owned,
            'stat_str':  stat_str,
            'reason':    reason,
        }
    return None

def _check_position_relevance(text, my_roster):
    pos_signals = {
        'SP': ['pitcher', 'starter', 'right-hander', 'left-hander', 'righty', 'lefty', 'ace'],
        'RP': ['reliever', 'closer', 'bullpen'],
        'C':  ['catcher'],
        '1B': ['first base', 'first baseman'],
        '2B': ['second base', 'second baseman'],
        '3B': ['third base', 'third baseman'],
        'SS': ['shortstop'],
        'OF': ['outfielder', 'outfield', 'center field', 'left field', 'right field'],
    }
    weak = get_weak_positions(my_roster)
    for pos, signals in pos_signals.items():
        if any(s in text for s in signals):
            if pos in weak or pos in ['SP', 'RP']:
                return True
            if pos in MY_CLOGGED_POSITIONS:
                return False
            return True
    return True

def send_overnight_digest():
    queue = load_sleep_queue()
    if not queue:
        print("  No overnight alerts queued")
        return
    by_cat = {}
    for item in queue:
        cat = item.get('category', 'other')
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append(item)
    lines        = [f"🌅 OVERNIGHT ({len(queue)} alert{'s' if len(queue) > 1 else ''}):\n"]
    max_priority = max(item.get('priority', 0) for item in queue)
    for cat, items in by_cat.items():
        for item in items:
            lines.append(f"{item['title']}\n{item['message'][:200]}\n")
    send_pushover("🌅 OVERNIGHT DIGEST", '\n'.join(lines)[:1024], priority=max_priority)
    save_sleep_queue([])
    print(f"  Sent overnight digest: {len(queue)} items")

# ============================================================
# IL RETURN HELPER
# ============================================================
def _get_pitchers_including_il_returns(roster, week_mon=None, week_sun=None):
    """
    Return set of pitcher name norms for start counting.
    Includes all SPs not on IL, plus IL pitchers whose return date falls this week.
    """
    norms  = set()
    target = roster if roster else []
    for p in target:
        pos = p.get('position', '')
        if pos not in ['SP', 'P']:
            continue
        status = p.get('status', '') or ''
        if 'IL' not in status:
            norms.add(p['name_normalized'])
            continue
        # On IL — check expected return date from injury_note
        return_date = p.get('injury_note', '') or ''
        if week_mon and week_sun:
            date_match = re.search(r'(\d{1,2})/(\d{1,2})', return_date)
            if date_match:
                try:
                    ret = date(date.today().year,
                               int(date_match.group(1)),
                               int(date_match.group(2)))
                    if week_mon <= ret <= week_sun:
                        norms.add(p['name_normalized'])
                        print(f"  IL return included: {p.get('name','')} (returns ~{ret})")
                except Exception:
                    pass
    return norms

def _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun):
    """Fetch opponent roster live and return pitcher norm set including IL returns."""
    opp_pitcher_norms = set()
    if not opp_team_id:
        return opp_pitcher_norms
    try:
        query          = get_yahoo_query()
        opp_roster_raw = query.get_team_roster_player_info_by_date(opp_team_id, today)
        opp_roster_list = []
        for p in (opp_roster_raw or []):
            try:
                pos    = str(getattr(p, 'primary_position', '') or '')
                name   = p.name.full
                status = str(getattr(p, 'status', '') or '')
                inj    = str(getattr(p, 'injury_note', '') or '')
                opp_roster_list.append({
                    'name': name, 'name_normalized': normalize_name(name),
                    'position': pos, 'status': status, 'injury_note': inj
                })
            except Exception:
                pass
        opp_pitcher_norms = _get_pitchers_including_il_returns(
            opp_roster_list, week_mon=week_mon, week_sun=week_sun
        )
    except Exception as e:
        print(f"  Opp roster fetch error: {e}")
    return opp_pitcher_norms

# ============================================================
# ALERT: CURRENT WEEK SP ANALYSIS (Monday 8:45am)
# ============================================================
def send_current_week_sp_analysis(taken, my_roster, team_ops):
    print("Running current week SP analysis...")
    today    = datetime.now(ET_TZ).date()
    week_mon = monday_of_week(today)
    week_sun = sunday_of_week(today)

    all_starters     = get_probable_pitchers(week_mon, week_sun, team_ops)
    my_pitcher_norms = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)

    matchup     = get_matchup_data()
    opp_team_id = matchup.get('opp_team_id') if matchup else None
    opp_pitcher_norms = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_starts  = []
    opp_starts = []
    for name, info in all_starters.items():
        norm  = normalize_name(name)
        stats = get_pitcher_stats_blended(info['id'])
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.720]))
        entry = {
            'name': name, 'count': info['count'],
            'stats': stats, 'is_hq': is_hq,
            'dates': info['dates'], 'opponents': info['opponents'],
            'opp_ops': info['opp_ops']
        }
        if norm in my_pitcher_norms:
            my_starts.append(entry)
        elif norm in opp_pitcher_norms:
            opp_starts.append(entry)

    my_total  = sum(s['count'] for s in my_starts)
    opp_total = sum(s['count'] for s in opp_starts)
    my_hq     = sum(s['count'] for s in my_starts if s['is_hq'])
    opp_hq    = sum(s['count'] for s in opp_starts if s['is_hq'])

    starts_deficit = opp_total - my_total
    hq_deficit     = opp_hq - my_hq

    lines = [
        f"⚾ WEEK SP ANALYSIS\n",
        f"📊 Probable starts: You {my_total} vs Opp {opp_total}",
        f"🌟 High-quality starts: You {my_hq} vs Opp {opp_hq}\n",
    ]

    needs_action = starts_deficit > 1 or hq_deficit > 2
    if needs_action:
        lines.append("⚠️ You're behind — scanning for adds (Mon–Wed):\n")
        wed            = week_mon + timedelta(days=2)
        early_starters = get_probable_pitchers(week_mon, wed, team_ops)
        candidates     = []
        for name, info in early_starters.items():
            if normalize_name(name) in taken:
                continue
            stats = get_pitcher_stats_blended(info['id'])
            if is_opener(stats):
                continue
            canonical, avail = validate_player_in_yahoo(name, taken)
            if not avail or canonical is None:
                continue
            hq    = [is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.720])]
            value = (
                info['count'] * 10
                + sum(5 for h in hq if h)
                - sum(3 for ops in info.get('opp_ops', []) if ops > 0.730)
            )
            candidates.append((value, canonical, info, stats, hq))
        candidates.sort(key=lambda x: x[0], reverse=True)
        if candidates:
            for _, cname, info, stats, hq in candidates[:2]:
                matchups = ', '.join(
                    f"{format_date(d)} vs {opp} {matchup_label(ops)}"
                    for d, opp, ops in zip(
                        info['dates'][:3], info['opponents'][:3], info['opp_ops'][:3]
                    )
                )
                stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                            if stats and stats.get('ip', 0) >= 5 else "Limited stats")
                lines.append(f"• {cname} ({info['count']} start{'s' if info['count']>1 else ''})\n"
                             f"  {stat_str}\n  {matchups}")
            drop_cand = find_best_drop(my_roster, team_ops)
            if drop_cand:
                lines.append(f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")
        else:
            lines.append("✅ No quality adds available Mon–Wed.")
    else:
        lines.append("✅ Staff looks solid — no adds needed.")

    send_pushover("⚾ WEEK SP ANALYSIS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: STREAMERS (Wed-Sun 7:00am)
# ============================================================
def send_streamers_alert(taken, my_roster, team_ops):
    print("Running streamers alert...")
    today    = datetime.now(ET_TZ).date()
    week_mon = monday_of_week(today)
    week_sun = sunday_of_week(today)

    matchup   = get_matchup_data()
    my_stats  = matchup.get('my_stats', {})  if matchup else {}
    opp_stats = matchup.get('opp_stats', {}) if matchup else {}

    all_starters     = get_probable_pitchers(today, week_sun, team_ops)
    my_pitcher_norms = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)

    # Fetch opponent roster live to capture same-day adds
    opp_team_id       = matchup.get('opp_team_id') if matchup else None
    opp_pitcher_norms = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_remaining  = []
    opp_remaining = []
    for name, info in all_starters.items():
        norm  = normalize_name(name)
        stats = get_pitcher_stats_blended(info['id'])
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.720]))
        entry = {'name': name, 'count': info['count'], 'is_hq': is_hq,
                 'stats': stats, 'dates': info['dates'],
                 'opponents': info['opponents'], 'opp_ops': info['opp_ops']}
        if norm in my_pitcher_norms:
            my_remaining.append(entry)
        elif norm in opp_pitcher_norms:
            opp_remaining.append(entry)

    my_starts  = sum(s['count'] for s in my_remaining)
    opp_starts = sum(s['count'] for s in opp_remaining)
    my_hq      = sum(s['count'] for s in my_remaining if s['is_hq'])
    opp_hq     = sum(s['count'] for s in opp_remaining if s['is_hq'])

    # Assess H2H pitching category standings
    pitching_cats = ['W', 'SV', 'K', 'ERA', 'WHIP', 'KBB']
    cats_losing   = []
    cats_winning  = []
    for cat in pitching_cats:
        my_val  = my_stats.get(cat)
        opp_val = opp_stats.get(cat)
        if my_val is None or opp_val is None:
            continue
        if cat in ['ERA', 'WHIP']:
            losing  = my_val > opp_val + 0.10
            winning = my_val < opp_val - 0.10
        else:
            losing  = my_val < opp_val * 0.92
            winning = my_val > opp_val * 1.08
        if losing:
            cats_losing.append(cat)
        elif winning:
            cats_winning.append(cat)

    starts_deficit = opp_starts - my_starts
    hq_deficit     = opp_hq - my_hq
    days_left      = days_left_in_week()

    # Streaming is needed if behind in starts OR losing categories with fewer starts
    need_streaming = (
        (starts_deficit > 0 and days_left >= 2)
        or (hq_deficit > 1 and days_left >= 2)
        or (len(cats_losing) >= 2 and my_starts <= opp_starts and days_left >= 1)
    )

    # If dominating and have more or equal starts, no need
    if len(cats_winning) >= 4 and my_hq >= opp_hq and my_starts >= opp_starts:
        need_streaming = False

    lines = [
        f"🌊 STREAMERS — {days_left}d remaining\n",
        f"Starts: You {my_starts} vs Opp {opp_starts} | HQ: You {my_hq} vs Opp {opp_hq}",
    ]
    if my_stats and opp_stats:
        if cats_losing:
            lines.append(f"📉 Losing: {', '.join(cats_losing)}")
        if cats_winning:
            lines.append(f"📈 Winning: {', '.join(cats_winning)}")
    lines.append("")

    if not need_streaming:
        lines.append("✅ No streaming needed — staff looks solid.")
        send_pushover("🌊 STREAMERS", '\n'.join(lines), priority=0)
        return

    lines.append("⚠️ Consider streaming:\n")
    candidates = []
    for name, info in all_starters.items():
        if normalize_name(name) in taken:
            continue
        stats = get_pitcher_stats_blended(info['id'])
        if is_opener(stats):
            continue
        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None:
            continue
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.720]))
        if not is_hq and info['count'] < 2:
            continue
        value = score_sp(stats, min(info.get('opp_ops', [0.720])))
        candidates.append((value, canonical, info, stats))
    candidates.sort(key=lambda x: x[0], reverse=True)
    if candidates:
        for _, cname, info, stats in candidates[:2]:
            matchups = ', '.join(
                f"{format_date(d)} vs {opp} {matchup_label(ops)}"
                for d, opp, ops in zip(
                    info['dates'][:3], info['opponents'][:3], info['opp_ops'][:3]
                )
            )
            stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                        if stats and stats.get('ip', 0) >= 5 else "Limited stats")
            lines.append(f"• {cname}\n  {stat_str}\n  {matchups}")
        drop_cand = find_best_drop(my_roster, team_ops)
        if drop_cand:
            lines.append(f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")
    else:
        lines.append("No quality streamers available.")

    send_pushover("🌊 STREAMERS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: 2-START SPs (Fri-Sun 8:30am)
# ============================================================
def send_two_start_alert(taken, my_roster, team_ops, preliminary=False):
    label = "preliminary" if preliminary else "full"
    print(f"Running {label} 2-start alert...")
    today      = datetime.now(ET_TZ).date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    next_mon   = today + timedelta(days=days_ahead)
    next_tue   = next_mon + timedelta(days=1)
    next_sun   = next_mon + timedelta(days=6)

    all_starters = get_probable_pitchers(next_mon, next_sun, team_ops)
    candidates   = []

    for name, info in all_starters.items():
        if normalize_name(name) in taken:
            continue
        stats = get_pitcher_stats_blended(info['id'])
        if is_opener(stats):
            continue
        confirmed_two = info['count'] >= 2
        early_start   = (info['dates'] and info['dates'][0] <= next_tue.isoformat())
        if not confirmed_two and not early_start:
            continue
        opp_ops_list = info.get('opp_ops', [0.720, 0.720])
        starts_hq    = [is_high_quality_sp(stats, ops) for ops in opp_ops_list[:2]]
        if not starts_hq[0]:
            continue
        second_hq = len(starts_hq) > 1 and starts_hq[1]
        if not second_hq and confirmed_two:
            continue
        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None:
            continue
        value = score_sp(stats)
        if confirmed_two:
            value += 20
        if second_hq:
            value += 10
        candidates.append((value, canonical, info, stats, confirmed_two, second_hq))

    if not candidates:
        if not preliminary:
            send_pushover("⚾ 2-START SPs",
                          f"No available 2-start quality options found for week of {next_mon}.",
                          priority=0)
        return

    candidates.sort(key=lambda x: x[0], reverse=True)
    prefix = "📋 EARLY LOOK — " if preliminary else ""
    lines  = [f"{prefix}⚾ 2-START SPs | Week of {next_mon}\n"]

    for _, cname, info, stats, conf_two, sec_hq in candidates[:3]:
        stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | "
                    f"{stats['k']}K | K/BB {stats['kbb']:.1f} ({stats.get('blend_note','')})"
                    if stats and stats.get('ip', 0) >= 5 else "Limited stats")
        start_lines = []
        for i, (d, opp, ops) in enumerate(zip(
            info['dates'][:2], info['opponents'][:2], info['opp_ops'][:2]
        )):
            hq_tag = "✅" if is_high_quality_sp(stats, ops) else "⚠️"
            start_lines.append(f"  Start {i+1}: {format_date(d)} vs {opp} {hq_tag} {matchup_label(ops)}")
        if not conf_two:
            start_lines.append("  Start 2: projected via rotation")
        lines.append(f"• {cname}\n  {stat_str}\n" + '\n'.join(start_lines))

    drop_cand = find_best_drop(my_roster, team_ops)
    if drop_cand:
        lines.append(f"\n💀 Drop candidate: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")

    title = "⚾ 2-START EARLY LOOK" if preliminary else "⚾ 2-START SPs"
    send_pushover(title, '\n'.join(lines), priority=0)

# ============================================================
# ALERT: START/SIT (Daily 9am)
# ============================================================
def send_start_sit_alert(my_roster, team_ops, taken):
    print("Running start/sit alert...")
    today_date   = date.today()
    all_starters = get_probable_pitchers(today_date, today_date, team_ops)

    # Include ALL SPs not on IL — bench players will be activated on start day
    my_sp_norms = {
        normalize_name(p['name']): p for p in my_roster
        if p['position'] in ['SP', 'P']
        and 'IL' not in (p['status'] or '')
    }

    sit_alerts  = []
    start_notes = []

    for name, info in all_starters.items():
        norm = normalize_name(name)
        if norm not in my_sp_norms:
            continue
        p         = my_sp_norms[norm]
        stats     = get_pitcher_stats_blended(info['id']) if info.get('id') else None
        opp_ops   = info['opp_ops'][0] if info['opp_ops'] else 0.720
        opp_name  = info['opponents'][0] if info['opponents'] else 'unknown'
        is_hq     = is_high_quality_sp(stats, opp_ops)
        long_term = sp_long_term_value(p, stats)

        if is_hq:
            start_notes.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")
        elif opp_ops > 0.750:
            sit_alerts.append({
                'name': name, 'opp': opp_name, 'opp_ops': opp_ops,
                'long_term': long_term, 'stats': stats, 'p': p
            })

    if not sit_alerts and not start_notes:
        print("  No SP starts today or no alerts needed")
        return

    lines = ["🎯 START/SIT — Today\n"]
    for note in start_notes:
        lines.append(note)

    for alert in sit_alerts:
        if alert['long_term']:
            lines.append(f"⚠️ SIT?: {alert['name']} vs {alert['opp']} "
                         f"{matchup_label(alert['opp_ops'])} — tough matchup, but keep (long-term value)")
        else:
            lines.append(f"❌ SIT: {alert['name']} vs {alert['opp']} "
                         f"{matchup_label(alert['opp_ops'])} — low long-term value + tough matchup")
            for avail_name, avail_info in all_starters.items():
                if normalize_name(avail_name) in taken:
                    continue
                avail_ops = avail_info['opp_ops'][0] if avail_info['opp_ops'] else 0.720
                if not is_high_quality_matchup(avail_ops):
                    continue
                avail_stats = get_pitcher_stats_blended(avail_info['id'])
                if not is_high_quality_sp(avail_stats, avail_ops):
                    continue
                canonical, avail = validate_player_in_yahoo(avail_name, taken)
                if not avail or canonical is None:
                    continue
                stat_str = (f"ERA {avail_stats['era']:.2f} | WHIP {avail_stats['whip']:.2f}"
                            if avail_stats and avail_stats.get('ip', 0) >= 5 else "Limited stats")
                lines.append(f"  🔄 Add instead: {canonical} vs "
                             f"{avail_info['opponents'][0] if avail_info['opponents'] else '?'} "
                             f"{matchup_label(avail_ops)} | {stat_str}")
                lines.append(f"  💀 Drop: {alert['name']} ({alert['p']['pct_owned']:.0f}%)")
                break

    if len(lines) > 1:
        send_pushover("🎯 START/SIT", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: PITCHER SCRATCH (Hourly 11am-6pm)
# ============================================================
def check_pitcher_scratch(my_roster, games):
    print("Checking pitcher scratches...")
    morning_probables = load_morning_probables()
    scratch_alerted   = load_scratch_alerted()
    if not morning_probables:
        print("  No morning probables stored — skipping")
        return
    current_probables = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']:
            continue
        if game['home_probable']:
            current_probables[game['home_team']] = game['home_probable']
        if game['away_probable']:
            current_probables[game['away_team']] = game['away_probable']
    # Check all SPs not on IL (bench players included — they may be starting)
    my_active_sps = [
        p for p in my_roster
        if p['position'] == 'SP'
        and 'IL' not in (p['status'] or '')
    ]
    for sp in my_active_sps:
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
            key = normalize_name(sp['name'])
            if key in scratch_alerted:
                continue
            send_pushover(
                f"🚫 SCRATCH: {sp['name']}",
                f"{sp['name']} was this morning's probable for {team_name} "
                f"but has been replaced by {current_starter}.\n\n"
                f"⚠️ Swap in a bench SP or grab a same-day streamer!",
                priority=1
            )
            scratch_alerted[key] = True
            save_scratch_alerted(scratch_alerted)

# ============================================================
# ALERT: BATTER SITTING / POSTPONED (Hourly 11am-6pm)
# ============================================================
def check_lineups_and_weather(my_roster, games):
    print("Checking lineups and postponements...")
    sitting_alerted = load_sitting_alerts()
    newly_alerted   = dict(sitting_alerted)
    my_hitters = [
        p for p in my_roster
        if p['position'] not in ['SP', 'RP', 'P']
        and 'IL' not in (p['status'] or '')
        and p['selected_position'] not in ['BN', 'IL']
    ]
    for game in games:
        home_team     = game['home_team']
        away_team     = game['away_team']
        status        = game['status']
        all_lineup    = game['home_lineup'] + game['away_lineup']
        lineup_posted = len(all_lineup) > 0
        game_soon     = _game_starts_soon(game, hours=3)
        for hitter in my_hitters:
            team_name = TEAM_NAME_MAP.get(hitter['team_abbr'], '')
            if not team_name or team_name not in (home_team, away_team):
                continue
            key = normalize_name(hitter['name'])
            if status in ['Postponed', 'Suspended']:
                if key not in sitting_alerted:
                    send_pushover(
                        f"🌧️ POSTPONED: {hitter['name']}",
                        f"{away_team} @ {home_team} postponed.\n"
                        f"{hitter['name']} will not play today.\n\n"
                        f"⚠️ Swap in a bench hitter!",
                        priority=1
                    )
                    newly_alerted[key] = 'postponed'
                continue
            if not game_soon:
                continue
            if lineup_posted and status not in ['Final', 'Game Over', 'In Progress']:
                if key in sitting_alerted:
                    continue
                in_lineup = any(
                    normalize_name(hitter['name']) in normalize_name(lp) or
                    normalize_name(lp) in normalize_name(hitter['name'])
                    for lp in all_lineup
                )
                if not in_lineup:
                    send_pushover(
                        f"🪑 SITTING: {hitter['name']}",
                        f"{hitter['name']} is NOT in today's lineup for {team_name}.\n\n"
                        f"⚠️ Swap in a bench hitter before lock!",
                        priority=1
                    )
                    newly_alerted[key] = 'sitting'
    save_sitting_alerts(newly_alerted)

def _game_starts_soon(game, hours=3):
    try:
        game_time   = game.get('game_time_utc', '')
        if not game_time:
            return True
        game_dt     = datetime.strptime(game_time[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
        hours_until = (game_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        return -1 <= hours_until <= hours
    except Exception:
        return True

# ============================================================
# ALERT: WAIVER DROPS (Daily 9am)
# ============================================================
def send_waiver_drops_alert(taken, my_roster, team_ops):
    print("Running waiver drops check...")
    recent_drops = get_waiver_drops_to_review(taken, my_roster)
    if not recent_drops:
        print("  No recent drops to review")
        return

    today            = date.today()
    week_sun         = sunday_of_week(today)
    my_week_starters = get_probable_pitchers(today, week_sun, team_ops)
    my_by_pos        = {}
    for p in my_roster:
        pos = p['position']
        if pos not in my_by_pos:
            my_by_pos[pos] = []
        my_by_pos[pos].append(p)

    alerts = []

    for drop in recent_drops:
        name = drop['name']
        pos  = drop['position']

        # Validate exists in Yahoo and is available
        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None:
            continue

        # Resolve blank position via MLB Stats API fallback
        if not pos:
            mlb_id_tmp = get_player_id_from_name(name)
            if mlb_id_tmp:
                try:
                    url = f"https://statsapi.mlb.com/api/v1/people/{mlb_id_tmp}"
                    d   = requests.get(url, timeout=3).json()
                    pos = d.get('people', [{}])[0].get('primaryPosition', {}).get('abbreviation', '')
                except Exception:
                    pass

        # Skip clogged positions
        if pos in MY_CLOGGED_POSITIONS:
            continue

        # Check IL status — handle stash logic separately
        il_signals   = ['60-day', '10-day', '15-day', 'injured list', 'il']
        drop_reason  = drop.get('notes', '').lower()
        days_out_est = _estimate_days_out(drop_reason)
        player_on_il = any(s in drop_reason for s in il_signals) or days_out_est is not None

        if player_on_il:
            # Check if worth stashing
            il_used  = count_my_il_slots_used(my_roster)
            has_slot = il_used < MY_IL_SLOTS
            worst_il = get_worst_il_stash(my_roster)
            can_bump = (worst_il and worst_il['pct_owned'] < 30 and il_used >= MY_IL_SLOTS)

            # Stash only if high value AND short-ish IL (≤60 days for elite, ≤21 for others)
            mlb_id = get_player_id_from_name(name)
            if not mlb_id:
                continue
            is_elite = False
            if pos in ['SP', 'RP', 'P']:
                stats    = get_pitcher_stats_blended(mlb_id)
                is_elite = (stats and stats.get('era', 99) < 3.60 and stats.get('ip', 0) >= 20)
            else:
                stats    = get_hitter_stats(mlb_id)
                is_elite = (stats and stats.get('ops', 0) >= 0.850 and stats.get('pa', 0) >= 50)
            is_elite = is_elite or (normalize_name(name) in TOP_PROSPECTS)

            stash_worthy = (
                (is_elite and days_out_est and days_out_est <= 60)
                or (not is_elite and days_out_est and days_out_est <= 21)
            )
            if not stash_worthy or (not has_slot and not can_bump):
                continue

            stat_str = ''
            if pos in ['SP', 'RP', 'P'] and stats and stats.get('ip', 0) >= 5:
                stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
            elif stats and stats.get('pa', 0) >= 20:
                stat_str = f"OPS {stats['ops']:.3f} | HR {stats['hr']}"

            slot_str  = "Open IL slot" if has_slot else f"Bump {worst_il['name']} from IL"
            drop_ts   = drop.get('timestamp', 0)
            avail_dt  = datetime.fromtimestamp(drop_ts, tz=ET_TZ) + timedelta(days=2)
            alerts.append({
                'name':       canonical,
                'pos':        pos,
                'stat_str':   stat_str,
                'reason':     f"IL stash — {slot_str} | ~{days_out_est}d timeline",
                'drop':       worst_il or {'name': 'IL slot player', 'pct_owned': 0},
                'avail_date': avail_dt.strftime('%a %-m/%-d'),
                'stash':      True,
            })
            continue

        # Standard (healthy) player — full relevance gate
        mlb_id   = get_player_id_from_name(name)
        stats    = None
        stat_str = ''
        value    = 0
        is_pitcher = pos in ['SP', 'RP', 'P']

        if is_pitcher:
            if mlb_id:
                stats = get_pitcher_stats_blended(mlb_id)
                if stats and stats.get('ip', 0) >= 5:
                    value    = score_sp(stats)
                    stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
        else:
            if mlb_id:
                stats = get_hitter_stats(mlb_id)
                if stats and stats.get('pa', 0) >= 20:
                    value    = stats.get('ops', 0) * 100
                    stat_str = f"AVG {stats['avg']:.3f} | OPS {stats['ops']:.3f} | HR {stats['hr']}"

        if not stats:
            continue

        passes      = False
        drop_target = None
        reason      = ''
        norm_drop   = normalize_name(name)

        if is_pitcher:
            my_pitchers = (my_by_pos.get('SP', []) + my_by_pos.get('RP', [])
                           + my_by_pos.get('P', []))
            for mp in my_pitchers:
                if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE:
                    continue
                mp_id    = get_player_id_from_name(mp['name'])
                mp_stats = get_pitcher_stats_blended(mp_id) if mp_id else None
                mp_value = score_sp(mp_stats) if mp_stats else -999
                if value > mp_value + 5:
                    passes      = True
                    drop_target = mp
                    reason      = f"Better long-term value than {mp['name']}"
                    break
            if not passes:
                my_streamers   = [mp for mp in my_pitchers
                                   if not sp_long_term_value(mp, None) and mp['pct_owned'] < 50]
                drop_has_start = norm_drop in {normalize_name(k) for k in my_week_starters}
                for mp in my_streamers:
                    mp_norm      = normalize_name(mp['name'])
                    mp_id        = get_player_id_from_name(mp['name'])
                    mp_stats     = get_pitcher_stats_blended(mp_id) if mp_id else None
                    mp_value     = score_sp(mp_stats) if mp_stats else -999
                    if value > mp_value + 5 and drop_has_start:
                        for sp_name, sp_info in my_week_starters.items():
                            if normalize_name(sp_name) == norm_drop:
                                opp_ops = sp_info['opp_ops'][0] if sp_info['opp_ops'] else 0.720
                                if is_high_quality_matchup(opp_ops):
                                    passes      = True
                                    drop_target = mp
                                    reason      = f"Better streaming option than {mp['name']} this week"
                                break
                    if passes:
                        break
        else:
            compatible_positions = _get_compatible_positions(pos)
            for comp_pos in compatible_positions:
                my_at_pos = my_by_pos.get(comp_pos, [])
                for mp in my_at_pos:
                    if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE:
                        continue
                    others_at_pos = [x for x in my_at_pos if x['name'] != mp['name']]
                    if not others_at_pos and comp_pos not in ['Util', 'BN']:
                        continue
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_hitter_stats(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    if value > mp_value + 8:
                        passes      = True
                        drop_target = mp
                        reason      = f"Better than {mp['name']} at {comp_pos}"
                        break
                if passes:
                    break
            if not passes:
                bench = my_by_pos.get('BN', []) + my_by_pos.get('Util', [])
                for mp in bench:
                    if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE:
                        continue
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_hitter_stats(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    if value > mp_value + 8:
                        passes      = True
                        drop_target = mp
                        reason      = f"Upgrade over bench player {mp['name']}"
                        break

        if not passes or not drop_target:
            continue

        drop_ts  = drop.get('timestamp', 0)
        avail_dt = datetime.fromtimestamp(drop_ts, tz=ET_TZ) + timedelta(days=2)
        alerts.append({
            'name':       canonical,
            'pos':        pos,
            'stat_str':   stat_str,
            'reason':     reason,
            'drop':       drop_target,
            'avail_date': avail_dt.strftime('%a %-m/%-d'),
            'stash':      False,
        })

    if not alerts:
        print("  No meaningful waiver drops found")
        return

    lines = ["🗑️ WAIVER DROPS — Worth considering:\n"]
    for a in alerts[:3]:
        stash_label = " 🏥 IL STASH" if a.get('stash') else ""
        lines.append(
            f"• {a['name']} ({a['pos']}){stash_label}"
            f"{' — ' + a['stat_str'] if a['stat_str'] else ''}\n"
            f"  {a['reason']}\n"
            f"  Available: {a['avail_date']}\n"
            f"  💀 Drop/move: {a['drop']['name']} ({a['drop'].get('pct_owned', 0):.0f}%)"
        )
    send_pushover("🗑️ WAIVER DROPS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: NEW POSITIONAL ELIGIBILITY (Daily 9am)
# ============================================================
def check_positional_eligibility(my_roster, team_ops):
    print("Checking positional eligibility...")
    weak_positions = get_weak_positions(my_roster)
    if not weak_positions:
        print("  No weak positions")
        return
    hitter_weak = [p for p in weak_positions if p not in ['SP', 'RP', 'P']]
    if not hitter_weak:
        return
    alerted = load_pos_eligibility_alerts()
    found   = []
    for pos in hitter_weak:
        fa = get_league_free_agents(position=pos, count=15)
        for player in fa:
            name = player['name']
            norm = normalize_name(name)
            key  = f"{norm}:{pos}"
            if key in alerted:
                continue
            pid = get_player_id_from_name(name)
            if not pid:
                continue
            try:
                url  = (f"https://statsapi.mlb.com/api/v1/people/{pid}/stats"
                        f"?stats=season&group=fielding&season={date.today().year}")
                data = requests.get(url, timeout=5).json()
                for sg in data.get('stats', []):
                    for split in sg.get('splits', []):
                        split_pos = split.get('position', {}).get('abbreviation', '')
                        games_at  = int(split.get('stat', {}).get('games', 0) or 0)
                        if split_pos == pos and 5 <= games_at < 10:
                            found.append({
                                'name': name, 'pos': pos,
                                'games_at': games_at, 'key': key,
                                'pct_owned': player['pct_owned']
                            })
                            alerted[key] = True
            except Exception:
                continue
    if found:
        lines = ["📍 NEW POS ELIGIBILITY APPROACHING:\n"]
        for f in found[:5]:
            lines.append(
                f"• {f['name']} — {f['games_at']}/10 games at {f['pos']} "
                f"({f['pct_owned']:.0f}% owned)\n"
                f"  Would fill your weak {f['pos']} spot once eligible!"
            )
        send_pushover("📍 POS ELIGIBILITY", '\n'.join(lines), priority=0)
        save_pos_eligibility_alerts(alerted)
    else:
        if datetime.now(ET_TZ).weekday() == 6:
            if not any(k for k in alerted):
                send_pushover(
                    "📍 POS ELIGIBILITY",
                    "No players approaching new positional eligibility at your weak spots this week.",
                    priority=0
                )

# ============================================================
# ALERT: TRADE SUGGESTIONS (Friday 1pm)
# ============================================================
def send_trade_suggestions(my_roster, all_rosters, team_ops):
    print("Running trade suggestion analysis...")
    trade_history = load_trade_history()
    week_ago      = datetime.now(timezone.utc).timestamp() - (14 * 86400)

    def player_value(p):
        pid = get_player_id_from_name(p['name'])
        if p['position'] in ['SP', 'P', 'RP']:
            stats = get_pitcher_stats_blended(pid) if pid else None
            return score_sp(stats) + p['pct_owned'] * 0.3
        else:
            stats = get_hitter_stats(pid) if pid else None
            if stats and stats.get('pa', 0) >= 20:
                return stats.get('ops', 0) * 80 + p['pct_owned'] * 0.3
            return p['pct_owned'] * 0.5

    def roster_score_by_pos(roster):
        by_pos = {}
        for p in roster:
            pos = p['position']
            if pos in ['BN', 'IL', 'Util']:
                continue
            if pos not in by_pos:
                by_pos[pos] = []
            by_pos[pos].append(player_value(p))
        return {pos: round(sum(vals) / len(vals), 1) for pos, vals in by_pos.items() if vals}

    my_scores    = roster_score_by_pos(my_roster)
    hitting_pos  = ['C', '1B', '2B', '3B', 'SS', 'OF']
    pitching_pos = ['SP', 'RP']

    my_weak_hit = sorted([p for p in hitting_pos  if p in my_scores],
                         key=lambda x: my_scores.get(x, 0))[:2]
    my_str_hit  = sorted([p for p in hitting_pos  if p in my_scores],
                         key=lambda x: my_scores.get(x, 0), reverse=True)[:2]
    my_weak_pit = sorted([p for p in pitching_pos if p in my_scores],
                         key=lambda x: my_scores.get(x, 0))[:1]
    my_str_pit  = sorted([p for p in pitching_pos if p in my_scores],
                         key=lambda x: my_scores.get(x, 0), reverse=True)[:1]

    my_weak  = my_weak_hit + my_weak_pit
    my_strong = my_str_hit + my_str_pit

    proposals = []

    for team_id, their_roster in (all_rosters or {}).items():
        if team_id == MY_TEAM_ID or not their_roster:
            continue
        their_scores   = roster_score_by_pos(their_roster)
        their_weak_pos = sorted(their_scores.keys(), key=lambda x: their_scores.get(x, 0))[:3]
        their_str_pos  = sorted(their_scores.keys(), key=lambda x: their_scores.get(x, 0),
                                reverse=True)[:3]

        for give_pos in my_strong:
            if give_pos not in their_weak_pos:
                continue
            for get_pos in my_weak:
                if get_pos not in their_str_pos:
                    continue
                if give_pos == get_pos:
                    continue

                my_give_candidates = sorted(
                    [p for p in my_roster
                     if p['position'] == give_pos
                     and not p['is_undroppable']
                     and p['name_normalized'] not in MY_UNDROPPABLE
                     and p['pct_owned'] >= 40],
                    key=player_value, reverse=True
                )
                their_get_candidates = sorted(
                    [p for p in their_roster
                     if p['position'] == get_pos
                     and not p.get('is_undroppable', False)
                     and p['pct_owned'] >= 40],
                    key=player_value, reverse=True
                )

                if not my_give_candidates or not their_get_candidates:
                    continue

                my_give   = my_give_candidates[0]
                their_get = their_get_candidates[0]

                # Fairness check
                if abs(my_give['pct_owned'] - their_get['pct_owned']) > 25:
                    continue

                # Validate incoming player fills genuine need
                if my_scores.get(give_pos, 0) <= my_scores.get(get_pos, 0):
                    continue

                # POSITIONAL COVERAGE CHECK: ensure I still have a hitter at give_pos after trade
                if give_pos in hitting_pos:
                    remaining_at_give_pos = [
                        p for p in my_roster
                        if p['position'] == give_pos
                        and p['name'] != my_give['name']
                        and 'IL' not in (p['status'] or '')
                    ]
                    if not remaining_at_give_pos:
                        continue  # Would leave me with no player at that position

                # Trade history check
                pair_key = tuple(sorted([
                    normalize_name(my_give['name']),
                    normalize_name(their_get['name'])
                ]))
                prior        = [t for t in trade_history if t.get('pair') == list(pair_key)]
                recent_prior = [t for t in prior if t.get('ts', 0) > week_ago]
                if len(prior) >= 2 or recent_prior:
                    continue

                rationale = (
                    f"Your {give_pos} depth ({my_give['name']}) fills their {give_pos} need; "
                    f"their {get_pos} asset ({their_get['name']}) upgrades your {get_pos}."
                )
                proposals.append({
                    'give':      my_give['name'], 'give_pos': give_pos,
                    'get':       their_get['name'], 'get_pos': get_pos,
                    'give_pct':  my_give['pct_owned'],
                    'get_pct':   their_get['pct_owned'],
                    'rationale': rationale,
                    'pair':      list(pair_key),
                })

    if not proposals:
        print("  No fair trade opportunities found this week")
        send_pushover("🔄 TRADE IDEA",
                      "No equitable trade opportunities identified this week.", priority=0)
        return

    lines = ["🔄 TRADE IDEA — Friday 1pm\n"]
    for prop in proposals[:2]:
        lines.append(
            f"📤 You give: {prop['give']} ({prop['give_pos']}, {prop['give_pct']:.0f}% owned)\n"
            f"📥 You get: {prop['get']} ({prop['get_pos']}, {prop['get_pct']:.0f}% owned)\n"
            f"💡 {prop['rationale']}\n"
        )
        trade_history.append({
            'pair': prop['pair'],
            'ts':   datetime.now(timezone.utc).timestamp()
        })
    save_trade_history(trade_history)
    send_pushover("🔄 TRADE IDEA", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: LEAGUEMATE INTEL (Sunday 9pm)
# ============================================================
def send_leaguemate_intel():
    print("Running leaguemate intel...")
    transactions = load_transactions()
    if not transactions:
        send_pushover("🕵️ LEAGUE INTEL",
                      "Not enough transaction data yet. Check back next week.", priority=0)
        return
    total = len(transactions)
    adds  = [t for t in transactions if 'add' in t.get('type', '')]
    drops = [t for t in transactions if 'drop' in t.get('type', '')]
    lines = [f"🕵️ LEAGUEMATE INTEL\n",
             f"Season: {total} transactions | {len(adds)} adds | {len(drops)} drops\n"]
    team_add_counts = {}
    for t in adds:
        for p in t.get('players', []):
            dest = p.get('dest_team', '')
            if dest:
                team_add_counts[dest] = team_add_counts.get(dest, 0) + 1
    if team_add_counts:
        sorted_teams = sorted(team_add_counts.items(), key=lambda x: x[1], reverse=True)
        lines.append("🔥 Most active managers (adds this season):")
        for team_key, count in sorted_teams[:5]:
            lines.append(f"  • Team {team_key}: {count} adds")
    week_ago = datetime.now(timezone.utc).timestamp() - (7 * 86400)
    recent   = [t for t in adds if t.get('timestamp', 0) > week_ago]
    lines.append(f"\n📅 Last 7 days: {len(recent)} adds")
    if recent:
        lines.append("⚡ Watch these managers — they move fast.")
    lines.append("\n📱 Check Yahoo transactions for full detail.")
    send_pushover("🕵️ LEAGUE INTEL", '\n'.join(lines), priority=0)

# ============================================================
# MORNING PROBABLES SNAPSHOT (Daily 8am)
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

# ============================================================
# MAIN
# ============================================================
def main():
    now_utc   = datetime.now(timezone.utc)
    now_et    = datetime.now(ET_TZ)
    hour_et   = now_et.hour
    minute_et = now_et.minute
    weekday   = now_et.weekday()  # 0=Mon … 6=Sun

    print(f"\n{'='*52}")
    print(f"Run: {now_utc.strftime('%Y-%m-%d %H:%M UTC')} | "
          f"{now_et.strftime('%H:%M ET %A')} | Week {get_current_week()}")
    print(f"{'='*52}")

    def at(h, m_start=0, m_end=14):
        return hour_et == h and m_start <= minute_et <= m_end

    def between(h1, h2):
        return h1 <= hour_et <= h2

    in_sleep = hour_et >= 22 or hour_et < 6 or (hour_et == 6 and minute_et < 30)
    is_awake = not in_sleep

    taken, my_roster, all_rosters = None, None, None
    games                          = None
    team_ops                       = None

    def ensure_rosters():
        nonlocal taken, my_roster, all_rosters
        if taken is None:
            taken, my_roster, all_rosters = get_all_rosters()
        return taken is not None

    def ensure_games():
        nonlocal games
        if games is None:
            games = get_todays_schedule()
        return games

    def ensure_team_ops():
        nonlocal team_ops
        if team_ops is None:
            team_ops = get_team_batting_stats()
        return team_ops

    # ── 6:30am: OVERNIGHT DIGEST ────────────────────────────
    if at(6, 30, 44):
        print("\n--- OVERNIGHT DIGEST ---")
        send_overnight_digest()

    # ── 8:00am: MORNING PROBABLES SNAPSHOT ──────────────────
    if at(8, 0, 14):
        print("\n--- MORNING PROBABLES SNAPSHOT ---")
        ensure_games()
        store_morning_probables(games)

    # ── 8:45am Monday: CURRENT WEEK SP ANALYSIS ─────────────
    if weekday == 0 and at(8, 45, 59):
        print("\n--- CURRENT WEEK SP ANALYSIS ---")
        if ensure_rosters() and ensure_team_ops():
            send_current_week_sp_analysis(taken, my_roster, team_ops)

    # ── 9:00am daily: START/SIT + WAIVER DROPS + POS ELIG ───
    if at(9, 0, 14):
        print("\n--- DAILY 9AM ALERTS ---")
        if ensure_rosters() and ensure_team_ops():
            ensure_games()
            send_start_sit_alert(my_roster, team_ops, taken)
            send_waiver_drops_alert(taken, my_roster, team_ops)
            check_positional_eligibility(my_roster, team_ops)

    # ── 7:00am Wed-Sun: STREAMERS ───────────────────────────
    if weekday in [2, 3, 4, 5, 6] and at(7, 0, 14):
        print("\n--- STREAMERS ALERT ---")
        if ensure_rosters() and ensure_team_ops():
            send_streamers_alert(taken, my_roster, team_ops)

    # ── 8:30am Fri/Sat/Sun: 2-START SPs ─────────────────────
    if weekday in [4, 5, 6] and at(8, 30, 44):
        print("\n--- 2-START SPs ---")
        preliminary = (weekday == 4)
        if ensure_rosters() and ensure_team_ops():
            send_two_start_alert(taken, my_roster, team_ops, preliminary=preliminary)

    # ── Hourly 11am-6pm: PITCHER SCRATCH ────────────────────
    if between(11, 18) and minute_et < 5:
        print("\n--- PITCHER SCRATCH CHECK ---")
        if ensure_rosters():
            ensure_games()
            check_pitcher_scratch(my_roster, games)

    # ── Hourly 11am-6pm: BATTER SITTING / POSTPONED ─────────
    if between(11, 18) and 5 <= minute_et < 10:
        print("\n--- LINEUP / SITTING CHECK ---")
        if ensure_rosters():
            ensure_games()
            check_lineups_and_weather(my_roster, games)

    # ── 1:00pm Friday: TRADE SUGGESTIONS ────────────────────
    if weekday == 4 and at(13, 0, 14):
        print("\n--- TRADE SUGGESTIONS ---")
        if ensure_rosters() and ensure_team_ops():
            send_trade_suggestions(my_roster, all_rosters, team_ops)

    # ── Sunday 9pm: LEAGUEMATE INTEL ────────────────────────
    if weekday == 6 and at(21, 0, 14):
        print("\n--- LEAGUEMATE INTEL ---")
        send_leaguemate_intel()

    # ── BREAKING NEWS (every 15 min, 24/7) ──────────────────
    print("\n--- BREAKING NEWS CHECK ---")
    ensure_team_ops()
    news = get_all_news(lookback_minutes=20)
    if ensure_rosters():
        process_breaking_news(news, taken, my_roster, team_ops)
        try:
            sync_league_transactions()
        except Exception as e:
            print(f"  Transaction sync skipped: {e}")

    if in_sleep:
        print("\n[Sleep window — alerts queued for 6:30am digest]")

    print("\nDone.")

if __name__ == "__main__":
    main()
