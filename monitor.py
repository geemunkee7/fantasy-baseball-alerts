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

# ── State files ──────────────────────────────────────────────
PROBABLES_FILE       = '/tmp/morning_probables.json'
SITTING_ALERTS_FILE  = '/tmp/sitting_alerts.json'
SEEN_ALERTS_FILE     = '/tmp/seen_alerts.json'
TRANSACTIONS_FILE    = '/tmp/league_transactions.json'
MATCHUP_CACHE_FILE   = '/tmp/matchup_cache.json'
CLOSER_CACHE_FILE    = '/tmp/closer_cache.json'
SLEEP_QUEUE_FILE     = '/tmp/sleep_queue.json'
LEAGUEMATE_FILE      = '/tmp/leaguemate_profiles.json'
TRADE_HISTORY_FILE   = '/tmp/trade_proposals.json'
POS_ELIGIBILITY_FILE = '/tmp/pos_eligibility_alerts.json'
SCRATCH_ALERTED_FILE = '/tmp/scratch_alerted.json'

# ============================================================
# CONSTANTS
# ============================================================
MY_CLOGGED_POSITIONS = {'SS', 'OF'}
MY_UNDROPPABLE = {
    "gunnar henderson", "trea turner", "matt olson",
    "shohei ohtani", "nico hoerner"
}
MY_IL_SLOTS  = 3
LEAGUE_TEAMS = 12
ROSTER_SIZE  = 25

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
    "dean kremer", "connor prielipp",
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
    {"name": "MLB-orioles",   "url": "https://www.mlb.com/orioles/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-redsox",    "url": "https://www.mlb.com/red-sox/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-yankees",   "url": "https://www.mlb.com/yankees/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-rays",      "url": "https://www.mlb.com/rays/feeds/news/rss.xml",      "type": "team"},
    {"name": "MLB-bluejays",  "url": "https://www.mlb.com/blue-jays/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-whitesox",  "url": "https://www.mlb.com/white-sox/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-guardians", "url": "https://www.mlb.com/guardians/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-tigers",    "url": "https://www.mlb.com/tigers/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-royals",    "url": "https://www.mlb.com/royals/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-twins",     "url": "https://www.mlb.com/twins/feeds/news/rss.xml",     "type": "team"},
    {"name": "MLB-astros",    "url": "https://www.mlb.com/astros/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-angels",    "url": "https://www.mlb.com/angels/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-athletics", "url": "https://www.mlb.com/athletics/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-mariners",  "url": "https://www.mlb.com/mariners/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-rangers",   "url": "https://www.mlb.com/rangers/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-braves",    "url": "https://www.mlb.com/braves/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-marlins",   "url": "https://www.mlb.com/marlins/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-mets",      "url": "https://www.mlb.com/mets/feeds/news/rss.xml",      "type": "team"},
    {"name": "MLB-phillies",  "url": "https://www.mlb.com/phillies/feeds/news/rss.xml",  "type": "team"},
    {"name": "MLB-nationals", "url": "https://www.mlb.com/nationals/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-cubs",      "url": "https://www.mlb.com/cubs/feeds/news/rss.xml",      "type": "team"},
    {"name": "MLB-reds",      "url": "https://www.mlb.com/reds/feeds/news/rss.xml",      "type": "team"},
    {"name": "MLB-brewers",   "url": "https://www.mlb.com/brewers/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-pirates",   "url": "https://www.mlb.com/pirates/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-cardinals", "url": "https://www.mlb.com/cardinals/feeds/news/rss.xml", "type": "team"},
    {"name": "MLB-dbacks",    "url": "https://www.mlb.com/d-backs/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-rockies",   "url": "https://www.mlb.com/rockies/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-dodgers",   "url": "https://www.mlb.com/dodgers/feeds/news/rss.xml",   "type": "team"},
    {"name": "MLB-padres",    "url": "https://www.mlb.com/padres/feeds/news/rss.xml",    "type": "team"},
    {"name": "MLB-giants",    "url": "https://www.mlb.com/giants/feeds/news/rss.xml",    "type": "team"},
]

# ============================================================
# DYNAMIC THRESHOLDS — all derived from 2026 MLB data + league structure
# ============================================================

def get_days_into_season():
    return max(0, (date.today() - SEASON_START).days)

def get_season_blend():
    """
    Prior/current weight blend based on days into season.
    Breakpoints tied to IP stabilization milestones:
    ~30 IP = early signal, ~80 IP = meaningful, ~160 IP = full stabilization.
    """
    d = get_days_into_season()
    if d < 21:    return 0.85, 0.15
    elif d < 42:  return 0.70, 0.30
    elif d < 77:  return 0.50, 0.50
    elif d < 112: return 0.30, 0.70
    else:         return 0.10, 0.90

def prior_season_ip_weight(prior_ip):
    """
    Scale prior season weight by actual IP sample.
    ERA stabilizes at ~160 IP. Below that, reduce prior weight proportionally.
    Prevents penalizing pitchers with injury-shortened prior seasons.
    """
    return min(1.0, prior_ip / 160.0)

def get_min_ip_for_significance():
    """
    Minimum IP for SP stats to carry meaningful signal.
    Scales with days into season — 3 starts early, 7-8 starts mid-season.
    """
    d = get_days_into_season()
    if d < 21:    return 9
    elif d < 42:  return 18
    elif d < 77:  return 30
    else:         return 45

def get_min_ip_prior_season():
    """
    Minimum prior season IP for any predictive value.
    Below 80 IP, prior year sample is too noisy.
    """
    return 80

def get_opp_ops_tiers():
    """
    OPS matchup tiers from 2026 MLB team distribution (30 teams).
    Data: LAD .863 (top), median ~.730, bottom ~.680-.700.
    Tiers = percentile bands across 30 teams.
    very_weak = bottom 17% (5 teams), weak = bottom 33% (10 teams),
    average = middle, strong = top 33%, elite = top 17%.
    """
    return {
        'very_weak': 0.696,
        'weak':      0.719,
        'average':   0.750,
        'strong':    0.787,
    }

def matchup_label(opp_ops):
    t = get_opp_ops_tiers()
    if opp_ops   <= t['very_weak']: return '✅ Great'
    elif opp_ops <= t['weak']:      return '✅ Good'
    elif opp_ops <= t['average']:   return '⚠️ Neutral'
    elif opp_ops <= t['strong']:    return '❌ Tough'
    else:                           return '❌❌ Elite Offense'

def is_high_quality_matchup(opp_ops):
    return opp_ops <= get_opp_ops_tiers()['weak']

def get_sp_quality_thresholds():
    """
    SP tier thresholds from 2026 MLB SP population.
    League avg ERA ~4.20, WHIP ~1.27, K/BB ~2.2.
    Tiers = approximate percentile bands among qualified starters.
    """
    return {
        'elite':     {'era': 3.20, 'whip': 1.10, 'kbb': 3.0},
        'above_avg': {'era': 3.85, 'whip': 1.22, 'kbb': 2.4},
        'average':   {'era': 4.25, 'whip': 1.32, 'kbb': 1.9},
        'below_avg': {'era': 4.75, 'whip': 1.42, 'kbb': 1.5},
    }

def get_sp_tier(stats):
    if not stats or stats.get('ip', 0) < get_min_ip_for_significance():
        return 'unknown'
    era  = stats.get('era',  99)
    whip = stats.get('whip', 9.99)
    kbb  = stats.get('kbb',  0)
    t    = get_sp_quality_thresholds()
    if era <= t['elite']['era'] and whip <= t['elite']['whip'] and kbb >= t['elite']['kbb']:
        return 'elite'
    if era <= t['above_avg']['era'] and whip <= t['above_avg']['whip'] and kbb >= t['above_avg']['kbb']:
        return 'above_avg'
    if era <= t['average']['era'] and whip <= t['average']['whip'] and kbb >= t['average']['kbb']:
        return 'average'
    if era <= t['below_avg']['era'] and whip <= t['below_avg']['whip'] and kbb >= t['below_avg']['kbb']:
        return 'below_avg'
    return 'replacement'

def get_ownership_thresholds():
    """
    Ownership thresholds for 12-team Yahoo league.
    Fractions of 12 teams: majority=7/12=58%, significant=4/12=33%,
    meaningful=2/12=16.7%, speculative=1/12=8.3%.
    """
    return {
        'majority':    58.0,
        'significant': 33.0,
        'meaningful':  16.7,
        'speculative':  8.3,
    }

def get_hitter_thresholds():
    """
    Hitter quality thresholds from 2026 MLB batting data.
    League avg OPS for starters ~.740.
    PA stabilization: OPS meaningful at ~60 PA early season, full signal at 300 PA.
    """
    return {
        'elite_ops':       0.870,
        'above_avg_ops':   0.800,
        'average_ops':     0.730,
        'replacement_ops': 0.680,
        'elite_il_ops':    0.850,
        'min_pa_display':  20,
        'min_pa_signal':   60,
        'min_pa_full':     300,
    }

def get_h2h_margin_thresholds():
    """
    H2H category win/loss margins for 12-team league.
    ERA/WHIP: 0.08 = ~1 SD in weekly roster totals.
    Counting stats: 8% gap = ~1 SD separation in weekly H2H.
    """
    return {
        'era_whip_margin':   0.08,
        'counting_pct':      0.08,
        'hq_starts_deficit': 2,
        'starts_deficit':    1,
    }

def get_relief_pitcher_thresholds():
    """
    Non-closer RP roster value thresholds.
    Must beat league avg RP ERA (~3.80) by meaningful margin.
    Must project enough weekly IP to actually impact weekly category totals.
    K/9 >= 10.0 = top ~20% of relievers for K contribution.
    min_weekly_ip = 3.0 based on ~2.5 apps/wk x ~1.1 IP/app.
    """
    return {
        'min_era':       3.50,
        'min_whip':      1.18,
        'min_kbb':       2.8,
        'min_k9':        10.0,
        'min_weekly_ip': 3.0,
        'ip_per_app':    1.1,
    }

# ============================================================
# UTILITY
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
    return max(1, (get_days_into_season() // 7) + 1)

def monday_of_week(d=None):
    if d is None: d = date.today()
    return d - timedelta(days=d.weekday())

def sunday_of_week(d=None):
    return monday_of_week(d) + timedelta(days=6)

def days_left_in_week():
    return max(0, 6 - date.today().weekday())

def format_date(d):
    if isinstance(d, str):
        try: d = datetime.strptime(d, '%Y-%m-%d').date()
        except Exception: return d
    return d.strftime('%a %-m/%-d')

def _estimate_days_out(text):
    if 'tommy john' in text:                   return 365
    if 'season-ending' in text:                return 180
    if '60-day' in text:                       return 60
    if '6-8 weeks' in text:                    return 49
    if '4-6 weeks' in text:                    return 35
    if '2-4 weeks' in text:                    return 21
    if '15-day' in text:                       return 15
    if '1-2 weeks' in text:                    return 10
    if '10-day' in text:                       return 10
    if 'week to week' in text:                 return 14
    if 'day-to-day' in text or 'dtd' in text:  return 3
    return None

def _estimate_reaction_window(transactions):
    """
    Estimate minutes before fastest leaguemate acts on breaking news.
    Derived from actual transaction history: find rapid-succession adds
    (multiple teams adding within 6 hours = likely same news event).
    Returns 25th percentile reaction gap — how fast the fastest ~25% move.
    Floor: 15 min. Ceiling: 120 min.
    """
    if not transactions:
        return 90
    sorted_adds = sorted(
        [t for t in transactions if 'add' in t.get('type', '')],
        key=lambda x: x.get('timestamp', 0)
    )
    if len(sorted_adds) < 3:
        return 90
    reaction_gaps = []
    for i in range(1, len(sorted_adds)):
        gap_min = (sorted_adds[i]['timestamp'] - sorted_adds[i-1]['timestamp']) / 60
        if 0 < gap_min < 360:
            reaction_gaps.append(gap_min)
    if not reaction_gaps:
        return 90
    reaction_gaps.sort()
    p25_idx = max(0, int(len(reaction_gaps) * 0.25) - 1)
    return max(15, min(120, round(reaction_gaps[p25_idx])))

# ============================================================
# PUSHOVER
# ============================================================
def send_pushover(title, message, priority=0):
    if not PUSHOVER_TOKEN or not PUSHOVER_USER:
        print(f"  [PUSHOVER SKIPPED] {title}: {message[:80]}")
        return
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={"token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
                  "title": title[:100], "message": message[:1024],
                  "priority": priority, "sound": "siren"},
            timeout=10
        )
        print(f"  Alert sent ({r.status_code}): {title}")
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

def save_seen_alerts(seen):      _save_json(SEEN_ALERTS_FILE, seen)
def is_alert_seen(key, seen):    return key in seen
def mark_alert_seen(key, seen):  seen[key] = datetime.now(timezone.utc).timestamp()

def load_sleep_queue():
    data   = _load_json(SLEEP_QUEUE_FILE, [])
    cutoff = datetime.now(timezone.utc).timestamp() - (12 * 3600)
    return [x for x in data if x.get('ts', 0) > cutoff]

def save_sleep_queue(q):         _save_json(SLEEP_QUEUE_FILE, q)

def load_sitting_alerts():
    data = _load_json(SITTING_ALERTS_FILE, {})
    if data.get('date') != date.today().isoformat(): return {}
    return data.get('alerted', {})

def save_sitting_alerts(a):
    _save_json(SITTING_ALERTS_FILE, {'date': date.today().isoformat(), 'alerted': a})

def load_morning_probables():
    data = _load_json(PROBABLES_FILE, {})
    if data.get('date') != date.today().isoformat(): return {}
    return data.get('probables', {})

def save_morning_probables(p):
    _save_json(PROBABLES_FILE, {'date': date.today().isoformat(), 'probables': p})

def load_scratch_alerted():
    data = _load_json(SCRATCH_ALERTED_FILE, {})
    if data.get('date') != date.today().isoformat(): return {}
    return data.get('alerted', {})

def save_scratch_alerted(a):
    _save_json(SCRATCH_ALERTED_FILE, {'date': date.today().isoformat(), 'alerted': a})

def load_transactions():
    data   = _load_json(TRANSACTIONS_FILE, [])
    cutoff = datetime.now(timezone.utc).timestamp() - (90 * 86400)
    return [t for t in data if t.get('timestamp', 0) > cutoff]

def save_transactions(t):        _save_json(TRANSACTIONS_FILE, t)
def load_leaguemate_profiles():  return _load_json(LEAGUEMATE_FILE, {})
def save_leaguemate_profiles(p): _save_json(LEAGUEMATE_FILE, p)
def load_trade_history():        return _load_json(TRADE_HISTORY_FILE, [])
def save_trade_history(h):       _save_json(TRADE_HISTORY_FILE, h)

def load_pos_eligibility_alerts():
    data = _load_json(POS_ELIGIBILITY_FILE, {})
    if data.get('week') != get_current_week(): return {}
    return data.get('alerted', {})

def save_pos_eligibility_alerts(a):
    _save_json(POS_ELIGIBILITY_FILE, {'week': get_current_week(), 'alerted': a})

# ============================================================
# YAHOO API
# ============================================================
def get_yahoo_query():
    from yfpy.query import YahooFantasySportsQuery
    return YahooFantasySportsQuery(
        league_id=LEAGUE_ID, game_code="mlb",
        yahoo_consumer_key=YAHOO_CLIENT_ID,
        yahoo_consumer_secret=YAHOO_CLIENT_SECRET,
        env_file_location=Path("."), env_var_fallback=True,
        save_token_data_to_env_file=True
    )

def get_all_rosters():
    try:
        query       = get_yahoo_query()
        today       = date.today()
        taken       = set()
        my_roster   = []
        all_rosters = {}
        for team_id in range(1, 13):
            try:
                roster = query.get_team_roster_player_info_by_date(team_id, today)
                if not roster: continue
                team_players = []
                for player in roster:
                    try: name = player.name.full
                    except Exception:
                        try: name = str(player.name)
                        except Exception: name = None
                    if not name: continue
                    norm = normalize_name(name)
                    taken.add(norm)
                    pdata = {
                        'name': name, 'name_normalized': norm,
                        'position': str(getattr(player, 'primary_position', '') or ''),
                        'pct_owned': float(getattr(player.percent_owned, 'value', 0) or 0) if hasattr(player, 'percent_owned') else 0.0,
                        'is_undroppable': int(getattr(player, 'is_undroppable', 0) or 0),
                        'status': str(getattr(player, 'status', '') or ''),
                        'injury_note': str(getattr(player, 'injury_note', '') or ''),
                        'selected_position': (player.selected_position.position if hasattr(player, 'selected_position') else ''),
                        'team_abbr': str(getattr(player, 'editorial_team_abbr', '') or ''),
                        'player_id': str(getattr(player, 'player_id', '') or ''),
                        'eligible_positions': [],
                    }
                    try:
                        ep = player.eligible_positions
                        if ep:
                            pdata['eligible_positions'] = [str(getattr(p, 'position', p)) for p in (ep if isinstance(ep, list) else [ep])]
                    except Exception: pass
                    team_players.append(pdata)
                    if team_id == MY_TEAM_ID: my_roster.append(pdata)
                all_rosters[team_id] = team_players
            except Exception as e:
                print(f"  Team {team_id} error: {e}")
        if len(taken) < MIN_EXPECTED_ROSTERED:
            print(f"  ⚠️ Only {len(taken)} players")
            send_pushover("⚠️ SYSTEM WARNING", f"Yahoo returned only {len(taken)} players.", priority=0)
            return None, None, None
        print(f"  {len(taken)} rostered, {len(my_roster)} on my team")
        return taken, my_roster, all_rosters
    except Exception as e:
        print(f"  Yahoo error: {e}")
        send_pushover("⚠️ SYSTEM WARNING", f"Yahoo failed: {str(e)[:200]}", priority=0)
        return None, None, None

def validate_player_in_yahoo(player_name, taken=None):
    norm = normalize_name(player_name)
    if taken and norm in taken: return player_name, False
    try:
        url    = f"https://statsapi.mlb.com/api/v1/people/search?names={requests.utils.quote(player_name)}&sportId=1"
        data   = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        if not people: return None, False
        canonical = people[0].get('fullName', player_name)
        is_avail  = taken is None or normalize_name(canonical) not in taken
        return canonical, is_avail
    except Exception:
        return player_name, (taken is None or norm not in taken)

def get_league_free_agents(position=None, count=25):
    try:
        query   = get_yahoo_query()
        players = query.get_league_players(player_count=count, position_filter=position, status_filter='FA')
        result  = []
        for p in (players or []):
            try:
                result.append({'name': p.name.full,
                               'pct_owned': float(getattr(p.percent_owned, 'value', 0) or 0),
                               'position': str(getattr(p, 'primary_position', '') or ''),
                               'player_id': str(getattr(p, 'player_id', '') or '')})
            except Exception: continue
        return result
    except Exception as e:
        print(f"  FA fetch error: {e}")
        return []

def count_my_il_slots_used(my_roster):
    return sum(1 for p in my_roster if p.get('selected_position') == 'IL')

def get_worst_il_stash(my_roster):
    il_players = [p for p in my_roster if p.get('selected_position') == 'IL']
    return min(il_players, key=lambda p: p['pct_owned']) if il_players else None

# ============================================================
# MLB STATS API
# ============================================================
def get_team_batting_stats():
    try:
        url  = f"https://statsapi.mlb.com/api/v1/teams/stats?season={date.today().year}&group=hitting&stats=season&sportId=1"
        data = requests.get(url, timeout=10).json()
        result = {}
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                name = split.get('team', {}).get('name', '')
                try: result[name] = float(split.get('stat', {}).get('ops', '') or '')
                except (ValueError, TypeError): pass
        print(f"  Team batting stats: {len(result)} teams")
        return result
    except Exception as e:
        print(f"  Team stats error: {e}")
        return {}

def get_schedule(start_date, end_date, hydrate='probablePitcher'):
    try:
        url  = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}&gameType=R&hydrate={hydrate}"
        return requests.get(url, timeout=15).json()
    except Exception as e:
        print(f"  Schedule error: {e}")
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
        avg_ops  = get_opp_ops_tiers()['average']
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
                        opp_ops = team_ops.get(opp_team, avg_ops)
                        if n not in pitchers:
                            pitchers[n] = {'count': 0, 'id': pid, 'dates': [],
                                           'opponents': [], 'opp_ops': [], 'team': my_team}
                        pitchers[n]['count'] += 1
                        pitchers[n]['dates'].append(game_date)
                        pitchers[n]['opponents'].append(opp_team)
                        pitchers[n]['opp_ops'].append(opp_ops)
        return pitchers
    except Exception as e:
        print(f"  Probable pitchers error: {e}")
        return {}

def get_pitcher_stats(player_id, season=None):
    if season is None: season = date.today().year
    try:
        url  = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&group=pitching&season={season}"
        data = requests.get(url, timeout=5).json()
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                s  = split.get('stat', {})
                ip = float(s.get('inningsPitched', '0') or '0')
                gs = int(s.get('gamesStarted', 0) or 0)
                try:
                    return {
                        'era':          float(s.get('era',  '99.99') or '99.99'),
                        'whip':         float(s.get('whip', '9.99')  or '9.99'),
                        'k':            int(s.get('strikeOuts', 0)    or 0),
                        'ip':           ip,
                        'gs':           gs,
                        'ip_per_start': round(ip / gs, 1) if gs > 0 else 0,
                        'kbb':          float(s.get('strikeoutWalkRatio', '0') or '0'),
                        'wins':         int(s.get('wins', 0) or 0),
                        'g':            int(s.get('gamesPlayed', gs) or gs),
                    }
                except Exception: pass
    except Exception: pass
    return None

def get_pitcher_stats_blended(player_id):
    """
    Blend prior/current season stats.
    Prior weight scaled by BOTH season progress AND prior IP sample.
    This prevents penalizing injury-shortened prior seasons (e.g. Ray 2025).
    """
    w_prior_base, w_curr_base = get_season_blend()
    curr  = get_pitcher_stats(player_id, date.today().year)
    prior = get_pitcher_stats(player_id, date.today().year - 1)
    empty = {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0,
             'gs': 0, 'ip_per_start': 0, 'kbb': 0.0, 'wins': 0, 'g': 0}
    if prior is None and curr is not None:
        curr['blend_note'] = 'no prior — current only'; return curr
    if curr is None and prior is not None:
        prior['blend_note'] = 'no current stats yet';   return prior
    if curr is None and prior is None:
        return {**empty, 'blend_note': 'no stats'}
    # Scale prior weight by prior IP sample size
    prior_ip_factor = prior_season_ip_weight(prior.get('ip', 0))
    w_prior = w_prior_base * prior_ip_factor
    w_curr  = 1.0 - w_prior
    return {
        'era':          round(prior['era']  * w_prior + curr['era']  * w_curr, 2),
        'whip':         round(prior['whip'] * w_prior + curr['whip'] * w_curr, 2),
        'kbb':          round(prior['kbb']  * w_prior + curr['kbb']  * w_curr, 2),
        'k':            curr['k'],
        'ip':           curr['ip'],
        'gs':           curr.get('gs', 0),
        'ip_per_start': prior.get('ip_per_start', 0) or curr.get('ip_per_start', 0),
        'wins':         curr.get('wins', 0),
        'g':            curr.get('g', 0),
        'blend_note':   f"prior {int(w_prior*100)}% [{prior.get('ip',0):.0f}IP] / curr {int(w_curr*100)}%"
    }

def get_player_id_from_name(name):
    try:
        url    = f"https://statsapi.mlb.com/api/v1/people/search?names={requests.utils.quote(name)}&sportId=1"
        data   = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        return people[0].get('id') if people else None
    except Exception: return None

def get_hitter_stats(player_id, season=None):
    if season is None: season = date.today().year
    try:
        url  = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=season&group=hitting&season={season}"
        data = requests.get(url, timeout=5).json()
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                s = split.get('stat', {})
                try:
                    return {
                        'avg': float(s.get('avg', '.000') or '.000'),
                        'ops': float(s.get('ops', '.000') or '.000'),
                        'hr':  int(s.get('homeRuns', 0)   or 0),
                        'rbi': int(s.get('rbi', 0)        or 0),
                        'sb':  int(s.get('stolenBases', 0) or 0),
                        'pa':  int(s.get('plateAppearances', 0) or 0),
                    }
                except Exception: pass
    except Exception: pass
    return None

def is_opener(stats):
    if not stats: return False
    gs = stats.get('gs', 0)
    ip = stats.get('ip', 0)
    if gs < 2: return (ip / gs) < 3.0 if gs > 0 and ip > 0 else False
    return stats.get('ip_per_start', 0) < 3.0

def is_high_quality_sp(stats, opp_ops):
    """
    OPS-tiered HQ start thresholds from 2026 MLB SP population and team OPS distribution.
    Easier matchups = relaxed requirements; tougher offenses = tighter requirements.
    """
    if not stats or is_opener(stats): return False
    if stats.get('ip', 0) < get_min_ip_for_significance(): return False
    era  = stats.get('era',  99)
    whip = stats.get('whip', 9.99)
    kbb  = stats.get('kbb',  0)
    t    = get_opp_ops_tiers()
    if opp_ops   <= t['very_weak']: return era < 4.25 and whip < 1.32 and kbb > 1.9
    elif opp_ops <= t['weak']:      return era < 3.85 and whip < 1.22 and kbb > 2.4
    elif opp_ops <= t['average']:   return era < 3.60 and whip < 1.18 and kbb > 2.6
    elif opp_ops <= t['strong']:    return era < 3.20 and whip < 1.10 and kbb > 3.0
    else:                           return era < 2.90 and whip < 1.05 and kbb > 3.3

def sp_long_term_value(p, stats):
    """
    Majority-owned (7/12 teams = 58%) OR above_avg tier stats with sufficient sample.
    """
    own_t = get_ownership_thresholds()
    if p['is_undroppable'] or p['name_normalized'] in MY_UNDROPPABLE: return True
    if p['pct_owned'] >= own_t['majority']: return True
    sp_t = get_sp_quality_thresholds()
    if (stats and stats.get('ip', 0) >= get_min_ip_for_significance()
            and stats.get('era', 99) <= sp_t['above_avg']['era']
            and stats.get('whip', 9) <= sp_t['above_avg']['whip']):
        return True
    return False

def score_sp(stats, opp_ops=None):
    """
    Numerical SP score normalized to 2026 league averages (ERA 4.20, WHIP 1.27, K/BB 2.2).
    Each coefficient derived: ERA 5pts/unit, WHIP 12pts/unit, K/BB 4pts/unit.
    """
    if not stats or stats.get('ip', 0) < get_min_ip_for_significance() or is_opener(stats):
        return -999
    s = ((4.20 - stats.get('era', 4.20))  * 5
       + (1.27 - stats.get('whip', 1.27)) * 12
       + (stats.get('kbb', 0) - 2.20)     * 4
       +  stats.get('k', 0) * 0.3)
    if opp_ops is not None:
        s += (get_opp_ops_tiers()['average'] - opp_ops) * 40
    return s

def _is_reliever_worth_rostering(stats, my_roster):
    """
    Non-closer RP: must project enough weekly contribution to impact H2H categories.
    Thresholds derived from 2026 RP population and weekly category variance.
    Returns (bool, stat_str).
    """
    if not stats: return False, ''
    rp_t     = get_relief_pitcher_thresholds()
    era      = stats.get('era',  99)
    whip     = stats.get('whip', 9.99)
    kbb      = stats.get('kbb',  0)
    ip       = stats.get('ip',   0)
    min_ip_rp = max(5, int(get_min_ip_for_significance() * 0.4))
    if ip < min_ip_rp or era > rp_t['min_era'] or whip > rp_t['min_whip'] or kbb < rp_t['min_kbb']:
        return False, ''
    days_elapsed     = max(1, get_days_into_season())
    apps             = stats.get('g', max(1, int(ip / rp_t['ip_per_app'])))
    projected_weekly = (apps / days_elapsed) * 7 * rp_t['ip_per_app']
    if projected_weekly < rp_t['min_weekly_ip']:
        return False, ''
    k9 = (stats.get('k', 0) / ip * 9) if ip > 0 else 0
    if k9 < rp_t['min_k9']:
        return False, ''
    # Compare vs my roster average ERA
    my_eras = []
    for p in my_roster:
        if p['position'] in ['SP', 'RP', 'P'] and 'IL' not in (p['status'] or ''):
            pid = get_player_id_from_name(p['name'])
            s   = get_pitcher_stats_blended(pid) if pid else None
            if s and s.get('ip', 0) >= get_min_ip_for_significance():
                my_eras.append(s['era'])
    if my_eras:
        my_avg_era  = sum(my_eras) / len(my_eras)
        era_margin  = get_h2h_margin_thresholds()['era_whip_margin']
        if era >= my_avg_era - era_margin:
            return False, ''
    stat_str = f"ERA {era:.2f} | WHIP {whip:.2f} | K/9 {k9:.1f} | ~{projected_weekly:.1f} IP/wk"
    return True, stat_str

# ============================================================
# CLOSER DATA
# ============================================================
def fetch_closer_data():
    """
    Primary: ESPN closer org chart. Fallback: FantasyPros.
    Last resort: stale cache. Never hardcodes player names.
    """
    try:
        cached = _load_json(CLOSER_CACHE_FILE, {})
        age    = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
        if age < 14400 and len(cached.get('data', {}).get('closer_lookup', {})) >= 15:
            return cached['data']
    except Exception: pass

    data = _try_espn_closers()
    if not data or len(data.get('closer_lookup', {})) < 15:
        print("  ESPN closer parse thin — trying FantasyPros")
        data = _try_fantasypros_closers()

    if data and len(data.get('closer_lookup', {})) >= 10:
        _save_json(CLOSER_CACHE_FILE, {'ts': datetime.now(timezone.utc).timestamp(), 'data': data})
        print(f"  Closer data: {len(data.get('closer_lookup', {}))} closers loaded")
        return data

    stale = _load_json(CLOSER_CACHE_FILE, {}).get('data', {})
    if stale: print(f"  Using stale closer cache ({len(stale.get('closer_lookup', {}))} closers)")
    return stale

ESPN_TEAM_MAP = {
    'ARIZONA DIAMONDBACKS': 'Arizona Diamondbacks', 'ATLANTA BRAVES': 'Atlanta Braves',
    'BALTIMORE ORIOLES': 'Baltimore Orioles',       'BOSTON RED SOX': 'Boston Red Sox',
    'CHICAGO CUBS': 'Chicago Cubs',                 'CHICAGO WHITE SOX': 'Chicago White Sox',
    'CINCINNATI REDS': 'Cincinnati Reds',           'CLEVELAND GUARDIANS': 'Cleveland Guardians',
    'COLORADO ROCKIES': 'Colorado Rockies',         'DETROIT TIGERS': 'Detroit Tigers',
    'HOUSTON ASTROS': 'Houston Astros',             'KANSAS CITY ROYALS': 'Kansas City Royals',
    'LOS ANGELES ANGELS': 'Los Angeles Angels',     'LOS ANGELES DODGERS': 'Los Angeles Dodgers',
    'MIAMI MARLINS': 'Miami Marlins',               'MILWAUKEE BREWERS': 'Milwaukee Brewers',
    'MINNESOTA TWINS': 'Minnesota Twins',           'NEW YORK METS': 'New York Mets',
    'NEW YORK YANKEES': 'New York Yankees',         'THE ATHLETICS': 'Athletics',
    'OAKLAND ATHLETICS': 'Athletics',               'PHILADELPHIA PHILLIES': 'Philadelphia Phillies',
    'PITTSBURGH PIRATES': 'Pittsburgh Pirates',     'SAN DIEGO PADRES': 'San Diego Padres',
    'SAN FRANCISCO GIANTS': 'San Francisco Giants', 'SEATTLE MARINERS': 'Seattle Mariners',
    'ST. LOUIS CARDINALS': 'St. Louis Cardinals',   'TAMPA BAY RAYS': 'Tampa Bay Rays',
    'TEXAS RANGERS': 'Texas Rangers',               'TORONTO BLUE JAYS': 'Toronto Blue Jays',
    'WASHINGTON NATIONALS': 'Washington Nationals',
}

def _try_espn_closers():
    try:
        r     = requests.get('https://www.espn.com/fantasy/baseball/flb/story?page=REcloserorgchart',
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        clean = re.sub(r'\s+', ' ', html.unescape(re.sub('<[^<]+?>', ' ', r.text)))
        pattern = '|'.join(re.escape(k) for k in ESPN_TEAM_MAP.keys())
        blocks  = re.split(f'\\b({pattern})\\b', clean)
        closer_re = re.compile(r'Closer(?:-by-committee)?:\s*([A-Z][a-z]+(?:\s+[A-Z][a-z\']+){1,3})')
        setup_re  = re.compile(r'(?:Primary setup|Secondary setup|Sleeper):\s*([A-Z][a-z]+(?:\s+[A-Z][a-z\']+){1,3})')
        depth_charts  = {}
        closer_lookup = {}
        i = 0
        while i < len(blocks) - 1:
            raw = blocks[i].strip()
            if raw in ESPN_TEAM_MAP:
                team_name = ESPN_TEAM_MAP[raw]
                section   = blocks[i+1] if i+1 < len(blocks) else ''
                pitchers  = []
                for m in closer_re.finditer(section):
                    n = normalize_name(m.group(1).strip())
                    if n and n not in pitchers: pitchers.append(n)
                for m in setup_re.finditer(section):
                    n = normalize_name(m.group(1).strip())
                    if n and n not in pitchers: pitchers.append(n)
                if pitchers:
                    depth_charts[team_name]    = pitchers
                    closer_lookup[pitchers[0]] = team_name
                i += 2
            else: i += 1
        return {'depth_charts': depth_charts, 'closer_lookup': closer_lookup}
    except Exception as e:
        print(f"  ESPN closer error: {e}")
        return {}

def _try_fantasypros_closers():
    try:
        r     = requests.get('https://www.fantasypros.com/mlb/closer-depth-charts.php',
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        clean = re.sub(r'\s+', ' ', html.unescape(re.sub('<[^<]+?>', ' ', r.text)))
        team_re   = re.compile(r'(Arizona Diamondbacks|Atlanta Braves|Baltimore Orioles|Boston Red Sox|Chicago Cubs|Chicago White Sox|Cincinnati Reds|Cleveland Guardians|Colorado Rockies|Detroit Tigers|Houston Astros|Kansas City Royals|Los Angeles Angels|Los Angeles Dodgers|Miami Marlins|Milwaukee Brewers|Minnesota Twins|New York Mets|New York Yankees|Athletics|Philadelphia Phillies|Pittsburgh Pirates|San Diego Padres|San Francisco Giants|Seattle Mariners|St\. Louis Cardinals|Tampa Bay Rays|Texas Rangers|Toronto Blue Jays|Washington Nationals)')
        closer_re = re.compile(r'Current Closer[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z\']+){1,3})')
        sections  = team_re.split(clean)
        depth_charts  = {}
        closer_lookup = {}
        i = 0
        while i < len(sections) - 1:
            if team_re.match(sections[i].strip()):
                team_name = sections[i].strip()
                section   = sections[i+1] if i+1 < len(sections) else ''
                pitchers  = []
                for m in closer_re.finditer(section):
                    n = normalize_name(m.group(1).strip())
                    if n and n not in pitchers: pitchers.append(n)
                if pitchers:
                    depth_charts[team_name]    = pitchers
                    closer_lookup[pitchers[0]] = team_name
                i += 2
            else: i += 1
        return {'depth_charts': depth_charts, 'closer_lookup': closer_lookup}
    except Exception as e:
        print(f"  FantasyPros closer error: {e}")
        return {}

def get_all_closers():        return set(fetch_closer_data().get('closer_lookup', {}).keys())
def get_closer_team(norm):    return fetch_closer_data().get('closer_lookup', {}).get(norm)

def get_closer_candidates(team_name, taken, limit=3):
    """
    Available closer candidates with fantasy relevance gate.
    Ownership >= 16.7% (2/12 teams) OR top prospect OR strong prior stats.
    """
    chart  = fetch_closer_data().get('depth_charts', {}).get(team_name, [])
    own_t  = get_ownership_thresholds()
    rp_t   = get_relief_pitcher_thresholds()
    fa     = get_league_free_agents(position='RP', count=30)
    fa_map = {normalize_name(p['name']): p for p in fa}
    result = []
    for norm in chart[1:limit+3]:
        if norm in taken: continue
        fa_p = fa_map.get(norm)
        if not fa_p:      continue
        pct  = fa_p['pct_owned']
        if pct >= own_t['meaningful'] or norm in TOP_PROSPECTS:
            result.append(norm.title())
        else:
            pid = get_player_id_from_name(norm.title())
            if pid:
                s = get_pitcher_stats_blended(pid)
                if s and s.get('ip', 0) >= get_min_ip_for_significance() * 0.4 and s.get('era', 99) < rp_t['min_era']:
                    result.append(norm.title())
        if len(result) >= limit: break
    return result

# ============================================================
# NAME EXTRACTION
# ============================================================
def looks_like_player_name(text):
    if not text: return False
    words = text.strip().split()
    if not (2 <= len(words) <= 4): return False
    for word in words:
        if word.lower() in {'jr.', 'sr.', 'ii', 'iii', 'iv'}: continue
        if not word[0].isupper(): return False
    non_name = {
        'mlb', 'nfl', 'nba', 'nhl', 'espn', 'the', 'for', 'and', 'or', 'power',
        'rankings', 'trade', 'deadline', 'spring', 'training', 'opening', 'day',
        'world', 'series', 'all-star', 'free', 'agency', 'report', 'update',
        'breaking', 'fantasy', 'baseball', 'weekly', 'daily', 'morning', 'sources',
        'video', 'watch', 'review', 'week', 'angels', 'orioles', 'yankees', 'rays',
        'red', 'sox', 'blue', 'jays', 'white', 'guardians', 'tigers', 'royals',
        'twins', 'astros', 'athletics', 'mariners', 'rangers', 'braves', 'marlins',
        'mets', 'phillies', 'nationals', 'cubs', 'reds', 'brewers', 'pirates',
        'cardinals', 'diamondbacks', 'rockies', 'dodgers', 'padres', 'giants',
        'rotowire', 'cbssports', 'rotoballer', 'pitcherlist',
    }
    return not any(w.lower() in non_name for w in words)

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
    candidates = re.findall(r'\b([A-Z][a-z\']+(?:\s+[A-Z][a-z\']+){1,3})\b', full_text)
    for c in candidates:
        if not looks_like_player_name(c): continue
        words = c.lower().split()
        if any(w in INVALID_NAME_WORDS for w in words): continue
        if any(w in ACTION_VERBS for w in words):       continue
        if c.lower() in MINOR_LEAGUE_TEAMS:              continue
        return c
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
    except Exception: pass
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
                        opp_team_id = tid; break
                if opp_team_id: break
        except Exception as e:
            print(f"  Matchup lookup error: {e}")

        def parse_stats(raw):
            result = {}
            try:
                team_stats = getattr(raw, 'team_stats', None) or raw
                stats      = getattr(team_stats, 'stats', None)
                if stats is None: return result
                stat_list = getattr(stats, 'stat', None) or stats
                if not isinstance(stat_list, list): stat_list = [stat_list]
                id_map = {'60': 'R', '7': 'H', '12': 'HR', '13': 'RBI', '16': 'SB',
                          '3': 'AVG', '55': 'OPS', '28': 'W', '32': 'SV', '27': 'K',
                          '26': 'ERA', '29': 'WHIP', '72': 'KBB'}
                for s in stat_list:
                    sid = str(getattr(s, 'stat_id', '') or '')
                    val = getattr(s, 'value', None)
                    if val is not None and sid in id_map:
                        try: result[id_map[sid]] = float(val)
                        except (ValueError, TypeError): pass
            except Exception as e:
                print(f"  Stats parse error: {e}")
            return result

        my_stats  = {}
        opp_stats = {}
        try:
            my_stats = parse_stats(query.get_team_stats_by_week(MY_TEAM_ID, week))
        except Exception as e:
            print(f"  My stats error: {e}")
        if opp_team_id:
            try:
                opp_stats = parse_stats(query.get_team_stats_by_week(opp_team_id, week))
            except Exception as e:
                print(f"  Opp stats error: {e}")

        data = {'my_stats': my_stats, 'opp_stats': opp_stats, 'opp_team_id': opp_team_id, 'week': week}
        _save_json(MATCHUP_CACHE_FILE, {'ts': datetime.now(timezone.utc).timestamp(), 'data': data})
        print(f"  Matchup: {len(my_stats)} my cats, {len(opp_stats)} opp cats")
        return data
    except Exception as e:
        print(f"  Matchup error: {e}")
        return None

# ============================================================
# TRANSACTION TRACKING
# ============================================================
def sync_league_transactions():
    try:
        query        = get_yahoo_query()
        transactions = load_transactions()
        existing_ids = {t.get('id') for t in transactions}
        league_trans = query.get_league_transactions()
        if not league_trans: return
        now_ts    = datetime.now(timezone.utc).timestamp()
        new_count = 0
        for trans in (league_trans if isinstance(league_trans, list) else [league_trans]):
            try:
                trans_id   = str(getattr(trans, 'transaction_id', '') or '')
                trans_type = str(getattr(trans, 'type', '') or '').lower()
                timestamp  = float(getattr(trans, 'timestamp', now_ts) or now_ts)
                if trans_id in existing_ids: continue
                players     = getattr(trans, 'players', None) or {}
                player_info = []
                try:
                    player_list = players if isinstance(players, list) else getattr(players, 'player', []) or []
                    if not isinstance(player_list, list): player_list = [player_list]
                    for pl in player_list:
                        pname = ''
                        try: pname = pl.name.full
                        except Exception: pass
                        tdata     = getattr(pl, 'transaction_data', None)
                        dest_team = str(getattr(tdata, 'destination_team_key', '') or '')
                        src_team  = str(getattr(tdata, 'source_team_key', '') or '')
                        ptype     = str(getattr(tdata, 'type', '') or '').lower()
                        pid       = str(getattr(pl, 'player_id', '') or '')
                        pos = ''
                        try: pos = str(pl.primary_position or '')
                        except Exception: pass
                        if not pos:
                            try: pos = str(pl.eligible_positions.position or '')
                            except Exception: pass
                        if not pos and pid:
                            try:
                                d   = requests.get(f"https://statsapi.mlb.com/api/v1/people/{pid}?hydrate=currentTeam", timeout=3).json()
                                pos = d.get('people', [{}])[0].get('primaryPosition', {}).get('abbreviation', '')
                            except Exception: pass
                        player_info.append({'name': pname, 'player_id': pid, 'position': pos,
                                            'type': ptype, 'dest_team': dest_team, 'src_team': src_team})
                except Exception: pass
                transactions.append({'id': trans_id, 'type': trans_type, 'timestamp': timestamp,
                                     'logged_at': now_ts, 'players': player_info})
                new_count += 1
            except Exception: continue
        if new_count > 0:
            save_transactions(transactions)
            print(f"  Logged {new_count} new transactions ({len(transactions)} total)")
        _build_leaguemate_profiles(transactions)
    except Exception as e:
        print(f"  Transaction sync error: {e}")

def _build_leaguemate_profiles(transactions):
    profiles = {}
    for t in transactions:
        if 'add' not in t.get('type', ''): continue
        for p in t.get('players', []):
            if p.get('type') != 'add': continue
            team_key = p.get('dest_team', '')
            if not team_key: continue
            if team_key not in profiles:
                profiles[team_key] = {'adds': [], 'total_adds': 0, 'adds_last_30d': 0}
            profiles[team_key]['adds'].append({'player': p['name'], 'position': p['position'],
                                               'timestamp': t['timestamp']})
    cutoff = datetime.now(timezone.utc).timestamp() - 30 * 86400
    for team_key, profile in profiles.items():
        adds = profile['adds']
        profile['total_adds']    = len(adds)
        profile['adds_last_30d'] = sum(1 for a in adds if a['timestamp'] > cutoff)
    save_leaguemate_profiles(profiles)
    print(f"  Built profiles for {len(profiles)} teams")

def get_waiver_drops_to_review(taken, my_roster):
    transactions = load_transactions()
    cutoff       = datetime.now(timezone.utc).timestamp() - 86400
    recent_drops = []
    my_norms     = {normalize_name(r['name']) for r in my_roster}
    for t in transactions:
        if t.get('timestamp', 0) < cutoff: continue
        if 'drop' not in t.get('type', ''): continue
        for p in t.get('players', []):
            if p.get('type') not in ('drop', 'release'): continue
            name = p.get('name', '')
            if not name or normalize_name(name) in my_norms: continue
            recent_drops.append({'name': name, 'position': p.get('position', ''),
                                  'player_id': p.get('player_id', ''), 'timestamp': t['timestamp'],
                                  'notes': t.get('type', '')})
    return recent_drops

# ============================================================
# DROP CANDIDATE LOGIC
# ============================================================
def get_weak_positions(my_roster):
    own_t  = get_ownership_thresholds()
    strong = {'SS', '1B', 'OF'}
    weak   = []
    by_pos = {}
    for p in my_roster:
        pos = p['position']
        if pos not in by_pos: by_pos[pos] = []
        by_pos[pos].append(p)
    for pos, players in by_pos.items():
        if pos in strong or pos in ['BN', 'Util', 'IL']: continue
        if any('IL' in (p['status'] or '') or p['pct_owned'] < own_t['significant'] for p in players):
            if pos not in weak: weak.append(pos)
    return weak

def find_best_drop(my_roster, team_ops, protect_closer=True, prefer_position=None):
    own_t       = get_ownership_thresholds()
    today       = datetime.now(ET_TZ).date()
    week_starts = get_probable_pitchers(today, sunday_of_week(today), team_ops)
    rp_players  = [p for p in my_roster if p['position'] == 'RP']
    only_closer = len(rp_players) == 1
    candidates  = []
    for p in my_roster:
        if p['is_undroppable'] or p['name_normalized'] in MY_UNDROPPABLE: continue
        if 'IL' in (p['status'] or ''): continue
        if protect_closer and only_closer and p['position'] == 'RP': continue
        if p['pct_owned'] >= own_t['majority']: continue
        score = 0
        if p['position'] in ['SP', 'RP', 'P']:
            pid       = get_player_id_from_name(p['name'])
            stats     = get_pitcher_stats_blended(pid) if pid else None
            has_start = normalize_name(p['name']) in {normalize_name(k) for k in week_starts}
            if sp_long_term_value(p, stats):
                if not has_start: score += 30
                else:
                    remaining_ops = [0.730]
                    for k in week_starts:
                        if normalize_name(k) == normalize_name(p['name']):
                            remaining_ops = week_starts[k].get('opp_ops', [0.730])
                    t_ops = get_opp_ops_tiers()
                    if max(remaining_ops) > t_ops['strong'] and p['pct_owned'] < own_t['significant']:
                        score += 20
                    else: continue
            else:
                score += 50
                if has_start:
                    remaining_ops = [0.730]
                    for k in week_starts:
                        if normalize_name(k) == normalize_name(p['name']):
                            remaining_ops = week_starts[k].get('opp_ops', [0.730])
                    if max(remaining_ops) <= get_opp_ops_tiers()['weak']:
                        score -= 25
        else:
            if p['pct_owned'] < own_t['meaningful']:  score += 40
            elif p['pct_owned'] < own_t['significant']: score += 20
            else: continue
        if prefer_position and p['position'] == prefer_position: score += 15
        score -= p['pct_owned'] * 0.3
        candidates.append((score, p))
    if not candidates: return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def _get_compatible_positions(pos):
    return {'C': ['C','Util','BN'], '1B': ['1B','Util','BN'], '2B': ['2B','Util','BN'],
            '3B': ['3B','Util','BN'], 'SS': ['SS','Util','BN'], 'OF': ['OF','Util','BN'],
            'SP': ['SP','P','BN'],   'RP': ['RP','P','BN']}.get(pos, ['Util','BN'])

# ============================================================
# RSS FETCHING
# ============================================================
def fetch_feed(source, lookback_minutes=20):
    try:
        feed   = feedparser.parse(source["url"], request_headers={"User-Agent": "Mozilla/5.0 fantasy-monitor/1.0"})
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        items  = []
        for entry in feed.entries:
            try:
                pub = (datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                       if hasattr(entry, 'published_parsed') and entry.published_parsed
                       else datetime.now(timezone.utc))
                if pub < cutoff: continue
                title   = clean_text(entry.get('title', ''))
                summary = clean_text(entry.get('summary', entry.get('description', title)))
                summary = summary[:400] + '...' if len(summary) > 400 else summary
                items.append({'source': source["name"], 'type': source["type"],
                              'title': title, 'summary': summary, 'published': pub})
            except Exception: continue
        if items: print(f"  {source['name']}: {len(items)} items")
        return items
    except Exception as e:
        print(f"  {source['name']} error: {e}")
        return []

def get_all_news(lookback_minutes=20):
    items = []
    print("Checking Tier 1 sources...")
    for s in TIER1_SOURCES: items.extend(fetch_feed(s, lookback_minutes))
    m = datetime.now(timezone.utc).minute
    if m < 16 or 30 <= m < 46:
        print("Checking Reddit (Tier 2)...")
        for s in TIER2_SOURCES: items.extend(fetch_feed(s, lookback_minutes))
    else:
        print("Skipping Reddit this run")
    print("Checking Tier 3 (30 MLB team feeds)...")
    t3 = 0
    for s in TIER3_SOURCES:
        new = fetch_feed(s, lookback_minutes)
        items.extend(new); t3 += len(new)
    print(f"  Tier 3 total: {t3} items")
    print(f"Total: {len(items)} raw items")
    return items

# ============================================================
# BREAKING NEWS
# ============================================================
def awake_hours():
    now = datetime.now(ET_TZ)
    return 6 <= now.hour < 22 or (now.hour == 6 and now.minute >= 30)

def _fire_or_queue(title, message, priority, seen, key, sleep_queue, queue_category):
    mark_alert_seen(key, seen)
    if awake_hours(): send_pushover(title, message, priority)
    else: sleep_queue.append({'title': title, 'message': message, 'priority': priority,
                              'category': queue_category, 'ts': datetime.now(timezone.utc).timestamp()})

def _extract_injury_detail(text):
    injuries  = ['hamstring','oblique','elbow','shoulder','knee','wrist','back','thumb',
                 'ankle','hip','quad','calf','groin','forearm','bicep','tricep','finger',
                 'hand','rib','concussion','surgery','fracture','torn']
    timelines = ['10-day','15-day','60-day','1-2 weeks','2-4 weeks','4-6 weeks',
                 '6-8 weeks','season-ending','indefinitely','out for','expected back']
    return f"{next((i for i in injuries if i in text), 'injury')} — {next((t for t in timelines if t in text), 'timeline TBD')}"

def _check_position_relevance(text, my_roster):
    pos_signals = {
        'SP': ['pitcher','starter','right-hander','left-hander','righty','lefty','ace'],
        'RP': ['reliever','closer','bullpen'], 'C': ['catcher'],
        '1B': ['first base','first baseman'], '2B': ['second base','second baseman'],
        '3B': ['third base','third baseman'], 'SS': ['shortstop'],
        'OF': ['outfielder','outfield','center field','left field','right field'],
    }
    weak = get_weak_positions(my_roster)
    for pos, signals in pos_signals.items():
        if any(s in text for s in signals):
            if pos in weak or pos in ['SP', 'RP']: return True
            if pos in MY_CLOGGED_POSITIONS:         return False
            return True
    return True

def _find_relevant_backup(text, taken, my_roster, team_ops):
    """
    Find specific fantasy-relevant backup created by an injury.
    All gates derived from 2026 MLB data and league structure.
    """
    pos_signals = {
        'SP': ['pitcher','starter','right-hander','left-hander','righty','lefty','ace','rotation'],
        'RP': ['reliever','closer','bullpen'], 'C': ['catcher'],
        '1B': ['first base','first baseman'], '2B': ['second base','second baseman'],
        '3B': ['third base','third baseman'], 'SS': ['shortstop'],
        'OF': ['outfielder','outfield','center field','left field','right field'],
    }
    injured_pos = None
    for pos, signals in pos_signals.items():
        if any(s in text for s in signals): injured_pos = pos; break
    if injured_pos in MY_CLOGGED_POSITIONS: return None

    candidates    = get_league_free_agents(position=injured_pos, count=20) if injured_pos else []
    if not candidates: candidates = get_league_free_agents(count=20)
    today         = date.today()
    week_starters = get_probable_pitchers(today, sunday_of_week(today), team_ops) if injured_pos == 'SP' else {}
    own_t = get_ownership_thresholds()
    h_t   = get_hitter_thresholds()
    sp_t  = get_sp_quality_thresholds()
    rp_t  = get_relief_pitcher_thresholds()
    min_ip = get_min_ip_for_significance()

    for player in candidates:
        name      = player['name']
        norm      = normalize_name(name)
        pct_owned = player['pct_owned']
        pos       = player.get('position', injured_pos or '')
        canonical_name, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical_name is None: continue
        reason = ''; stat_str = ''; passes = False

        # 1: Meaningful ownership (2/12 teams)
        if pct_owned >= own_t['meaningful']:
            passes = True; reason = f"Established asset ({pct_owned:.0f}% owned)"

        # 2: Top prospect
        if not passes and norm in TOP_PROSPECTS:
            passes = True; reason = "Top MLB prospect — injury may open path"

        # 3: Platoon → everyday
        if not passes and any(w in text for w in ['everyday','every day','full-time','regular','starting role']):
            pid = get_player_id_from_name(name)
            if pid:
                stats = get_hitter_stats(pid)
                if stats and stats.get('pa', 0) >= h_t['min_pa_signal'] and stats.get('ops', 0) >= h_t['average_ops']:
                    passes = True; reason = "Platoon becoming everyday"
                    stat_str = f"OPS {stats['ops']:.3f} | {stats['pa']} PA"

        # 4: Quality reliever → spot start
        if not passes and pos == 'RP' and injured_pos == 'SP':
            pid = get_player_id_from_name(name)
            if pid:
                stats = get_pitcher_stats_blended(pid)
                if (stats and stats.get('ip', 0) >= min_ip * 0.4
                        and stats.get('era', 99) < rp_t['min_era']
                        and stats.get('whip', 9) < rp_t['min_whip']
                        and stats.get('kbb', 0) > rp_t['min_kbb']):
                    for sp_name, sp_info in week_starters.items():
                        if normalize_name(sp_name) == norm:
                            opp_ops = sp_info['opp_ops'][0] if sp_info['opp_ops'] else 0.730
                            if is_high_quality_matchup(opp_ops):
                                passes = True
                                reason   = f"Quality reliever spot start vs {sp_info['opponents'][0] if sp_info['opponents'] else 'weak offense'}"
                                stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                            break

        # 5: Callup acceleration
        if not passes and any(w in text for w in ['called up','promoted','recalled','expected to join','performing well at aaa']):
            if norm in TOP_PROSPECTS or pct_owned >= own_t['speculative']:
                passes = True; reason = "Callup accelerated by injury"

        # 6: Prior season proven performer
        if not passes:
            pid = get_player_id_from_name(name)
            if pid:
                if pos in ['SP', 'P']:
                    prior = get_pitcher_stats(pid, date.today().year - 1)
                    if prior and prior.get('ip', 0) >= get_min_ip_prior_season() and prior.get('era', 99) <= sp_t['above_avg']['era']:
                        passes = True; reason = f"Proven SP — {prior['ip']:.0f} IP prior season"
                        stat_str = f"Prior ERA {prior['era']:.2f} | WHIP {prior['whip']:.2f}"
                else:
                    prior = get_hitter_stats(pid, date.today().year - 1)
                    if prior and prior.get('pa', 0) >= h_t['min_pa_full'] and prior.get('ops', 0) >= h_t['average_ops']:
                        passes = True; reason = f"Proven hitter — {prior['pa']} PA prior season"
                        stat_str = f"Prior OPS {prior['ops']:.3f} | HR {prior['hr']}"

        if not passes: continue
        if not stat_str:
            pid = get_player_id_from_name(name)
            if pid:
                if pos in ['SP','RP','P']:
                    s = get_pitcher_stats_blended(pid)
                    if s and s.get('ip', 0) >= min_ip:
                        stat_str = f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | K/BB {s['kbb']:.1f}"
                else:
                    s = get_hitter_stats(pid)
                    if s and s.get('pa', 0) >= h_t['min_pa_display']:
                        stat_str = f"OPS {s['ops']:.3f} | AVG {s['avg']:.3f} | HR {s['hr']}"
        return {'name': canonical_name, 'pct_owned': pct_owned, 'stat_str': stat_str, 'reason': reason}
    return None

def process_breaking_news(news, taken, my_roster, team_ops):
    seen         = load_seen_alerts()
    sleep_queue  = load_sleep_queue() if not awake_hours() else []
    alerts_sent  = 0
    reaction_min = _estimate_reaction_window(load_transactions())

    for item in news:
        title   = item['title']
        summary = item['summary']
        source  = item['source']
        text    = (title + ' ' + summary).lower()

        player = extract_player_name(title, summary, source)
        if not player: continue
        if normalize_name(player) in KNOWN_MEDIA_NAMES: continue

        canonical, is_available = validate_player_in_yahoo(player, taken)
        if canonical is None: continue
        player_norm = normalize_name(canonical)

        # ── SS INJURY WATCHLIST ─────────────────────────────────
        for ss in TOP_15_SS:
            if normalize_name(ss) == player_norm:
                if any(kw in text for kw in INJURY_KEYWORDS):
                    if any(kw in text for kw in MINOR_INJURY_KEYWORDS): break
                    key = f"ss_injury:{player_norm}"
                    if is_alert_seen(key, seen): break
                    is_mine = player_norm in [normalize_name(s) for s in ["Gunnar Henderson", "Trea Turner"]]
                    own_t   = get_ownership_thresholds()
                    if not is_mine:
                        available_ss = [p for p in get_league_free_agents(position='SS', count=10)
                                        if p['pct_owned'] >= own_t['meaningful'] or normalize_name(p['name']) in TOP_PROSPECTS]
                        if not available_ss: break
                    title_str = f"{'🚨' if is_mine else '👀'} SS INJURY: {canonical}"
                    if is_mine:
                        title_str += " ← YOUR PLAYER"
                        msg = f"{canonical} — {_extract_injury_detail(text)}\n\n⚠️ YOUR SS IS HURT. Check IL status immediately.\n\n⏱️ Act within ~{reaction_min} min\n\nSource: {source}"
                    else:
                        best = available_ss[0]
                        msg  = f"{canonical} — {_extract_injury_detail(text)}\n🎯 Available SS: {best['name']} ({best['pct_owned']:.0f}% owned)\n\n⏱️ Act within ~{reaction_min} min\n\nSource: {source}"
                    _fire_or_queue(title_str, msg, 1 if is_mine else 0, seen, key, sleep_queue, 'ss_injury')
                    alerts_sent += 1
                break

        if any(kw in text for kw in MINOR_INJURY_KEYWORDS): continue

        all_closers         = get_all_closers()
        is_confirmed_closer = player_norm in all_closers
        closer_role_change  = any(kw in text for kw in CLOSER_KEYWORDS)
        role_loss           = any(w in text for w in ['optioned','demoted','placed on il','injured list','released','suspended'])

        # ── CLOSER ON IL ────────────────────────────────────────
        if is_confirmed_closer and any(kw in text for kw in INJURY_KEYWORDS):
            key = f"saves_opp:{player_norm}"
            if not is_alert_seen(key, seen):
                closer_team = get_closer_team(player_norm)
                candidates  = get_closer_candidates(closer_team, taken) if closer_team else []
                if candidates:
                    drop_cand = find_best_drop(my_roster, team_ops)
                    drop_str  = f"\n\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)" if drop_cand else ""
                    msg = (f"⚡ {canonical} (closer) placed on IL.\n\n"
                           f"🎯 GRAB NOW: {', '.join(candidates[:2])} — available!\n"
                           f"Team: {closer_team or 'unknown'}{drop_str}\n\n"
                           f"⏱️ Act within ~{reaction_min} min\n\nSource: {source}")
                    _fire_or_queue(f"💾 SAVES OPP: {canonical} on IL", msg, 1, seen, key, sleep_queue, 'saves')
                    alerts_sent += 1
                else:
                    key_w = f"saves_watch:{player_norm}"
                    if not is_alert_seen(key_w, seen):
                        msg = (f"👀 {canonical} (closer) on IL.\n\n"
                               f"No clear available backup yet for {closer_team or 'their team'}.\n"
                               f"Watch for role announcement.\n\nSource: {source}")
                        _fire_or_queue(f"💾 SAVES WATCH: {canonical} on IL", msg, 0, seen, key_w, sleep_queue, 'saves')
                        alerts_sent += 1

        # ── CLOSER ROLE CHANGE ───────────────────────────────────
        elif closer_role_change and not role_loss and is_available:
            if any(kw in text for kw in INJURY_KEYWORDS):
                days_out = _estimate_days_out(text)
                if days_out is not None and days_out <= 21:
                    key = f"closer_role:{player_norm}"
                    if not is_alert_seen(key, seen):
                        il_used  = count_my_il_slots_used(my_roster)
                        has_slot = il_used < MY_IL_SLOTS
                        worst_il = get_worst_il_stash(my_roster)
                        own_t    = get_ownership_thresholds()
                        can_bump = worst_il and worst_il['pct_owned'] < own_t['meaningful'] and il_used >= MY_IL_SLOTS
                        if has_slot or can_bump:
                            slot_str = "Open IL slot" if has_slot else f"Bump {worst_il['name']} from IL"
                            msg = (f"💾 {canonical} returning from IL soon — closer when healthy!\n\n"
                                   f"🎯 Stash now: {slot_str}\nExpected return: ~{days_out} days\n\n"
                                   f"⏱️ Act within ~{reaction_min} min\n\nSource: {source}")
                            _fire_or_queue(f"💾 CLOSER STASH: {canonical}", msg, 1, seen, key, sleep_queue, 'saves')
                            alerts_sent += 1
            else:
                key = f"closer_role:{player_norm}"
                if not is_alert_seen(key, seen):
                    drop_cand = find_best_drop(my_roster, team_ops)
                    drop_str  = f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)" if drop_cand else ""
                    msg = (f"⚡ {canonical} taking over closing role!\n\n"
                           f"Available — add before leaguemates react.{drop_str}\n\n"
                           f"⏱️ Act within ~{reaction_min} min\n\nSource: {source}")
                    _fire_or_queue(f"💾 CLOSER ROLE: {canonical}", msg, 1, seen, key, sleep_queue, 'saves')
                    alerts_sent += 1

        # ── INJURY OPPORTUNITY ───────────────────────────────────
        elif any(kw in text for kw in INJURY_KEYWORDS) and not is_confirmed_closer:
            key = f"injury_opp:{player_norm}"
            if is_alert_seen(key, seen): continue
            if not _check_position_relevance(text, my_roster): continue
            if not any(w in text for w in ['replace','fill','opportunity','role','regular','everyday',
                                           'every day','platoon','takeover','lineup','starting','start',
                                           'called up','promoted','recalled','rotation spot','spot start']): continue
            backup = _find_relevant_backup(text, taken, my_roster, team_ops)
            if backup is None: continue
            drop_cand = find_best_drop(my_roster, team_ops)
            drop_str  = f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)" if drop_cand else ""
            msg = (f"🚑 {canonical} — {_extract_injury_detail(text)}\n\n"
                   f"🎯 Add: {backup['name']} ({backup['pct_owned']:.0f}% owned)"
                   f"{' — ' + backup['stat_str'] if backup['stat_str'] else ''}\n"
                   f"{backup['reason']}{drop_str}\n\n"
                   f"⏱️ Act within ~{reaction_min} min\n\nSource: {source}")
            _fire_or_queue(f"🚑 INJURY OPP: {canonical}", msg, 1, seen, key, sleep_queue, 'injury')
            alerts_sent += 1

        # ── TOP PROSPECT CALLUP ──────────────────────────────────
        elif player_norm in TOP_PROSPECTS:
            callup_signals = ['called up','promoted','recalled','selected','debut','call-up',
                              'joining','arrives','expected to start','set to make','will start','roster spot']
            if not any(s in text for s in callup_signals): continue
            if not is_available: continue
            key = f"prospect:{player_norm}"
            if is_alert_seen(key, seen): continue
            drop_cand = find_best_drop(my_roster, team_ops)
            drop_str  = f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)" if drop_cand else ""
            msg = (f"⚡ {canonical} called up!\n\n"
                   f"Top prospect — expected significant playing time. Add before leaguemates react.{drop_str}\n\n"
                   f"⏱️ Act within ~{reaction_min} min\n\nSource: {source}")
            _fire_or_queue(f"🔮 TOP PROSPECT: {canonical}", msg, 1, seen, key, sleep_queue, 'prospect')
            alerts_sent += 1

    save_seen_alerts(seen)
    if not awake_hours(): save_sleep_queue(sleep_queue)
    print(f"  Breaking news: {alerts_sent} alert(s)")
    return alerts_sent

def send_overnight_digest():
    queue = load_sleep_queue()
    if not queue: print("  No overnight alerts queued"); return
    by_cat = {}
    for item in queue:
        cat = item.get('category', 'other')
        if cat not in by_cat: by_cat[cat] = []
        by_cat[cat].append(item)
    lines        = [f"🌅 OVERNIGHT ({len(queue)} alert{'s' if len(queue) > 1 else ''}):\n"]
    max_priority = max(item.get('priority', 0) for item in queue)
    for cat, items in by_cat.items():
        for item in items: lines.append(f"{item['title']}\n{item['message'][:200]}\n")
    send_pushover("🌅 OVERNIGHT DIGEST", '\n'.join(lines)[:1024], priority=max_priority)
    save_sleep_queue([])
    print(f"  Sent overnight digest: {len(queue)} items")

# ============================================================
# IL RETURN + ROSTER PITCHER HELPERS
# ============================================================
def _get_pitchers_including_il_returns(roster, week_mon=None, week_sun=None):
    norms  = set()
    for p in (roster or []):
        if p.get('position', '') not in ['SP', 'P']: continue
        if 'IL' not in (p.get('status', '') or ''):
            norms.add(p['name_normalized']); continue
        return_note = p.get('injury_note', '') or ''
        if week_mon and week_sun:
            m = re.search(r'(\d{1,2})/(\d{1,2})', return_note)
            if m:
                try:
                    ret = date(date.today().year, int(m.group(1)), int(m.group(2)))
                    if week_mon <= ret <= week_sun:
                        norms.add(p['name_normalized'])
                        print(f"  IL return included: {p.get('name','')} (~{ret})")
                except Exception: pass
    return norms

def _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun):
    if not opp_team_id: return set()
    try:
        query           = get_yahoo_query()
        opp_roster_raw  = query.get_team_roster_player_info_by_date(opp_team_id, today)
        opp_roster_list = []
        for p in (opp_roster_raw or []):
            try:
                opp_roster_list.append({
                    'name': p.name.full, 'name_normalized': normalize_name(p.name.full),
                    'position': str(getattr(p, 'primary_position', '') or ''),
                    'status': str(getattr(p, 'status', '') or ''),
                    'injury_note': str(getattr(p, 'injury_note', '') or '')
                })
            except Exception: pass
        return _get_pitchers_including_il_returns(opp_roster_list, week_mon=week_mon, week_sun=week_sun)
    except Exception as e:
        print(f"  Opp roster error: {e}")
        return set()

# ============================================================
# ALERT: CURRENT WEEK SP ANALYSIS (Monday 8:45am)
# ============================================================
def send_current_week_sp_analysis(taken, my_roster, team_ops):
    print("Running current week SP analysis...")
    today    = datetime.now(ET_TZ).date()
    week_mon = monday_of_week(today)
    week_sun = sunday_of_week(today)
    min_ip   = get_min_ip_for_significance()
    h2h_t    = get_h2h_margin_thresholds()

    all_starters  = get_probable_pitchers(week_mon, week_sun, team_ops)
    my_norms      = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)

    matchup     = get_matchup_data()
    opp_team_id = matchup.get('opp_team_id') if matchup else None
    opp_norms   = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_starts  = []
    opp_starts = []
    for name, info in all_starters.items():
        norm  = normalize_name(name)
        stats = get_pitcher_stats_blended(info['id'])
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.730]))
        entry = {'name': name, 'count': info['count'], 'stats': stats, 'is_hq': is_hq,
                 'dates': info['dates'], 'opponents': info['opponents'], 'opp_ops': info['opp_ops']}
        if norm in my_norms:      my_starts.append(entry)
        elif norm in opp_norms:   opp_starts.append(entry)

    my_total  = sum(s['count'] for s in my_starts)
    opp_total = sum(s['count'] for s in opp_starts)
    my_hq     = sum(s['count'] for s in my_starts  if s['is_hq'])
    opp_hq    = sum(s['count'] for s in opp_starts if s['is_hq'])

    needs_action = (opp_total - my_total) > h2h_t['starts_deficit'] or (opp_hq - my_hq) > h2h_t['hq_starts_deficit']

    lines = [f"⚾ WEEK SP ANALYSIS\n",
             f"📊 Probable starts: You {my_total} vs Opp {opp_total}",
             f"🌟 High-quality starts: You {my_hq} vs Opp {opp_hq}\n"]

    if needs_action:
        lines.append("⚠️ You're behind — scanning for adds (Mon–Wed):\n")
        wed        = week_mon + timedelta(days=2)
        candidates = []
        for name, info in get_probable_pitchers(week_mon, wed, team_ops).items():
            if normalize_name(name) in taken: continue
            stats = get_pitcher_stats_blended(info['id'])
            if is_opener(stats): continue
            canonical, avail = validate_player_in_yahoo(name, taken)
            if not avail or canonical is None: continue
            hq    = [is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.730])]
            value = info['count'] * 10 + sum(5 for h in hq if h) - sum(3 for ops in info.get('opp_ops', []) if ops > get_opp_ops_tiers()['strong'])
            candidates.append((value, canonical, info, stats))
        candidates.sort(key=lambda x: x[0], reverse=True)
        if candidates:
            for _, cname, info, stats in candidates[:2]:
                matchups = ', '.join(f"{format_date(d)} vs {opp} {matchup_label(ops)}"
                                     for d, opp, ops in zip(info['dates'][:3], info['opponents'][:3], info['opp_ops'][:3]))
                stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                            if stats and stats.get('ip', 0) >= min_ip else "Limited stats")
                lines.append(f"• {cname} ({info['count']} start{'s' if info['count']>1 else ''})\n  {stat_str}\n  {matchups}")
            drop_cand = find_best_drop(my_roster, team_ops)
            if drop_cand: lines.append(f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")
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
    min_ip   = get_min_ip_for_significance()
    h2h_t    = get_h2h_margin_thresholds()

    matchup   = get_matchup_data()
    my_stats  = matchup.get('my_stats', {})  if matchup else {}
    opp_stats = matchup.get('opp_stats', {}) if matchup else {}

    all_starters = get_probable_pitchers(today, week_sun, team_ops)
    my_norms     = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)

    opp_team_id = matchup.get('opp_team_id') if matchup else None
    opp_norms   = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_remaining  = []
    opp_remaining = []
    for name, info in all_starters.items():
        norm  = normalize_name(name)
        stats = get_pitcher_stats_blended(info['id'])
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.730]))
        entry = {'name': name, 'count': info['count'], 'is_hq': is_hq, 'stats': stats,
                 'dates': info['dates'], 'opponents': info['opponents'], 'opp_ops': info['opp_ops']}
        if norm in my_norms:      my_remaining.append(entry)
        elif norm in opp_norms:   opp_remaining.append(entry)

    my_starts  = sum(s['count'] for s in my_remaining)
    opp_starts = sum(s['count'] for s in opp_remaining)
    my_hq      = sum(s['count'] for s in my_remaining  if s['is_hq'])
    opp_hq     = sum(s['count'] for s in opp_remaining if s['is_hq'])

    pitching_cats = ['W', 'SV', 'K', 'ERA', 'WHIP', 'KBB']
    cats_losing   = []
    cats_winning  = []
    for cat in pitching_cats:
        mv = my_stats.get(cat); ov = opp_stats.get(cat)
        if mv is None or ov is None: continue
        if cat in ['ERA', 'WHIP']:
            if mv > ov + h2h_t['era_whip_margin']:   cats_losing.append(cat)
            elif mv < ov - h2h_t['era_whip_margin']: cats_winning.append(cat)
        else:
            if mv < ov * (1 - h2h_t['counting_pct']):   cats_losing.append(cat)
            elif mv > ov * (1 + h2h_t['counting_pct']): cats_winning.append(cat)

    days_left      = days_left_in_week()
    need_streaming = (
        (opp_starts > my_starts and days_left >= 2)
        or (opp_hq - my_hq > h2h_t['hq_starts_deficit'] - 1 and days_left >= 2)
        or (len(cats_losing) >= 2 and my_starts <= opp_starts and days_left >= 1)
    )
    if len(cats_winning) >= 4 and my_hq >= opp_hq and my_starts >= opp_starts:
        need_streaming = False

    lines = [f"🌊 STREAMERS — {days_left}d remaining\n",
             f"Starts: You {my_starts} vs Opp {opp_starts} | HQ: You {my_hq} vs Opp {opp_hq}"]
    if my_stats and opp_stats:
        if cats_losing:  lines.append(f"📉 Losing: {', '.join(cats_losing)}")
        if cats_winning: lines.append(f"📈 Winning: {', '.join(cats_winning)}")
    lines.append("")

    if not need_streaming:
        lines.append("✅ No streaming needed — staff looks solid.")
        send_pushover("🌊 STREAMERS", '\n'.join(lines), priority=0)
        return

    lines.append("⚠️ Consider streaming:\n")
    candidates = []
    for name, info in all_starters.items():
        if normalize_name(name) in taken: continue
        stats = get_pitcher_stats_blended(info['id'])
        if is_opener(stats): continue
        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None: continue
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [0.730]))
        if not is_hq and info['count'] < 2: continue
        candidates.append((score_sp(stats, min(info.get('opp_ops', [0.730]))), canonical, info, stats))
    candidates.sort(key=lambda x: x[0], reverse=True)
    if candidates:
        for _, cname, info, stats in candidates[:2]:
            matchups = ', '.join(f"{format_date(d)} vs {opp} {matchup_label(ops)}"
                                 for d, opp, ops in zip(info['dates'][:3], info['opponents'][:3], info['opp_ops'][:3]))
            stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                        if stats and stats.get('ip', 0) >= min_ip else "Limited stats")
            lines.append(f"• {cname}\n  {stat_str}\n  {matchups}")
        drop_cand = find_best_drop(my_roster, team_ops)
        if drop_cand: lines.append(f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")
    else:
        lines.append("No quality streamers available.")
    send_pushover("🌊 STREAMERS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: 2-START SPs (Fri-Sun 8:30am)
# ============================================================
def send_two_start_alert(taken, my_roster, team_ops, preliminary=False):
    print(f"Running {'preliminary' if preliminary else 'full'} 2-start alert...")
    today      = datetime.now(ET_TZ).date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    next_mon   = today + timedelta(days=days_ahead)
    next_tue   = next_mon + timedelta(days=1)
    next_sun   = next_mon + timedelta(days=6)
    min_ip     = get_min_ip_for_significance()

    candidates = []
    for name, info in get_probable_pitchers(next_mon, next_sun, team_ops).items():
        if normalize_name(name) in taken: continue
        stats = get_pitcher_stats_blended(info['id'])
        if is_opener(stats): continue
        confirmed_two = info['count'] >= 2
        early_start   = info['dates'] and info['dates'][0] <= next_tue.isoformat()
        if not confirmed_two and not early_start: continue
        opp_ops_list = info.get('opp_ops', [0.730, 0.730])
        starts_hq    = [is_high_quality_sp(stats, ops) for ops in opp_ops_list[:2]]
        if not starts_hq[0]: continue
        second_hq = len(starts_hq) > 1 and starts_hq[1]
        if not second_hq and confirmed_two: continue
        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None: continue
        value = score_sp(stats)
        if confirmed_two: value += 20
        if second_hq:     value += 10
        candidates.append((value, canonical, info, stats, confirmed_two, second_hq))

    if not candidates:
        if not preliminary:
            send_pushover("⚾ 2-START SPs", f"No available 2-start quality options for week of {next_mon}.", priority=0)
        return

    candidates.sort(key=lambda x: x[0], reverse=True)
    prefix = "📋 EARLY LOOK — " if preliminary else ""
    lines  = [f"{prefix}⚾ 2-START SPs | Week of {next_mon}\n"]
    for _, cname, info, stats, conf_two, sec_hq in candidates[:3]:
        stat_str = (f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | {stats['k']}K | K/BB {stats['kbb']:.1f} ({stats.get('blend_note','')})"
                    if stats and stats.get('ip', 0) >= min_ip else "Limited stats")
        start_lines = []
        for i, (d, opp, ops) in enumerate(zip(info['dates'][:2], info['opponents'][:2], info['opp_ops'][:2])):
            hq_tag = "✅" if is_high_quality_sp(stats, ops) else "⚠️"
            start_lines.append(f"  Start {i+1}: {format_date(d)} vs {opp} {hq_tag} {matchup_label(ops)}")
        if not conf_two: start_lines.append("  Start 2: projected via rotation")
        lines.append(f"• {cname}\n  {stat_str}\n" + '\n'.join(start_lines))
    drop_cand = find_best_drop(my_roster, team_ops)
    if drop_cand: lines.append(f"\n💀 Drop candidate: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)")
    send_pushover("⚾ 2-START EARLY LOOK" if preliminary else "⚾ 2-START SPs", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: START/SIT (Daily 9am)
# ============================================================
def send_start_sit_alert(my_roster, team_ops, taken, matchup_data=None):
    print("Running start/sit alert...")
    today_date   = date.today()
    all_starters = get_probable_pitchers(today_date, today_date, team_ops)
    min_ip       = get_min_ip_for_significance()
    h2h_t        = get_h2h_margin_thresholds()

    my_sp_norms = {normalize_name(p['name']): p for p in my_roster
                   if p['position'] in ['SP', 'P'] and 'IL' not in (p['status'] or '')}

    my_stats  = matchup_data.get('my_stats', {})  if matchup_data else {}
    opp_stats = matchup_data.get('opp_stats', {}) if matchup_data else {}
    era_m     = h2h_t['era_whip_margin']
    winning_ratios = (my_stats.get('ERA') is not None and opp_stats.get('ERA') is not None
                      and my_stats['ERA'] < opp_stats['ERA'] - era_m
                      and my_stats.get('WHIP', 99) < opp_stats.get('WHIP', 99) - era_m)
    losing_ratios  = (my_stats.get('ERA') is not None and opp_stats.get('ERA') is not None
                      and my_stats['ERA'] > opp_stats['ERA'] + era_m)

    sit_alerts  = []
    start_notes = []
    t_ops       = get_opp_ops_tiers()

    for name, info in all_starters.items():
        norm = normalize_name(name)
        if norm not in my_sp_norms: continue
        p         = my_sp_norms[norm]
        stats     = get_pitcher_stats_blended(info['id']) if info.get('id') else None
        opp_ops   = info['opp_ops'][0] if info['opp_ops'] else 0.730
        opp_name  = info['opponents'][0] if info['opponents'] else 'unknown'
        tier      = get_sp_tier(stats)
        is_hq     = is_high_quality_sp(stats, opp_ops)
        long_term = sp_long_term_value(p, stats)

        if tier == 'elite':
            if opp_ops > t_ops['strong'] and winning_ratios:
                start_notes.append(f"⚠️ CONSIDER SIT: {name} vs {opp_name} {matchup_label(opp_ops)} — elite offense + you're leading ratios. Your call.")
            else:
                start_notes.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")
        elif tier in ('above_avg', 'average'):
            if is_hq:
                start_notes.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")
            elif opp_ops > t_ops['strong']:
                if losing_ratios:
                    start_notes.append(f"⚠️ START (need ratios): {name} vs {opp_name} {matchup_label(opp_ops)} — tough matchup but you're losing ERA/WHIP.")
                elif long_term:
                    start_notes.append(f"⚠️ SIT?: {name} vs {opp_name} {matchup_label(opp_ops)} — tough matchup. Sit if protecting ERA/WHIP lead.")
                else:
                    sit_alerts.append({'name': name, 'opp': opp_name, 'opp_ops': opp_ops, 'long_term': long_term, 'stats': stats, 'p': p})
        else:
            if is_hq: start_notes.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)} — good matchup")
            else:      sit_alerts.append({'name': name, 'opp': opp_name, 'opp_ops': opp_ops, 'long_term': long_term, 'stats': stats, 'p': p})

    if not sit_alerts and not start_notes:
        print("  No SP starts today")
        return

    lines = ["🎯 START/SIT — Today\n"]
    for note in start_notes: lines.append(note)

    for alert in sit_alerts:
        stat_str = ''
        if alert['stats'] and alert['stats'].get('ip', 0) >= min_ip:
            stat_str = f"ERA {alert['stats']['era']:.2f} | WHIP {alert['stats']['whip']:.2f} | Tier: {get_sp_tier(alert['stats'])}"
        lines.append(f"❌ SIT: {alert['name']} vs {alert['opp']} {matchup_label(alert['opp_ops'])}{' — ' + stat_str if stat_str else ''}")
        if not alert['long_term']:
            for avail_name, avail_info in all_starters.items():
                if normalize_name(avail_name) in taken: continue
                avail_ops = avail_info['opp_ops'][0] if avail_info['opp_ops'] else 0.730
                if not is_high_quality_matchup(avail_ops): continue
                avail_stats = get_pitcher_stats_blended(avail_info['id'])
                if not is_high_quality_sp(avail_stats, avail_ops): continue
                canonical, avail = validate_player_in_yahoo(avail_name, taken)
                if not avail or canonical is None: continue
                avail_stat_str = ''
                if avail_stats and avail_stats.get('ip', 0) >= min_ip:
                    avail_stat_str = f"ERA {avail_stats['era']:.2f} | WHIP {avail_stats['whip']:.2f}"
                lines.append(f"  🔄 Add instead: {canonical} vs {avail_info['opponents'][0] if avail_info['opponents'] else '?'} {matchup_label(avail_ops)}{' | ' + avail_stat_str if avail_stat_str else ''}")
                lines.append(f"  💀 Drop: {alert['name']} ({alert['p']['pct_owned']:.0f}%)")
                break

    if len(lines) > 1: send_pushover("🎯 START/SIT", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: PITCHER SCRATCH (Hourly 11am-6pm)
# ============================================================
def check_pitcher_scratch(my_roster, games):
    print("Checking pitcher scratches...")
    morning_probables = load_morning_probables()
    scratch_alerted   = load_scratch_alerted()
    if not morning_probables: print("  No morning probables — skipping"); return
    current_probables = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']: continue
        if game['home_probable']: current_probables[game['home_team']] = game['home_probable']
        if game['away_probable']: current_probables[game['away_team']] = game['away_probable']
    for sp in [p for p in my_roster if p['position'] == 'SP' and 'IL' not in (p['status'] or '')]:
        team_name = TEAM_NAME_MAP.get(sp['team_abbr'], '')
        if not team_name: continue
        morning_starter = morning_probables.get(team_name, '')
        if normalize_name(morning_starter) != normalize_name(sp['name']): continue
        current_starter = current_probables.get(team_name, '')
        if not current_starter: continue
        if normalize_name(current_starter) != normalize_name(sp['name']):
            key = normalize_name(sp['name'])
            if key in scratch_alerted: continue
            send_pushover(f"🚫 SCRATCH: {sp['name']}",
                          f"{sp['name']} replaced by {current_starter} for {team_name}.\n\n⚠️ Swap in a bench SP or grab a same-day streamer!",
                          priority=1)
            scratch_alerted[key] = True
            save_scratch_alerted(scratch_alerted)

# ============================================================
# ALERT: BATTER SITTING / POSTPONED (Hourly 11am-6pm)
# ============================================================
def check_lineups_and_weather(my_roster, games):
    print("Checking lineups and postponements...")
    sitting_alerted = load_sitting_alerts()
    newly_alerted   = dict(sitting_alerted)
    my_hitters = [p for p in my_roster
                  if p['position'] not in ['SP', 'RP', 'P']
                  and 'IL' not in (p['status'] or '')
                  and p['selected_position'] not in ['BN', 'IL']]
    for game in games:
        home_team     = game['home_team']
        away_team     = game['away_team']
        status        = game['status']
        all_lineup    = game['home_lineup'] + game['away_lineup']
        lineup_posted = len(all_lineup) > 0
        game_soon     = _game_starts_soon(game)
        for hitter in my_hitters:
            team_name = TEAM_NAME_MAP.get(hitter['team_abbr'], '')
            if not team_name or team_name not in (home_team, away_team): continue
            key = normalize_name(hitter['name'])
            if status in ['Postponed', 'Suspended']:
                if key not in sitting_alerted:
                    send_pushover(f"🌧️ POSTPONED: {hitter['name']}",
                                  f"{away_team} @ {home_team} postponed.\n{hitter['name']} will not play.\n\n⚠️ Swap in a bench hitter!", priority=1)
                    newly_alerted[key] = 'postponed'
                continue
            if not game_soon: continue
            if lineup_posted and status not in ['Final', 'Game Over', 'In Progress']:
                if key in sitting_alerted: continue
                in_lineup = any(normalize_name(hitter['name']) in normalize_name(lp) or
                                normalize_name(lp) in normalize_name(hitter['name'])
                                for lp in all_lineup)
                if not in_lineup:
                    send_pushover(f"🪑 SITTING: {hitter['name']}",
                                  f"{hitter['name']} is NOT in today's lineup for {team_name}.\n\n⚠️ Swap in a bench hitter before lock!", priority=1)
                    newly_alerted[key] = 'sitting'
    save_sitting_alerts(newly_alerted)

def _game_starts_soon(game, hours=3):
    try:
        game_time   = game.get('game_time_utc', '')
        if not game_time: return True
        game_dt     = datetime.strptime(game_time[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
        hours_until = (game_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        return -1 <= hours_until <= hours
    except Exception: return True

# ============================================================
# ALERT: WAIVER DROPS (Daily 9am)
# ============================================================
def send_waiver_drops_alert(taken, my_roster, team_ops):
    print("Running waiver drops check...")
    recent_drops = get_waiver_drops_to_review(taken, my_roster)
    if not recent_drops: print("  No recent drops to review"); return

    today            = date.today()
    week_sun         = sunday_of_week(today)
    my_week_starters = get_probable_pitchers(today, week_sun, team_ops)
    own_t            = get_ownership_thresholds()
    h_t              = get_hitter_thresholds()
    sp_t             = get_sp_quality_thresholds()
    min_ip           = get_min_ip_for_significance()

    my_by_pos = {}
    for p in my_roster:
        pos = p['position']
        if pos not in my_by_pos: my_by_pos[pos] = []
        my_by_pos[pos].append(p)

    alerts = []

    for drop in recent_drops:
        name = drop['name']
        pos  = drop['position']

        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None: continue

        # Resolve blank position
        if not pos:
            mlb_id = get_player_id_from_name(name)
            if mlb_id:
                try:
                    d   = requests.get(f"https://statsapi.mlb.com/api/v1/people/{mlb_id}", timeout=3).json()
                    pos = d.get('people', [{}])[0].get('primaryPosition', {}).get('abbreviation', '')
                except Exception: pass

        if pos in MY_CLOGGED_POSITIONS: continue

        # IL stash logic
        days_out_est = _estimate_days_out(drop.get('notes', '').lower())
        if days_out_est is not None:
            mlb_id = get_player_id_from_name(name)
            if not mlb_id: continue
            is_elite = False
            if pos in ['SP', 'RP', 'P']:
                stats    = get_pitcher_stats_blended(mlb_id)
                is_elite = (stats and stats.get('ip', 0) >= min_ip and stats.get('era', 99) <= sp_t['elite']['era'])
            else:
                stats    = get_hitter_stats(mlb_id)
                is_elite = (stats and stats.get('pa', 0) >= h_t['min_pa_signal'] and stats.get('ops', 0) >= h_t['elite_il_ops'])
            is_elite = is_elite or normalize_name(name) in TOP_PROSPECTS
            # Duration gates: elite = up to 60 days (typical injury cycle for roster-worthy elites)
            # Non-elite = up to 21 days (3-week window where roster cost is justified)
            stash_worthy = (is_elite and days_out_est <= 60) or (not is_elite and days_out_est <= 21)
            il_used  = count_my_il_slots_used(my_roster)
            has_slot = il_used < MY_IL_SLOTS
            worst_il = get_worst_il_stash(my_roster)
            can_bump = worst_il and worst_il['pct_owned'] < own_t['meaningful'] and il_used >= MY_IL_SLOTS
            if not stash_worthy or (not has_slot and not can_bump): continue
            stat_str = ''
            if pos in ['SP','RP','P'] and stats and stats.get('ip', 0) >= min_ip:
                stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f}"
            elif stats and stats.get('pa', 0) >= h_t['min_pa_display']:
                stat_str = f"OPS {stats['ops']:.3f} | HR {stats['hr']}"
            slot_str  = "Open IL slot" if has_slot else f"Bump {worst_il['name']} from IL"
            avail_dt  = datetime.fromtimestamp(drop['timestamp'], tz=ET_TZ) + timedelta(days=2)
            alerts.append({'name': canonical, 'pos': pos, 'stat_str': stat_str,
                           'reason': f"IL stash — {slot_str} | ~{days_out_est}d timeline",
                           'drop': worst_il or {'name': 'IL occupant', 'pct_owned': 0},
                           'avail_date': avail_dt.strftime('%a %-m/%-d'), 'stash': True})
            continue

        # Standard add — full relevance gate
        mlb_id     = get_player_id_from_name(name)
        stats      = None
        stat_str   = ''
        value      = 0
        is_pitcher = pos in ['SP', 'RP', 'P']

        if is_pitcher and mlb_id:
            stats = get_pitcher_stats_blended(mlb_id)
            if stats and stats.get('ip', 0) >= min_ip:
                if pos == 'RP':
                    qualifies, rp_stat = _is_reliever_worth_rostering(stats, my_roster)
                    if not qualifies: continue
                    value    = score_sp(stats)
                    stat_str = rp_stat
                else:
                    value    = score_sp(stats)
                    stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
        elif not is_pitcher and mlb_id:
            stats = get_hitter_stats(mlb_id)
            if stats and stats.get('pa', 0) >= h_t['min_pa_display']:
                value    = stats.get('ops', 0) * 100
                stat_str = f"AVG {stats['avg']:.3f} | OPS {stats['ops']:.3f} | HR {stats['hr']}"

        if not stats: continue

        passes      = False
        drop_target = None
        reason      = ''
        norm_drop   = normalize_name(name)

        if is_pitcher:
            my_pitchers = my_by_pos.get('SP', []) + my_by_pos.get('RP', []) + my_by_pos.get('P', [])
            for mp in my_pitchers:
                if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE: continue
                if mp['pct_owned'] >= own_t['majority']: continue
                mp_id    = get_player_id_from_name(mp['name'])
                mp_stats = get_pitcher_stats_blended(mp_id) if mp_id else None
                mp_value = score_sp(mp_stats) if mp_stats else -999
                # Threshold: > 5 score points = meaningful ERA/WHIP improvement (~1 SD)
                if value > mp_value + 5:
                    passes = True; drop_target = mp; reason = f"Better long-term value than {mp['name']}"; break
            if not passes:
                for mp in [m for m in my_pitchers if not sp_long_term_value(m, None) and m['pct_owned'] < own_t['significant']]:
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_pitcher_stats_blended(mp_id) if mp_id else None
                    mp_value = score_sp(mp_stats) if mp_stats else -999
                    if value > mp_value + 5 and norm_drop in {normalize_name(k) for k in my_week_starters}:
                        for sp_name, sp_info in my_week_starters.items():
                            if normalize_name(sp_name) == norm_drop:
                                if is_high_quality_matchup(sp_info['opp_ops'][0] if sp_info['opp_ops'] else 0.730):
                                    passes = True; drop_target = mp; reason = f"Better streaming option than {mp['name']} this week"
                                break
                    if passes: break
        else:
            for comp_pos in _get_compatible_positions(pos):
                for mp in my_by_pos.get(comp_pos, []):
                    if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE: continue
                    if mp['pct_owned'] >= own_t['majority']: continue
                    others = [x for x in my_by_pos.get(comp_pos, []) if x['name'] != mp['name']]
                    if not others and comp_pos not in ['Util', 'BN']: continue
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_hitter_stats(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    # Threshold: OPS difference > 0.030 = ~1 SD in weekly batting impact
                    if value > mp_value + 3.0:
                        passes = True; drop_target = mp; reason = f"Better than {mp['name']} at {comp_pos}"; break
                if passes: break
            if not passes:
                for mp in my_by_pos.get('BN', []) + my_by_pos.get('Util', []):
                    if mp['is_undroppable'] or mp['name_normalized'] in MY_UNDROPPABLE: continue
                    if mp['pct_owned'] >= own_t['majority']: continue
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_hitter_stats(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    if value > mp_value + 3.0:
                        passes = True; drop_target = mp; reason = f"Upgrade over bench {mp['name']}"; break

        if not passes or not drop_target: continue

        avail_dt = datetime.fromtimestamp(drop['timestamp'], tz=ET_TZ) + timedelta(days=2)
        alerts.append({'name': canonical, 'pos': pos, 'stat_str': stat_str, 'reason': reason,
                       'drop': drop_target, 'avail_date': avail_dt.strftime('%a %-m/%-d'), 'stash': False})

    if not alerts: print("  No meaningful waiver drops found"); return

    lines = ["🗑️ WAIVER DROPS — Worth considering:\n"]
    for a in alerts[:3]:
        stash_label = " 🏥 IL STASH" if a.get('stash') else ""
        lines.append(f"• {a['name']} ({a['pos']}){stash_label}{' — ' + a['stat_str'] if a['stat_str'] else ''}\n"
                     f"  {a['reason']}\n  Available: {a['avail_date']}\n"
                     f"  💀 Drop/move: {a['drop']['name']} ({a['drop'].get('pct_owned', 0):.0f}%)")
    send_pushover("🗑️ WAIVER DROPS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: NEW POSITIONAL ELIGIBILITY (Daily 9am)
# ============================================================
def check_positional_eligibility(my_roster, team_ops):
    print("Checking positional eligibility...")
    hitter_weak = [p for p in get_weak_positions(my_roster) if p not in ['SP', 'RP', 'P']]
    if not hitter_weak: return
    alerted = load_pos_eligibility_alerts()
    found   = []
    for pos in hitter_weak:
        for player in get_league_free_agents(position=pos, count=15):
            key = f"{normalize_name(player['name'])}:{pos}"
            if key in alerted: continue
            pid = get_player_id_from_name(player['name'])
            if not pid: continue
            try:
                url  = f"https://statsapi.mlb.com/api/v1/people/{pid}/stats?stats=season&group=fielding&season={date.today().year}"
                data = requests.get(url, timeout=5).json()
                for sg in data.get('stats', []):
                    for split in sg.get('splits', []):
                        split_pos = split.get('position', {}).get('abbreviation', '')
                        games_at  = int(split.get('stat', {}).get('games', 0) or 0)
                        # Yahoo grants eligibility at 10 games; 5-9 = approaching threshold
                        if split_pos == pos and 5 <= games_at < 10:
                            found.append({'name': player['name'], 'pos': pos, 'games_at': games_at,
                                          'key': key, 'pct_owned': player['pct_owned']})
                            alerted[key] = True
            except Exception: continue
    if found:
        lines = ["📍 NEW POS ELIGIBILITY APPROACHING:\n"]
        for f in found[:5]:
            lines.append(f"• {f['name']} — {f['games_at']}/10 games at {f['pos']} ({f['pct_owned']:.0f}% owned)\n  Would fill your weak {f['pos']} spot once eligible!")
        send_pushover("📍 POS ELIGIBILITY", '\n'.join(lines), priority=0)
        save_pos_eligibility_alerts(alerted)
    elif datetime.now(ET_TZ).weekday() == 6 and not any(k for k in alerted):
        send_pushover("📍 POS ELIGIBILITY", "No players approaching new positional eligibility at your weak spots this week.", priority=0)

# ============================================================
# ALERT: TRADE SUGGESTIONS (Friday 1pm)
# ============================================================
def send_trade_suggestions(my_roster, all_rosters, team_ops):
    print("Running trade suggestion analysis...")
    trade_history = load_trade_history()
    two_weeks_ago = datetime.now(timezone.utc).timestamp() - (14 * 86400)
    own_t         = get_ownership_thresholds()
    h_t           = get_hitter_thresholds()

    def player_value(p):
        pid = get_player_id_from_name(p['name'])
        if p['position'] in ['SP', 'P', 'RP']:
            stats = get_pitcher_stats_blended(pid) if pid else None
            return score_sp(stats) + p['pct_owned'] * 0.3
        stats = get_hitter_stats(pid) if pid else None
        if stats and stats.get('pa', 0) >= h_t['min_pa_display']:
            return stats.get('ops', 0) * 80 + p['pct_owned'] * 0.3
        return p['pct_owned'] * 0.5

    def roster_score_by_pos(roster):
        by_pos = {}
        for p in roster:
            pos = p['position']
            if pos in ['BN', 'IL', 'Util']: continue
            if pos not in by_pos: by_pos[pos] = []
            by_pos[pos].append(player_value(p))
        return {pos: round(sum(vals)/len(vals), 1) for pos, vals in by_pos.items() if vals}

    my_scores    = roster_score_by_pos(my_roster)
    hitting_pos  = ['C', '1B', '2B', '3B', 'SS', 'OF']
    pitching_pos = ['SP', 'RP']
    my_weak      = sorted([p for p in hitting_pos  if p in my_scores], key=lambda x: my_scores.get(x, 0))[:2] + \
                   sorted([p for p in pitching_pos if p in my_scores], key=lambda x: my_scores.get(x, 0))[:1]
    my_strong    = sorted([p for p in hitting_pos  if p in my_scores], key=lambda x: my_scores.get(x, 0), reverse=True)[:2] + \
                   sorted([p for p in pitching_pos if p in my_scores], key=lambda x: my_scores.get(x, 0), reverse=True)[:1]

    proposals = []
    for team_id, their_roster in (all_rosters or {}).items():
        if team_id == MY_TEAM_ID or not their_roster: continue
        their_scores   = roster_score_by_pos(their_roster)
        their_weak_pos = sorted(their_scores.keys(), key=lambda x: their_scores.get(x, 0))[:3]
        their_str_pos  = sorted(their_scores.keys(), key=lambda x: their_scores.get(x, 0), reverse=True)[:3]

        for give_pos in my_strong:
            if give_pos not in their_weak_pos: continue
            for get_pos in my_weak:
                if get_pos not in their_str_pos or give_pos == get_pos: continue
                my_give_cands = sorted([p for p in my_roster if p['position'] == give_pos
                                        and not p['is_undroppable'] and p['name_normalized'] not in MY_UNDROPPABLE
                                        and p['pct_owned'] >= own_t['significant']], key=player_value, reverse=True)
                their_get_cands = sorted([p for p in their_roster if p['position'] == get_pos
                                          and not p.get('is_undroppable', False)
                                          and p['pct_owned'] >= own_t['significant']], key=player_value, reverse=True)
                if not my_give_cands or not their_get_cands: continue
                my_give   = my_give_cands[0]
                their_get = their_get_cands[0]
                # Fairness: within 25 pct_owned points (3 teams in 12-team league)
                if abs(my_give['pct_owned'] - their_get['pct_owned']) > 25: continue
                if my_scores.get(give_pos, 0) <= my_scores.get(get_pos, 0): continue
                # Positional coverage: must retain healthy player at give_pos
                if give_pos in hitting_pos:
                    remaining = [p for p in my_roster if p['position'] == give_pos
                                 and p['name'] != my_give['name'] and 'IL' not in (p['status'] or '')]
                    if not remaining: continue
                pair_key = tuple(sorted([normalize_name(my_give['name']), normalize_name(their_get['name'])]))
                prior    = [t for t in trade_history if t.get('pair') == list(pair_key)]
                if len(prior) >= 2 or any(t.get('ts', 0) > two_weeks_ago for t in prior): continue
                proposals.append({
                    'give': my_give['name'], 'give_pos': give_pos, 'give_pct': my_give['pct_owned'],
                    'get': their_get['name'], 'get_pos': get_pos, 'get_pct': their_get['pct_owned'],
                    'rationale': f"Your {give_pos} depth ({my_give['name']}) fills their need; their {get_pos} asset ({their_get['name']}) upgrades your {get_pos}.",
                    'pair': list(pair_key)
                })

    if not proposals:
        send_pushover("🔄 TRADE IDEA", "No equitable trade opportunities identified this week.", priority=0)
        return

    lines = ["🔄 TRADE IDEA — Friday 1pm\n"]
    for prop in proposals[:2]:
        lines.append(f"📤 You give: {prop['give']} ({prop['give_pos']}, {prop['give_pct']:.0f}% owned)\n"
                     f"📥 You get: {prop['get']} ({prop['get_pos']}, {prop['get_pct']:.0f}% owned)\n"
                     f"💡 {prop['rationale']}\n")
        trade_history.append({'pair': prop['pair'], 'ts': datetime.now(timezone.utc).timestamp()})
    save_trade_history(trade_history)
    send_pushover("🔄 TRADE IDEA", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: LEAGUEMATE INTEL (Sunday 9pm)
# ============================================================
def send_leaguemate_intel():
    print("Running leaguemate intel...")
    transactions = load_transactions()
    if not transactions:
        send_pushover("🕵️ LEAGUE INTEL", "Not enough transaction data yet.", priority=0); return
    adds  = [t for t in transactions if 'add' in t.get('type', '')]
    drops = [t for t in transactions if 'drop' in t.get('type', '')]
    lines = [f"🕵️ LEAGUEMATE INTEL\n",
             f"Season: {len(transactions)} transactions | {len(adds)} adds | {len(drops)} drops\n"]
    team_add_counts = {}
    for t in adds:
        for p in t.get('players', []):
            dest = p.get('dest_team', '')
            if dest: team_add_counts[dest] = team_add_counts.get(dest, 0) + 1
    if team_add_counts:
        lines.append("🔥 Most active managers:")
        for team_key, count in sorted(team_add_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"  • Team {team_key}: {count} adds")
    reaction_min = _estimate_reaction_window(transactions)
    lines.append(f"\n⏱️ Estimated reaction window: ~{reaction_min} min")
    week_ago = datetime.now(timezone.utc).timestamp() - (7 * 86400)
    lines.append(f"📅 Last 7 days: {sum(1 for t in adds if t.get('timestamp', 0) > week_ago)} adds")
    send_pushover("🕵️ LEAGUE INTEL", '\n'.join(lines), priority=0)

# ============================================================
# MORNING PROBABLES SNAPSHOT (Daily 8am)
# ============================================================
def store_morning_probables(games):
    probables = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']: continue
        if game['home_probable']: probables[game['home_team']] = game['home_probable']
        if game['away_probable']: probables[game['away_team']] = game['away_probable']
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
    weekday   = now_et.weekday()

    print(f"\n{'='*52}")
    print(f"Run: {now_utc.strftime('%Y-%m-%d %H:%M UTC')} | "
          f"{now_et.strftime('%H:%M ET %A')} | Week {get_current_week()} | "
          f"Day {get_days_into_season()} | Blend: {int(get_season_blend()[0]*100)}% prior")
    print(f"{'='*52}")

    def at(h, m_start=0, m_end=14):
        return hour_et == h and m_start <= minute_et <= m_end
    def between(h1, h2):
        return h1 <= hour_et <= h2

    in_sleep = hour_et >= 22 or hour_et < 6 or (hour_et == 6 and minute_et < 30)

    taken, my_roster, all_rosters = None, None, None
    games    = None
    team_ops = None
    matchup_data = None

    def ensure_rosters():
        nonlocal taken, my_roster, all_rosters
        if taken is None: taken, my_roster, all_rosters = get_all_rosters()
        return taken is not None
    def ensure_games():
        nonlocal games
        if games is None: games = get_todays_schedule()
        return games
    def ensure_team_ops():
        nonlocal team_ops
        if team_ops is None: team_ops = get_team_batting_stats()
        return team_ops
    def ensure_matchup():
        nonlocal matchup_data
        if matchup_data is None: matchup_data = get_matchup_data()
        return matchup_data

    if at(6, 30, 44):
        print("\n--- OVERNIGHT DIGEST ---")
        send_overnight_digest()

    if at(8, 0, 14):
        print("\n--- MORNING PROBABLES SNAPSHOT ---")
        ensure_games()
        store_morning_probables(games)

    if weekday == 0 and at(8, 45, 59):
        print("\n--- CURRENT WEEK SP ANALYSIS ---")
        if ensure_rosters() and ensure_team_ops():
            send_current_week_sp_analysis(taken, my_roster, team_ops)

    if at(9, 0, 14):
        print("\n--- DAILY 9AM ALERTS ---")
        if ensure_rosters() and ensure_team_ops():
            ensure_games()
            ensure_matchup()
            send_start_sit_alert(my_roster, team_ops, taken, matchup_data)
            send_waiver_drops_alert(taken, my_roster, team_ops)
            check_positional_eligibility(my_roster, team_ops)

    if weekday in [2, 3, 4, 5, 6] and at(7, 0, 14):
        print("\n--- STREAMERS ALERT ---")
        if ensure_rosters() and ensure_team_ops():
            send_streamers_alert(taken, my_roster, team_ops)

    if weekday in [4, 5, 6] and at(8, 30, 44):
        print("\n--- 2-START SPs ---")
        if ensure_rosters() and ensure_team_ops():
            send_two_start_alert(taken, my_roster, team_ops, preliminary=(weekday == 4))

    if between(11, 18) and minute_et < 5:
        print("\n--- PITCHER SCRATCH CHECK ---")
        if ensure_rosters():
            ensure_games()
            check_pitcher_scratch(my_roster, games)

    if between(11, 18) and 5 <= minute_et < 10:
        print("\n--- LINEUP / SITTING CHECK ---")
        if ensure_rosters():
            ensure_games()
            check_lineups_and_weather(my_roster, games)

    if weekday == 4 and at(13, 0, 14):
        print("\n--- TRADE SUGGESTIONS ---")
        if ensure_rosters() and ensure_team_ops():
            send_trade_suggestions(my_roster, all_rosters, team_ops)

    if weekday == 6 and at(21, 0, 14):
        print("\n--- LEAGUEMATE INTEL ---")
        send_leaguemate_intel()

    print("\n--- BREAKING NEWS CHECK ---")
    ensure_team_ops()
    news = get_all_news(lookback_minutes=20)
    if ensure_rosters():
        process_breaking_news(news, taken, my_roster, team_ops)
        try: sync_league_transactions()
        except Exception as e: print(f"  Transaction sync skipped: {e}")

    if in_sleep:
        print("\n[Sleep window — alerts queued for 6:30am digest]")
    print("\nDone.")

if __name__ == "__main__":
    main()
