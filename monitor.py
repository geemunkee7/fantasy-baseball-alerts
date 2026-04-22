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

PROBABLES_FILE       = '/tmp/morning_probables.json'
SITTING_ALERTS_FILE  = '/tmp/sitting_alerts.json'
SEEN_ALERTS_FILE     = '/tmp/seen_alerts.json'
TRANSACTIONS_FILE    = '/tmp/league_transactions.json'
MATCHUP_CACHE_FILE   = '/tmp/matchup_cache.json'
CLOSERMONKEY_CACHE   = '/tmp/closermonkey_cache.json'

SEASON_START = date(2026, 3, 20)

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
TRANSACTION_SOURCES  = {'Rotowire', 'MLB Trade Rumors'}

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
    'injured', 'il ', 'injured list', 'day-to-day', 'placed on',
    'disabled', 'hamstring', 'oblique', 'knee', 'wrist', 'shoulder',
    'elbow', 'back', 'thumb', 'ankle', 'concussion', 'surgery', 'fracture'
]

CLOSER_KEYWORDS = [
    'closer', 'closing role', 'save opportunity', 'saves role',
    'ninth inning', 'closing duties', 'closing games', 'save situation'
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

NON_EVENT_LIST_KEYWORDS = [
    'paternity', 'bereavement', 'family medical',
    'paternity list', 'bereavement list'
]

DEBUT_FORWARD_SIGNALS = [
    'set to debut', 'will debut', 'expected to debut',
    'scheduled to debut', 'could debut', 'may debut',
    'first career', 'first major league', 'first mlb'
]

# Depth/roster filler signals — generic recalls not worth alerting on
DEPTH_MOVE_SIGNALS = [
    'depth move', 'roster move', 'corresponding move',
    'long man', 'mop-up', 'spot start only', 'rule 5',
    'non-roster', 'fill-in', 'recalled to fill'
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
    "grayson rodriguez", "dean kremer", "coleman crow",
    "walbert urena"
}

# My current roster for logjam analysis
# Positions that are genuinely clogged with quality players
MY_CLOGGED_POSITIONS = {'SS', 'OF'}  # Henderson+Turner at SS, 4 OFs
MY_UNDROPPABLE = {
    "gunnar henderson", "trea turner", "matt olson",
    "shohei ohtani", "nico hoerner"
}

# ============================================================
# SEASONAL CALIBRATION
# ============================================================
def get_season_thresholds():
    days_in = (date.today() - SEASON_START).days
    if days_in < 26:
        return {
            'streaming_era': 4.50, 'streaming_whip': 1.35,
            'streaming_kbb': 1.8,  'streaming_ip':   5,
            'twostart_era':  4.00, 'twostart_whip':  1.30,
            'twostart_kbb':  2.0,  'twostart_ip':    10,
            'spotstart_ip':  5,    'drop_era_floor':  4.50,
            'drop_own_ceil': 40,   'blend_prior':     0.80,
            'blend_curr':    0.20,
        }
    elif days_in < 57:
        return {
            'streaming_era': 4.30, 'streaming_whip': 1.30,
            'streaming_kbb': 2.0,  'streaming_ip':   15,
            'twostart_era':  3.90, 'twostart_whip':  1.25,
            'twostart_kbb':  2.1,  'twostart_ip':    20,
            'spotstart_ip':  10,   'drop_era_floor':  4.30,
            'drop_own_ceil': 40,   'blend_prior':     0.60,
            'blend_curr':    0.40,
        }
    elif days_in < 103:
        return {
            'streaming_era': 4.10, 'streaming_whip': 1.25,
            'streaming_kbb': 2.2,  'streaming_ip':   25,
            'twostart_era':  3.75, 'twostart_whip':  1.20,
            'twostart_kbb':  2.2,  'twostart_ip':    35,
            'spotstart_ip':  20,   'drop_era_floor':  4.10,
            'drop_own_ceil': 35,   'blend_prior':     0.35,
            'blend_curr':    0.65,
        }
    else:
        return {
            'streaming_era': 4.00, 'streaming_whip': 1.20,
            'streaming_kbb': 2.4,  'streaming_ip':   35,
            'twostart_era':  3.60, 'twostart_whip':  1.15,
            'twostart_kbb':  2.3,  'twostart_ip':    50,
            'spotstart_ip':  30,   'drop_era_floor':  4.00,
            'drop_own_ceil': 30,   'blend_prior':     0.10,
            'blend_curr':    0.90,
        }

def get_current_week():
    days_in = (date.today() - SEASON_START).days
    return max(1, (days_in // 7) + 1)

def days_remaining_in_week():
    today   = date.today()
    weekday = today.weekday()  # 0=Mon
    # Yahoo weeks typically run Mon-Sun
    return max(0, 6 - weekday)

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
    # Must be 2-4 words — single word names are never valid
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
        'daily', 'morning', 'sources', 'video', 'watch', 'review', 'week',
        'angels', 'orioles', 'yankees', 'rays', 'red', 'sox', 'blue', 'jays',
        'white', 'guardians', 'tigers', 'royals', 'twins', 'astros',
        'athletics', 'mariners', 'rangers', 'braves', 'marlins', 'mets',
        'phillies', 'nationals', 'cubs', 'reds', 'brewers', 'pirates',
        'cardinals', 'diamondbacks', 'rockies', 'dodgers', 'padres', 'giants'
    }
    for word in words:
        if word.lower() in non_name_words:
            return False
    return True

def clean_text(text):
    """Strip HTML tags AND decode HTML entities properly."""
    if not text:
        return ''
    # Decode HTML entities first
    text = html.unescape(text)
    # Then strip any remaining HTML tags
    text = re.sub('<[^<]+?>', '', text)
    return text.strip()

# ============================================================
# TRANSACTION ARTICLE FILTER
# ============================================================
def is_transaction_article(item):
    """
    Returns True only if this article describes an actual roster move.
    Applies to ALL alert types including SS injury — no exceptions.
    """
    source = item.get('source', '')
    if source in TRANSACTION_SOURCES:
        return True
    text = (item['title'] + ' ' + item['summary']).lower()
    if not any(verb in text for verb in TRANSACTION_VERBS):
        print(f"  Skipping [{source}] — no transaction verb")
        return False
    if 'debut' in text:
        if not any(signal in text for signal in DEBUT_FORWARD_SIGNALS):
            print(f"  Skipping [{source}] — debut in recap context")
            return False
    return True

# ============================================================
# FANTASY RELEVANCE FILTER
# Gate 1: Is player available?
# Gate 2a: Does it help win a category this week?
# Gate 2b: Does it have long-term season value?
# ============================================================
def is_fantasy_relevant(player_name, text, taken,
                        my_roster=None, matchup_data=None):
    norm = normalize_name(player_name)

    # Gate 1 — must not be rostered, no exceptions including prospects
    if norm in taken:
        print(f"  {player_name} is rostered — skipping")
        return False, None

    # Gate 2b — closer situation always relevant
    if any(w in text for w in CLOSER_KEYWORDS):
        return True, 'closer'

    # Gate 2b — high owned RP on IL always relevant (saves situation)
    # even if article doesn't use closer keywords
    if any(w in text for w in ['placed on il', 'injured list',
                                'day-to-day', 'goes on il']):
        # Try to find ownership from my_roster context or text signals
        high_ownership_signals = [
            'all-star', 'elite', 'top reliever', 'key reliever',
            'primary', 'team\'s closer', 'saves leader'
        ]
        if any(s in text for s in high_ownership_signals):
            return True, 'high_owned_rp_il'

    # Gate 2b — top prospects always relevant
    if norm in TOP_PROSPECTS:
        return True, 'prospect'

    # Gate 2b — confirmed everyday role
    everyday_words = [
        'everyday', 'regular', 'starting', 'full-time',
        'every day', 'leadoff', 'cleanup'
    ]
    if any(w in text for w in everyday_words):
        return True, 'role'

    # Suppress — paternity/bereavement never actionable
    if any(w in text for w in NON_EVENT_LIST_KEYWORDS):
        return False, None

    # Suppress — depth moves
    low_value  = ['utility', 'bench', 'depth', 'non-roster',
                  'september', 'corresponding move']
    high_value = ['prospect', 'top', 'ranked', 'first call',
                  'role', 'opportunity', 'closer', 'save',
                  'replace', 'everyday', 'starting']
    low_count  = sum(1 for w in low_value  if w in text)
    high_count = sum(1 for w in high_value if w in text)
    if low_count > high_count and low_count >= 2:
        print(f"  {player_name} looks like depth move — skipping")
        return False, None

    # Logjam check
    if my_roster:
        fits, reason = check_roster_fit(player_name, text, my_roster)
        if not fits:
            print(f"  {player_name} fails roster fit — {reason}")
            return False, None

    # Gate 2a — category volatility
    if matchup_data:
        cat_relevant, cat_reason = check_category_relevance(
            text, matchup_data
        )
        if cat_relevant:
            return True, cat_reason

    return True, 'general'
                            
def check_roster_fit(player_name, text, my_roster):
    """
    Returns (fits, reason).
    Checks whether adding this player makes sense given current roster.
    Compares against weakest overall hitter, not just position match.
    """
    norm = normalize_name(player_name)
    t    = get_season_thresholds()

    # Determine player's likely position from text
    pos_signals = {
        'SS': ['shortstop', 'short stop'],
        'C':  ['catcher'],
        '1B': ['first base', 'first baseman'],
        '2B': ['second base', 'second baseman'],
        '3B': ['third base', 'third baseman'],
        'OF': ['outfielder', 'outfield', 'center field', 'left field',
               'right field', 'center fielder'],
        'SP': ['starter', 'starting pitcher', 'right-hander', 'left-hander',
               'righty', 'lefty'],
        'RP': ['reliever', 'closer', 'bullpen'],
    }

    player_pos = None
    for pos, signals in pos_signals.items():
        if any(s in text for s in signals):
            player_pos = pos
            break

    # SS-specific logjam check
    if player_pos == 'SS':
        my_ss_count = sum(
            1 for p in my_roster
            if p['position'] == 'SS'
            and 'IL' not in (p['status'] or '')
        )
        if my_ss_count >= 2:
            return False, f"already have {my_ss_count} SS"

    # Get weakest hitter on roster for comparison
    hitters = [
        p for p in my_roster
        if p['position'] not in ['SP', 'RP', 'P']
        and not p['is_undroppable']
        and 'IL' not in (p['status'] or '')
    ]

    if not hitters:
        return True, 'no hitters to compare'

    # Score each hitter using blended preseason/current value
    # pct_owned as preseason proxy, fetch current OPS if possible
    scored = []
    for h in hitters:
        preseason_score = h['pct_owned']
        # Blended score
        blended = (preseason_score * t['blend_prior'] +
                   preseason_score * t['blend_curr'])  # curr OPS fetched below
        scored.append((h['name'], blended, h['pct_owned']))

    scored.sort(key=lambda x: x[1])
    weakest_name, weakest_score, weakest_own = scored[0]

    # Proposed player must be plausibly better than weakest hitter
    # Use pct_owned as rough proxy for now
    # Top prospects always pass this check
    if norm in TOP_PROSPECTS:
        return True, f'top prospect beats {weakest_name} ({weakest_own:.0f}%)'

    # For others, require meaningful ownership advantage
    # or explicit role signal
    role_signals = ['everyday', 'starting', 'regular', 'cleanup', 'leadoff']
    if any(s in text for s in role_signals):
        return True, f'role player vs {weakest_name} ({weakest_own:.0f}%)'

    # If weakest hitter is low owned, new player could be upgrade
    if weakest_own < 40:
        return True, f'potential upgrade over {weakest_name} ({weakest_own:.0f}%)'

    return True, 'general fit'

# ============================================================
# CATEGORY VOLATILITY CHECK (Gate 2a)
# ============================================================

# Per-category closeability thresholds
# Format: (gap_actionable, gap_borderline, days_needed_borderline)
CATEGORY_THRESHOLDS = {
    'R':    {'count': True,  'easy': 4,    'hard': 9,    'days': 4},
    'H':    {'count': True,  'easy': 6,    'hard': 13,   'days': 4},
    'HR':   {'count': True,  'easy': 2,    'hard': 5,    'days': 4},
    'RBI':  {'count': True,  'easy': 4,    'hard': 9,    'days': 4},
    'SB':   {'count': True,  'easy': 1,    'hard': 3,    'days': 4},
    'AVG':  {'count': False, 'easy': 0.010,'hard': 0.021,'days': 3},
    'OPS':  {'count': False, 'easy': 0.020,'hard': 0.041,'days': 3},
    'W':    {'count': True,  'easy': 1,    'hard': 3,    'days': 4},
    'SV':   {'count': True,  'easy': 1,    'hard': 3,    'days': 4},
    'K':    {'count': True,  'easy': 8,    'hard': 16,   'days': 2},
    'ERA':  {'count': False, 'easy': 0.15, 'hard': 0.31, 'days': 2},
    'WHIP': {'count': False, 'easy': 0.08, 'hard': 0.16, 'days': 2},
    'K/BB': {'count': False, 'easy': 0.4,  'hard': 0.9,  'days': 2},
}

def check_category_relevance(text, matchup_data):
    """
    Returns (is_relevant, reason) based on current week category standings.
    Considers both sides — can I catch up AND can opponent overtake me?
    """
    if not matchup_data:
        return True, 'no matchup data'

    days_left = days_remaining_in_week()
    my_cats   = matchup_data.get('my_stats', {})
    opp_cats  = matchup_data.get('opp_stats', {})
    opp_remaining_starts = matchup_data.get('opp_remaining_starts', 0)

    # Determine which categories this player likely affects
    pitcher_cats = ['K', 'ERA', 'WHIP', 'K/BB', 'W', 'SV']
    hitter_cats  = ['R', 'H', 'HR', 'RBI', 'SB', 'AVG', 'OPS']

    is_pitcher = any(w in text for w in [
        'pitcher', 'starter', 'reliever', 'closer',
        'right-hander', 'left-hander', 'righty', 'lefty', 'hurler'
    ])
    relevant_cats = pitcher_cats if is_pitcher else hitter_cats

    for cat in relevant_cats:
        thresh = CATEGORY_THRESHOLDS.get(cat)
        if not thresh:
            continue

        my_val  = my_cats.get(cat)
        opp_val = opp_cats.get(cat)
        if my_val is None or opp_val is None:
            continue

        gap = abs(my_val - opp_val)
        i_am_losing = my_val < opp_val if thresh['count'] else my_val > opp_val

        # Am I behind and gap is closeable?
        if i_am_losing:
            if gap <= thresh['easy']:
                return True, f"can close {cat} gap ({gap:.2f} behind)"
            if gap <= thresh['hard'] and days_left >= thresh['days']:
                return True, f"borderline {cat} gap ({gap:.2f} behind, {days_left}d left)"

        # Am I ahead but opponent could overtake with their remaining pitching?
        if not i_am_losing and cat in ['ERA', 'WHIP', 'K/BB']:
            if opp_remaining_starts >= 2 and gap <= thresh['hard']:
                return True, f"opponent's {opp_remaining_starts} starts could flip {cat}"

        # Category is close enough that normal variance could flip it
        if gap <= thresh['easy']:
            return True, f"{cat} is close — category live"

    return False, 'no categories in play'

def get_matchup_data(my_roster, team_ops):
    try:
        cache_file = Path(MATCHUP_CACHE_FILE)
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                cached = json.load(f)
            age = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
            if age < 1800:
                print("  Using cached matchup data")
                return cached.get('data')

        query = get_yahoo_query()
        week  = get_current_week()

        my_stats  = {}
        opp_stats = {}
        opp_team_id = None

        # Get opponent team ID
        try:
            matchups = query.get_team_matchups(MY_TEAM_ID)
            if matchups:
                for m in (matchups if isinstance(matchups, list) else [matchups]):
                    try:
                        teams = getattr(m, 'teams', []) or []
                        for team in (teams if isinstance(teams, list) else [teams]):
                            tid = int(getattr(team, 'team_id', 0) or 0)
                            if tid != MY_TEAM_ID:
                                opp_team_id = tid
                                break
                    except Exception:
                        pass
                    if opp_team_id:
                        break
        except Exception as e:
            print(f"  Opponent ID error: {e}")

        def parse_team_stats(stats_obj):
            """Extract category stats from yfpy team stats response."""
            result = {}
            try:
                # yfpy returns team stats as object with team_stats attribute
                team_stats = getattr(stats_obj, 'team_stats', None)
                if team_stats is None:
                    team_stats = stats_obj
                stats = getattr(team_stats, 'stats', None)
                if stats is None:
                    return result
                stat_list = getattr(stats, 'stat', None)
                if stat_list is None:
                    stat_list = stats
                if not isinstance(stat_list, list):
                    stat_list = [stat_list]
                stat_id_map = {
                    '60': 'R',  '7': 'H',   '12': 'HR',
                    '13': 'RBI','16': 'SB',  '3':  'AVG',
                    '55': 'OPS','28': 'W',   '32': 'SV',
                    '27': 'K',  '26': 'ERA', '29': 'WHIP',
                    '72': 'K/BB'
                }
                for s in stat_list:
                    try:
                        sid = str(getattr(s, 'stat_id', '') or '')
                        val = getattr(s, 'value', None)
                        if val is not None and sid in stat_id_map:
                            try:
                                result[stat_id_map[sid]] = float(val)
                            except (ValueError, TypeError):
                                pass
                    except Exception:
                        pass
            except Exception as e:
                print(f"  Stats parse error: {e}")
            return result

        try:
            my_raw = query.get_team_stats_by_week(MY_TEAM_ID, week)
            my_stats = parse_team_stats(my_raw)
        except Exception as e:
            print(f"  My stats error: {e}")

        opp_remaining_starts = 0
        try:
            if opp_team_id:
                opp_raw = query.get_team_stats_by_week(opp_team_id, week)
                opp_stats = parse_team_stats(opp_raw)

                today       = datetime.now(ET_TZ).date()
                end_of_week = today + timedelta(days=(6 - today.weekday()))
                opp_roster  = query.get_team_roster_player_info_by_date(
                    opp_team_id, today
                )
                if opp_roster:
                    opp_pitchers = {
                        normalize_name(p.name.full)
                        for p in opp_roster
                        if hasattr(p, 'primary_position')
                        and p.primary_position in ['SP', 'P']
                    }
                    all_starters = get_probable_pitchers_with_matchups(
                        today, end_of_week, team_ops
                    )
                    opp_remaining_starts = sum(
                        1 for name in all_starters
                        if normalize_name(name) in opp_pitchers
                    )
        except Exception as e:
            print(f"  Opponent data error: {e}")

        data = {
            'my_stats':             my_stats,
            'opp_stats':            opp_stats,
            'opp_remaining_starts': opp_remaining_starts,
            'week':                 week,
        }

        with open(MATCHUP_CACHE_FILE, 'w') as f:
            json.dump({
                'ts':   datetime.now(timezone.utc).timestamp(),
                'data': data
            }, f)

        print(f"  Matchup data: {len(my_stats)} cats, "
              f"opp has {opp_remaining_starts} starts remaining")
        return data

    except Exception as e:
        print(f"  Matchup data error: {e}")
        return None

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

def load_transactions():
    try:
        if not Path(TRANSACTIONS_FILE).exists():
            return []
        with open(TRANSACTIONS_FILE, 'r') as f:
            data = json.load(f)
        cutoff = datetime.now(timezone.utc).timestamp() - (30 * 86400)
        return [t for t in data if t.get('timestamp', 0) > cutoff]
    except Exception:
        return []

def save_transactions(transactions):
    try:
        with open(TRANSACTIONS_FILE, 'w') as f:
            json.dump(transactions, f)
    except Exception as e:
        print(f"  Could not save transactions: {e}")

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
                                'player_id':         str(getattr(player, 'player_id', '') or ''),
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

CLOSERMONKEY_CACHE = '/tmp/closermonkey_cache.json'

def fetch_closermonkey():
    """
    Fetch active closers and depth charts from Closermonkey.
    Returns dict: {team_name: [closer, backup, ...]}
    Cached 4 hours.
    """
    try:
        cache_file = Path(CLOSERMONKEY_CACHE)
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                cached = json.load(f)
            age = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
            if age < 14400:
                return cached.get('data', {})
    except Exception:
        pass

    try:
        response = requests.get(
            'https://www.closermonkey.com',
            headers={"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"},
            timeout=15
        )
        text = response.text

        # Parse depth chart table
        # Closermonkey lists team then closer then backups
        depth_charts = {}
        current_team = None

        # Extract all table rows
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', text, re.DOTALL | re.IGNORECASE)
        for row in rows:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL | re.IGNORECASE)
            cells = [clean_text(c) for c in cells if clean_text(c).strip()]
            if not cells:
                continue

            # Check if this row contains a team name
            for abbr, full_name in TEAM_NAME_MAP.items():
                if any(abbr in c or full_name in c for c in cells):
                    current_team = full_name
                    break

            # Check if cells contain player names
            if current_team:
                for cell in cells:
                    cell = cell.strip()
                    if looks_like_player_name(cell):
                        norm = normalize_name(cell)
                        # Skip if it's a team name word
                        if any(t.lower() in norm for t in ['angels', 'astros', 'athletics',
                               'blue jays', 'braves', 'brewers', 'cardinals', 'cubs',
                               'diamondbacks', 'dodgers', 'giants', 'guardians', 'mariners',
                               'marlins', 'mets', 'nationals', 'orioles', 'padres',
                               'phillies', 'pirates', 'rangers', 'rays', 'red sox',
                               'reds', 'rockies', 'royals', 'tigers', 'twins',
                               'white sox', 'yankees']):
                            continue
                        if current_team not in depth_charts:
                            depth_charts[current_team] = []
                        if norm not in depth_charts[current_team]:
                            depth_charts[current_team].append(norm)

        # Build flat closer lookup: normalized_name -> team
        closer_lookup = {}
        for team, pitchers in depth_charts.items():
            if pitchers:
                closer_lookup[pitchers[0]] = team

        data = {
            'depth_charts':   depth_charts,
            'closer_lookup':  closer_lookup,
        }

        with open(CLOSERMONKEY_CACHE, 'w') as f:
            json.dump({'ts': datetime.now(timezone.utc).timestamp(), 'data': data}, f)

        print(f"  Closermonkey: {len(closer_lookup)} closers loaded")
        return data

    except Exception as e:
        print(f"  Closermonkey error: {e}")
        return {}

def get_closer_team(player_name):
    """Returns team name if player is a confirmed closer, else None."""
    data = fetch_closermonkey()
    norm = normalize_name(player_name)
    return data.get('closer_lookup', {}).get(norm)

def get_closer_backup(team_name):
    """Returns normalized name of #2 in bullpen depth chart, or None."""
    data  = fetch_closermonkey()
    chart = data.get('depth_charts', {}).get(team_name, [])
    return chart[1] if len(chart) >= 2 else None

def get_all_closers():
    """Returns set of normalized closer names."""
    data = fetch_closermonkey()
    return set(data.get('closer_lookup', {}).keys())

def track_league_transactions(taken):
    try:
        query        = get_yahoo_query()
        transactions = load_transactions()
        existing_ids = {t.get('id') for t in transactions}
        league_trans = query.get_league_transactions()
        if not league_trans:
            return
        now_ts    = datetime.now(timezone.utc).timestamp()
        new_count = 0
        for trans in league_trans:
            try:
                trans_id   = str(getattr(trans, 'transaction_id', '') or '')
                trans_type = str(getattr(trans, 'type', '') or '')
                timestamp  = float(getattr(trans, 'timestamp', now_ts) or now_ts)
                if trans_id in existing_ids:
                    continue
                transactions.append({
                    'id':        trans_id,
                    'type':      trans_type,
                    'timestamp': timestamp,
                    'logged_at': now_ts
                })
                new_count += 1
            except Exception:
                continue
        if new_count > 0:
            save_transactions(transactions)
            print(f"  Logged {new_count} new transactions")
    except Exception as e:
        print(f"  Transaction tracking error: {e}")

def send_weekly_leaguemate_intel():
    try:
        transactions = load_transactions()
        if not transactions:
            print("  No transaction data yet")
            return
        week_ago   = datetime.now(timezone.utc).timestamp() - (7 * 86400)
        recent     = [t for t in transactions if t.get('timestamp', 0) > week_ago]
        add_count  = len([t for t in recent if 'add'  in t.get('type', '').lower()])
        drop_count = len([t for t in recent if 'drop' in t.get('type', '').lower()])
        lines = [
            f"🕵️ WEEKLY LEAGUE INTEL\n",
            f"Last 7 days: {add_count} adds, {drop_count} drops\n",
            f"Full behavior profiles after 2+ weeks of data.\n",
            f"📱 Check Yahoo transactions for details."
        ]
        send_pushover("🕵️ LEAGUE INTEL", '\n'.join(lines), priority=0)
    except Exception as e:
        print(f"  Weekly intel error: {e}")

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

def is_team_game_active_or_done(team_name, games):
    for game in games:
        if team_name in (game['home_team'], game['away_team']):
            if game['status'] in [
                'In Progress', 'Final', 'Game Over',
                'Manager challenge', 'Delay', 'Rain Delay'
            ]:
                return True
    return False

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
    t = get_season_thresholds()

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

    curr  = fetch_stats(date.today().year)
    prior = fetch_stats(date.today().year - 1)

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
        'era':          round(prior['era']  * t['blend_prior'] + curr['era']  * t['blend_curr'], 2),
        'whip':         round(prior['whip'] * t['blend_prior'] + curr['whip'] * t['blend_curr'], 2),
        'kbb':          round(prior['kbb']  * t['blend_prior'] + curr['kbb']  * t['blend_curr'], 2),
        'k':            curr['k'],
        'ip':           curr['ip'],
        'gs':           curr.get('gs', 0),
        'ip_per_start': ip_per_start,
        'blend_note':   f"{int(t['blend_prior']*100)}% prior / {int(t['blend_curr']*100)}% current"
    }

def get_pitcher_recent_stats(player_id, num_starts=3):
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                f"?stats=gameLog&group=pitching&season=2026")
        data = requests.get(url, timeout=5).json()
        games = []
        for sg in data.get('stats', []):
            for split in sg.get('splits', []):
                s = split.get('stat', {})
                if int(s.get('gamesStarted', 0) or 0) == 0:
                    continue
                try:
                    ip = float(s.get('inningsPitched', '0') or '0')
                    games.append({
                        'date': split.get('date', ''),
                        'ip':   ip,
                        'era':  float(s.get('era', '99.99') or '99.99'),
                        'whip': float(s.get('whip', '9.99') or '9.99'),
                        'kbb':  float(s.get('strikeoutWalkRatio', '0') or '0'),
                    })
                except Exception:
                    pass
        recent = sorted(games, key=lambda x: x['date'], reverse=True)[:num_starts]
        if not recent:
            return None
        return {
            'recent_era':  round(sum(g['era']  for g in recent) / len(recent), 2),
            'recent_whip': round(sum(g['whip'] for g in recent) / len(recent), 2),
            'recent_kbb':  round(sum(g['kbb']  for g in recent) / len(recent), 2),
            'starts':      len(recent)
        }
    except Exception:
        return None

def get_mlb_player_id_from_name(player_name):
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/people/search"
                f"?names={requests.utils.quote(player_name)}&sportId=1")
        data = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        if people:
            return people[0].get('id')
    except Exception:
        pass
    return None

def get_my_pitcher_starts_remaining(my_roster, team_ops):
    try:
        today        = datetime.now(ET_TZ).date()
        end_of_week  = today + timedelta(days=(6 - today.weekday()))
        all_starters = get_probable_pitchers_with_matchups(today, end_of_week, team_ops)
        my_pitchers  = {
            normalize_name(p['name']) for p in my_roster
            if p['position'] in ['SP', 'RP', 'P']
            and 'IL' not in (p['status'] or '')
        }
        return sum(
            1 for name in all_starters.keys()
            if normalize_name(name) in my_pitchers
        )
    except Exception:
        return 0

def get_smart_drop_candidates(my_roster, team_ops, count=2):
    t = get_season_thresholds()
    rp_players   = [p for p in my_roster if p['position'] == 'RP']
    closer_names = {normalize_name(p['name']) for p in rp_players}
    only_one_rp  = len(closer_names) == 1

    today       = datetime.now(ET_TZ).date()
    end_of_week = today + timedelta(days=(6 - today.weekday()))
    try:
        week_starters = get_probable_pitchers_with_matchups(today, end_of_week, team_ops)
        start_info    = {}
        for name, info in week_starters.items():
            start_info[normalize_name(name)] = {
                'opp_ops':   max(info.get('opp_ops', [0.720])),
                'opponents': info.get('opponents', [])
            }
    except Exception:
        start_info = {}

    candidates = []
    for p in my_roster:
        if p['is_undroppable']:
            continue
        if 'IL' in (p['status'] or ''):
            continue
        if p['position'] not in ['SP', 'RP', 'P']:
            continue

        norm = p['name_normalized']

        if norm in closer_names and only_one_rp:
            print(f"  Protecting {p['name']} — only closer")
            continue

        has_start    = norm in start_info
        upcoming_ops = start_info[norm]['opp_ops'] if has_start else 0.0

        if has_start:
            mlb_id = get_mlb_player_id_from_name(p['name'])
            stats  = get_pitcher_stats_blended(mlb_id) if mlb_id else None
            era    = stats['era'] if stats else 99.99

            matchup_hard  = upcoming_ops > 0.730
            era_poor      = era > t['drop_era_floor']
            ownership_low = p['pct_owned'] < t['drop_own_ceil']

            if not (matchup_hard and era_poor and ownership_low):
                print(f"  Protecting {p['name']} — has start this week")
                continue
            else:
                print(f"  {p['name']} override: tough matchup+poor stats+low own")

        recent = None
        try:
            mlb_id = get_mlb_player_id_from_name(p['name'])
            if mlb_id:
                recent = get_pitcher_recent_stats(mlb_id)
        except Exception:
            pass

        drop_score = (100 - p['pct_owned']) * 0.3
        if recent:
            drop_score += min(recent['recent_era'], 10) * 5
            drop_score += min(recent['recent_whip'], 3) * 8
            drop_score += max(0, 3 - recent['recent_kbb']) * 4
        else:
            drop_score += (100 - p['pct_owned']) * 0.5

        if has_start and upcoming_ops > 0.730:
            drop_score += 15

        candidates.append({
            'name':         p['name'],
            'pct_owned':    p['pct_owned'],
            'drop_score':   drop_score,
            'recent':       recent,
            'has_start':    has_start,
            'upcoming_ops': upcoming_ops
        })

    candidates.sort(key=lambda x: x['drop_score'], reverse=True)
    return candidates[:count]

def format_drop_str(drops):
    if not drops:
        return "✅ Staff looks solid — no obvious drops"
    parts = []
    for d in drops:
        note = f"{d['name']} ({d['pct_owned']:.0f}%"
        if d.get('recent'):
            note += f", L3 ERA {d['recent']['recent_era']:.2f}"
        if d.get('has_start') and d.get('upcoming_ops', 0) > 0.730:
            note += ", tough matchup"
        note += ")"
        parts.append(note)
    return ' | '.join(parts)

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
    t = get_season_thresholds()
    if stats.get('ip', 0) < t['spotstart_ip']:
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
    t = get_season_thresholds()
    if is_opener(stats):
        return False
    if strict:
        return (
            stats.get('ip', 0)   >= t['twostart_ip']
            and stats.get('era',  99) < t['twostart_era']
            and stats.get('whip',  9) < t['twostart_whip']
            and stats.get('kbb',   0) > t['twostart_kbb']
        )
    else:
        return (
            stats.get('ip', 0)   >= t['streaming_ip']
            and stats.get('era',  99) < t['streaming_era']
            and stats.get('whip',  9) < t['streaming_whip']
            and stats.get('kbb',   0) > t['streaming_kbb']
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

def format_game_date(date_str):
    try:
        d = datetime.strptime(date_str, '%Y-%m-%d')
        return d.strftime('%a %-m/%-d')
    except Exception:
        return date_str[5:] if len(date_str) >= 7 else date_str

# ============================================================
# PLAYER NAME EXTRACTION — improved
# ============================================================
# Words that indicate a phrase fragment, not a player name
INVALID_NAME_WORDS = {
    'on', 'to', 'from', 'with', 'after', 'before', 'the', 'and', 'or',
    'in', 'at', 'by', 'for', 'of', 'injured', 'list', 'il', 'right',
    'left', 'elbow', 'knee', 'shoulder', 'wrist', 'hamstring', 'back',
    'thumb', 'ankle', 'concussion', 'surgery', 'fracture', 'sign',
    'place', 'trade', 'acquire', 'release', 'option', 'demote',
    'recall', 'promote', 'activate', 'reinstate', 'suspend',
    'designate', 'assign', 'claim', 'select', 'transfer', 'loose',
    'tight', 'sore', 'strained', 'sprained'
}

# Minor league team names that could be mistaken for player names
MINOR_LEAGUE_TEAMS = {
    'sugar land', 'salt lake', 'round rock', 'las vegas', 'el paso',
    'oklahoma city', 'iowa cubs', 'lehigh valley', 'durham bulls',
    'charlotte knights', 'columbus clippers', 'buffalo bisons',
    'scranton wilkes', 'pawtucket', 'norfolk tides', 'toledo mud',
    'louisville bats', 'gwinnett stripers', 'memphis redbirds',
    'nashville sounds', 'new orleans', 'reno aces', 'tacoma rainiers',
    'sacramento river', 'albuquerque isotopes'
}

def extract_player_name(item):
    source  = item.get('source', '')
    title   = item.get('title', '')
    summary = item.get('summary', '')

    # For trusted colon-format sources, split on colon
    if source in COLON_FORMAT_SOURCES and ':' in title:
        candidate = title.split(':')[0].strip()
        if looks_like_player_name(candidate):
            return candidate

    # For all sources, scan full text
    full_text  = clean_text(title + ' ' + summary)
    pattern    = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z\']+){1,3})\b'
    candidates = re.findall(pattern, full_text)

    for candidate in candidates:
        if not looks_like_player_name(candidate):
            continue
        # Reject if any word is an invalid/action/injury word
        words = candidate.lower().split()
        if any(w in INVALID_NAME_WORDS for w in words):
            continue
        # Reject minor league team names
        if candidate.lower() in MINOR_LEAGUE_TEAMS:
            continue
        # Reject if starts with action verb pattern
        # e.g. "Sign Dylan" — first word is a verb
        first_word = words[0]
        if first_word in {
            'sign', 'place', 'trade', 'acquire', 'release', 'option',
            'demote', 'recall', 'promote', 'activate', 'reinstate',
            'suspend', 'designate', 'assign', 'claim', 'select'
        }:
            continue
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

def build_concise_summary(title, summary, alert_type, player):
    """
    Build a true summary under 200 characters.
    Structured per alert type, not a raw RSS snippet.
    """
    # Clean HTML entities
    title   = clean_text(title)
    summary = clean_text(summary)

    if alert_type == '🚀 CALLUP':
        # Extract team and role from summary
        team_match = re.search(
            r'(Angels|Orioles|Yankees|Rays|Red Sox|Blue Jays|White Sox|'
            r'Guardians|Tigers|Royals|Twins|Astros|Athletics|Mariners|'
            r'Rangers|Braves|Marlins|Mets|Phillies|Nationals|Cubs|Reds|'
            r'Brewers|Pirates|Cardinals|Diamondbacks|Rockies|Dodgers|'
            r'Padres|Giants)', summary
        )
        team = team_match.group(1) if team_match else 'team'
        return f"{player} recalled by {team}. Check role/lineup spot."[:200]

    if alert_type == '💾 CLOSER ROLE':
        return f"{player} taking over closing role. Saves opportunity — add now."[:200]

    if alert_type == '✅ IL RETURN':
        return f"{player} activated from IL. Confirmed role — add if available."[:200]

    if alert_type == '🚑 INJURY OPP':
        # First sentence of summary only
        first_sent = summary.split('.')[0].strip()
        return f"{first_sent}. Role opportunity exists."[:200]

    if alert_type in ('💾 SAVES OPP', '💾 SAVES WATCH'):
        first_sent = summary.split('.')[0].strip()
        return first_sent[:200]

    if alert_type == '🔄 DFA→CALLUP OPP':
        return f"{player} DFA'd — prospect callup likely. Check wire."[:200]

    if alert_type == '🔁 TRADE OPP':
        first_sent = summary.split('.')[0].strip()
        return first_sent[:200]

    # Default — first sentence
    return summary.split('.')[0].strip()[:200]

# ============================================================
# ACTIONABILITY FILTER
# ============================================================
def get_actionability(item, taken, games=None,
                      my_roster=None, matchup_data=None):
    if item['type'] == 'reddit':
        return False, '', 0, None, {}

    # ALL alert types must pass transaction filter — no exceptions
    if not is_transaction_article(item):
        return False, '', 0, None, {}

    text   = clean_text(item['title'] + ' ' + item['summary']).lower()
    player = extract_player_name(item)

    if not player:
        return False, '', 0, None, {}

    # Fantasy relevance — includes availability check, logjam, category gate
    relevant, reason = is_fantasy_relevant(
        player, text, taken, my_roster, matchup_data
    )
    if not relevant:
        return False, '', 0, None, {}

    if not any(kw in text for kw in ACTION_KEYWORDS):
        return False, '', 0, None, {}

    extra = {'relevance_reason': reason}

    # Game-in-progress timing check
    if games:
        player_normalized = normalize_name(player)
        for game in games:
            for prob in [game.get('home_probable', ''),
                         game.get('away_probable', '')]:
                if normalize_name(prob) == player_normalized:
                    team = (game['home_team']
                            if normalize_name(game.get('home_probable', '')) == player_normalized
                            else game['away_team'])
                    if is_team_game_active_or_done(team, games):
                        extra['add_tomorrow'] = True
                    break

    # ── CALLUP ────────────────────────────────────────────────
    if any(w in text for w in ['called up', 'promoted', 'recalled', 'call-up']):
        return True, '🚀 CALLUP', 1, player, extra

    # ── CLOSER ROLE ───────────────────────────────────────────
    if any(w in text for w in CLOSER_KEYWORDS):
        return True, '💾 CLOSER ROLE', 1, player, extra

    # ── IL RETURN ─────────────────────────────────────────────
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
        full_text = clean_text(item['title'] + ' ' + item['summary'])

        # Check Closermonkey — most reliable closer confirmation
        # This fires regardless of whether the closer is owned
        all_closers      = get_all_closers()
        player_norm      = normalize_name(player)
        is_closer_injury = (
            player_norm in all_closers
            or any(w in text for w in CLOSER_KEYWORDS)
        )

        if is_closer_injury:
            # Find the team this closer plays for
            closer_team = get_closer_team(player_norm)

            # Get backup from Closermonkey depth chart
            backup_norm = get_closer_backup(closer_team) if closer_team else None

            # Check availability — backup is what we care about, not the closer
            available_replacements = []
            owned_replacements     = []

            if backup_norm:
                # Find display name from taken context
                backup_display = backup_norm.title()
                if backup_norm not in taken:
                    available_replacements = [(backup_display, True)]
                else:
                    owned_replacements = [(backup_display, False)]
            else:
                # Fall back to scanning article text
                replacements           = find_named_replacements(full_text, taken)
                available_replacements = [r for r in replacements if r[1]]
                owned_replacements     = [r for r in replacements if not r[1]]

            extra['available_replacements'] = available_replacements
            extra['owned_replacements']     = owned_replacements
            extra['injured_team']           = closer_team

            if available_replacements:
                return True, '💾 SAVES OPP', 1, player, extra
            else:
                extra['watch_mode'] = True
                return True, '💾 SAVES WATCH', 0, player, extra

        # Regular injury — only alert if role opportunity explicit
        opp_words = ['start', 'lineup', 'replac', 'fill', 'opportunit',
                     'role', 'regular', 'everyday', 'every day',
                     'platoon', 'takeover']
        if any(w in text for w in opp_words):
            return True, '🚑 INJURY OPP', 1, player, extra
        return False, '', 0, None, {}

    # ── DFA → CALLUP ──────────────────────────────────────────
    if any(w in text for w in ['designated for assignment', 'dfa', 'outrighted']):
        # Must be a known prospect — not just any DFA
        player_norm = normalize_name(player)
        if player_norm not in TOP_PROSPECTS:
            print(f"  Skipping DFA {player} — not a known prospect")
            return False, '', 0, None, {}
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

def build_alert_message(alert_type, player, item, extra):
    title        = item.get('title', '')
    summary      = item.get('summary', '')
    source       = item.get('source', '')
    add_tomorrow = extra.get('add_tomorrow', False)
    timing       = ("⏰ Add tomorrow — game already started."
                    if add_tomorrow else "✅ Act now!")
    reason       = extra.get('relevance_reason', '')
    reason_note  = f" [{reason}]" if reason and reason != 'general' else ''

    concise = build_concise_summary(title, summary, alert_type, player)

    if alert_type == '💾 SAVES OPP':
        available = extra.get('available_replacements', [])
        grab = ', '.join(r[0] for r in available[:2]) if available else 'check bullpen'
        return f"{concise}\n\n🎯 {grab} may inherit saves.\n{timing}"

    if alert_type == '💾 SAVES WATCH':
        owned = extra.get('owned_replacements', [])
        watch = ', '.join(r[0] for r in owned[:2]) if owned else 'monitor situation'
        return f"{concise}\n\n👀 {watch} — already owned. Watch for role news."

    return f"{concise}{reason_note}\n\n{timing}\nSource: {source}"

# ============================================================
# PRE-NEWS SIGNALS
# ============================================================
def check_milb_promotions(taken):
    try:
        headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
        feed    = feedparser.parse(
            "https://www.milb.com/feeds/news/rss.xml",
            request_headers=headers
        )
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        for entry in feed.entries:
            try:
                pub = (
                    datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    if hasattr(entry, 'published_parsed') and entry.published_parsed
                    else datetime.now(timezone.utc)
                )
                if pub < cutoff:
                    continue
                title = clean_text(entry.get('title', ''))
                text  = (title + ' ' + clean_text(entry.get('summary', ''))).lower()
                promo  = ['promoted to', 'called up', 'selected from', 'recalled']
                levels = ['triple-a', 'aaa', 'major', 'mlb']
                if not (any(p in text for p in promo) and
                        any(l in text for l in levels)):
                    continue
                pattern    = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b'
                candidates = re.findall(pattern, title)
                for candidate in candidates:
                    if not looks_like_player_name(candidate):
                        continue
                    norm = normalize_name(candidate)
                    if norm in taken:
                        continue
                    if norm in TOP_PROSPECTS:
                        send_pushover(
                            f"🔮 PROSPECT MOVE: {candidate}",
                            f"{title}\n\n"
                            f"⚡ Top prospect moving — MLB callup may be imminent.",
                            priority=1
                        )
                        break
            except Exception:
                continue
    except Exception as e:
        print(f"  MiLB check error: {e}")

# ============================================================
# SS INJURY DETECTION
# Now requires transaction article filter — no exceptions
# ============================================================
def is_ss_injury_news(item):
    # Must be a transaction article — no speculation pieces
    if not is_transaction_article(item):
        return False, None, False

    text = clean_text(item['title'] + ' ' + item['summary']).lower()
    if not any(kw in text for kw in SS_INJURY_KEYWORDS):
        return False, None, False

    for ss in TOP_15_SS:
        if normalize_name(ss) in normalize_name(
                clean_text(item['title'] + ' ' + item['summary'])):
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
                    if hasattr(entry, 'published_parsed') and entry.published_parsed
                    else datetime.now(timezone.utc)
                )
                if pub < cutoff:
                    continue
                title   = clean_text(entry.get('title', ''))
                summary = clean_text(
                    entry.get('summary', entry.get('description', title))
                )
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
# ALERT FUNCTIONS
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

    drops    = get_smart_drop_candidates(my_roster, team_ops, count=2)
    drop_str = format_drop_str(drops)

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
            f"{format_game_date(d)} vs {opp} {matchup_label(ops)}"
            for d, opp, ops in zip(dates[:3], opponents[:3], opp_ops[:3])
        )
        lines.append(f"• {name}\n  {stat_line} ({blend})\n  {matchups}")

    lines.append(f"\n💀 Consider dropping:\n{drop_str}")
    send_pushover("🎯 SPOT START OPTIONS", '\n'.join(lines), priority=0)

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
                f"No confirmed 2-starters yet for {next_mon}.",
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
                f"No available 2-starters cleared filters for {next_mon}.",
                priority=0
            )
        return

    ranked = sorted(
        quality_options.items(),
        key=lambda x: score_pitcher(x[1].get('stats', {})),
        reverse=True
    )[:3]

    drops    = get_smart_drop_candidates(my_roster, team_ops, count=3)
    drop_str = format_drop_str(drops)
    prefix   = "📋 EARLY LOOK — " if preliminary else ""
    lines    = [f"{prefix}📅 Week of {next_mon}:\n"]

    for name, info in ranked:
        s           = info['stats']
        dates       = info.get('dates', [])
        opponents   = info.get('opponents', [])
        opp_ops     = info.get('opp_ops', [])
        blend       = s.get('blend_note', '')
        stat_line   = (
            f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f} ({blend})"
            if s['ip'] >= 5 else "Limited stats"
        )
        start_lines = []
        for i, (d, opp, ops) in enumerate(
                zip(dates[:2], opponents[:2], opp_ops[:2])):
            start_lines.append(
                f"  Start {i+1}: {format_game_date(d)} vs {opp} {matchup_label(ops)}"
            )
        lines.append(f"• {name}\n  {stat_line}\n" + '\n'.join(start_lines))

    if not preliminary:
        lines.append(f"\n💀 Potential drops:\n{drop_str}")

    title = "⚾ 2-START EARLY LOOK" if preliminary else "⚾ 2-START SP TARGETS"
    send_pushover(title, '\n'.join(lines), priority=0)

def send_streaming_alert(taken, my_roster, games=None):
    print("Running streaming pitcher alert...")
    today        = datetime.now(ET_TZ).date()
    end_of_week  = today + timedelta(days=(6 - today.weekday()))
    team_ops     = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(today, end_of_week, team_ops)
    active_teams = get_games_in_progress(games) if games else set()

    my_starts_remaining = get_my_pitcher_starts_remaining(my_roster, team_ops)
    print(f"  My pitchers: {my_starts_remaining} starts remaining")

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

    drops       = get_smart_drop_candidates(my_roster, team_ops, count=2)
    drop_str    = format_drop_str(drops)
    staff_solid = my_starts_remaining >= 4

    lines = []
    if staff_solid:
        lines.append(
            f"ℹ️ Your staff has {my_starts_remaining} starts left "
            f"— stream only if upgrading.\n"
        )
    lines.append(f"📅 Streaming through {end_of_week}:\n")

    for name, info in ranked:
        s         = info['stats']
        starts    = info['count']
        opponents = info.get('opponents', [])
        opp_ops   = info.get('opp_ops', [])
        dates     = info.get('dates', [])
        opp_str   = ', '.join(
            f"{format_game_date(d)} vs {opp} {matchup_label(ops)}"
            for d, opp, ops in zip(dates[:2], opponents[:2], opp_ops[:2])
        )
        lines.append(
            f"• {name} ({starts} start{'s' if starts > 1 else ''})\n"
            f"  ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f}\n"
            f"  {opp_str}"
        )

    lines.append(f"\n💀 Consider dropping:\n{drop_str}")
    send_pushover("🌊 STREAMING SP OPTIONS", '\n'.join(lines), priority=0)

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
                        pct  = float(getattr(player.percent_owned, 'value', 0) or 0)
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

        team_ops = get_team_batting_stats()
        drops    = get_smart_drop_candidates(my_roster, team_ops, count=3)
        drop_str = format_drop_str(drops)

        weak_str = ', '.join(weak_positions)
        lines    = [f"📋 Your weak spots: {weak_str}\n"]
        for i, r in enumerate(recommendations, 1):
            lines.append(f"{i}. {r['name']} ({r['pos']}, {r['pct']:.0f}% owned)")
        lines.append(f"\n💀 Potential drops:\n{drop_str}")
        lines.append("\n📱 Check Yahoo for full stats before acting.")
        send_pushover("📋 WIRE DIGEST", '\n'.join(lines), priority=0)

    except Exception as e:
        print(f"  Wire digest error: {e}")

def check_waiver_drops(taken, my_roster, team_ops):
    """
    Scan recent league drops and alert if a dropped player
    is better than your worst player at that position.
    Accounts for 2-day waiver period.
    """
    print("Checking waiver drops...")
    try:
        query        = get_yahoo_query()
        transactions = load_transactions()
        now_ts       = datetime.now(timezone.utc).timestamp()
        two_days_ago = now_ts - (2 * 86400)

        # Get recent drops from transaction log
        recent_drops = [
            t for t in transactions
            if 'drop' in t.get('type', '').lower()
            and t.get('timestamp', 0) > two_days_ago
        ]

        if not recent_drops:
            print("  No recent drops found")
            return

        # Get my weakest hitter for comparison
        my_hitters = [
            p for p in my_roster
            if p['position'] not in ['SP', 'RP', 'P']
            and not p['is_undroppable']
            and 'IL' not in (p['status'] or '')
        ]
        if not my_hitters:
            return

        t         = get_season_thresholds()
        my_hitters.sort(key=lambda x: x['pct_owned'])
        weakest   = my_hitters[0]

        # Get my weakest pitcher for comparison
        my_pitchers = [
            p for p in my_roster
            if p['position'] in ['SP', 'RP', 'P']
            and not p['is_undroppable']
            and 'IL' not in (p['status'] or '')
        ]
        my_pitchers.sort(key=lambda x: x['pct_owned'])
        weakest_pitcher = my_pitchers[0] if my_pitchers else None

        alerts = []
        for drop in recent_drops:
            for player_info in drop.get('players', []):
                name     = player_info.get('name', '')
                norm     = normalize_name(name)

                # Skip if re-rostered already
                if norm in taken:
                    continue

                # Look up ownership via Yahoo
                try:
                    search_results = query.get_league_players(
                        player_count=5,
                        player_filter_type='A'  # Available players
                    )
                    matched = None
                    for p in (search_results or []):
                        if normalize_name(p.name.full) == norm:
                            matched = p
                            break

                    if not matched:
                        continue

                    pct_owned = float(
                        getattr(matched.percent_owned, 'value', 0) or 0
                    )
                    position  = str(getattr(matched, 'primary_position', '') or '')

                    # Calculate waiver available date
                    drop_ts       = drop.get('timestamp', now_ts)
                    available_dt  = datetime.fromtimestamp(
                        drop_ts + (2 * 86400), tz=ET_TZ
                    )
                    available_str = available_dt.strftime('%a %-m/%-d %-I:%M%p ET')

                    # Compare against weakest player at position
                    if position in ['SP', 'RP', 'P'] and weakest_pitcher:
                        if pct_owned > weakest_pitcher['pct_owned'] + 15:
                            alerts.append({
                                'name':      name,
                                'position':  position,
                                'pct_owned': pct_owned,
                                'available': available_str,
                                'vs':        weakest_pitcher['name'],
                                'vs_pct':    weakest_pitcher['pct_owned']
                            })
                    elif position not in ['SP', 'RP', 'P']:
                        if pct_owned > weakest['pct_owned'] + 15:
                            alerts.append({
                                'name':      name,
                                'position':  position,
                                'pct_owned': pct_owned,
                                'available': available_str,
                                'vs':        weakest['name'],
                                'vs_pct':    weakest['pct_owned']
                            })

                except Exception as e:
                    print(f"  Waiver drop lookup error for {name}: {e}")
                    continue

        if not alerts:
            print("  No waiver upgrades found")
            return

        lines = ["🔄 WAIVER WIRE DROPS:\n"]
        for a in alerts:
            lines.append(
                f"• {a['name']} ({a['position']}, {a['pct_owned']:.0f}% owned)\n"
                f"  Available: {a['available']}\n"
                f"  Better than: {a['vs']} ({a['vs_pct']:.0f}%)"
            )
        send_pushover("🔄 WAIVER DROPS", '\n'.join(lines), priority=0)

    except Exception as e:
        print(f"  Waiver drop check error: {e}")

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
                f"{sp['name']} replaced by {current_starter} for {team_name}.\n\n"
                f"⚠️ Swap in a bench SP or grab a streamer!",
                priority=1
            )

def check_lineups_and_weather(my_roster, games):
    print("Checking lineups and postponements...")
    sitting_alerted = load_sitting_alerts()
    my_hitters = [
        p for p in my_roster
        if p['position'] not in ['SP', 'RP', 'P']
        and 'IL' not in (p['status'] or '')
        and p['selected_position'] not in ['BN', 'IL']
    ]
    # Build bench players by position for swap check
    bench_players = [
        p for p in my_roster
        if p['selected_position'] == 'BN'
        and 'IL' not in (p['status'] or '')
        and p['position'] not in ['SP', 'RP', 'P']
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
                    # Check if any bench hitter is available to swap in
                    available_bench = [
                        b for b in bench_players
                        if normalize_name(b['name']) != player_key
                    ]
                    if not available_bench:
                        print(f"  {hitter['name']} postponed but no bench swap available — skipping")
                        newly_alerted[player_key] = 'postponed_no_bench'
                        continue
                    send_pushover(
                        f"🌧️ POSTPONED: {hitter['name']}",
                        f"{away_team} @ {home_team} postponed.\n"
                        f"{hitter['name']} won't play today.\n\n"
                        f"⚠️ Swap in: {available_bench[0]['name']}",
                        priority=1
                    )
                    newly_alerted[player_key] = 'postponed'
                continue

            if not game_starts_soon(game, hours=3):
                continue

            if lineup_posted and status not in ['Final', 'Game Over', 'In Progress']:
                if player_key in sitting_alerted:
                    continue
                in_lineup = any(
                    normalize_name(hitter['name']) in normalize_name(lp)
                    or normalize_name(lp) in normalize_name(hitter['name'])
                    for lp in all_lineup
                )
                if not in_lineup:
                    # Check bench availability
                    available_bench = [
                        b for b in bench_players
                        if normalize_name(b['name']) != player_key
                    ]
                    if not available_bench:
                        print(f"  {hitter['name']} sitting but no bench swap — skipping")
                        newly_alerted[player_key] = 'sitting_no_bench'
                        continue
                    send_pushover(
                        f"🪑 SITTING: {hitter['name']}",
                        f"{hitter['name']} not in lineup for {team_name}.\n\n"
                        f"⚠️ Swap in: {available_bench[0]['name']}",
                        priority=1
                    )
                    newly_alerted[player_key] = 'sitting'

    save_sitting_alerts(newly_alerted)

# ============================================================
# NEWS PROCESSOR
# ============================================================
def process_news_alerts(news, taken, is_digest=False,
                        games=None, my_roster=None, matchup_data=None):
    actionable      = []
    alerted_players = set()
    alerted_ss      = set()
    seen_alerts     = load_seen_alerts()

    for item in news:
        # SS injury — now requires transaction filter
        ss_hit, ss_name, is_mine = is_ss_injury_news(item)
        if ss_hit and normalize_name(ss_name) not in alerted_ss:
            if is_alert_seen(ss_name, 'SS_INJURY', seen_alerts):
                continue
            alerted_ss.add(normalize_name(ss_name))
            concise = build_concise_summary(
                item['title'], item['summary'], 'SS_INJURY', ss_name
            )
            if not is_digest:
                send_pushover(
                    f"{'🚨' if is_mine else '👀'} SS INJURY: {ss_name}"
                    f"{' ← YOURS!' if is_mine else ''}",
                    f"{concise}\n\nSource: {item['source']}",
                    priority=1 if is_mine else 0
                )
                mark_alert_seen(ss_name, 'SS_INJURY', seen_alerts)
            else:
                actionable.append({
                    'alert_type': f"{'🚨' if is_mine else '👀'} SS INJURY",
                    'priority':   1 if is_mine else 0,
                    'player':     ss_name,
                    'concise':    concise,
                    'item':       item,
                    'extra':      {}
                })
            continue

        is_actionable, alert_type, priority, player, extra = \
            get_actionability(item, taken, games, my_roster, matchup_data)
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
            'concise':    build_concise_summary(
                item['title'], item['summary'], alert_type, player
            ),
            'item':       item,
            'extra':      extra
        })

    if not actionable:
        save_seen_alerts(seen_alerts)
        return 0

    if is_digest:
        lines = [
            f"🌅 OVERNIGHT "
            f"({len(actionable)} item{'s' if len(actionable) > 1 else ''}):\n"
        ]
        for a in actionable:
            lines.append(f"{a['alert_type']}: {a['player']}\n{a['concise']}\n")
        max_priority = max(a['priority'] for a in actionable)
        send_pushover("🌅 OVERNIGHT DIGEST", '\n'.join(lines), priority=max_priority)
    else:
        for a in actionable:
            msg = build_alert_message(
                a['alert_type'], a['player'], a['item'], a['extra']
            )
            send_pushover(
                f"{a['alert_type']}: {a['player']} [{a['item']['source']}]",
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
          f"{now_et.strftime('%H:%M ET %A')} | Week {get_current_week()}")
    print(f"{'='*50}")

    t = get_season_thresholds()
    print(f"  Thresholds: ERA<{t['streaming_era']} "
          f"WHIP<{t['streaming_whip']} IP>{t['streaming_ip']}")

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

    weekly_intel_window = (
        weekday == 6 and hour_et == 21 and minute_et < 15
    )

    taken, my_roster, games, matchup_data = None, None, None, None

    if overnight_digest_window:
        print("\n--- OVERNIGHT DIGEST ---")
        overnight_news = get_all_news(lookback_minutes=450)
        taken, my_roster = get_all_rosters()
        if taken is None:
            print("  Yahoo failed — skipping")
        else:
            sent = process_news_alerts(
                overnight_news, taken, is_digest=True,
                my_roster=my_roster
            )
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

    if weekly_intel_window:
        print("\n--- WEEKLY LEAGUEMATE INTEL ---")
        send_weekly_leaguemate_intel()

    if not in_sleep and not overnight_digest_window:
        print("\n--- BREAKING NEWS CHECK ---")
        if games is None:
            games = get_todays_schedule()
        news = get_all_news(lookback_minutes=15)
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            # Fetch matchup data for category gate
            if matchup_data is None:
                try:
                    team_ops     = get_team_batting_stats()
                    matchup_data = get_matchup_data(my_roster, team_ops)
                except Exception as e:
                    print(f"  Matchup data skipped: {e}")

            try:
                track_league_transactions(taken)
            except Exception as e:
                print(f"  Transaction tracking skipped: {e}")

            try:
                if minute_et < 15:
                    check_waiver_drops(taken, my_roster, team_ops)
            except Exception as e:
                print(f"  Waiver check skipped: {e}")

            sent = process_news_alerts(
                news, taken, is_digest=False,
                games=games, my_roster=my_roster,
                matchup_data=matchup_data
            )
            print(f"  {sent} alert(s) sent")

            try:
                check_milb_promotions(taken)
            except Exception as e:
                print(f"  MiLB check skipped: {e}")

    elif in_sleep:
        print("\n[Sleep window — alerts resume at 6:30am ET]")

    print("\nDone.")

if __name__ == "__main__":
    main()
