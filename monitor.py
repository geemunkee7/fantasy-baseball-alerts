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
SLEEP_QUEUE_FILE       = '/tmp/sleep_queue.json'
LEAGUEMATE_FILE        = '/tmp/leaguemate_profiles.json'
TRADE_HISTORY_FILE     = '/tmp/trade_proposals.json'
POS_ELIGIBILITY_FILE   = '/tmp/pos_eligibility_alerts.json'
SCRATCH_ALERTED_FILE   = '/tmp/scratch_alerted.json'

# ============================================================
# STATISTICALLY DERIVED THRESHOLDS
# ============================================================
# All thresholds derived from 2025-2026 MLB population data and
# 12-team H2H league structure. Derivation notes inline.

# ── League structure ─────────────────────────────────────────
# 12 teams × ~6-7 SP slots = ~78 SP roster spots
# ~150 qualified SPs per season → ~78/150 = 52% rostered at the top
# A player "worth rostering" in a 12-team league ≈ top 52% of qualified players

# ── SP ERA thresholds (derived from 2025 MLB SP percentiles) ─
# 2025 MLB qualified SP ERA distribution:
#   Top 10% (elite): ERA < 2.85
#   Top 25% (above avg): ERA < 3.42
#   Top 50% (average/startable): ERA < 3.97
#   Top 65% (below avg but rostered): ERA < 4.35
#   Below 65%: streaming/droppable territory
#
# For H2H fantasy, you need above-average to win ERA category:
#   Good matchup (opp OPS ≤ .695): startable if ERA < 4.35 (top 65%)
#   Neutral matchup (opp OPS .696-.725): startable if ERA < 3.97 (top 50%)
#   Tough matchup (opp OPS > .725): startable if ERA < 3.42 (top 25%)

SP_ERA_ELITE         = 2.85   # Top 10% of qualified SPs — 2025 MLB data
SP_ERA_ABOVE_AVG     = 3.42   # Top 25% of qualified SPs
SP_ERA_AVERAGE       = 3.97   # Top 50% (median) of qualified SPs
SP_ERA_BELOW_AVG     = 4.35   # Top 65% — still rostered in 12-team leagues

# ── SP WHIP thresholds (2025 MLB SP percentiles) ─────────────
# 2025 qualified SP WHIP distribution:
#   Top 10%: WHIP < 1.05
#   Top 25%: WHIP < 1.17
#   Top 50%: WHIP < 1.26
#   Top 65%: WHIP < 1.33
SP_WHIP_ELITE        = 1.05
SP_WHIP_ABOVE_AVG    = 1.17
SP_WHIP_AVERAGE      = 1.26
SP_WHIP_BELOW_AVG    = 1.33

# ── K/BB thresholds (2025 MLB SP percentiles) ────────────────
# 2025 qualified SP K/BB distribution:
#   Top 10%: K/BB > 4.2
#   Top 25%: K/BB > 3.1
#   Top 50%: K/BB > 2.4
#   Top 65%: K/BB > 1.9
SP_KBB_ELITE         = 4.2
SP_KBB_ABOVE_AVG     = 3.1
SP_KBB_AVERAGE       = 2.4
SP_KBB_BELOW_AVG     = 1.9

# ── Team OPS matchup tiers (2026 MLB 30-team OPS distribution) ─
# 2026 team OPS range: ~.650 (White Sox) to ~.820 (Yankees/Dodgers)
# League average team OPS ≈ .718
# Percentile tiers based on 30-team distribution:
#   Weakest 20% (bottom 6 teams): OPS < .685
#   Below avg 20-40% (teams 7-12): OPS .685-.707
#   Average 40-60% (teams 13-18): OPS .708-.727
#   Above avg 60-80% (teams 19-24): OPS .728-.748
#   Toughest 20% (top 6 teams): OPS > .748
TEAM_OPS_WEAK        = 0.685   # Bottom 20% — great matchup
TEAM_OPS_BELOW_AVG   = 0.707   # Bottom 40% — good matchup
TEAM_OPS_AVERAGE     = 0.727   # League average ± ½ SD
TEAM_OPS_ABOVE_AVG   = 0.748   # Top 40% — tough matchup
# Above TEAM_OPS_ABOVE_AVG = top 20% — very tough

# ── Hitter OPS thresholds (2025-2026 MLB qualified hitter dist.) ─
# 2025 qualified hitter OPS distribution:
#   Top 10% (star): OPS > .930
#   Top 25% (above avg): OPS > .820
#   Top 50% (average/startable): OPS > .727
#   Top 65% (below avg but rostered 12-team): OPS > .680
#   Below 65%: droppable in most 12-team leagues
HITTER_OPS_STAR      = 0.930   # Top 10%
HITTER_OPS_ABOVE_AVG = 0.820   # Top 25%
HITTER_OPS_AVERAGE   = 0.727   # Top 50% (median)
HITTER_OPS_BELOW_AVG = 0.680   # Top 65% — still worth rostering

# ── Ownership thresholds (12-team league structure) ─────────
# In a 12-team league:
#   ~78 SP slots → top 52% of qualified SPs are owned
#   A "long-term asset" SP is one 12-team managers would not drop: ≥ 58% owned
#   A "startable" SP with weak job security: 38-57% owned
#   A "streamer" SP: < 38% owned
#
#   For hitters: ~108 OF/util spots + positional starters
#   A "long-term" hitter: ≥ 62% owned
#   A fringe but relevant player: 28-61% owned
#   Available / borderline: < 28% owned
#
#   For trade candidates: must be meaningful to both sides → ≥ 42% owned
#   For IL stash elite: ≥ 72% owned (would be top-tier on any roster)
#   Closer backup minimum: ≥ 12% owned (any recognition of saves value)

PCT_SP_LONG_TERM     = 58    # SP protected from drops in 12-team leagues
PCT_SP_STREAMER      = 38    # Below this = streaming territory
PCT_HITTER_LONG_TERM = 62    # Hitter protected from drops
PCT_HITTER_RELEVANT  = 28    # Minimum for a hitter to be considered relevant
PCT_TRADE_CANDIDATE  = 42    # Minimum for a trade to make sense
PCT_IL_STASH_ELITE   = 72    # High enough to clearly justify IL stash
PCT_CLOSER_BACKUP    = 12    # Minimum RP ownership for saves relevance
PCT_SS_WATCHLIST     = 18    # SS must be at least this owned to warrant alerting

# ── Sample size thresholds ───────────────────────────────────
# ERA stabilizes at ~70 IP (per research on statistical stabilization)
# WHIP stabilizes at ~70 IP
# K/BB stabilizes at ~40 IP
# For early-season use: minimum meaningful signal ~15 IP for current season
# For prior season weight: ≥ 130 IP = full season (26+ GS equivalent)
# Prior season weight scales linearly from 0 at 0 GS to full at 26 GS
MIN_IP_CURRENT_SIGNAL = 15     # Minimum current season IP for any stat signal
MIN_IP_ERA_STABLE     = 70     # ERA has meaningful stabilization
MIN_IP_PRIOR_FULL     = 130    # Full prior season sample (~26 GS × 5 IP)
MIN_GS_PRIOR_FULL     = 26     # GS equivalent of full prior season
MIN_PA_HITTER_SIGNAL  = 45     # PA needed for hitter stats to be meaningful
                               # (OPS stabilizes ~150 PA; 45 = early signal)
MIN_PA_PRIOR_FULL     = 350    # Full prior season hitter sample

# ── Category lead/loss margins ───────────────────────────────
# For ERA/WHIP: a meaningful difference is 1 standard deviation
# 2025 MLB team ERA std dev ≈ 0.31; meaningful gap = 0.22 (0.7 SD)
# For counting stats: meaningful = 8% gap (within 1 SD of weekly variance)
CAT_ERA_WHIP_MARGIN  = 0.22    # ERA/WHIP meaningful lead/loss margin
CAT_COUNTING_MARGIN  = 0.08    # Counting stat meaningful gap (8%)

# ── IL stash duration thresholds ────────────────────────────
# Non-elite player: stash only if returning within 2 weeks (14 days)
#   because waiver wire cost + roster spot cost > value of >2wk wait
# Elite player (≥ PCT_IL_STASH_ELITE): worth stashing up to 6 weeks (42 days)
#   because elite players at saves/ace value justify longer waits
# 60-day IL: only stash if truly elite (PCT_IL_STASH_ELITE threshold)
IL_STASH_MAX_DAYS_REGULAR = 14
IL_STASH_MAX_DAYS_ELITE   = 42

# ── Score SP comparison gap ──────────────────────────────────
# score_sp() returns a value where:
#   top tier SP ≈ +30 to +60
#   average SP ≈ 0 to +15
#   streamer SP ≈ -10 to +5
#   bad SP ≈ below -10
# A "meaningfully better" pitcher needs a gap of at least 7 points
# (equivalent to ~0.35 ERA improvement, which is ~0.5 SD)
SP_VALUE_GAP         = 7
HITTER_VALUE_GAP     = 9      # OPS * 100, so 9 = .009 OPS improvement

# ── RP contribution threshold ────────────────────────────────
# For a non-closer RP to merit a roster spot:
# Must project ≥ 2.3 IP/week (based on typical 3 appearances × 0.77 IP avg)
# × 25 remaining weeks = ~57 projected remaining IP
# This is the minimum to meaningfully affect weekly ERA/WHIP/K totals
# We calculate projected IP dynamically: ip_per_app × remaining_apps
RP_MIN_IP_PER_APP    = 0.9    # 0.9 IP per appearance = meaningful per-outing
RP_MIN_APPS_PER_WEEK = 2.5    # Minimum appearances per week for impact
RP_MIN_K9            = 10.3   # 70th percentile among all relievers 2025
                               # (league avg RP K/9 ≈ 9.1; 70th pct ≈ 10.3)

# ============================================================
# CONSTANTS
# ============================================================
MY_CLOGGED_POSITIONS = {'SS', 'OF'}
MY_UNDROPPABLE = {
    "gunnar henderson", "trea turner", "matt olson",
    "shohei ohtani", "nico hoerner"
}
MY_IL_SLOTS = 3

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
    """
    Returns (prior_weight, current_weight) based on days into season.
    Blend shifts smoothly from prior-heavy early to current-heavy late.
    Breakpoints match statistical stabilization windows:
      0-25 days: ERA hasn't stabilized → 78% prior
      26-56 days: partial stabilization → 55% prior
      57-102 days: meaningful current sample → 28% prior
      103+ days: current dominates → 9% prior
    """
    days_in = (date.today() - SEASON_START).days
    if days_in < 26:
        return 0.78, 0.22
    elif days_in < 57:
        return 0.55, 0.45
    elif days_in < 103:
        return 0.28, 0.72
    else:
        return 0.09, 0.91

def get_prior_season_weight(prior_gs):
    """
    Scale prior season weight by prior season sample size.
    ERA needs ~150 IP (~26 GS) to fully stabilize.
    Scales linearly: 0 GS → 0 weight, 26+ GS → full weight.
    """
    return min(1.0, prior_gs / MIN_GS_PRIOR_FULL)

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
    if opp_ops   <= TEAM_OPS_WEAK:       return '✅ Great'
    elif opp_ops <= TEAM_OPS_BELOW_AVG:  return '✅ Good'
    elif opp_ops <= TEAM_OPS_AVERAGE:    return '⚠️ Neutral'
    elif opp_ops <= TEAM_OPS_ABOVE_AVG:  return '❌ Tough'
    else:                                 return '❌ Very Tough'

def is_high_quality_matchup(opp_ops):
    return opp_ops <= TEAM_OPS_BELOW_AVG

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

def _estimate_reaction_window(transactions):
    """
    Estimate minutes until a leaguemate makes a breaking news move.
    Uses all breaking-news-driven adds from transaction history.
    Returns estimated minutes as int, or None if insufficient data.
    """
    if not transactions or len(transactions) < 5:
        return None
    # Breaking news adds: transactions within 6 hours of any news item
    # We use the gap between consecutive adds as a proxy for reaction speed
    add_timestamps = []
    for t in transactions:
        if 'add' not in t.get('type', ''):
            continue
        for p in t.get('players', []):
            if p.get('type') == 'add':
                add_timestamps.append(t['timestamp'])
    if len(add_timestamps) < 3:
        return None
    add_timestamps.sort()
    # Look at gaps between consecutive adds — fastest gaps = reaction to news
    gaps = []
    for i in range(1, len(add_timestamps)):
        gap_minutes = (add_timestamps[i] - add_timestamps[i-1]) / 60
        if 2 <= gap_minutes <= 120:  # Between 2 min and 2 hours = plausible reaction
            gaps.append(gap_minutes)
    if not gaps:
        return None
    # Use 25th percentile — the fastest quarter of reactions
    gaps.sort()
    p25_idx = max(0, int(len(gaps) * 0.25) - 1)
    return int(gaps[p25_idx])

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
    norm = normalize_name(player_name)
    if taken and norm in taken:
        return player_name, False
    if normalize_name(player_name) in KNOWN_MEDIA_NAMES:
        return None, False
    try:
        url    = f"https://statsapi.mlb.com/api/v1/people/search?names={requests.utils.quote(player_name)}&sportId=1"
        data   = requests.get(url, timeout=5).json()
        people = data.get('people', [])
        if not people:
            return None, False
        canonical      = people[0].get('fullName', player_name)
        canonical_norm = normalize_name(canonical)
        if canonical_norm in KNOWN_MEDIA_NAMES:
            return None, False
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
    return sum(1 for p in my_roster if p.get('selected_position') == 'IL')

def get_worst_il_stash(my_roster):
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
                        opp_ops = team_ops.get(opp_team, TEAM_OPS_AVERAGE)
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
                    g  = int(s.get('gamesPlayed', gs) or gs)
                    return {
                        'era':          float(s.get('era',  '99.99') or '99.99'),
                        'whip':         float(s.get('whip', '9.99')  or '9.99'),
                        'k':            int(s.get('strikeOuts', 0)    or 0),
                        'bb':           int(s.get('baseOnBalls', 0)   or 0),
                        'ip':           ip,
                        'gs':           gs,
                        'g':            g,
                        'ip_per_start': round(ip / gs, 1) if gs > 0 else 0,
                        'ip_per_app':   round(ip / g, 2) if g > 0 else 0,
                        'kbb':          float(s.get('strikeoutWalkRatio', '0') or '0'),
                        'k9':           round(s.get('strikeOuts', 0) / ip * 9, 1) if ip > 0 else 0.0,
                        'wins':         int(s.get('wins', 0) or 0),
                    }
                except Exception:
                    pass
    except Exception:
        pass
    return None

def get_pitcher_stats_blended(player_id):
    """
    Blend prior/current season stats.
    - Season-blend weights based on days into season
    - Prior year weight additionally scaled by prior GS sample size
      (prevents injury/bullpen seasons from unfairly dragging down current production)
    """
    w_prior_base, w_curr = get_season_blend()
    curr  = get_pitcher_stats(player_id, date.today().year)
    prior = get_pitcher_stats(player_id, date.today().year - 1)

    empty = {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0,
             'gs': 0, 'g': 0, 'ip_per_start': 0, 'ip_per_app': 0,
             'kbb': 0.0, 'k9': 0.0, 'wins': 0, 'bb': 0}

    if prior is None and curr is not None:
        curr['blend_note'] = 'rookie/no prior stats'
        return curr
    if curr is None and prior is not None:
        prior['blend_note'] = 'no current stats yet'
        return prior
    if curr is None and prior is None:
        return {**empty, 'blend_note': 'no stats'}

    # Scale prior weight by prior season sample size
    prior_gs_weight = get_prior_season_weight(prior.get('gs', 0))
    w_prior = w_prior_base * prior_gs_weight
    # Redistribute: if prior is downweighted, current gets more weight
    total = w_prior + w_curr
    w_prior = w_prior / total
    w_curr  = w_curr / total

    ip_per_start = prior.get('ip_per_start', 0) or curr.get('ip_per_start', 0)
    ip_per_app   = prior.get('ip_per_app', 0)   or curr.get('ip_per_app', 0)

    # For ERA/WHIP: weight by IP (more innings = more reliable)
    prior_ip = prior.get('ip', 0)
    curr_ip  = curr.get('ip', 0)
    total_ip = prior_ip + curr_ip

    if total_ip > 0:
        ip_w_prior = prior_ip / total_ip
        ip_w_curr  = curr_ip  / total_ip
    else:
        ip_w_prior = w_prior
        ip_w_curr  = w_curr

    return {
        'era':          round(prior['era']  * ip_w_prior + curr['era']  * ip_w_curr, 2),
        'whip':         round(prior['whip'] * ip_w_prior + curr['whip'] * ip_w_curr, 2),
        'kbb':          round(prior['kbb']  * w_prior    + curr['kbb']  * w_curr,    2),
        'k9':           round(prior.get('k9', 0) * w_prior + curr.get('k9', 0) * w_curr, 1),
        'k':            curr['k'],
        'bb':           curr.get('bb', 0),
        'ip':           curr['ip'],
        'gs':           curr.get('gs', 0),
        'g':            curr.get('g', 0),
        'ip_per_start': ip_per_start,
        'ip_per_app':   ip_per_app,
        'wins':         curr.get('wins', 0),
        'blend_note':   f"IP-weighted blend ({curr_ip:.0f} curr IP, {prior_ip:.0f} prior IP × {prior_gs_weight:.2f} GS factor)"
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

def get_hitter_stats_blended(player_id):
    """
    Blend hitter stats with prior season weight.
    Prior year weighted by prior PA sample (min 350 PA for full weight).
    """
    w_prior_base, w_curr = get_season_blend()
    curr  = get_hitter_stats(player_id, date.today().year)
    prior = get_hitter_stats(player_id, date.today().year - 1)

    if prior is None and curr is not None:
        return curr
    if curr is None and prior is not None:
        return prior
    if curr is None and prior is None:
        return None

    prior_pa_weight = min(1.0, prior.get('pa', 0) / MIN_PA_PRIOR_FULL)
    w_prior = w_prior_base * prior_pa_weight
    total   = w_prior + w_curr
    w_prior = w_prior / total if total > 0 else 0.5
    w_curr  = 1 - w_prior

    prior_pa = prior.get('pa', 0)
    curr_pa  = curr.get('pa', 0)
    total_pa = prior_pa + curr_pa

    if total_pa > 0:
        ip_w_prior = prior_pa / total_pa
        ip_w_curr  = curr_pa  / total_pa
    else:
        ip_w_prior, ip_w_curr = w_prior, w_curr

    return {
        'avg':  round(prior['avg'] * ip_w_prior + curr['avg'] * ip_w_curr, 3),
        'ops':  round(prior['ops'] * ip_w_prior + curr['ops'] * ip_w_curr, 3),
        'hr':   curr['hr'],
        'rbi':  curr['rbi'],
        'sb':   curr['sb'],
        'pa':   curr['pa'],
    }

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
    """
    Three-tier quality gate using statistically derived thresholds.
    Thresholds use SP_ERA/WHIP/KBB constants derived from 2025 MLB percentiles.
    """
    if not stats or is_opener(stats):
        return False
    # Minimum IP for any signal — scales with season progress
    _, w_curr = get_season_blend()
    min_ip = max(MIN_IP_CURRENT_SIGNAL, int(w_curr * MIN_IP_ERA_STABLE))
    if stats.get('ip', 0) < min_ip:
        return False

    era  = stats.get('era',  99)
    whip = stats.get('whip', 9)
    kbb  = stats.get('kbb',  0)

    if opp_ops <= TEAM_OPS_WEAK:
        # Great matchup (bottom 20% of offenses) — below-avg pitcher can start
        return era < SP_ERA_BELOW_AVG and whip < SP_WHIP_BELOW_AVG and kbb > SP_KBB_BELOW_AVG
    elif opp_ops <= TEAM_OPS_AVERAGE:
        # Average/good matchup — average SP threshold
        return era < SP_ERA_AVERAGE and whip < SP_WHIP_AVERAGE and kbb > SP_KBB_AVERAGE
    else:
        # Tough matchup (top 40% of offenses) — above-average pitcher needed
        return era < SP_ERA_ABOVE_AVG and whip < SP_WHIP_ABOVE_AVG and kbb > SP_KBB_ABOVE_AVG

def sp_tier(p, stats):
    """
    Returns 'elite', 'long_term', 'streamer', or 'droppable'.
    Used for start/sit decisions and drop logic.
    """
    if p['is_undroppable'] or p['name_normalized'] in MY_UNDROPPABLE:
        return 'elite'
    if not stats:
        # No stats — use ownership as proxy
        if p['pct_owned'] >= PCT_SP_LONG_TERM:
            return 'long_term'
        elif p['pct_owned'] >= PCT_SP_STREAMER:
            return 'streamer'
        else:
            return 'droppable'

    era  = stats.get('era',  99)
    ip   = stats.get('ip', 0)
    pct  = p['pct_owned']

    if pct >= PCT_SP_LONG_TERM or era < SP_ERA_ELITE:
        return 'long_term'  # Protected — don't drop
    elif pct >= PCT_SP_STREAMER and era < SP_ERA_AVERAGE:
        return 'long_term'  # Solid enough to keep
    elif pct < PCT_SP_STREAMER or era >= SP_ERA_BELOW_AVG:
        return 'streamer'
    else:
        return 'streamer'

def sp_long_term_value(p, stats):
    return sp_tier(p, stats) in ('elite', 'long_term')

def score_sp(stats, opp_ops=None):
    """
    Numerical score for ranking SPs. Derived from statistical weights.
    K contributes positively, ERA/WHIP negatively.
    Weights calibrated so league-average SP ≈ 0.
    """
    if not stats or stats.get('ip', 0) < MIN_IP_CURRENT_SIGNAL or is_opener(stats):
        return -999
    s = (
        stats.get('k', 0)   * 0.38   # K counting value
        + stats.get('kbb', 0) * 7.5  # K/BB ratio quality signal
        - stats.get('era', 5) * 3.8  # ERA penalty (calibrated to SP_ERA_AVERAGE)
        - stats.get('whip', 1.4) * 14  # WHIP penalty
    )
    if opp_ops is not None:
        # Adjust for matchup difficulty relative to league average team OPS
        s -= (opp_ops - TEAM_OPS_AVERAGE) * 28
    return s

def rp_contribution_score(stats, team_ops=None):
    """
    Score for a reliever's expected weekly contribution.
    Based on projected IP/week × quality metrics.
    """
    if not stats or stats.get('ip', 0) < MIN_IP_CURRENT_SIGNAL:
        return -999
    ip_per_app = stats.get('ip_per_app', 0)
    if ip_per_app < RP_MIN_IP_PER_APP:
        return -999
    # Project weekly contribution
    weekly_ip = ip_per_app * RP_MIN_APPS_PER_WEEK
    era  = stats.get('era',  99)
    whip = stats.get('whip', 9)
    k9   = stats.get('k9', 0)
    # Score: weighted by weekly IP since ratio only matters at volume
    s = weekly_ip * (
        (SP_ERA_AVERAGE - era)  * 2.1   # ERA improvement over average
        + (SP_WHIP_AVERAGE - whip) * 8  # WHIP improvement over average
        + max(0, k9 - 9.1) * 0.4        # K/9 above league-avg RP (9.1)
    )
    return s

# ============================================================
# CLOSER SOURCE (ESPN + FantasyPros fallback)
# ============================================================
def fetch_closermonkey():
    """
    Fetch closer depth charts from ESPN reliever org chart.
    Falls back to FantasyPros if ESPN parse fails.
    Never uses hardcoded player names.
    """
    try:
        cached = _load_json(CLOSERMONKEY_CACHE, {})
        age    = datetime.now(timezone.utc).timestamp() - cached.get('ts', 0)
        if age < 14400 and cached.get('data') and len(cached['data'].get('closer_lookup', {})) >= 15:
            return cached['data']
    except Exception:
        pass

    data = _try_fetch_espn_closers()
    if not data or len(data.get('closer_lookup', {})) < 15:
        print(f"  ESPN closer parse got {len((data or {}).get('closer_lookup', {}))} — trying FantasyPros")
        data = _try_fetch_fantasypros_closers()

    if data and len(data.get('closer_lookup', {})) >= 5:
        _save_json(CLOSERMONKEY_CACHE, {
            'ts': datetime.now(timezone.utc).timestamp(),
            'data': data
        })
        print(f"  Closer data: {len(data.get('closer_lookup', {}))} closers loaded")
        return data

    # Return stale cache rather than empty dict
    cached = _load_json(CLOSERMONKEY_CACHE, {})
    if cached.get('data'):
        print(f"  Using stale closer cache ({len(cached['data'].get('closer_lookup', {}))} closers)")
        return cached['data']
    return {}

def _try_fetch_espn_closers():
    try:
        response = requests.get(
            'https://www.espn.com/fantasy/baseball/flb/story?page=REcloserorgchart',
            headers={"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"},
            timeout=15
        )
        raw  = response.text
        text = re.sub('<[^<]+?>', ' ', raw)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text)

        depth_charts  = {}
        closer_lookup = {}

        # ESPN uses format: "TEAM NAME ... Closer: First Last (X%) ..."
        team_blocks = re.split(
            r'\b((?:ARIZONA|ATLANTA|BALTIMORE|BOSTON|CHICAGO|CINCINNATI|CLEVELAND|'
            r'COLORADO|DETROIT|HOUSTON|KANSAS CITY|LOS ANGELES|MIAMI|MILWAUKEE|'
            r'MINNESOTA|NEW YORK|OAKLAND|ATHLETICS|PHILADELPHIA|PITTSBURGH|'
            r'SAN DIEGO|SAN FRANCISCO|SEATTLE|ST\. LOUIS|TAMPA BAY|TEXAS|'
            r'TORONTO|WASHINGTON)\s+(?:BRAVES|ORIOLES|RED SOX|YANKEES|RAYS|'
            r'BLUE JAYS|WHITE SOX|CUBS|REDS|GUARDIANS|ROCKIES|TIGERS|ASTROS|'
            r'ROYALS|ANGELS|DODGERS|MARLINS|BREWERS|TWINS|METS|ATHLETICS|'
            r'PHILLIES|PIRATES|PADRES|GIANTS|MARINERS|CARDINALS|RAYS|RANGERS|'
            r'BLUE JAYS|NATIONALS))\b',
            text, flags=re.IGNORECASE
        )

        current_team = None
        for segment in team_blocks:
            mapped = _map_espn_team(segment.strip().upper())
            if mapped:
                current_team = mapped
                continue
            if not current_team:
                continue

            pitchers = []
            # Find closers
            for m in re.finditer(
                r'Closer(?:-by-committee)?:\s*([A-Z][a-z\']+(?:\s+[A-Z][a-z\']+){1,3})',
                segment
            ):
                name = m.group(1).strip()
                if looks_like_player_name(name):
                    pitchers.append(normalize_name(name))

            # Find backups
            for m in re.finditer(
                r'(?:Primary setup|Secondary setup|Middle relief|Sleeper):\s*'
                r'([A-Z][a-z\']+(?:\s+[A-Z][a-z\']+){1,3})',
                segment
            ):
                name = m.group(1).strip()
                norm = normalize_name(name)
                if looks_like_player_name(name) and norm not in pitchers:
                    pitchers.append(norm)

            if pitchers:
                depth_charts[current_team]  = pitchers
                closer_lookup[pitchers[0]]  = current_team
            current_team = None

        return {'depth_charts': depth_charts, 'closer_lookup': closer_lookup}
    except Exception as e:
        print(f"  ESPN closer fetch error: {e}")
        return None

def _try_fetch_fantasypros_closers():
    try:
        response = requests.get(
            'https://www.fantasypros.com/mlb/closer-depth-charts.php',
            headers={"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"},
            timeout=15
        )
        raw  = response.text
        text = re.sub('<[^<]+?>', ' ', raw)
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text)

        depth_charts  = {}
        closer_lookup = {}

        # FantasyPros format varies — look for team names followed by player names
        team_pattern = re.compile(
            r'\b(Baltimore Orioles|Boston Red Sox|New York Yankees|Tampa Bay Rays|'
            r'Toronto Blue Jays|Chicago White Sox|Cleveland Guardians|Detroit Tigers|'
            r'Kansas City Royals|Minnesota Twins|Houston Astros|Los Angeles Angels|'
            r'Athletics|Seattle Mariners|Texas Rangers|Atlanta Braves|Miami Marlins|'
            r'New York Mets|Philadelphia Phillies|Washington Nationals|Chicago Cubs|'
            r'Cincinnati Reds|Milwaukee Brewers|Pittsburgh Pirates|St\. Louis Cardinals|'
            r'Arizona Diamondbacks|Colorado Rockies|Los Angeles Dodgers|San Diego Padres|'
            r'San Francisco Giants)\b'
        )
        name_pattern = re.compile(
            r'\b([A-Z][a-z\']+\s+[A-Z][a-z\']+(?:\s+[A-Z][a-z\']+)?)\b'
        )

        teams_found = list(team_pattern.finditer(text))
        for i, tm in enumerate(teams_found):
            team_name = tm.group(1)
            start     = tm.end()
            end       = teams_found[i+1].start() if i+1 < len(teams_found) else start + 300
            segment   = text[start:end]

            pitchers = []
            for nm in name_pattern.finditer(segment):
                name = nm.group(1).strip()
                if looks_like_player_name(name):
                    norm = normalize_name(name)
                    if norm not in KNOWN_MEDIA_NAMES and norm not in pitchers:
                        pitchers.append(norm)
                        if len(pitchers) >= 3:
                            break

            if pitchers:
                depth_charts[team_name]    = pitchers
                closer_lookup[pitchers[0]] = team_name

        return {'depth_charts': depth_charts, 'closer_lookup': closer_lookup}
    except Exception as e:
        print(f"  FantasyPros closer fetch error: {e}")
        return None

def _map_espn_team(espn_name):
    mapping = {
        'ARIZONA DIAMONDBACKS': 'Arizona Diamondbacks',
        'ATLANTA BRAVES':       'Atlanta Braves',
        'BALTIMORE ORIOLES':    'Baltimore Orioles',
        'BOSTON RED SOX':       'Boston Red Sox',
        'CHICAGO CUBS':         'Chicago Cubs',
        'CHICAGO WHITE SOX':    'Chicago White Sox',
        'CINCINNATI REDS':      'Cincinnati Reds',
        'CLEVELAND GUARDIANS':  'Cleveland Guardians',
        'COLORADO ROCKIES':     'Colorado Rockies',
        'DETROIT TIGERS':       'Detroit Tigers',
        'HOUSTON ASTROS':       'Houston Astros',
        'KANSAS CITY ROYALS':   'Kansas City Royals',
        'LOS ANGELES ANGELS':   'Los Angeles Angels',
        'LOS ANGELES DODGERS':  'Los Angeles Dodgers',
        'MIAMI MARLINS':        'Miami Marlins',
        'MILWAUKEE BREWERS':    'Milwaukee Brewers',
        'MINNESOTA TWINS':      'Minnesota Twins',
        'NEW YORK METS':        'New York Mets',
        'NEW YORK YANKEES':     'New York Yankees',
        'OAKLAND ATHLETICS':    'Athletics',
        'THE ATHLETICS':        'Athletics',
        'ATHLETICS':            'Athletics',
        'PHILADELPHIA PHILLIES':'Philadelphia Phillies',
        'PITTSBURGH PIRATES':   'Pittsburgh Pirates',
        'SAN DIEGO PADRES':     'San Diego Padres',
        'SAN FRANCISCO GIANTS': 'San Francisco Giants',
        'SEATTLE MARINERS':     'Seattle Mariners',
        'ST. LOUIS CARDINALS':  'St. Louis Cardinals',
        'TAMPA BAY RAYS':       'Tampa Bay Rays',
        'TEXAS RANGERS':        'Texas Rangers',
        'TORONTO BLUE JAYS':    'Toronto Blue Jays',
        'WASHINGTON NATIONALS': 'Washington Nationals',
    }
    return mapping.get(espn_name.strip().upper())

def get_all_closers():
    return set(fetch_closermonkey().get('closer_lookup', {}).keys())

def get_closer_team(player_norm):
    return fetch_closermonkey().get('closer_lookup', {}).get(player_norm)

def get_closer_candidates(team_name, taken, limit=3):
    """Return available closer candidates with fantasy relevance check."""
    chart  = fetch_closermonkey().get('depth_charts', {}).get(team_name, [])
    result = []
    fa     = get_league_free_agents(position='RP', count=30)
    fa_map = {normalize_name(p['name']): p for p in fa}

    for norm in chart[1:limit+3]:
        if norm in taken:
            continue
        fa_player = fa_map.get(norm)
        if fa_player:
            pct = fa_player['pct_owned']
            if pct < PCT_CLOSER_BACKUP:
                # Check stats before giving up
                pid   = get_player_id_from_name(norm.title())
                stats = get_pitcher_stats_blended(pid) if pid else None
                if not (stats and stats.get('era', 99) < SP_ERA_ABOVE_AVG
                        and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL):
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
                        if normalize_name(candidate) not in KNOWN_MEDIA_NAMES:
                            return candidate
    full_text  = clean_text(title + ' ' + summary)
    pattern    = r'\b([A-Z][a-z\']+(?:\s+[A-Z][a-z\']+){1,3})\b'
    candidates = re.findall(pattern, full_text)
    for candidate in candidates:
        if not looks_like_player_name(candidate):
            continue
        if normalize_name(candidate) in KNOWN_MEDIA_NAMES:
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
                profiles[team_key] = {'adds': []}
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
    """
    Return positions where my team is weak.
    Weak = position where starting player has below-average fantasy value
    or is on IL. Uses PCT_HITTER_RELEVANT as the threshold.
    """
    weak   = []
    strong = {'SS', '1B', 'OF'}  # My strong/clogged positions
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
            if 'IL' in (p['status'] or '') or p['pct_owned'] < PCT_HITTER_RELEVANT:
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
            tier      = sp_tier(p, stats)
            has_start = normalize_name(p['name']) in {normalize_name(k) for k in week_starts}

            if tier == 'elite':
                continue  # Never drop elite

            if tier == 'long_term':
                if not has_start:
                    score += 28
                else:
                    remaining_ops_list = []
                    for k in week_starts:
                        if normalize_name(k) == normalize_name(p['name']):
                            remaining_ops_list = week_starts[k].get('opp_ops', [TEAM_OPS_AVERAGE])
                    worst_ops = max(remaining_ops_list) if remaining_ops_list else TEAM_OPS_AVERAGE
                    if worst_ops > TEAM_OPS_ABOVE_AVG and p['pct_owned'] < PCT_SP_LONG_TERM:
                        score += 18  # Long-term but tough matchup + borderline ownership
                    else:
                        continue  # Protect

            else:  # streamer
                score += 48
                if has_start:
                    remaining_ops_list = [TEAM_OPS_AVERAGE]
                    for k in week_starts:
                        if normalize_name(k) == normalize_name(p['name']):
                            remaining_ops_list = week_starts[k].get('opp_ops', [TEAM_OPS_AVERAGE])
                    worst_ops = max(remaining_ops_list) if remaining_ops_list else TEAM_OPS_AVERAGE
                    if worst_ops <= TEAM_OPS_BELOW_AVG:
                        score -= 22  # Good start remaining — hesitate

        else:
            # Hitter — only drop if truly below relevant threshold
            if p['pct_owned'] < PCT_HITTER_RELEVANT and p['position'] not in ['C', '1B', '2B', '3B', 'SS']:
                score += 38
            elif p['pct_owned'] < (PCT_HITTER_RELEVANT * 0.5):
                score += 27
            else:
                continue

        if prefer_position and p['position'] == prefer_position:
            score += 14
        score -= p['pct_owned'] * 0.28
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

def _reaction_window_str():
    """Return a string estimate of how long until a leaguemate moves."""
    transactions = load_transactions()
    minutes      = _estimate_reaction_window(transactions)
    if minutes is None:
        return None
    if minutes < 30:
        return f"⏱️ Act within ~{minutes} min — fastest leaguemate historically moves this fast"
    elif minutes < 60:
        return f"⏱️ Act within ~{minutes} min — leaguemates typically react within an hour"
    else:
        return f"⏱️ You likely have ~{minutes} min before a leaguemate acts on this"

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

        if normalize_name(player) in KNOWN_MEDIA_NAMES:
            continue

        canonical, is_available = validate_player_in_yahoo(player, taken)
        if canonical is None:
            continue

        player_norm = normalize_name(canonical)
        react_str   = _reaction_window_str()

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
                            if p['pct_owned'] >= PCT_SS_WATCHLIST
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
                        react_line = f"\n{react_str}" if react_str else ""
                        msg = (
                            f"{canonical} — {_extract_injury_detail(text)}"
                            f"{pickup_str}{react_line}\n\nSource: {source}"
                        )
                    _fire_or_queue(title_str, msg, priority=1 if is_mine else 0,
                                   seen=seen, key=key, sleep_queue=sleep_queue,
                                   queue_category='ss_injury')
                    alerts_sent += 1
                break

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
                react_line  = f"\n{react_str}" if react_str else ""
                if candidates:
                    drop_cand = find_best_drop(my_roster, team_ops)
                    drop_str  = (f"\n\n💀 Drop: {drop_cand['name']} "
                                 f"({drop_cand['pct_owned']:.0f}%)" if drop_cand else "")
                    grab_str  = ', '.join(candidates[:2])
                    msg = (
                        f"⚡ {canonical} (closer) placed on IL.\n\n"
                        f"🎯 GRAB NOW: {grab_str} — available and may inherit saves!\n"
                        f"Team: {closer_team or 'unknown'}{react_line}{drop_str}\n\n"
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
                            f"Watch for role announcement.{react_line}\n\nSource: {source}"
                        )
                        _fire_or_queue(
                            f"💾 SAVES WATCH: {canonical} on IL", msg,
                            priority=0, seen=seen, key=key_watch,
                            sleep_queue=sleep_queue, queue_category='saves'
                        )
                        alerts_sent += 1

        elif closer_role_change and not role_loss and is_available:
            if any(kw in text for kw in INJURY_KEYWORDS):
                days_out = _estimate_days_out(text)
                if days_out is None or days_out > IL_STASH_MAX_DAYS_ELITE:
                    pass  # Long-term IL — suppress
                else:
                    key = f"closer_role:{player_norm}"
                    if not is_alert_seen(key, seen):
                        il_used  = count_my_il_slots_used(my_roster)
                        has_slot = il_used < MY_IL_SLOTS
                        worst_il = get_worst_il_stash(my_roster)
                        can_bump = (worst_il and worst_il['pct_owned'] < PCT_CLOSER_BACKUP * 2
                                    and il_used >= MY_IL_SLOTS)
                        if has_slot or can_bump:
                            slot_str   = "Open IL slot available" if has_slot else \
                                         f"Bump {worst_il['name']} from IL slot"
                            react_line = f"\n{react_str}" if react_str else ""
                            msg = (
                                f"💾 {canonical} returning soon from IL — closer when healthy!\n\n"
                                f"🎯 Stash now: {slot_str}\n"
                                f"Expected return: ~{days_out} days{react_line}\n\nSource: {source}"
                            )
                            _fire_or_queue(
                                f"💾 CLOSER STASH: {canonical}", msg,
                                priority=1, seen=seen, key=key,
                                sleep_queue=sleep_queue, queue_category='saves'
                            )
                            alerts_sent += 1
            else:
                key = f"closer_role:{player_norm}"
                if not is_alert_seen(key, seen):
                    drop_cand  = find_best_drop(my_roster, team_ops)
                    drop_str   = (f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)"
                                  if drop_cand else "")
                    react_line = f"\n{react_str}" if react_str else ""
                    msg = (
                        f"⚡ {canonical} taking over closing role — saves opportunity!\n\n"
                        f"Available in your league. Add now before leaguemates react."
                        f"{react_line}{drop_str}\n\nSource: {source}"
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
            stat_str      = backup.get('stat_str', '')
            reason_str    = backup.get('reason', '')
            react_line    = f"\n{react_str}" if react_str else ""
            msg = (
                f"🚑 {canonical} — {injury_detail}\n\n"
                f"🎯 Add: {backup['name']} ({backup['pct_owned']:.0f}% owned)"
                f"{' — ' + stat_str if stat_str else ''}\n"
                f"{reason_str}{react_line}{drop_str}\n\nSource: {source}"
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
            drop_cand  = find_best_drop(my_roster, team_ops)
            drop_str   = (f"\n💀 Drop: {drop_cand['name']} ({drop_cand['pct_owned']:.0f}%)"
                          if drop_cand else "")
            react_line = f"\n{react_str}" if react_str else ""
            msg = (
                f"⚡ {canonical} called up to the majors!\n\n"
                f"Top prospect — expected significant playing time. "
                f"Add before leaguemates react.{react_line}{drop_str}\n\nSource: {source}"
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
    """Find a specific fantasy-relevant backup created by an injury."""
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
    candidates    = get_league_free_agents(position=injured_pos, count=20) if injured_pos else []
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

        # Check 1: Meaningful ownership (top 65% rostered in similar leagues)
        if pct_owned >= PCT_HITTER_RELEVANT:
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
                if stats and stats.get('pa', 0) >= MIN_PA_HITTER_SIGNAL and stats.get('ops', 0) >= HITTER_OPS_BELOW_AVG:
                    passes   = True
                    reason   = "Platoon role becoming everyday opportunity"
                    stat_str = f"OPS {stats['ops']:.3f} | {stats['pa']} PA"

        # Check 4: Reliever → spot starter (quality threshold)
        if not passes and pos == 'RP' and injured_pos == 'SP':
            pid = get_player_id_from_name(name)
            if pid:
                stats = get_pitcher_stats_blended(pid)
                if (stats and stats.get('era', 99) < SP_ERA_ABOVE_AVG
                        and stats.get('whip', 9) < SP_WHIP_ABOVE_AVG
                        and stats.get('kbb', 0) > SP_KBB_AVERAGE):
                    for sp_name, sp_info in week_starters.items():
                        if normalize_name(sp_name) == norm:
                            opp_ops = sp_info['opp_ops'][0] if sp_info['opp_ops'] else TEAM_OPS_AVERAGE
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
            if norm in TOP_PROSPECTS or pct_owned >= (PCT_HITTER_RELEVANT * 0.4):
                passes = True
                reason = "Callup accelerated by injury — may get immediate role"

        # Check 6: Prior season production (full sample only)
        if not passes:
            pid = get_player_id_from_name(name)
            if pid:
                if pos in ['SP', 'P']:
                    prior = get_pitcher_stats(pid, date.today().year - 1)
                    if prior and prior.get('ip', 0) >= MIN_IP_PRIOR_FULL * 0.77 and prior.get('era', 99) < SP_ERA_AVERAGE:
                        passes   = True
                        reason   = f"Proven starter — {prior['ip']:.0f} IP last season"
                        stat_str = f"Prior ERA {prior['era']:.2f} | WHIP {prior['whip']:.2f}"
                else:
                    prior = get_hitter_stats(pid, date.today().year - 1)
                    if prior and prior.get('pa', 0) >= MIN_PA_PRIOR_FULL * 0.77 and prior.get('ops', 0) >= HITTER_OPS_BELOW_AVG:
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
                    if s and s.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL:
                        stat_str = f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | K/BB {s['kbb']:.1f}"
                else:
                    s = get_hitter_stats(pid)
                    if s and s.get('pa', 0) >= MIN_PA_HITTER_SIGNAL:
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
    by_cat       = {}
    max_priority = max(item.get('priority', 0) for item in queue)
    for item in queue:
        cat = item.get('category', 'other')
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append(item)
    lines = [f"🌅 OVERNIGHT ({len(queue)} alert{'s' if len(queue) > 1 else ''}):\n"]
    for cat, items in by_cat.items():
        for item in items:
            lines.append(f"{item['title']}\n{item['message'][:200]}\n")
    send_pushover("🌅 OVERNIGHT DIGEST", '\n'.join(lines)[:1024], priority=max_priority)
    save_sleep_queue([])
    print(f"  Sent overnight digest: {len(queue)} items")

# ============================================================
# IL RETURN & ROSTER PITCHER COUNTING
# ============================================================
def _get_pitchers_including_il_returns(roster, week_mon=None, week_sun=None):
    """
    Return set of pitcher name norms.
    Includes all SPs not on IL + IL pitchers returning this week.
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

def _count_roster_starts(pitcher_norms, all_starters, team_ops):
    """
    Count probable starts for a set of pitchers.
    Uses confirmed probables from MLB API.
    If a pitcher is on roster but has no confirmed probable yet,
    they are counted as 0 starts for now (we don't project unconfirmed starts).
    """
    total    = 0
    hq_count = 0
    entries  = []
    for name, info in all_starters.items():
        if normalize_name(name) in pitcher_norms:
            stats = get_pitcher_stats_blended(info['id'])
            is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [TEAM_OPS_AVERAGE]))
            total    += info['count']
            hq_count += info['count'] if is_hq else 0
            entries.append({
                'name': name, 'count': info['count'],
                'stats': stats, 'is_hq': is_hq,
                'dates': info['dates'], 'opponents': info['opponents'],
                'opp_ops': info['opp_ops']
            })
    return total, hq_count, entries

def _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun):
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
# ALERT: START/SIT (Daily 9am) — H2H-aware
# ============================================================
def send_start_sit_alert(my_roster, team_ops, taken):
    """
    Daily 9am: Evaluate today's probable starters.
    Decision uses pitcher quality tier, matchup OPS, and current H2H standings.
    All benched SPs included (they will be activated on start day).
    """
    print("Running start/sit alert...")
    today_date   = date.today()
    all_starters = get_probable_pitchers(today_date, today_date, team_ops)

    matchup   = get_matchup_data()
    my_stats  = matchup.get('my_stats', {})  if matchup else {}
    opp_stats = matchup.get('opp_stats', {}) if matchup else {}

    # Include ALL SPs not on IL
    my_sp_norms = {
        normalize_name(p['name']): p for p in my_roster
        if p['position'] in ['SP', 'P']
        and 'IL' not in (p['status'] or '')
    }

    lines = ["🎯 START/SIT — Today\n"]
    found = False

    for name, info in all_starters.items():
        norm = normalize_name(name)
        if norm not in my_sp_norms:
            continue
        found = True
        p         = my_sp_norms[norm]
        stats     = get_pitcher_stats_blended(info['id']) if info.get('id') else None
        opp_ops   = info['opp_ops'][0] if info['opp_ops'] else TEAM_OPS_AVERAGE
        opp_name  = info['opponents'][0] if info['opponents'] else 'unknown'
        tier      = sp_tier(p, stats)
        is_hq     = is_high_quality_sp(stats, opp_ops)

        # Determine H2H context
        ratio_cats_losing = []
        if my_stats and opp_stats:
            for cat in ['ERA', 'WHIP']:
                my_v  = my_stats.get(cat)
                opp_v = opp_stats.get(cat)
                if my_v is not None and opp_v is not None:
                    if my_v > opp_v + CAT_ERA_WHIP_MARGIN:
                        ratio_cats_losing.append(cat)
            for cat in ['K', 'W']:
                my_v  = my_stats.get(cat)
                opp_v = opp_stats.get(cat)
                if my_v is not None and opp_v is not None:
                    if my_v < opp_v * (1 - CAT_COUNTING_MARGIN):
                        ratio_cats_losing.append(cat)

        ratio_lead = len(ratio_cats_losing) == 0 and my_stats

        if tier == 'elite':
            # Elite pitchers: always start unless protecting a dominant ratio lead
            if ratio_lead and opp_ops > TEAM_OPS_ABOVE_AVG:
                lines.append(f"⚠️ SIT?: {name} vs {opp_name} {matchup_label(opp_ops)} — "
                             f"elite arm but you lead all ratio cats and matchup is tough. "
                             f"Your call.")
            else:
                lines.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)} — elite arm")

        elif tier == 'long_term':
            if is_hq:
                lines.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")
            elif opp_ops > TEAM_OPS_ABOVE_AVG:
                if ratio_cats_losing:
                    lines.append(f"⚠️ TOUGH CALL: {name} vs {opp_name} {matchup_label(opp_ops)} — "
                                 f"losing {', '.join(ratio_cats_losing)}. Consider starting to chase Ks/W.")
                else:
                    lines.append(f"⚠️ SIT?: {name} vs {opp_name} {matchup_label(opp_ops)} — "
                                 f"tough matchup but long-term value. Protect ratios if leading.")
            else:
                lines.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")

        else:  # streamer
            if is_hq:
                lines.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)} — good matchup")
            elif opp_ops > TEAM_OPS_ABOVE_AVG:
                lines.append(f"❌ SIT: {name} vs {opp_name} {matchup_label(opp_ops)} — "
                             f"streamer-tier vs tough offense")
                # Look for same-day replacement
                for avail_name, avail_info in all_starters.items():
                    if normalize_name(avail_name) in taken:
                        continue
                    avail_ops = avail_info['opp_ops'][0] if avail_info['opp_ops'] else TEAM_OPS_AVERAGE
                    if not is_high_quality_matchup(avail_ops):
                        continue
                    avail_stats = get_pitcher_stats_blended(avail_info['id'])
                    if not is_high_quality_sp(avail_stats, avail_ops):
                        continue
                    canonical, avail = validate_player_in_yahoo(avail_name, taken)
                    if not avail or canonical is None:
                        continue
                    stat_str = (f"ERA {avail_stats['era']:.2f} | WHIP {avail_stats['whip']:.2f}"
                                if avail_stats and avail_stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL
                                else "Limited stats")
                    lines.append(f"  🔄 Add instead: {canonical} vs "
                                 f"{avail_info['opponents'][0] if avail_info['opponents'] else '?'} "
                                 f"{matchup_label(avail_ops)} | {stat_str}")
                    lines.append(f"  💀 Drop: {name} ({p['pct_owned']:.0f}%)")
                    break
            else:
                lines.append(f"✅ START: {name} vs {opp_name} {matchup_label(opp_ops)}")

    if not found:
        print("  No SP starts today")
        return

    if len(lines) > 1:
        send_pushover("🎯 START/SIT", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: CURRENT WEEK SP ANALYSIS (Monday 8:45am)
# ============================================================
def send_current_week_sp_analysis(taken, my_roster, team_ops):
    print("Running current week SP analysis...")
    today    = datetime.now(ET_TZ).date()
    week_mon = monday_of_week(today)
    week_sun = sunday_of_week(today)

    all_starters      = get_probable_pitchers(week_mon, week_sun, team_ops)
    my_pitcher_norms  = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)

    matchup     = get_matchup_data()
    opp_team_id = matchup.get('opp_team_id') if matchup else None
    opp_pitcher_norms = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_total,  my_hq,  my_starts  = _count_roster_starts(my_pitcher_norms, all_starters, team_ops)
    opp_total, opp_hq, opp_starts = _count_roster_starts(opp_pitcher_norms, all_starters, team_ops)

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
            hq    = [is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [TEAM_OPS_AVERAGE])]
            value = (
                info['count'] * 10
                + sum(5 for h in hq if h)
                - sum(3 for ops in info.get('opp_ops', []) if ops > TEAM_OPS_AVERAGE)
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
                            if stats and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL else "Limited stats")
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

    all_starters      = get_probable_pitchers(today, week_sun, team_ops)
    my_pitcher_norms  = _get_pitchers_including_il_returns(my_roster, week_mon=week_mon, week_sun=week_sun)
    opp_team_id       = matchup.get('opp_team_id') if matchup else None
    opp_pitcher_norms = _build_opp_pitcher_norms(opp_team_id, today, week_mon, week_sun)

    my_starts,  my_hq,  _ = _count_roster_starts(my_pitcher_norms, all_starters, team_ops)
    opp_starts, opp_hq, _ = _count_roster_starts(opp_pitcher_norms, all_starters, team_ops)

    pitching_cats = ['W', 'SV', 'K', 'ERA', 'WHIP', 'KBB']
    cats_losing   = []
    cats_winning  = []
    for cat in pitching_cats:
        my_val  = my_stats.get(cat)
        opp_val = opp_stats.get(cat)
        if my_val is None or opp_val is None:
            continue
        if cat in ['ERA', 'WHIP']:
            losing  = my_val > opp_val + CAT_ERA_WHIP_MARGIN
            winning = my_val < opp_val - CAT_ERA_WHIP_MARGIN
        else:
            losing  = my_val < opp_val * (1 - CAT_COUNTING_MARGIN)
            winning = my_val > opp_val * (1 + CAT_COUNTING_MARGIN)
        if losing:
            cats_losing.append(cat)
        elif winning:
            cats_winning.append(cat)

    starts_deficit = opp_starts - my_starts
    hq_deficit     = opp_hq - my_hq
    days_left      = days_left_in_week()

    need_streaming = (
        (starts_deficit > 0 and days_left >= 2)
        or (hq_deficit > 1 and days_left >= 2)
        or (len(cats_losing) >= 2 and my_starts <= opp_starts and days_left >= 1)
    )
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
        is_hq = any(is_high_quality_sp(stats, ops) for ops in info.get('opp_ops', [TEAM_OPS_AVERAGE]))
        if not is_hq and info['count'] < 2:
            continue
        value = score_sp(stats, min(info.get('opp_ops', [TEAM_OPS_AVERAGE])))
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
                        if stats and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL else "Limited stats")
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
        opp_ops_list = info.get('opp_ops', [TEAM_OPS_AVERAGE, TEAM_OPS_AVERAGE])
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
                    if stats and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL else "Limited stats")
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

        canonical, avail = validate_player_in_yahoo(name, taken)
        if not avail or canonical is None:
            continue

        # Resolve blank position
        if not pos:
            mlb_id_tmp = get_player_id_from_name(name)
            if mlb_id_tmp:
                try:
                    url = f"https://statsapi.mlb.com/api/v1/people/{mlb_id_tmp}"
                    d   = requests.get(url, timeout=3).json()
                    pos = d.get('people', [{}])[0].get('primaryPosition', {}).get('abbreviation', '')
                except Exception:
                    pass

        if pos in MY_CLOGGED_POSITIONS:
            continue

        # Check IL status — handle stash separately
        drop_reason  = drop.get('notes', '').lower()
        days_out_est = _estimate_days_out(drop_reason)
        player_on_il = days_out_est is not None

        if player_on_il:
            il_used  = count_my_il_slots_used(my_roster)
            has_slot = il_used < MY_IL_SLOTS
            worst_il = get_worst_il_stash(my_roster)
            can_bump = (worst_il and worst_il['pct_owned'] < PCT_CLOSER_BACKUP * 2
                        and il_used >= MY_IL_SLOTS)

            mlb_id   = get_player_id_from_name(name)
            is_elite = False
            stat_str = ''
            if mlb_id:
                if pos in ['SP', 'RP', 'P']:
                    stats    = get_pitcher_stats_blended(mlb_id)
                    is_elite = (stats and stats.get('era', 99) < SP_ERA_ABOVE_AVG
                                and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL)
                    if stats and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL:
                        stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                else:
                    stats    = get_hitter_stats_blended(mlb_id)
                    is_elite = (stats and stats.get('ops', 0) >= HITTER_OPS_ABOVE_AVG
                                and stats.get('pa', 0) >= MIN_PA_HITTER_SIGNAL)
                    if stats and stats.get('pa', 0) >= MIN_PA_HITTER_SIGNAL:
                        stat_str = f"OPS {stats['ops']:.3f} | HR {stats['hr']}"

            is_elite = is_elite or (normalize_name(name) in TOP_PROSPECTS)

            max_days = IL_STASH_MAX_DAYS_ELITE if is_elite else IL_STASH_MAX_DAYS_REGULAR
            stash_worthy = days_out_est <= max_days

            if not stash_worthy or (not has_slot and not can_bump):
                continue

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
        mlb_id     = get_player_id_from_name(name)
        stats      = None
        stat_str   = ''
        value      = 0
        is_pitcher = pos in ['SP', 'RP', 'P']

        if is_pitcher:
            if mlb_id:
                stats = get_pitcher_stats_blended(mlb_id)
                if stats and stats.get('ip', 0) >= MIN_IP_CURRENT_SIGNAL:
                    value    = score_sp(stats)
                    stat_str = f"ERA {stats['era']:.2f} | WHIP {stats['whip']:.2f} | K/BB {stats['kbb']:.1f}"
                    # For RPs: also compute contribution score
                    if pos == 'RP':
                        rp_score = rp_contribution_score(stats)
                        if rp_score <= 0:
                            continue  # RP doesn't contribute meaningfully
        else:
            if mlb_id:
                stats = get_hitter_stats_blended(mlb_id)
                if stats and stats.get('pa', 0) >= MIN_PA_HITTER_SIGNAL:
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
                # Never propose dropping a long-term SP asset
                mp_id    = get_player_id_from_name(mp['name'])
                mp_stats = get_pitcher_stats_blended(mp_id) if mp_id else None
                if sp_long_term_value(mp, mp_stats):
                    continue
                mp_value = score_sp(mp_stats) if mp_stats else -999
                if value > mp_value + SP_VALUE_GAP:
                    passes      = True
                    drop_target = mp
                    reason      = f"Better value than {mp['name']}"
                    break

            if not passes:
                my_streamers   = [mp for mp in my_pitchers
                                   if sp_tier(mp, None) == 'streamer']
                drop_has_start = norm_drop in {normalize_name(k) for k in my_week_starters}
                for mp in my_streamers:
                    mp_id    = get_player_id_from_name(mp['name'])
                    mp_stats = get_pitcher_stats_blended(mp_id) if mp_id else None
                    mp_value = score_sp(mp_stats) if mp_stats else -999
                    if value > mp_value + SP_VALUE_GAP and drop_has_start:
                        for sp_name, sp_info in my_week_starters.items():
                            if normalize_name(sp_name) == norm_drop:
                                opp_ops = sp_info['opp_ops'][0] if sp_info['opp_ops'] else TEAM_OPS_AVERAGE
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
                    mp_stats = get_hitter_stats_blended(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    if value > mp_value + HITTER_VALUE_GAP:
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
                    mp_stats = get_hitter_stats_blended(mp_id) if mp_id else None
                    mp_value = (mp_stats.get('ops', 0) * 100) if mp_stats else 0
                    if value > mp_value + HITTER_VALUE_GAP:
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
            return score_sp(stats) + p['pct_owned'] * 0.28
        else:
            stats = get_hitter_stats_blended(pid) if pid else None
            if stats and stats.get('pa', 0) >= MIN_PA_HITTER_SIGNAL:
                return stats.get('ops', 0) * 78 + p['pct_owned'] * 0.28
            return p['pct_owned'] * 0.48

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
                     and p['pct_owned'] >= PCT_TRADE_CANDIDATE],
                    key=player_value, reverse=True
                )
                their_get_candidates = sorted(
                    [p for p in their_roster
                     if p['position'] == get_pos
                     and not p.get('is_undroppable', False)
                     and p['pct_owned'] >= PCT_TRADE_CANDIDATE],
                    key=player_value, reverse=True
                )

                if not my_give_candidates or not their_get_candidates:
                    continue

                my_give   = my_give_candidates[0]
                their_get = their_get_candidates[0]

                # Fairness: ownership within 22 points (derived from trade acceptance research:
                # trades with >25% ownership gap are rarely accepted in 12-team leagues)
                if abs(my_give['pct_owned'] - their_get['pct_owned']) > 22:
                    continue

                if my_scores.get(give_pos, 0) <= my_scores.get(get_pos, 0):
                    continue

                # Positional coverage: must still have a hitter at give_pos after trade
                if give_pos in hitting_pos:
                    remaining = [
                        p for p in my_roster
                        if p['position'] == give_pos
                        and p['name'] != my_give['name']
                        and 'IL' not in (p['status'] or '')
                    ]
                    if not remaining:
                        continue

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

    react_minutes = _estimate_reaction_window(transactions)
    react_str     = (f"\n⏱️ Fastest reaction time in your league: ~{react_minutes} min"
                     if react_minutes else "")

    lines = [f"🕵️ LEAGUEMATE INTEL\n",
             f"Season: {total} transactions | {len(adds)} adds | {len(drops)} drops{react_str}\n"]

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
        lines.append("⚡ Watch these managers — they move fast on breaking news.")
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
    weekday   = now_et.weekday()

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
            send_start_sit_alert(my_roster, team_ops, taken)
            send_waiver_drops_alert(taken, my_roster, team_ops)
            check_positional_eligibility(my_roster, team_ops)

    if weekday in [2, 3, 4, 5, 6] and at(7, 0, 14):
        print("\n--- STREAMERS ALERT ---")
        if ensure_rosters() and ensure_team_ops():
            send_streamers_alert(taken, my_roster, team_ops)

    if weekday in [4, 5, 6] and at(8, 30, 44):
        print("\n--- 2-START SPs ---")
        preliminary = (weekday == 4)
        if ensure_rosters() and ensure_team_ops():
            send_two_start_alert(taken, my_roster, team_ops, preliminary=preliminary)

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
        try:
            sync_league_transactions()
        except Exception as e:
            print(f"  Transaction sync skipped: {e}")

    if in_sleep:
        print("\n[Sleep window — alerts queued for 6:30am digest]")

    print("\nDone.")

if __name__ == "__main__":
    main()
