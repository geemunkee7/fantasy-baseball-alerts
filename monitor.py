import os
import re
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

# Safety threshold — if Yahoo returns fewer than this many
# rostered players something went wrong. Abort all alerts.
MIN_EXPECTED_ROSTERED = 100

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

# CRITICAL FIX 4: Only these sources use reliable
# "Player Name: news summary" colon format.
# All other sources get full-text scanning instead.
COLON_FORMAT_SOURCES = {'Rotowire', 'MLB Trade Rumors'}

# Known non-player phrases that appear before colons in headlines
# Used to reject false player name extractions
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

ACTION_KEYWORDS = [
    'called up', 'promoted', 'recalled', 'call-up', 'debut',
    'closer', 'closing role', 'save opportunity', 'ninth inning',
    'activated', 'reinstated', 'returns from il', 'comes off il',
    'placed on il', 'injured list', 'day-to-day', 'goes on il',
    'designated for assignment', 'dfa', 'outrighted',
    'trade', 'acquired', 'traded', 'signed', 'released',
    'starting lineup', 'leadoff', 'everyday', 'regular',
    'optioned', 'demoted', 'scratched', 'suspended'
]

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

# ============================================================
# CRITICAL FIX 1 + 2: NAME NORMALIZATION
# Strips parentheticals: "Shohei Ohtani (Batter)" -> "shohei ohtani"
# Strips accents: "Teoscar Hernández" -> "teoscar hernandez"
# Also handles Jr./Sr. suffixes and extra whitespace
# ============================================================
def normalize_name(name):
    if not name:
        return ''
    # Remove parenthetical suffixes e.g. "(Batter)", "(Pitcher)"
    name = re.sub(r'\s*\(.*?\)', '', name).strip()
    # Normalize unicode accents to plain ASCII
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    # Lowercase and collapse whitespace
    return ' '.join(name.lower().split())

def looks_like_player_name(text):
    """
    Returns True if text looks like a real human name.
    Requires 2-4 words, each starting with a capital letter.
    Rejects known non-player phrases.
    """
    if not text:
        return False
    text = text.strip()
    # Reject if it's a known non-player prefix
    if text.lower() in NON_PLAYER_PREFIXES:
        return False
    if any(text.lower().startswith(p) for p in NON_PLAYER_PREFIXES):
        return False
    # Must be 2-4 words
    words = text.split()
    if not (2 <= len(words) <= 4):
        return False
    # Each word must start with a capital letter or be Jr./Sr./II/III
    suffixes = {'jr.', 'sr.', 'ii', 'iii', 'iv'}
    for word in words:
        if word.lower() in suffixes:
            continue
        if not word[0].isupper():
            return False
    # Reject if any word is a common non-name word
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
# CRITICAL FIX 3: YAHOO ROSTER SAFETY CHECK
# If fewer than MIN_EXPECTED_ROSTERED players come back,
# Yahoo failed — abort everything and send a system warning.
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
    """
    Returns (taken_set, my_roster_list).
    CRITICAL FIX 3: Returns (None, None) if Yahoo fails or
    returns suspiciously few players — caller must check for None
    before sending any alerts.
    """
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
                    # CRITICAL FIX 1+2: normalize before storing
                    taken.add(normalize_name(name))
                    if team_id == MY_TEAM_ID:
                        try:
                            my_roster.append({
                                'name':     name,
                                'name_normalized': normalize_name(name),
                                'position': player.primary_position,
                                'pct_owned': float(
                                    getattr(player.percent_owned, 'value', 0) or 0),
                                'is_undroppable': int(
                                    getattr(player, 'is_undroppable', 0) or 0),
                                'status':   str(getattr(player, 'status', '') or ''),
                                'selected_position': (
                                    player.selected_position.position
                                    if hasattr(player, 'selected_position') else ''),
                                'team_abbr': str(
                                    getattr(player, 'editorial_team_abbr', '') or ''),
                            })
                        except Exception:
                            pass
            except Exception as e:
                print(f"  Team {team_id} error: {e}")

        # CRITICAL FIX 3: Safety check
        if len(taken) < MIN_EXPECTED_ROSTERED:
            print(f"  ⚠️ WARNING: Only {len(taken)} players returned — Yahoo may have failed")
            send_pushover(
                "⚠️ SYSTEM WARNING",
                f"Yahoo roster fetch returned only {len(taken)} players "
                f"(expected {MIN_EXPECTED_ROSTERED}+). "
                f"Alerts suppressed this run to prevent false positives. "
                f"Will retry next run.",
                priority=0
            )
            return None, None

        print(f"  {len(taken)} rostered, {len(my_roster)} on my team")
        return taken, my_roster

    except Exception as e:
        print(f"  Yahoo error: {e}")
        send_pushover(
            "⚠️ SYSTEM WARNING",
            f"Yahoo connection failed: {str(e)[:200]}. "
            f"Alerts suppressed this run.",
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
        url  = (
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
                    'home_lineup':   [p.get('fullName', '') for p in lineups.get('homePlayers', [])],
                    'away_lineup':   [p.get('fullName', '') for p in lineups.get('awayPlayers', [])],
                })
        print(f"  Schedule: {len(games)} games today")
        return games
    except Exception as e:
        print(f"  Schedule API error: {e}")
        return []

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

def get_pitcher_stats(player_id):
    try:
        url  = (f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
                f"?stats=season&group=pitching&season=2026")
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
                        'wins': int(s.get('wins', 0) or 0),
                    }
                except Exception:
                    pass
    except Exception:
        pass
    return {'era': 99.99, 'whip': 9.99, 'k': 0, 'ip': 0.0, 'kbb': 0.0, 'wins': 0}

def passes_quality_gate(stats, strict=True):
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
# Every alert must represent a specific action in Yahoo.
# CRITICAL FIX 4 applied here: source-aware player extraction.
# ============================================================
def extract_player_name(item):
    """
    CRITICAL FIX 4: Only use colon-split for trusted sources.
    For all others, scan the summary for a real player name
    by looking for two consecutive capitalized words that are
    NOT known non-player phrases.
    """
    source  = item.get('source', '')
    title   = item.get('title', '')
    summary = item.get('summary', '')

    # Trusted sources: split on colon
    if source in COLON_FORMAT_SOURCES and ':' in title:
        candidate = title.split(':')[0].strip()
        if looks_like_player_name(candidate):
            return candidate

    # All other sources: scan full text for a real player name
    # Look for two consecutive Title Case words in the summary
    full_text = title + ' ' + summary
    # Find all sequences of 2-3 consecutive capitalized words
    pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b'
    candidates = re.findall(pattern, full_text)
    for candidate in candidates:
        if looks_like_player_name(candidate):
            return candidate

    return None

def get_actionability(item, taken):
    """
    Returns (is_actionable, alert_type, priority, player_name).
    Only True if there is a clear specific action to take in Yahoo.
    """
    if item['type'] == 'reddit':
        return False, '', 0, None

    text   = (item['title'] + ' ' + item['summary']).lower()
    player = extract_player_name(item)

    if not player:
        return False, '', 0, None

    # CRITICAL FIX 1+2: normalize before availability check
    player_normalized = normalize_name(player)
    if player_normalized in taken:
        return False, '', 0, None

    # Require at least one action keyword in the text
    if not any(kw in text for kw in ACTION_KEYWORDS):
        return False, '', 0, None

    # ── TIER A: Always actionable ──────────────────────────────
    if any(w in text for w in ['called up', 'promoted', 'recalled',
                                'debut', 'call-up']):
        return True, '🚀 CALLUP', 1, player

    if any(w in text for w in ['closer', 'closing role', 'save opportunity',
                                'ninth inning', 'saves role', 'closing duties']):
        return True, '💾 CLOSER ROLE', 1, player

    if any(w in text for w in ['activated', 'reinstated', 'returns from il',
                                'comes off il', 'off the il',
                                'cleared to return']):
        return True, '✅ IL RETURN', 1, player

    # ── TIER B: Conditional ────────────────────────────────────
    if any(w in text for w in ['placed on il', 'injured list',
                                'day-to-day', 'goes on il', 'to the il']):
        opp_words = ['start', 'lineup', 'replac', 'fill', 'opportunit',
                     'role', 'regular', 'everyday', 'every day',
                     'platoon', 'takeover']
        if any(w in text for w in opp_words):
            return True, '🚑 INJURY OPP', 1, player
        return False, '', 0, None

    if any(w in text for w in ['designated for assignment', 'dfa', 'outrighted']):
        callup_words = ['prospect', 'called up', 'promoted', 'minor league',
                        'aaa', 'triple-a', 'recall', 'top prospect']
        if any(w in text for w in callup_words):
            return True, '🔄 DFA→CALLUP OPP', 1, player
        return False, '', 0, None

    if any(w in text for w in ['trade', 'acquired', 'traded']):
        role_words = ['everyday', 'starting', 'regular', 'lineup',
                      'closer', 'opportunit', 'full-time', 'every day']
        if any(w in text for w in role_words):
            return True, '🔁 TRADE OPP', 0, player
        return False, '', 0, None

    return False, '', 0, None

# ============================================================
# ALERT: SATURDAY 8AM — 2-START PITCHERS
# ============================================================
def send_two_start_alert(taken, my_roster):
    print("Running Saturday 2-start alert...")
    today      = datetime.now(ET_TZ).date()
    days_ahead = (7 - today.weekday()) % 7 or 7
    next_mon   = today + timedelta(days=days_ahead)
    next_sun   = next_mon + timedelta(days=6)

    team_ops     = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(next_mon, next_sun, team_ops)
    two_starters = {n: i for n, i in all_starters.items() if i['count'] >= 2}

    if not two_starters:
        send_pushover(
            "⚾ 2-START ALERT",
            f"No confirmed 2-starters posted yet for {next_mon}.\n"
            "Check back Sunday morning.",
            priority=0
        )
        return

    quality_options = {}
    for name, info in two_starters.items():
        if normalize_name(name) in taken:
            continue
        stats = get_pitcher_stats(info['id'])
        info['stats'] = stats
        if not passes_quality_gate(stats, strict=True):
            print(f"  {name} failed quality gate")
            continue
        opp_ops_list = info.get('opp_ops', [0.720, 0.720])
        if min(opp_ops_list) > 0.750:
            print(f"  {name} — no favorable matchups")
            continue
        quality_options[name] = info

    if not quality_options:
        send_pushover(
            "⚾ 2-START ALERT",
            f"No available 2-starters cleared quality + matchup filters "
            f"for {next_mon}.\nYour current staff may already be your best options.",
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

    lines = [f"📅 Week of {next_mon}:\n"]
    for name, info in ranked:
        s         = info['stats']
        dates     = info.get('dates', [])
        opponents = info.get('opponents', [])
        opp_ops   = info.get('opp_ops', [])
        stat_line = (
            f"ERA {s['era']:.2f} | WHIP {s['whip']:.2f} | "
            f"{s['k']}K | K/BB {s['kbb']:.1f}"
            if s['ip'] >= 5 else "No 2026 stats yet"
        )
        start_lines = []
        for i, (d, opp, ops) in enumerate(
                zip(dates[:2], opponents[:2], opp_ops[:2])):
            start_lines.append(
                f"  Start {i+1}: {d[5:]} vs {opp} {matchup_label(ops)}"
            )
        lines.append(
            f"• {name}\n  {stat_line}\n" + '\n'.join(start_lines)
        )

    lines.append(f"\n💀 Potential drops:\n{drop_str}")
    send_pushover("⚾ 2-START SP TARGETS", '\n'.join(lines), priority=0)

# ============================================================
# ALERT: THU-SUN 8AM + 8PM — STREAMING PITCHERS
# ============================================================
def send_streaming_alert(taken, my_roster):
    print("Running streaming pitcher alert...")
    today       = datetime.now(ET_TZ).date()
    end_of_week = today + timedelta(days=(6 - today.weekday()))
    team_ops    = get_team_batting_stats()
    all_starters = get_probable_pitchers_with_matchups(today, end_of_week, team_ops)

    available = {}
    for name, info in all_starters.items():
        if normalize_name(name) in taken:
            continue
        s = get_pitcher_stats(info['id'])
        info['stats'] = s
        if not passes_quality_gate(s, strict=False):
            continue
        opp_ops_list = info.get('opp_ops', [0.720])
        if min(opp_ops_list) > 0.750:
            continue
        available[name] = info

    if not available:
        print("  No quality streaming options this window")
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
# ALERT: MON/FRI/SUN 8:50AM — WIRE DIGEST
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
# ALERT: 8:30AM / 11AM / 2PM — PITCHER SCRATCHED
# ============================================================
def check_pitcher_scratched(my_roster, games):
    print("Checking pitcher scratches...")
    my_sps = [
        p for p in my_roster
        if p['position'] == 'SP'
        and 'IL' not in (p['status'] or '')
        and p['selected_position'] not in ['BN', 'IL']
    ]
    team_probable = {}
    for game in games:
        if game['status'] in ['Final', 'Game Over', 'Postponed', 'Suspended']:
            continue
        if game['home_probable']:
            team_probable[game['home_team']] = game['home_probable']
        if game['away_probable']:
            team_probable[game['away_team']] = game['away_probable']

    for sp in my_sps:
        team_name = TEAM_NAME_MAP.get(sp['team_abbr'], '')
        if not team_name or team_name not in team_probable:
            continue
        probable = team_probable[team_name]
        if normalize_name(probable) != normalize_name(sp['name']):
            send_pushover(
                f"🚫 SCRATCH: {sp['name']}",
                f"{sp['name']} is NOT today's probable for {team_name}.\n"
                f"Listed starter: {probable}\n\n"
                f"⚠️ Swap in a bench SP or grab a streamer!",
                priority=1
            )

# ============================================================
# ALERT: 10:30AM / 1:30PM / 4:30PM — BATTER SITTING + POSTPONED
# ============================================================
def check_lineups_and_weather(my_roster, games):
    print("Checking lineups and postponements...")
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

        for hitter in my_hitters:
            team_name = TEAM_NAME_MAP.get(hitter['team_abbr'], '')
            if not team_name or team_name not in (home_team, away_team):
                continue
            if status in ['Postponed', 'Suspended']:
                send_pushover(
                    f"🌧️ POSTPONED: {hitter['name']}",
                    f"{away_team} @ {home_team} has been {status.lower()}.\n"
                    f"{hitter['name']} will not play today.\n\n"
                    f"⚠️ Swap in a bench hitter!",
                    priority=1
                )
                continue
            if lineup_posted and status not in ['Final', 'Game Over', 'In Progress']:
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

# ============================================================
# ALERT: DAILY 8AM — TOP 15 SS INJURY WATCHLIST
# ============================================================
def check_ss_injury_watchlist():
    print("Checking SS injury watchlist...")
    cutoff  = datetime.now(timezone.utc) - timedelta(hours=24)
    headers = {"User-Agent": "Mozilla/5.0 fantasy-baseball-monitor/1.0"}
    alerts, seen = [], set()
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
                        if normalize_name(ss) in normalize_name(text) \
                                and normalize_name(ss) not in seen:
                            if any(kw in text for kw in SS_INJURY_KEYWORDS):
                                seen.add(normalize_name(ss))
                                alerts.append({
                                    'player':  ss,
                                    'is_mine': normalize_name(ss) in MY_SS,
                                    'summary': summary[:250],
                                    'source':  source['name']
                                })
                except Exception:
                    continue
        except Exception:
            continue
    print(f"  {len(alerts)} SS alert(s)")
    for a in alerts:
        send_pushover(
            f"{'🚨' if a['is_mine'] else '👀'} SS INJURY: {a['player']}"
            f"{' ← YOUR PLAYER!' if a['is_mine'] else ''}",
            f"{a['summary']}\n\nSource: {a['source']}",
            priority=1 if a['is_mine'] else 0
        )

# ============================================================
# RSS FEED FETCHING
# ============================================================
def fetch_feed(source, lookback_minutes=11):
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
                title   = strip_html(entry.get('title', ''))
                summary = strip_html(
                    entry.get('summary', entry.get('description', title))
                )
                summary = summary[:300] + '...' if len(summary) > 300 else summary
                items.append({
                    'source':    source["name"],
                    'type':      source["type"],
                    'title':     title,
                    'summary':   summary,
                    'published': pub,
                })
            except Exception:
                continue
        print(f"  {source['name']}: {len(items)} items")
        return items
    except Exception as e:
        print(f"  {source['name']} error: {e}")
        return []

def should_check_reddit():
    m = datetime.now(timezone.utc).minute
    return m < 11 or 30 <= m < 41

def get_all_news(lookback_minutes=11):
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
    print(f"Total: {len(items)} raw items")
    return items

# ============================================================
# NEWS PROCESSOR
# ============================================================
def process_news_alerts(news, taken, is_digest=False):
    actionable      = []
    alerted_players = set()

    for item in news:
        is_actionable, alert_type, priority, player = get_actionability(item, taken)
        if not is_actionable:
            continue
        player_norm = normalize_name(player or '')
        if player_norm in alerted_players:
            continue
        alerted_players.add(player_norm)
        actionable.append({
            'alert_type': alert_type,
            'priority':   priority,
            'player':     player,
            'summary':    item['summary'],
            'source':     item['source']
        })

    if not actionable:
        return 0

    if is_digest:
        lines = [
            f"🌅 OVERNIGHT "
            f"({len(actionable)} item{'s' if len(actionable) > 1 else ''}):\n"
        ]
        for a in actionable:
            lines.append(
                f"{a['alert_type']}: {a['player']}\n"
                f"{a['summary'][:150]}\n"
                f"✅ Available in your league\n"
            )
        max_priority = max(a['priority'] for a in actionable)
        send_pushover(
            "🌅 OVERNIGHT DIGEST", '\n'.join(lines), priority=max_priority
        )
        return 1
    else:
        for a in actionable:
            send_pushover(
                f"{a['alert_type']}: {a['player']} [{a['source']}]",
                f"{a['summary']}\n\n✅ AVAILABLE — act now!",
                a['priority']
            )
        return len(actionable)

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

    in_sleep = (
        hour_et >= 23
        or hour_et < 6
        or (hour_et == 6 and minute_et < 30)
    )

    overnight_digest_window = (hour_et == 6  and 30 <= minute_et < 40)
    daily_window            = (hour_et == 8  and minute_et < 10)
    twice_daily_window      = (hour_et in [8, 20] and minute_et < 10)
    digest_window           = (hour_et == 8  and 50 <= minute_et < 60)
    pitcher_scratch_window  = (
        (hour_et == 8  and 30 <= minute_et < 40) or
        (hour_et == 11 and minute_et < 10)        or
        (hour_et == 14 and minute_et < 10)
    )
    lineup_weather_window   = (
        (hour_et == 10 and 30 <= minute_et < 40) or
        (hour_et == 13 and 30 <= minute_et < 40) or
        (hour_et == 16 and 30 <= minute_et < 40)
    )
    streaming_window = (
        twice_daily_window and
        (weekday in [3, 4, 5] or (weekday == 6 and hour_et == 8))
    )

    taken, my_roster, games = None, None, None

    # ── 6:30AM: OVERNIGHT DIGEST ────────────────────────────────
    if overnight_digest_window:
        print("\n--- OVERNIGHT DIGEST ---")
        overnight_news = get_all_news(lookback_minutes=450)
        taken, my_roster = get_all_rosters()
        if taken is None:
            print("  Yahoo failed — skipping overnight digest")
        else:
            sent = process_news_alerts(overnight_news, taken, is_digest=True)
            if sent == 0:
                print("  Nothing actionable overnight — no digest sent")

    # ── DAILY 8AM: SS INJURY WATCHLIST ─────────────────────────
    if daily_window:
        print("\n--- DAILY SS INJURY WATCHLIST ---")
        check_ss_injury_watchlist()

    # ── SATURDAY 8AM: 2-START PITCHER ALERT ────────────────────
    if weekday == 5 and daily_window:
        print("\n--- SATURDAY 2-START PITCHER ALERT ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_two_start_alert(taken, my_roster)

    # ── THU-SUN 8AM + 8PM: STREAMING ALERT ─────────────────────
    if streaming_window:
        print("\n--- STREAMING PITCHER ALERT ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_streaming_alert(taken, my_roster)

    # ── MON/FRI/SUN 8:50AM: WIRE DIGEST ────────────────────────
    if digest_window and weekday in [0, 4, 6]:
        print("\n--- WIRE DIGEST ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            send_wire_digest(taken, my_roster)

    # ── 8:30AM / 11AM / 2PM: PITCHER SCRATCH ───────────────────
    if pitcher_scratch_window:
        print("\n--- PITCHER SCRATCH CHECK ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            if games is None:
                games = get_todays_schedule()
            check_pitcher_scratched(my_roster, games)

    # ── 10:30AM / 1:30PM / 4:30PM: LINEUP + WEATHER ────────────
    if lineup_weather_window:
        print("\n--- LINEUP + WEATHER CHECK ---")
        if taken is None:
            taken, my_roster = get_all_rosters()
        if taken is not None:
            if games is None:
                games = get_todays_schedule()
            check_lineups_and_weather(my_roster, games)

    # ── AWAKE HOURS: BREAKING NEWS EVERY 10 MIN ─────────────────
    if not in_sleep and not overnight_digest_window:
        print("\n--- BREAKING NEWS CHECK ---")
        news = get_all_news(lookback_minutes=11)
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
