


import os
import json
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from typing import List, Tuple

def zero_rows_summary(df, team_col: str = "team_name") -> Tuple[int, List[str]]:
    """
    Counts rows where *all* core activity metrics are 0 (or missing).
    Returns (zero_count, up to 10 team names).
    """
    metric_candidates = [
        "total_mentions",
        "tweet_count",
        "total_likes",
        "total_retweets",
        "total_replies",
        # if you have these in some outputs, they’ll be included automatically:
        "positive_mentions",
        "negative_mentions",
        "neutral_mentions",
    ]
    metrics = [c for c in metric_candidates if c in df.columns]
    if not metrics:
        return 0, []

    # treat NaN as 0, then mark rows where sum across metrics == 0
    m = df[metrics].fillna(0)
    zero_mask = (m.sum(axis=1) == 0)

    teams = []
    if team_col in df.columns:
        teams = (
            df.loc[zero_mask, team_col]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )

    return int(zero_mask.sum()), teams[:10]


def build_run_summary_lines(results: list) -> str:
    """
    results: list of dicts, each like:
      {"label": "BL cumulative", "df": df, "csv_path": "..."}
    Returns a single Slack message (no multiple threads).
    """
    lines = ["✅ Daily Signals run finished"]

    cutoff = os.getenv("CUTOFF_HOUR_UTC")
    if cutoff is not None:
        lines.append(f"Cutoff UTC hour: {cutoff}")

    for r in results:
        df = r["df"]
        label = r["label"]

        zero_n, zero_teams = zero_rows_summary(df, team_col="team_name")
        teams_str = ""
        if zero_n > 0 and zero_teams:
            teams_str = f" ({', '.join(zero_teams)}{'…' if zero_n > len(zero_teams) else ''})"

        lines.append(f"{label}: {len(df)} rows | zero-rows: {zero_n}{teams_str}")

    return "\n".join(lines) 


def load_lineups(league_key: str) -> pd.DataFrame:
    """
    Expected columns in the CSV: team_name, player_name
    Returns a cleaned dataframe with lowercase keys.
    """
    if league_key == "bundesliga":
        p = LINEUPS_DIR / "bundesliga_lineups_2025_26.csv"
        df = pd.read_csv(p, sep=",", engine="python")
    elif league_key == "champions_league":
        p = LINEUPS_DIR / "cl_lineups_list.csv"
        # if your CL file is comma-separated, keep default.
        # if it's semicolon-separated, change to: sep=";"
        df = pd.read_csv(p, sep=";")
    else:
        raise ValueError(f"Unknown league_key: {league_key}")
    
    print("LINEUP PATH:", p)
    print("LINEUP COLS:", list(df.columns))

    # normalize column names just in case
        # normalize column names just in case
    df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]

    # accept common variants
    col_map = {}
    if "team_name" not in df.columns:
        if "team" in df.columns:
            col_map["team"] = "team_name"
        if "teamname" in df.columns:
            col_map["teamname"] = "team_name"

    if "player_name" not in df.columns:
        if "player" in df.columns:
            col_map["player"] = "player_name"
        if "playername" in df.columns:
            col_map["playername"] = "player_name"

    if col_map:
        df = df.rename(columns=col_map)

    if "team_name" not in df.columns or "player_name" not in df.columns:
        raise RuntimeError(
            f"Lineup CSV for {league_key} must contain team_name and player_name columns"
        )

    df = df[["team_name", "player_name"]].dropna()

    df["team_key"] = df["team_name"].astype(str).str.strip().str.lower()
    df["player_key"] = df["player_name"].astype(str).str.strip().str.lower()

    return df


def apply_lineup_filter_and_team_mapping(df: pd.DataFrame, league_key: str) -> pd.DataFrame:
    if df is None:
        return df
    if df.empty:
        return df

    lineups = load_lineups(league_key)

    teams = set(lineups["team_key"].tolist())
    players = set(lineups["player_key"].tolist())
    player_to_team = dict(zip(lineups["player_key"], lineups["team_name"]))

    df = df.copy()

    if "entity_name" not in df.columns or "team_name" not in df.columns:
        # if the query output doesn't have expected columns, don't break the run
        return df

    df["entity_key"] = df["entity_name"].astype(str).str.strip().str.lower()
    df["team_key"] = df["team_name"].astype(str).str.strip().str.lower()

    # override team_name for players
    df["team_name"] = df.apply(lambda r: player_to_team.get(r["entity_key"], r["team_name"]), axis=1)
    df["team_key"] = df["team_name"].astype(str).str.strip().str.lower()

    # strict filter
    if "entity_type" in df.columns:
        is_team = df["entity_type"].astype(str).str.lower().eq("team")
        df = df[(is_team & df["team_key"].isin(teams)) | (~is_team & df["entity_key"].isin(players))].copy()
    else:
        df = df[df["entity_key"].isin(players) | df["team_key"].isin(teams)].copy()

    df.drop(columns=["entity_key", "team_key"], errors="ignore", inplace=True)
    return df
    # fallback if entity_type missing
    df = df[df["entity_key"].isin(players) | df["team_key"].isin(teams)].copy()

BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "queries"
OUT_DIR = BASE_DIR / "outputs"

BASE_DIR = Path(__file__).resolve().parent
SQL_DIR = BASE_DIR / "queries"
OUT_DIR = BASE_DIR / "outputs"
LINEUPS_DIR = BASE_DIR / "lineups"

SQL_PATH = SQL_DIR / "league.sql"

LEAGUES = {
    "bundesliga": ("%Bundesliga%", "%Bundesliga%"),
    "champions_league": ("%UEFA Champions League%", "%Champions League%"),
}

QUERIES = [
    ("bundesliga_cumulative", SQL_PATH, "bundesliga", "cumulative"),
    ("bundesliga_trailing_24h", SQL_PATH, "bundesliga", "trailing_24h"),
    ("champions_league_cumulative", SQL_PATH, "champions_league", "cumulative"),
    ("champions_league_trailing_24h", SQL_PATH, "champions_league", "trailing_24h"),
]


# -----------------------
# ENV + SLACK
# -----------------------
def load_env():
    load_dotenv(BASE_DIR / ".env")


def get_required_env(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise RuntimeError(f"{key} missing in .env")
    return val


def send_slack_alert(message: str):
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        print("SLACK_WEBHOOK_URL not set. Skipping Slack alert.")
        return

    msg = (message or "").strip()
    if not msg:
        print("Slack alert skipped: empty message")
        return

    try:
        r = requests.post(webhook, json={"text": msg}, timeout=10)
        print("Slack response:", r.status_code, r.text)
    except Exception as e:
        print("Slack send failed:", e)


# -----------------------
# DB
# -----------------------
def connect_db(db_url: str):
    return psycopg2.connect(db_url, connect_timeout=10)


def read_sql(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def run_one(conn, sql_path: Path, params):
    sql = read_sql(sql_path)
    t0 = time.time()
    df = pd.read_sql_query(sql, conn, params=params)
    secs = round(time.time() - t0, 3)
    return df, secs


# -----------------------
# KPI formulas
# -----------------------
def calculate_interest_rate(mention_count: int, sentiment_score: float, engagement_score: float) -> float:
    volume_score = min(mention_count / 1000, 1.0) * 40
    sentiment_boost = (sentiment_score + 1) / 2 * 30
    engagement_boost = engagement_score * 30
    return round(volume_score + sentiment_boost + engagement_boost, 2)


def calculate_hype_score(mention_count: int, recent_mention_count: int, sentiment_trend: float) -> float:
    if mention_count > 0:
        momentum = (recent_mention_count / mention_count) * 50
    else:
        momentum = 0

    trend_boost = (sentiment_trend + 1) / 2 * 30
    volume_component = min(mention_count / 500, 1.0) * 20
    return round(momentum + trend_boost + volume_component, 2)


def enrich_with_interest_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure numeric
    for c in ["total_mentions", "positive_mentions", "negative_mentions", "neutral_mentions",
              "total_retweets", "total_likes", "total_replies", "tweet_count"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # sentiment_score
    df["sentiment_score"] = df.apply(
        lambda r: round(((r["positive_mentions"] - r["negative_mentions"]) / r["total_mentions"]), 3)
        if r["total_mentions"] > 0 else 0.0,
        axis=1
    )

    # total_engagement, avg_engagement_per_tweet, engagement_score (normalized 0..1)
    df["total_engagement"] = df["total_retweets"] + df["total_likes"] + df["total_replies"]
    df["avg_engagement_per_tweet"] = df.apply(
        lambda r: round((r["total_engagement"] / r["tweet_count"]), 2) if r["tweet_count"] > 0 else 0.0,
        axis=1
    )
    df["engagement_score"] = df.apply(
        lambda r: round(min((r["avg_engagement_per_tweet"] / 1000), 1.0), 3) if r["tweet_count"] > 0 else 0.0,
        axis=1
    )

    # sentiment pct
    df["positive_sentiment_pct"] = df.apply(
        lambda r: round((r["positive_mentions"] / r["total_mentions"]) * 100, 2) if r["total_mentions"] > 0 else 0.0,
        axis=1
    )
    df["negative_sentiment_pct"] = df.apply(
        lambda r: round((r["negative_mentions"] / r["total_mentions"]) * 100, 2) if r["total_mentions"] > 0 else 0.0,
        axis=1
    )

    # interest_rate
    df["interest_rate"] = df.apply(
        lambda r: calculate_interest_rate(int(r["total_mentions"]), float(r["sentiment_score"]), float(r["engagement_score"])),
        axis=1
    )

    return df


def enrich_with_hype_metrics(conn, df: pd.DataFrame, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    """
    Exact style like Nimish weekly module:
    - Split window into two halves: historic = [start, midpoint), recent = [midpoint, end)
    - Batch query per entity_id to get recent/historic mentions + pos/neg
    - sentiment_trend = recent_sentiment - historic_sentiment
    - hype_score = calculate_hype_score(total_mentions, recent_mentions, sentiment_trend)
    """
    if df.empty or "entity_id" not in df.columns:
        df["hype_score"] = 0.0
        return df

    midpoint = start_ts + ((end_ts - start_ts) / 2)

    entity_ids = df["entity_id"].dropna().astype(int).unique().tolist()
    if not entity_ids:
        df["hype_score"] = 0.0
        return df

    batch_size = 500
    comp_map = {}

    for i in range(0, len(entity_ids), batch_size):
        batch = entity_ids[i:i + batch_size]
        ids_str = ",".join(map(str, batch))

        # Same idea as Nimish: recent vs historic in one query
        q = f"""
            WITH recent_period AS (
                SELECT
                    tae.entity_id,
                    COUNT(DISTINCT ts.id) AS mentions,
                    COUNT(DISTINCT CASE WHEN ts.sentiment='positive' THEN ts.id END) AS positive,
                    COUNT(DISTINCT CASE WHEN ts.sentiment='negative' THEN ts.id END) AS negative
                FROM tweet_about_entities tae
                JOIN twitter_sentiments ts ON ts.tweet_about_entity_id = tae.id
                JOIN vw_tweets vt ON vt.id = tae.tweet_id
                WHERE tae.entity_id IN ({ids_str})
                  AND vt.tweet_date >= %s
                  AND vt.tweet_date < %s
                GROUP BY tae.entity_id
            ),
            historic_period AS (
                SELECT
                    tae.entity_id,
                    COUNT(DISTINCT ts.id) AS mentions,
                    COUNT(DISTINCT CASE WHEN ts.sentiment='positive' THEN ts.id END) AS positive,
                    COUNT(DISTINCT CASE WHEN ts.sentiment='negative' THEN ts.id END) AS negative
                FROM tweet_about_entities tae
                JOIN twitter_sentiments ts ON ts.tweet_about_entity_id = tae.id
                JOIN vw_tweets vt ON vt.id = tae.tweet_id
                WHERE tae.entity_id IN ({ids_str})
                  AND vt.tweet_date >= %s
                  AND vt.tweet_date < %s
                GROUP BY tae.entity_id
            )
            SELECT
                COALESCE(r.entity_id, h.entity_id) AS entity_id,
                COALESCE(r.mentions, 0) AS recent_mentions,
                COALESCE(h.mentions, 0) AS historic_mentions,
                COALESCE(r.positive, 0) AS recent_positive,
                COALESCE(h.positive, 0) AS historic_positive,
                COALESCE(r.negative, 0) AS recent_negative,
                COALESCE(h.negative, 0) AS historic_negative
            FROM recent_period r
            FULL OUTER JOIN historic_period h
              ON r.entity_id = h.entity_id;
        """

        comp_df = pd.read_sql_query(q, conn, params=(midpoint, end_ts, start_ts, midpoint))
        for _, row in comp_df.iterrows():
            comp_map[int(row["entity_id"])] = {
                "recent_mentions": int(row["recent_mentions"]),
                "historic_mentions": int(row["historic_mentions"]),
                "recent_positive": int(row["recent_positive"]),
                "historic_positive": int(row["historic_positive"]),
                "recent_negative": int(row["recent_negative"]),
                "historic_negative": int(row["historic_negative"]),
            }

    # Apply hype_score
    hype_scores = []
    for _, r in df.iterrows():
        eid = int(r["entity_id"])
        total_mentions = int(r.get("total_mentions", 0) or 0)

        c = comp_map.get(eid, {})
        recent_mentions = int(c.get("recent_mentions", 0))
        # sentiment trend exactly like Nimish simplified calc
        recent_sent = ((c.get("recent_positive", 0) - c.get("recent_negative", 0)) / max(recent_mentions, 1))
        historic_mentions = int(c.get("historic_mentions", 0))
        historic_sent = ((c.get("historic_positive", 0) - c.get("historic_negative", 0)) / max(historic_mentions, 1))
        sentiment_trend = recent_sent - historic_sent

        hype_scores.append(calculate_hype_score(total_mentions, recent_mentions, sentiment_trend))

    df["hype_score"] = hype_scores
    return df


# -----------------------
# STATUS FILES + ALERTS
# -----------------------
def write_status_files(status: dict):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "run_status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")

    lines = []
    lines.append(f"# Daily Runner Status ({status['run_id']})")
    lines.append(f"- Timestamp: {status['timestamp']}")
    lines.append(f"- DB Connected: {'✅' if status['db_connected'] else '❌'}")
    lines.append("")
    lines.append("## Steps")
    for step in status["steps"]:
        icon = "✅" if step["ok"] else "❌"
        lines.append(f"- {icon} **{step['name']}** ({step['seconds']}s, rows={step.get('rows', 0)})")
        if not step["ok"]:
            lines.append(f"  - error: `{step.get('error','')}`")

    (OUT_DIR / "run_status.md").write_text("\n".join(lines), encoding="utf-8")


def build_alert_message(status: dict) -> str:
    run_id = status["run_id"]
    if not status.get("db_connected"):
        return f"🚨 Bundesliga Daily Pipeline FAILED\nDB connection failed\nRun: {run_id}"

    failed = [s for s in status.get("steps", []) if not s.get("ok")]
    if failed:
        names = ", ".join(s["name"] for s in failed)
        return f"🚨 Bundesliga Daily Pipeline FAILED\n{names}\nRun: {run_id}"

    warn = [s for s in status.get("steps", []) if s.get("ok") and s.get("rows", 1) == 0]
    if warn:
        names = ", ".join(s["name"] for s in warn)
        return f"⚠️ Bundesliga Daily Pipeline WARNING\n0 rows: {names}\nRun: {run_id}"

    # include row counts
    parts = []
    for s in status.get("steps", []):
        parts.append(f"{s['name']}: {s.get('rows', 0)} rows")
    return f"✅ Bundesliga Daily Pipeline OK\n" + "\n".join(parts) + f"\nRun: {run_id}"


# -----------------------
# TIME WINDOWS (Neil logic)
# -----------------------
def get_end_cutoff_utc():
    # Default 9AM UTC unless overridden
    cutoff_hour = int(os.getenv("CUTOFF_HOUR_UTC", "9"))
    now = datetime.now(timezone.utc)
    cutoff = now.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    if now < cutoff:
        cutoff = cutoff - timedelta(days=1)
    return cutoff


def get_cumulative_window_utc(end_ts: datetime):
    # previous week Friday 19:30 -> end_ts
    days_since_friday = (end_ts.weekday() - 4) % 7
    this_week_friday = end_ts - timedelta(days=days_since_friday)
    start_ts = (this_week_friday - timedelta(days=7)).replace(hour=19, minute=30, second=0, microsecond=0)
    return start_ts, end_ts


def get_trailing_24h_window_utc(end_ts: datetime):
    return end_ts - timedelta(hours=24), end_ts


# -----------------------
# MAIN
# -----------------------
def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    status = {"run_id": run_id, "timestamp": datetime.now().isoformat(timespec="seconds"),
              "db_connected": False, "steps": []}

    print("=== Daily Signals Runner ===")
    print("Loading env from .env ...")
    load_env()

    try:
        db_url = get_required_env("DATABASE_URL")
    except Exception as e:
        print("❌ ENV error:", e)
        status["steps"].append({"name": "load_env", "ok": False, "seconds": 0, "rows": 0, "error": str(e)})
        write_status_files(status)
        send_slack_alert(build_alert_message(status))
        return

    print("Connecting to Production DB...")
    try:
        conn = connect_db(db_url)
        status["db_connected"] = True
        print("✅ Connected")
    except Exception as e:
        print("❌ DB connect failed:", e)
        status["steps"].append({"name": "db_connect", "ok": False, "seconds": 0, "rows": 0, "error": str(e)})
        write_status_files(status)
        send_slack_alert(build_alert_message(status))
        return

    end_ts = get_end_cutoff_utc()
    cum_start, cum_end = get_cumulative_window_utc(end_ts)
    t24_start, t24_end = get_trailing_24h_window_utc(end_ts)

    print(f"Cutoff (UTC): {end_ts}")
    print(f"Cumulative (UTC): {cum_start} -> {cum_end}")
    print(f"Trailing24h (UTC): {t24_start} -> {t24_end}")
    
    results = []  # collect dfs for one Slack summary
    try:
        for name, path, league_key, window_key in QUERIES:
            print(f"Running {name} ...")
            step = {"name": name, "ok": False, "seconds": 0, "rows": 0, "error": ""}


            try:
                like1, like2 = LEAGUES[league_key]
                if window_key == "trailing_24h":
                    window_start, window_end = t24_start, t24_end
                else:
                    window_start, window_end = cum_start, cum_end

                params = (window_start, window_end, like1, like2)

                df, secs = run_one(conn, path, params)

                df = enrich_with_interest_metrics(df)
                df = enrich_with_hype_metrics(conn, df, window_start, window_end)

                df["league"] = league_key
                df["window_start_utc"] = window_start
                df["window_end_utc"] = window_end
                df = apply_lineup_filter_and_team_mapping(df, league_key)
                print(f"After lineup filter: rows={len(df)}, unique teams={df['team_name'].nunique()}")
                
                results.append({"label": name, "df": df})

                step["ok"] = True
                step["seconds"] = secs
                step["rows"] = int(len(df))

                OUT_DIR.mkdir(parents=True, exist_ok=True)
                out_csv = OUT_DIR / f"{name}_{run_id}.csv"
                df.to_csv(out_csv, index=False)

                print(f"✅ {name} OK ({secs}s, rows={len(df)}) -> {out_csv.name}")

            except Exception as e:
                step["ok"] = False
                step["error"] = str(e)
                print(f"❌ {name} FAILED: {e}")

                OUT_DIR.mkdir(parents=True, exist_ok=True)
                tb_path = OUT_DIR / f"{name}_{run_id}_error.txt"
                tb_path.write_text(traceback.format_exc(), encoding="utf-8")
                print(f"   saved error trace -> {tb_path.name}")

            status["steps"].append(step)

    finally:
        try:
            conn.close()
        except Exception:
            pass

        write_status_files(status)
        print("Status written -> outputs/run_status.md and outputs/run_status.json")
        try:
            summary_text = build_run_summary_lines(results)
            send_slack_alert(summary_text)
        except Exception as e:
            print("Slack summary failed:", e)
        
        

if __name__ == "__main__":
    main()
