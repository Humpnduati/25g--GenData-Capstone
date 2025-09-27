import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import random
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import aiofiles
import aiohttp
from dateutil import parser as dateparser
from jsonschema import Draft7Validator, RefResolver
from tqdm import tqdm


try:
    import fastf1
    FASTF1_AVAILABLE = True
except Exception:
    FASTF1_AVAILABLE = False

# ---------------- Configuration ----------------

ERGAST_BASE = "https://ergast.com/api/f1"
OPENF1_BASE = "https://api.openf1.org/v1"
DEFAULT_SCHEMA_PATH = "f1db.schema.json"
DEFAULT_OUTPUT = "f1db.json"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=45)
ERGAST_PAGE_LIMIT = 1000
DEFAULT_CONCURRENCY = 12
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 4
BACKOFF_BASE = 0.6

# Logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("f1_orchestrator_final")

# ---------------- Utility / Cache helpers ----------------

def url_to_cache_path(cache_dir: Optional[str], url: str, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Return deterministic cache path filename for a request (url + sorted params)."""
    if not cache_dir:
        return None
    key = url
    if params:
        try:
            items = sorted(params.items())
        except Exception:
            items = list(params.items())
        key += "?" + "&".join(f"{k}={v}" for k, v in items)
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return os.path.join(cache_dir, f"{h}.json")

async def read_json_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as fh:
            txt = await fh.read()
            return json.loads(txt)
    except Exception:
        return None

async def write_json_file(path: str, data: Any):
    dirname = os.path.dirname(path)
    if dirname and not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)
    async with aiofiles.open(path, "w", encoding="utf-8") as fh:
        await fh.write(json.dumps(data, indent=2, default=str))


# ---------------- Async HTTP client w/ cache & resume ----------------

class AsyncHTTPClient:
    def __init__(self, concurrency: int = DEFAULT_CONCURRENCY, headers: Optional[Dict[str, str]] = None,
                 cache_dir: Optional[str] = None, resume: bool = False):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.headers = headers or {"User-Agent": "f1-orchestrator-final/1.0"}
        self.session: Optional[aiohttp.ClientSession] = None
        self.cache_dir = cache_dir
        self.resume = resume
        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=REQUEST_TIMEOUT, headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()
            self.session = None

    async def get_json(self, url: str, params: Optional[Dict[str, Any]] = None, allow_retry: bool = True) -> Optional[Dict[str, Any]]:
        params = params or {}
        cache_path = url_to_cache_path(self.cache_dir, url, params) if self.cache_dir else None
        # If resume and cache exists, return cached
        if cache_path and self.resume:
            if os.path.exists(cache_path):
                data = await read_json_file(cache_path)
                if data is not None:
                    return data
        backoff = BACKOFF_BASE
        for attempt in range(1, MAX_RETRIES + 1):
            async with self.semaphore:
                try:
                    assert self.session is not None
                    async with self.session.get(url, params=params) as resp:
                        status = resp.status
                        text = await resp.text()
                        if status == 200:
                            try:
                                parsed = json.loads(text)
                            except Exception:
                                parsed = None
                            if cache_path:
                                try:
                                    await write_json_file(cache_path, parsed)
                                except Exception:
                                    logger.debug("Failed to write cache %s", cache_path)
                            return parsed
                        if status in RETRY_STATUS and allow_retry and attempt < MAX_RETRIES:
                            logger.warning("GET %s returned %s. Retrying %d/%d after %.1fs", url, status, attempt, MAX_RETRIES, backoff)
                            await asyncio.sleep(backoff + random.random() * 0.3)
                            backoff *= 2
                            continue
                        logger.error("GET %s failed: status=%s body=%s", url, status, (text[:400] + "...") if text else "")
                        # fallback to cached response if available
                        if cache_path and os.path.exists(cache_path):
                            data = await read_json_file(cache_path)
                            if data is not None:
                                logger.info("Using cached fallback for %s", url)
                                return data
                        return None
                except asyncio.TimeoutError:
                    logger.warning("Timeout fetching %s (attempt %d/%d).", url, attempt, MAX_RETRIES)
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    if cache_path and os.path.exists(cache_path):
                        data = await read_json_file(cache_path)
                        if data is not None:
                            logger.info("Timeout -> using cache for %s", url)
                            return data
                    return None
                except aiohttp.ClientError as e:
                    logger.exception("ClientError fetching %s: %s", url, e)
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    if cache_path and os.path.exists(cache_path):
                        data = await read_json_file(cache_path)
                        if data is not None:
                            logger.info("ClientError -> using cache for %s", url)
                            return data
                    return None
        return None

# ---------------- Mapping helpers (hybrid mapping + extra) ----------------

def map_ergast_driver_to_schema(d: Dict[str, Any]) -> Dict[str, Any]:
    driver_id = d.get("driverId") or f"driver_{hash(d.get('url',''))%100000}"
    first = d.get("givenName", "") or ""
    last = d.get("familyName", "") or ""
    full = f"{first} {last}".strip()
    abbr = (last[:3].upper() if last else (first[:3].upper() or "UNK"))[:3]
    return {
        "id": driver_id,
        "name": full,
        "firstName": first,
        "lastName": last,
        "fullName": full,
        "abbreviation": abbr,
        "permanentNumber": None,
        "gender": "male",
        "dateOfBirth": d.get("dateOfBirth"),
        "dateOfDeath": None,
        "placeOfBirth": None,
        "countryOfBirthCountryId": None,
        "nationalityCountryId": d.get("nationality"),
        "secondNationalityCountryId": None,
        "familyRelationships": None,
        "bestChampionshipPosition": None,
        "bestStartingGridPosition": None,
        "bestRaceResult": None,
        "totalChampionshipWins": 0,
        "totalRaceEntries": 0,
        "totalRaceStarts": 0,
        "totalRaceWins": 0,
        "totalRaceLaps": 0,
        "totalPodiums": 0,
        "totalPoints": 0.0,
        "totalChampionshipPoints": 0.0,
        "totalPolePositions": 0,
        "totalFastestLaps": 0,
        "totalDriverOfTheDay": 0,
        "totalGrandSlams": 0,
        "extra": {k: v for k, v in d.items() if k not in ["driverId", "givenName", "familyName", "dateOfBirth", "nationality", "url"]}
    }

def map_ergast_constructor_to_schema(c: Dict[str, Any]) -> Dict[str, Any]:
    cid = c.get("constructorId") or f"constructor_{hash(c.get('name',''))%100000}"
    name = c.get("name") or cid
    return {
        "id": cid,
        "name": name,
        "fullName": c.get("name"),
        "countryId": None,
        "chronology": None,
        "bestChampionshipPosition": None,
        "bestStartingGridPosition": None,
        "bestRaceResult": None,
        "totalChampionshipWins": 0,
        "totalRaceEntries": 0,
        "totalRaceStarts": 0,
        "totalRaceWins": 0,
        "total1And2Finishes": 0,
        "totalRaceLaps": 0,
        "totalPodiums": 0,
        "totalPodiumRaces": 0,
        "totalPoints": 0.0,
        "totalChampionshipPoints": 0.0,
        "totalPolePositions": 0,
        "totalFastestLaps": 0,
        "extra": {k: v for k, v in c.items() if k not in ["constructorId", "name", "url"]}
    }

def map_ergast_circuit_to_schema(c: Dict[str, Any]) -> Dict[str, Any]:
    cid = c.get("circuitId") or f"circuit_{hash(c.get('circuitName',''))%100000}"
    name = c.get("circuitName")
    loc = c.get("Location", {}) or {}
    lat = float(loc["lat"]) if loc.get("lat") else None
    lon = float(loc["long"]) if loc.get("long") else None
    return {
        "id": cid,
        "name": name,
        "fullName": name,
        "previousNames": None,
        "type": "road",
        "direction": "clockwise",
        "placeName": loc.get("locality"),
        "countryId": loc.get("country"),
        "latitude": lat,
        "longitude": lon,
        "length": None,
        "turns": None,
        "totalRacesHeld": 0,
        "extra": {k: v for k, v in c.items() if k not in ["circuitId", "circuitName", "Location", "url"]}
    }

def map_ergast_race_to_schema(race: Dict[str, Any]) -> Dict[str, Any]:
    season = int(race.get("season", 0))
    rnd = int(race.get("round", 0))
    race_id = season * 100 + rnd
    gp_name = race.get("raceName")
    circuit_id = race.get("Circuit", {}).get("circuitId")
    skeleton = {
        "id": race_id,
        "year": season,
        "round": rnd,
        "date": race.get("date"),
        "time": race.get("time") or None,
        "grandPrixId": (gp_name or f"gp_{race_id}").replace(" ", "_"),
        "officialName": race.get("raceName"),
        "qualifyingFormat": None,
        "sprintQualifyingFormat": None,
        "circuitId": circuit_id,
        "circuitType": None,
        "direction": None,
        "courseLength": None,
        "turns": None,
        "laps": None,
        "distance": None,
        "scheduledLaps": None,
        "scheduledDistance": None,
        # placeholders for sessions & results
        "preQualifyingDate": None,
        "preQualifyingTime": None,
        "preQualifyingResults": None,
        "freePractice1Date": None,
        "freePractice1Time": None,
        "freePractice1Results": None,
        "freePractice2Date": None,
        "freePractice2Time": None,
        "freePractice2Results": None,
        "freePractice3Date": None,
        "freePractice3Time": None,
        "freePractice3Results": None,
        "freePractice4Date": None,
        "freePractice4Time": None,
        "freePractice4Results": None,
        "qualifying1Date": None,
        "qualifying1Time": None,
        "qualifying1Results": None,
        "qualifying2Date": None,
        "qualifying2Time": None,
        "qualifying2Results": None,
        "qualifyingDate": None,
        "qualifyingTime": None,
        "qualifyingResults": None,
        "sprintQualifyingDate": None,
        "sprintQualifyingTime": None,
        "sprintQualifyingResults": None,
        "sprintStartingGridPositions": None,
        "sprintRaceDate": None,
        "sprintRaceTime": None,
        "sprintRaceResults": None,
        "warmingUpDate": None,
        "warmingUpTime": None,
        "warmingUpResults": None,
        "startingGridPositions": None,
        "raceResults": None,
        "fastestLaps": None,
        "pitStops": None,
        "driverOfTheDayResults": None,
        "driverStandings": None,
        "constructorStandings": None,
        "driversChampionshipDecider": False,
        "constructorsChampionshipDecider": False,
        "laps": None,
        "telemetry": None,
        "extra": {k: v for k, v in race.items() if k not in ["season", "round", "raceName", "Circuit", "date", "time"]}
    }
    return skeleton

# ---------------- Ergast item mappers ----------------

def map_ergast_result_item_to_schema(res_item: Dict[str, Any]) -> Dict[str, Any]:
    extra = dict(res_item)
    driver_block = extra.pop("Driver", {})
    constructor_block = extra.pop("Constructor", {})
    fastest_lap_block = extra.pop("FastestLap", None)
    time_block = extra.pop("Time", None)
    mapped = {
        "positionDisplayOrder": None,
        "positionNumber": int(res_item.get("position")) if res_item.get("position") else None,
        "positionText": res_item.get("positionText") or (str(res_item.get("position")) if res_item.get("position") else None),
        "driverNumber": res_item.get("number"),
        "driverId": driver_block.get("driverId") if driver_block else None,
        "constructorId": constructor_block.get("constructorId") if constructor_block else None,
        "engineManufacturerId": None,
        "tyreManufacturerId": None,
        "laps": int(res_item.get("laps")) if res_item.get("laps") else None,
        "time": time_block.get("time") if time_block else None,
        "timeMillis": None,
        "status": res_item.get("status"),
        "grid": int(res_item.get("grid")) if res_item.get("grid") else None,
        "points": float(res_item.get("points")) if res_item.get("points") not in (None, "") else 0.0,
        "extra": {
            "raw": {"driver": driver_block, "constructor": constructor_block, "fastestLap": fastest_lap_block, "timeBlock": time_block},
            "leftover": {k: v for k, v in extra.items() if k not in ["position", "positionText", "points", "number", "laps", "status", "grid"]}
        }
    }
    return mapped

def map_ergast_qualifying_result_item_to_schema(q_item: Dict[str, Any]) -> Dict[str, Any]:
    extra = dict(q_item)
    driver_block = extra.pop("Driver", {})
    constructor_block = extra.pop("Constructor", {})
    mapped = {
        "positionDisplayOrder": None,
        "positionNumber": int(q_item.get("position")) if q_item.get("position") else None,
        "positionText": q_item.get("positionText") or (str(q_item.get("position")) if q_item.get("position") else None),
        "driverNumber": q_item.get("number"),
        "driverId": driver_block.get("driverId") if driver_block else None,
        "constructorId": constructor_block.get("constructorId") if constructor_block else None,
        "engineManufacturerId": None,
        "tyreManufacturerId": None,
        "time": q_item.get("Q3") or q_item.get("Q2") or q_item.get("Q1") or None,
        "timeMillis": None,
        "q1": q_item.get("Q1"),
        "q1Millis": None,
        "q2": q_item.get("Q2"),
        "q2Millis": None,
        "q3": q_item.get("Q3"),
        "q3Millis": None,
        "gap": None,
        "gapMillis": None,
        "interval": None,
        "intervalMillis": None,
        "extra": {
            "raw": {"driver": driver_block, "constructor": constructor_block},
            "leftover": {k: v for k, v in extra.items() if k not in ["position", "positionText", "number", "Q1", "Q2", "Q3"]}
        }
    }
    return mapped

def map_ergast_standing_item_to_schema(s_item: Dict[str, Any], typ: str = "driver") -> Dict[str, Any]:
    extra = dict(s_item)
    pts = float(s_item.get("points")) if s_item.get("points") not in (None, "") else 0.0
    pos = int(s_item.get("position")) if s_item.get("position") not in (None, "") else None
    if typ == "driver":
        driver_block = extra.pop("Driver", {})
        return {
            "positionDisplayOrder": pos,
            "positionNumber": pos,
            "positionText": s_item.get("positionText") or (str(pos) if pos else None),
            "driverId": driver_block.get("driverId") if driver_block else None,
            "points": pts,
            "extra": {"raw": driver_block, "leftover": extra}
        }
    else:
        constructor_block = extra.pop("Constructor", {})
        return {
            "positionDisplayOrder": pos,
            "positionNumber": pos,
            "positionText": s_item.get("positionText") or (str(pos) if pos else None),
            "constructorId": constructor_block.get("constructorId") if constructor_block else None,
            "engineManufacturerId": None,
            "points": pts,
            "extra": {"raw": constructor_block, "leftover": extra}
        }

# ---------------- Ergast fetchers ----------------

async def fetch_race_results(client: AsyncHTTPClient, year: int, round_num: int) -> Optional[List[Dict[str, Any]]]:
    url = f"{ERGAST_BASE}/{year}/{round_num}/results.json"
    data = await client.get_json(url)
    if not data:
        return None
    raceslist = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not raceslist:
        return None
    results = raceslist[0].get("Results", []) if raceslist else []
    return [map_ergast_result_item_to_schema(r) for r in results]

async def fetch_qualifying_results(client: AsyncHTTPClient, year: int, round_num: int) -> Optional[List[Dict[str, Any]]]:
    url = f"{ERGAST_BASE}/{year}/{round_num}/qualifying.json"
    data = await client.get_json(url)
    if not data:
        return None
    raceslist = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
    if not raceslist:
        return None
    qualifying = raceslist[0].get("QualifyingResults", []) if raceslist else []
    return [map_ergast_qualifying_result_item_to_schema(q) for q in qualifying]

async def fetch_driver_standings_for_season(client: AsyncHTTPClient, year: int) -> Optional[List[Dict[str, Any]]]:
    url = f"{ERGAST_BASE}/{year}/driverStandings.json"
    data = await client.get_json(url)
    if not data:
        return None
    lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not lists:
        return None
    driver_standings = lists[0].get("DriverStandings", []) if lists else []
    return [map_ergast_standing_item_to_schema(s, typ="driver") for s in driver_standings]

async def fetch_constructor_standings_for_season(client: AsyncHTTPClient, year: int) -> Optional[List[Dict[str, Any]]]:
    url = f"{ERGAST_BASE}/{year}/constructorStandings.json"
    data = await client.get_json(url)
    if not data:
        return None
    lists = data.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
    if not lists:
        return None
    constructor_standings = lists[0].get("ConstructorStandings", []) if lists else []
    return [map_ergast_standing_item_to_schema(s, typ="constructor") for s in constructor_standings]

# ---------------- OpenF1 enrichment ----------------

async def fetch_openf1_meetings(client: AsyncHTTPClient, year: Optional[int] = None):
    url = f"{OPENF1_BASE}/meetings"
    params = {"year": year} if year else {}
    return await client.get_json(url, params=params)

async def fetch_openf1_laps_for_meeting(client: AsyncHTTPClient, meeting_key: str):
    url = f"{OPENF1_BASE}/laps"
    return await client.get_json(url, params={"meeting_key": meeting_key})

async def openf1_enrich_race(client: AsyncHTTPClient, race: Dict[str, Any], meetings_index: Dict[Tuple[int, str], str]):
    year = race.get("year")
    name = (race.get("officialName") or "").lower()
    key = None
    if (year, name) in meetings_index:
        key = meetings_index[(year, name)]
    else:
        for (myear, mname), mk in meetings_index.items():
            if myear != year:
                continue
            if mname in name or name in mname:
                key = mk
                break
    if key:
        laps = await fetch_openf1_laps_for_meeting(client, key)
        race["laps"] = laps
    return race

# ---------------- FastF1 enrichment ----------------

async def enrich_with_fastf1_threaded(race_obj: Dict[str, Any]) -> Dict[str, Any]:
    if not FASTF1_AVAILABLE:
        return race_obj
    year = race_obj.get("year")
    round_num = race_obj.get("round")
    try:
        try:
            fastf1.Cache.enable_cache("fastf1_cache")
        except Exception:
            pass
        def blocking():
            try:
                session = fastf1.get_session(year, round_num, "R")
                session.load(telemetry=False, laps=True, weather=False, messages=False)
                laps = session.laps
                fastest = []
                if not laps.empty:
                    grouped = laps.groupby("Driver")
                    for d, group in grouped:
                        best = group.nsmallest(1, "LapTime")
                        if not best.empty:
                            row = best.iloc[0]
                            lt = row.get("LapTime")
                            fastest.append({
                                "driverId": str(d),
                                "time": str(lt) if lt is not None else None,
                                "timeMillis": int(lt.total_seconds() * 1000) if lt is not None else None,
                                "lapNumber": int(row.get("LapNumber")) if row.get("LapNumber") is not None else None,
                            })
                pits_list = []
                try:
                    pits = session.pits
                    if not pits.empty:
                        for _, p in pits.iterrows():
                            dur = p.get("Duration")
                            dur_ms = int(dur.total_seconds() * 1000) if dur is not None else None
                            pits_list.append({
                                "driverId": p.get("Driver"),
                                "lap": int(p.get("Lap")) if p.get("Lap") is not None else None,
                                "stop": int(p.get("Stop")) if p.get("Stop") is not None else None,
                                "time": str(p.get("Time")) if p.get("Time") is not None else None,
                                "duration": str(dur) if dur is not None else None,
                                "durationMillis": dur_ms,
                                "tyre": p.get("Compound") if "Compound" in p else None
                            })
                except Exception:
                    pits_list = None
                return {"fastestLaps": fastest or None, "pitStops": pits_list or None}
            except Exception as e:
                logger.debug("FastF1 blocking error for %s R%s: %s", year, round_num, e)
                return {"fastestLaps": None, "pitStops": None}
        result = await asyncio.to_thread(blocking)
        race_obj.update(result)
    except Exception as e:
        logger.exception("Failed FastF1 enrichment for %s R%s: %s", year, round_num, e)
    return race_obj

# ---------------- Aggregation: compute stats ----------------

def aggregate_stats(drivers_list: List[Dict[str, Any]], constructors_list: List[Dict[str, Any]], races: List[Dict[str, Any]], seasons: List[Dict[str, Any]]):
    driver_idx = {d["id"]: d for d in drivers_list}
    constructor_idx = {c["id"]: c for c in constructors_list}
    driver_counters = defaultdict(lambda: {"starts": 0, "wins": 0, "podiums": 0, "points": 0.0, "fastest_laps": 0})
    constructor_counters = defaultdict(lambda: {"starts": 0, "wins": 0, "podiums": 0, "points": 0.0, "fastest_laps": 0})
    for r in races:
        results = r.get("raceResults") or []
        for res in results:
            did = res.get("driverId")
            cid = res.get("constructorId")
            pos = res.get("positionNumber")
            pts = float(res.get("points") or 0.0)
            if did:
                driver_counters[did]["starts"] += 1
                driver_counters[did]["points"] += pts
                if pos == 1:
                    driver_counters[did]["wins"] += 1
                if pos and pos <= 3:
                    driver_counters[did]["podiums"] += 1
            if cid:
                constructor_counters[cid]["starts"] += 1
                constructor_counters[cid]["points"] += pts
                if pos == 1:
                    constructor_counters[cid]["wins"] += 1
                if pos and pos <= 3:
                    constructor_counters[cid]["podiums"] += 1
            extra = res.get("extra", {})
            raw = extra.get("raw", {})
            fastest = raw.get("fastestLap")
            if fastest:
                if did:
                    driver_counters[did]["fastest_laps"] += 1
                if cid:
                    constructor_counters[cid]["fastest_laps"] += 1
    driver_champ_counts = defaultdict(int)
    constructor_champ_counts = defaultdict(int)
    for s in seasons:
        ds = s.get("driverStandings") or []
        cs = s.get("constructorStandings") or []
        for d in ds:
            if d.get("positionNumber") == 1 and d.get("driverId"):
                driver_champ_counts[d["driverId"]] += 1
        for c in cs:
            if c.get("positionNumber") == 1 and c.get("constructorId"):
                constructor_champ_counts[c["constructorId"]] += 1
    for did, counters in driver_counters.items():
        if did in driver_idx:
            d = driver_idx[did]
            d["totalRaceStarts"] = counters["starts"]
            d["totalRaceWins"] = counters["wins"]
            d["totalPodiums"] = counters["podiums"]
            d["totalPoints"] = round(counters["points"], 3)
            d["totalFastestLaps"] = counters["fastest_laps"]
            d["totalChampionshipWins"] = driver_champ_counts.get(did, 0)
    for cid, counters in constructor_counters.items():
        if cid in constructor_idx:
            c = constructor_idx[cid]
            c["totalRaceStarts"] = counters["starts"]
            c["totalRaceWins"] = counters["wins"]
            c["totalPodiums"] = counters["podiums"]
            c["totalPoints"] = round(counters["points"], 3)
            c["totalFastestLaps"] = counters["fastest_laps"]
            c["totalChampionshipWins"] = constructor_champ_counts.get(cid, 0)

# ---------------- Main orchestrator ----------------

async def orchestrate(
    schema_path: str,
    output_path: str,
    year_start: int,
    year_end: int,
    use_fastf1: bool,
    use_openf1: bool,
    concurrency: int,
    cache_dir: Optional[str],
    resume: bool
):
    if not os.path.exists(schema_path):
        logger.error("Schema file not found: %s", schema_path)
        raise SystemExit(1)
    with open(schema_path, "r", encoding="utf-8") as fh:
        schema = json.load(fh)
    resolver = RefResolver(base_uri=f"file://" + os.path.abspath(schema_path), referrer=schema)
    validator = Draft7Validator(schema, resolver=resolver)

    async with AsyncHTTPClient(concurrency=concurrency, cache_dir=cache_dir, resume=resume) as client:
        # fetch drivers/constructors/circuits
        logger.info("Fetching drivers, constructors, circuits (paginated)")
        drivers_raw_task = asyncio.create_task(ergast_fetch_all(client, "drivers", ["DriverTable", "Drivers"]))
        constructors_raw_task = asyncio.create_task(ergast_fetch_all(client, "constructors", ["ConstructorTable", "Constructors"]))
        circuits_raw_task = asyncio.create_task(ergast_fetch_all(client, "circuits", ["CircuitTable", "Circuits"]))
        drivers_raw, constructors_raw, circuits_raw = await asyncio.gather(drivers_raw_task, constructors_raw_task, circuits_raw_task)
        drivers = []
        seen_drivers = set()
        for d in drivers_raw:
            m = map_ergast_driver_to_schema(d)
            if m["id"] not in seen_drivers:
                drivers.append(m)
                seen_drivers.add(m["id"])
        constructors = [map_ergast_constructor_to_schema(c) for c in constructors_raw]
        circuits = [map_ergast_circuit_to_schema(c) for c in circuits_raw]

        # fetch season race lists concurrently
        logger.info("Fetching season race lists for %d-%d", year_start, year_end)
        season_tasks = []
        for year in range(year_start, year_end + 1):
            url = f"{ERGAST_BASE}/{year}.json"
            season_tasks.append(asyncio.create_task(client.get_json(url)))
        races: List[Dict[str, Any]] = []
        for fut in tqdm(asyncio.as_completed(season_tasks), total=len(season_tasks), desc="Season lists"):
            data = await fut
            if not data:
                continue
            rlist = data.get("MRData", {}).get("RaceTable", {}).get("Races", []) or []
            for r in rlist:
                races.append(map_ergast_race_to_schema(r))
            await asyncio.sleep(0.01)

        # seasons skeleton
        seasons = []
        for year in range(year_start, year_end + 1):
            seasons.append({
                "year": year,
                "entrants": None,
                "constructors": None,
                "engineManufacturers": None,
                "tyreManufacturers": None,
                "drivers": None,
                "driverStandings": None,
                "constructorStandings": None
            })

        # grandsPrix aggregate
        gp_map = {}
        for r in races:
            gp_id = r["grandPrixId"]
            if gp_id not in gp_map:
                gp_map[gp_id] = {
                    "id": gp_id,
                    "name": r.get("officialName"),
                    "fullName": r.get("officialName"),
                    "shortName": (r.get("officialName") or "")[:20],
                    "abbreviation": ((r.get("officialName") or "")[:3].upper() if r.get("officialName") else "GPX"),
                    "countryId": None,
                    "totalRacesHeld": 1
                }
            else:
                gp_map[gp_id]["totalRacesHeld"] += 1
        grands_prix = list(gp_map.values())

        # enrich races: results + qualifying (batched)
        logger.info("Enriching races with results & qualifying (batched)")
        race_index = {(r["year"], r["round"]): idx for idx, r in enumerate(races)}
        total_races = len(races)
        BATCH_SIZE = max(16, concurrency)
        for i in tqdm(range(0, total_races, BATCH_SIZE), desc="Race enrichment batches"):
            batch = races[i:i+BATCH_SIZE]
            result_coros = [fetch_race_results(client, r["year"], r["round"]) for r in batch]
            qual_coros = [fetch_qualifying_results(client, r["year"], r["round"]) for r in batch]
            batch_results = await asyncio.gather(*result_coros)
            batch_quals = await asyncio.gather(*qual_coros)
            for j, r in enumerate(batch):
                idx = race_index.get((r["year"], r["round"]))
                if idx is None:
                    continue
                races[idx]["raceResults"] = batch_results[j] if batch_results[j] is not None else None
                races[idx]["qualifyingResults"] = batch_quals[j] if batch_quals[j] is not None else None
            await asyncio.sleep(0.01)

        # FastF1 enrichment
        if use_fastf1 and FASTF1_AVAILABLE:
            logger.info("Running FastF1 enrichment for 2018+ (threaded)")
            ff_sem = asyncio.Semaphore(4)
            async def ff_wrapper(r):
                if r.get("year", 0) >= 2018:
                    async with ff_sem:
                        return await enrich_with_fastf1_threaded(r)
                return r
            ff_tasks = [asyncio.create_task(ff_wrapper(r)) for r in races]
            for fut in tqdm(asyncio.as_completed(ff_tasks), total=len(ff_tasks), desc="FastF1 tasks"):
                res = await fut
                key = (res.get("year"), res.get("round"))
                idx = race_index.get(key)
                if idx is not None:
                    if res.get("fastestLaps") is not None:
                        races[idx]["fastestLaps"] = res["fastestLaps"]
                    if res.get("pitStops") is not None:
                        races[idx]["pitStops"] = res["pitStops"]
        else:
            if use_fastf1 and not FASTF1_AVAILABLE:
                logger.warning("FastF1 requested but not installed. Skipping FastF1 enrichment.")
            else:
                logger.info("FastF1 enrichment disabled.")

        # season standings concurrently 
        logger.info("Fetching season standings concurrently")
        years = list(range(year_start, year_end + 1))
        season_B = max(8, concurrency)
        driver_standings_all = {}
        constructor_standings_all = {}
        for i in tqdm(range(0, len(years), season_B), desc="Standings batches"):
            batch_years = years[i:i+season_B]
            driver_coros = [fetch_driver_standings_for_season(client, y) for y in batch_years]
            constructor_coros = [fetch_constructor_standings_for_season(client, y) for y in batch_years]
            driver_res = await asyncio.gather(*driver_coros)
            constructor_res = await asyncio.gather(*constructor_coros)
            for k, y in enumerate(batch_years):
                driver_standings_all[y] = driver_res[k]
                constructor_standings_all[y] = constructor_res[k]
            await asyncio.sleep(0.02)
        for s in seasons:
            y = s["year"]
            s["driverStandings"] = driver_standings_all.get(y)
            s["constructorStandings"] = constructor_standings_all.get(y)

        # OpenF1 enrichment 
        if use_openf1:
            logger.info("Attempting OpenF1 enrichment for recent years (best-effort)")
            meetings_index = {}
            for y in range(max(year_start, year_end-3), year_end+1):
                meetings = await fetch_openf1_meetings(client, year=y)
                if isinstance(meetings, list):
                    for m in meetings:
                        mk = m.get("meeting_key")
                        mname = (m.get("meeting_name") or "").lower()
                        if mk and mname:
                            meetings_index[(y, mname)] = mk
                await asyncio.sleep(0.01)
            of_batch = max(8, concurrency//2)
            for i in tqdm(range(0, total_races, of_batch), desc="OpenF1 batches"):
                batch = races[i:i+of_batch]
                coros = [openf1_enrich_race(client, r, meetings_index) for r in batch]
                enriched = await asyncio.gather(*coros)
                for j, r in enumerate(enriched):
                    idx = race_index.get((r.get("year"), r.get("round")))
                    if idx is not None:
                        races[idx].update(r)
                await asyncio.sleep(0.01)
        else:
            logger.info("OpenF1 enrichment disabled.")

        # aggregate stats
        logger.info("Aggregating driver & constructor statistics")
        aggregate_stats(drivers, constructors, races, seasons)

        # final assembly
        f1db = {
            "drivers": drivers,
            "constructors": constructors,
            "chassis": [],
            "engineManufacturers": [],
            "engines": [],
            "tyreManufacturers": [],
            "entrants": [],
            "circuits": circuits,
            "grandsPrix": grands_prix,
            "seasons": seasons,
            "races": races,
            "continents": [],
            "countries": []
        }

        # Validate using jsonschema 
        logger.info("Validating result against schema . Expect missing-field warnings due to schema strictness.")
        errors = list(validator.iter_errors(f1db))
        logger.info("Validation errors: %d (showing up to 20)", len(errors))
        for e in errors[:20]:
            logger.warning("Validation error: path=%s message=%s", list(e.absolute_path), e.message)

        # write output
        logger.info("Writing output JSON to %s", output_path)
        async with aiofiles.open(output_path, "w", encoding="utf-8") as outf:
            await outf.write(json.dumps(f1db, indent=2, default=str))

        logger.info("Done. Dataset saved to %s", output_path)
        if errors:
            logger.info("There were validation errors. Use them to guide additional mapping of schema-required fields.")

# ---------------- Ergast paginated fetch helper (used above) ----------------

async def ergast_fetch_all(client: AsyncHTTPClient, path: str, result_path: List[str], per_page: int = ERGAST_PAGE_LIMIT) -> List[Dict[str, Any]]:
    base_url = f"{ERGAST_BASE}/{path}.json"
    items: List[Dict[str, Any]] = []
    offset = 0
    params = {"limit": per_page, "offset": offset}
    data = await client.get_json(base_url, params=params)
    if not data:
        return items
    mr = data.get("MRData", {})
    total = int(mr.get("total", "0"))
    cur = mr
    for p in result_path:
        cur = cur.get(p, {})
    if isinstance(cur, list):
        items.extend(cur)
    if total > per_page:
        pages = math.ceil(total / per_page)
        tasks = []
        for page_idx in range(1, pages):
            offs = page_idx * per_page
            params = {"limit": per_page, "offset": offs}
            tasks.append(asyncio.create_task(client.get_json(base_url, params=params)))
        for fut in asyncio.as_completed(tasks):
            page_data = await fut
            if not page_data:
                continue
            mr = page_data.get("MRData", {})
            cur = mr
            for p in result_path:
                cur = cur.get(p, {})
            if isinstance(cur, list):
                items.extend(cur)
            await asyncio.sleep(0.02)
    logger.info("Ergast fetched %d items from %s", len(items), path)
    return items

# ---------------- CLI ----------------
def parse_args():
    p = argparse.ArgumentParser(description="Full async F1 orchestrator (Ergast + FastF1 + OpenF1) with caching and aggregation")
    p.add_argument("--schema", default=DEFAULT_SCHEMA_PATH, help="Path to f1db.schema.json")
    p.add_argument("--out", default=DEFAULT_OUTPUT, help="Output JSON file")
    p.add_argument("--from-year", type=int, default=1950, help="Start year inclusive")
    p.add_argument("--to-year", type=int, default=2025, help="End year inclusive")
    p.add_argument("--no-fastf1", dest="fastf1", action="store_false", help="Disable FastF1 enrichment")
    p.add_argument("--openf1", action="store_true", help="Enable OpenF1 enrichment")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="HTTP concurrency")
    p.add_argument("--cache-dir", default="cache", help="Directory for cached API responses")
    p.add_argument("--resume", action="store_true", help="Resume using cached responses when available")
    return p.parse_args()

def main():
    args = parse_args()
    logger.info("Starting orchestrator for %d-%d (fastf1=%s, openf1=%s) cache_dir=%s resume=%s", args.from_year, args.to_year, args.fastf1, args.openf1, args.cache_dir, args.resume)
    asyncio.run(orchestrate(
        schema_path=args.schema,
        output_path=args.out,
        year_start=args.from_year,
        year_end=args.to_year,
        use_fastf1=args.fastf1,
        use_openf1=args.openf1,
        concurrency=args.concurrency,
        cache_dir=args.cache_dir,
        resume=args.resume
    ))

if __name__ == "__main__":
    main()

