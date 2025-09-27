import argparse
import json
import pandas as pd
from collections import defaultdict


def build_driver_summaries(f1db):
    """Aggregate driver stats into career totals and yearly breakdowns."""
    drivers_summary = defaultdict(lambda: {
        "driverId": None,
        "fullName": None,
        "nationality": None,
        "total_championship_wins": 0,
        "total_race_wins": 0,
        "total_podiums": 0,
        "total_pole_positions": 0,
        "total_points": 0.0,
        "total_race_starts": 0,
        "total_race_entries": 0,
        "career_start": None,
        "career_end": None,
        "seasons_competed": set(),
    })

    driver_year_summary = defaultdict(lambda: defaultdict(lambda: {
        "driverId": None,
        "fullName": None,
        "year": None,
        "race_starts": 0,
        "wins": 0,
        "podiums": 0,
        "poles": 0,
        "points": 0.0,
    }))

    for season in f1db.get("seasons", []):
        year = season["year"]
        
        # standings: driver championships
        
        for ds in season.get("driverStandings", []):
            if ds.get("position") == 1:
                drivers_summary[ds["driverId"]]["total_championship_wins"] += 1
        
        # races
        
        for race in season.get("races", []):
            round_year = year
            for result in race.get("raceResults", []):
                d_id = result["driverId"]
                d_name = result.get("driverName", "")
                entry = drivers_summary[d_id]
                entry["driverId"] = d_id
                entry["fullName"] = d_name
                entry["nationality"] = result.get("driverNationality")
                entry["total_race_entries"] += 1
                entry["total_race_starts"] += 1
                entry["total_points"] += float(result.get("points", 0))
                entry["career_start"] = (
                    round_year
                    if not entry["career_start"]
                    else min(entry["career_start"], round_year)
                )
                entry["career_end"] = (
                    round_year
                    if not entry["career_end"]
                    else max(entry["career_end"], round_year)
                )
                entry["seasons_competed"].add(round_year)
                
                # wins, podiums
                
                if int(result.get("position", 0)) == 1:
                    entry["total_race_wins"] += 1
                    driver_year_summary[d_id][year]["wins"] += 1
                if int(result.get("position", 0)) <= 3:
                    entry["total_podiums"] += 1
                    driver_year_summary[d_id][year]["podiums"] += 1
                
                # points
                
                driver_year_summary[d_id][year]["points"] += float(result.get("points", 0))
                driver_year_summary[d_id][year]["race_starts"] += 1
                driver_year_summary[d_id][year]["driverId"] = d_id
                driver_year_summary[d_id][year]["fullName"] = d_name
                driver_year_summary[d_id][year]["year"] = year
                
            # qualifying: poles
            
            for q in race.get("qualifyingResults", []):
                if int(q.get("position", 0)) == 1:
                    d_id = q["driverId"]
                    drivers_summary[d_id]["total_pole_positions"] += 1
                    driver_year_summary[d_id][year]["poles"] += 1

    
# finalize summaries

    rows = []
    for d_id, data in drivers_summary.items():
        row = data.copy()
        row["seasons_competed"] = len(data["seasons_competed"])
        rows.append(row)

    year_rows = []
    for d_id, years in driver_year_summary.items():
        for y, data in years.items():
            year_rows.append(data)

    return pd.DataFrame(rows), pd.DataFrame(year_rows)


def main():
    parser = argparse.ArgumentParser(description="Transform F1DB JSON to CSV summaries")
    parser.add_argument("json_file", help="f1db.json")
    parser.add_argument("--out-dir", default="data", help="Output directory (default: data)")
    args = parser.parse_args()

    with open(args.json_file, "r", encoding="utf-8") as f:
        f1db = json.load(f)

    df_drivers, df_years = build_driver_summaries(f1db)

    drivers_csv = f"{args.out_dir}/drivers_summary.csv"
    years_csv = f"{args.out_dir}/driver_year_summary.csv"

    df_drivers.to_csv(drivers_csv, index=False)
    df_years.to_csv(years_csv, index=False)

    print(f"✅ Wrote {drivers_csv} and {years_csv}")


if __name__ == "__main__":
    main()
```

