import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.request

BASE = "https://api.tmdb.org/3/movie/{id}?api_key={key}&language=en-US"
FIELDS = [
    "id", "title", "release_date", "budget", "revenue", "runtime", "status",
    "original_language", "overview", "popularity", "vote_average", "vote_count",
]


def fetch(tmdb_id, key, retries=3):
    url = BASE.format(id=tmdb_id, key=key)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 429:          # bị giới hạn tốc độ: chờ rồi thử lại
                time.sleep(2 * (attempt + 1))
                continue
            raise
    return None


def read_tmdb_ids(links_csv, limit):
    ids = []
    with open(links_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["tmdbId"]:
                ids.append(int(row["tmdbId"]))
            if len(ids) >= limit:
                break
    return ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p-date", required=True, help="Ngày chạy, dạng YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=300, help="Số phim cần lấy")
    ap.add_argument("--data-dir", default="/data")
    args = ap.parse_args()

    key = os.environ["TMDB_API_KEY"]
    links = os.path.join(args.data_dir, "landing/movielens/ml-latest-small/links.csv")
    out_dir = os.path.join(args.data_dir, "landing/tmdb/movie_details", f"p_date={args.p_date}")
    os.makedirs(out_dir, exist_ok=True)

    ids = read_tmdb_ids(links, args.limit)
    written = 0
    with open(os.path.join(out_dir, "movies.jsonl"), "w", encoding="utf-8") as f:
        for tmdb_id in ids:
            d = fetch(tmdb_id, key)
            if d is None:
                continue
            row = {k: d.get(k) for k in FIELDS}
            row["genres"] = "|".join(sorted(g["name"] for g in d.get("genres", [])))
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            written += 1
            time.sleep(0.05)
    print(f"Đã ghi {written} phim vào {out_dir}")


if __name__ == "__main__":
    main()