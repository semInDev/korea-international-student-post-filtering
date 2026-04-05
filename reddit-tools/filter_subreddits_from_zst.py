#!/usr/bin/env python3
import argparse
import io
import json
from pathlib import Path
import sys

try:
    import zstandard as zstd
except ImportError:
    print("Please install zstandard: pip install zstandard", file=sys.stderr)
    raise


def normalize_subreddit(value):
    if value is None:
        return None
    value = str(value).strip()
    if value.lower().startswith("r/"):
        value = value[2:]
    return value.lower()


def parse_args():
    p = argparse.ArgumentParser(
        description="Stream a Reddit .zst NDJSON dump and keep only selected subreddits."
    )
    p.add_argument("--input", required=True, help="Path to RC_*.zst or RS_*.zst file")
    p.add_argument("--output", required=True, help="Output JSONL path")
    p.add_argument(
        "--subreddits",
        nargs="+",
        required=True,
        help="Subreddits to keep, e.g. korea korean studyabroad teachinginkorea",
    )
    p.add_argument(
        "--fields",
        nargs="*",
        default=None,
        help="Optional list of fields to keep. If omitted, keep the full JSON object.",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=500000,
        help="Print progress every N lines (default: 500000)",
    )
    return p.parse_args()


def project_fields(obj, fields):
    if not fields:
        return obj
    return {field: obj.get(field) for field in fields}


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    target_subs = {normalize_subreddit(s) for s in args.subreddits}

    read_count = 0
    kept_count = 0
    bad_json_count = 0

    with input_path.open("rb") as fh, output_path.open("w", encoding="utf-8") as out:
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(fh) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for line in text_reader:
                read_count += 1
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    bad_json_count += 1
                    continue

                sub = normalize_subreddit(obj.get("subreddit"))
                if sub in target_subs:
                    kept_count += 1
                    payload = project_fields(obj, args.fields)
                    out.write(json.dumps(payload, ensure_ascii=False) + "\n")

                if args.progress_every and read_count % args.progress_every == 0:
                    print(
                        f"Processed {read_count:,} lines | kept {kept_count:,} | bad_json {bad_json_count:,}",
                        file=sys.stderr,
                    )

    print(
        f"Done. Processed {read_count:,} lines | kept {kept_count:,} | bad_json {bad_json_count:,}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
