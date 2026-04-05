import argparse
import csv
import json
from pathlib import Path


def clean_text(value):
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\r", " ").replace("\n", " ").strip()
    return text


def clean_joined(value):
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join(clean_text(item) for item in value)
    return clean_text(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input JSONL file path")
    parser.add_argument("--output", required=True, help="Output CSV file path")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []

    with input_path.open("r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            rows.append({
                "id": clean_text(row.get("id")),
                "subreddit": clean_text(row.get("subreddit")),
                "created_utc": clean_text(row.get("created_utc")),
                "title": clean_text(row.get("title")),
                "selftext": clean_text(row.get("selftext")),
                "author": clean_text(row.get("author")),
                "score": clean_text(row.get("score")),
                "num_comments": clean_text(row.get("num_comments")),
                "student_score": clean_text(row.get("student_score")),
                "life_score": clean_text(row.get("life_score")),
                "foreigner_score": clean_text(row.get("foreigner_score")),
                "context_score": clean_text(row.get("context_score")),
                "question_like": clean_text(row.get("question_like")),
                "is_question": clean_text(row.get("is_question")),
                "question_source": clean_text(row.get("question_source")),
                "filter_version": clean_text(row.get("filter_version")),
                "refine_filter_version": clean_text(row.get("refine_filter_version")),
                "refine_subreddit": clean_text(row.get("refine_subreddit")),
                "refine_reasons": clean_joined(row.get("refine_reasons")),
                "matched_korea_keywords": clean_joined(row.get("matched_korea_keywords")),
                "matched_student_keywords": clean_joined(row.get("matched_student_keywords")),
                "matched_visa_keywords": clean_joined(row.get("matched_visa_keywords")),
                "permalink": clean_text(row.get("permalink")),
                "url": clean_text(row.get("url")),
            })

    fieldnames = [
        "id",
        "subreddit",
        "created_utc",
        "title",
        "selftext",
        "author",
        "score",
        "num_comments",
        "student_score",
        "life_score",
        "foreigner_score",
        "context_score",
        "question_like",
        "is_question",
        "question_source",
        "filter_version",
        "refine_filter_version",
        "refine_subreddit",
        "refine_reasons",
        "matched_korea_keywords",
        "matched_student_keywords",
        "matched_visa_keywords",
        "permalink",
        "url",
    ]

    with output_path.open("w", encoding="utf-8-sig", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
    
