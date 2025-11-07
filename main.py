# main.py
import argparse, csv, json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# If your file is named transform.py, use this import:
from transform import SumByDateTransform
# If you kept the earlier name transforms.py, switch to:
# from transforms import SumByDateTransform

HEADERS = ["timestamp", "origin", "destination", "transaction_amount"]

def parse_csv_line(line: str):
    """Parse a CSV line into a dict using your schema."""
    return next(csv.DictReader([line], fieldnames=HEADERS))

def to_jsonl(kv):
    date_str, total = kv
    # round to 2 decimals for clean output
    return json.dumps({"date": date_str, "total_amount": round(float(total), 2)})

def run(input_path: str, output_dir: str):
    opts = PipelineOptions(["--runner=DirectRunner"])
    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadCSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "Parse" >> beam.Map(parse_csv_line)
            | "Transform" >> SumByDateTransform(min_amount=20.0, min_year=2010)
            | "ToJSONL" >> beam.Map(to_jsonl)
            | "WriteJSONLGZ" >> beam.io.WriteToText(
                file_path_prefix=f"{output_dir}/results",
                file_name_suffix=".jsonl.gz",
                compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                num_shards=1,
            )
        )

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to CSV (local or gs://)")
    ap.add_argument("--output_dir", default="output")
    args = ap.parse_args()
    run(args.input, args.output_dir)
