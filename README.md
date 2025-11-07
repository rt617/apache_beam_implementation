# Apache Beam Data Engineer Tech Test

This repository contains a simple Apache Beam (Python) batch job that:
- Reads a public CSV of sample transactions  
- Filters `transaction_amount > 20`  
- Excludes transactions before the year 2010  
- Aggregates the total by date (`YYYY-MM-DD`)  
- Writes gzipped JSONL output to `output/`

---

# Windows Command Prompt

```bat
REM 1) Clone the repository
git clone https://github.com/rt617/apache_beam_implementation.git
cd apache_beam_implementation

REM 2) Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip

REM 3) Install dependencies
pip install -r requirements.txt

REM 4) Download the sample CSV (no GCP account required)
mkdir data
curl -L "https://storage.googleapis.com/cloud-samples-data/bigquery/sample-transactions/transactions.csv" -o "data\transactions.csv"

REM 5) Run the Apache Beam pipeline using DirectRunner
python main.py --input data\transactions.csv --output_dir output

REM 6) Run the unit test
pytest -q
