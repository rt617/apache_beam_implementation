REM 1) Clone the repo
git clone https://github.com/rt617/apache_beam_implementation.git
cd virginmedia-beam

REM 2) (Optional) Create and activate a virtualenv
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install --upgrade pip

REM 3) Install dependencies
pip install -r requirements.txt

REM 4) Download the sample CSV (no GCP account needed)
mkdir data
curl -L "https://storage.googleapis.com/cloud-samples-data/bigquery/sample-transactions/transactions.csv" -o "data\transactions.csv"

REM 5) Run the Apache Beam pipeline (DirectRunner)
python main.py --input data\transactions.csv --output_dir output

REM 6) Run Unit Test
pytest -q
