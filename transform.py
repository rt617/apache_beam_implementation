# transform.py  (composite transform for your schema)
import apache_beam as beam
from typing import Dict, Tuple

MIN_AMOUNT_DEFAULT = 20.0
MIN_YEAR_DEFAULT = 2010

def _date_from_timestamp(ts: str) -> str:
    """
    Your timestamp looks like: '2017-03-18 14:09:16 UTC'
    We just need the date part 'YYYY-MM-DD'.
    """
    # First 10 chars are always 'YYYY-MM-DD'
    return ts[:10]

def to_kv_by_date(rec: Dict) -> Tuple[str, float]:
    date_str = _date_from_timestamp(rec["timestamp"])
    return (date_str, float(rec["transaction_amount"]))

class SumByDateTransform(beam.PTransform):
    """Filter amount > 20, year >= 2010; then sum amounts by date (YYYY-MM-DD)."""
    def __init__(self, min_amount: float = MIN_AMOUNT_DEFAULT, min_year: int = MIN_YEAR_DEFAULT):
        super().__init__()
        self.min_amount = float(min_amount)
        self.min_year = int(min_year)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            # amount > 20
            | "FilterAmount" >> beam.Filter(
                lambda r: float(r["transaction_amount"]) > self.min_amount
            )
            # year >= 2010 (first 4 chars of timestamp are the year)
            | "FilterYear" >> beam.Filter(
                lambda r: int(r["timestamp"][:4]) >= self.min_year
            )
            | "ToKV" >> beam.Map(to_kv_by_date)
            | "SumByDate" >> beam.CombinePerKey(sum)
        )
