# tests/test_transforms.py
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # so 'transform' can be imported

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from transform import SumByDateTransform

def test_sum_by_date_transform():
    # three rows: one filtered by amount (<=20), one filtered by year (<2010),
    # two that land on different valid dates
    sample = [
        {"timestamp": "2011-01-01 00:00:00 UTC", "origin":"A", "destination":"B", "transaction_amount": "25"},
        {"timestamp": "2011-01-01 01:00:00 UTC", "origin":"A", "destination":"B", "transaction_amount": "5"},   # <=20 -> filtered out
        {"timestamp": "2009-12-31 23:59:59 UTC", "origin":"B", "destination":"C", "transaction_amount": "100"}, # year < 2010 -> filtered out
        {"timestamp": "2011-02-02 12:30:00 UTC", "origin":"C", "destination":"D", "transaction_amount": "50"},
    ]

    with TestPipeline() as p:
        out = (
            p
            | beam.Create(sample)
            | SumByDateTransform(min_amount=20.0, min_year=2010)
        )
        # Beam's equal_to ignores ordering for small PCollections
        assert_that(out, equal_to([
            ("2011-01-01", 25.0),
            ("2011-02-02", 50.0),
        ]))
