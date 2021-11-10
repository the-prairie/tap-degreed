"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import json

with open('.secrets/config.json', 'r') as f:
    config = json.load(f)

from singer_sdk.testing import get_standard_tap_tests

from tap_degreed.tap import TapDegreed

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "client_id": config['client_id'],
    "client_secret": config['client_secret'],
    # Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapDegreed,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
