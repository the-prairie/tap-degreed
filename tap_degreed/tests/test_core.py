"""Tests standard tap features using the built-in SDK tests library."""

import datetime

import json
import os



with open('.secrets/config.json', 'r') as f:
    config = json.load(f)

from singer_sdk.testing import get_standard_tap_tests

from tap_degreed.tap import TapDegreed

SAMPLE_CONFIG = {
    "start_date": "2021-11-01",
    "client_id":  config["TAP_DEGREED_CLIENT_ID"],
    "client_secret": config["TAP_DEGREED_CLIENT_SECRET"],
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
