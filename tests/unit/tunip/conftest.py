"""Mock google cloud packages that are not installed in the test environment."""

import sys
from unittest import mock

# The following google.cloud sub-packages are not installed; inject mocks
# before test modules are collected so the import chain does not fail.
# google.oauth2 and googleapiclient ARE installed — do not patch them.
for _module in (
    "google.cloud.pubsub",
    "google.cloud.bigquery",
    "google.cloud.storage",
    "google.cloud.storage.fileio",
):
    sys.modules.setdefault(_module, mock.MagicMock())
