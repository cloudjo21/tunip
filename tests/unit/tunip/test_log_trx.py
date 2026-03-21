"""Unit tests for LogTransmitter."""

import base64
from unittest import mock

import pytest

from tunip.log_trx import LogTransmitter


class TestLogTransmitter:
    """Tests for LogTransmitter class."""

    @pytest.fixture
    def mock_publisher(self) -> mock.Mock:
        return mock.Mock()

    @pytest.fixture
    def transmitter(self, mock_publisher: mock.Mock) -> LogTransmitter:
        return LogTransmitter(name="log_transmit", publisher=mock_publisher)

    def test_init_creates_transmitter(
        self, transmitter: LogTransmitter, mock_publisher: mock.Mock
    ) -> None:
        """Tests that LogTransmitter stores constructor arguments correctly."""
        assert transmitter is not None
        assert transmitter.name == "log_transmit"
        assert transmitter.publisher is mock_publisher
        assert transmitter.use_encoding is True

    def test_call_publishes_message(
        self, transmitter: LogTransmitter, mock_publisher: mock.Mock
    ) -> None:
        """Tests that __call__ invokes publisher.publish exactly once."""
        transmitter("error occurred")
        mock_publisher.publish.assert_called_once()

    def test_call_encodes_payload_as_base64(
        self, transmitter: LogTransmitter, mock_publisher: mock.Mock
    ) -> None:
        """Tests that __call__ passes base64-encoded bytes as payload."""
        message = "error occurred"
        transmitter(message)

        log_request = mock_publisher.publish.call_args.args[0]
        expected = base64.b64encode(message.encode("utf-8")).decode("utf-8")
        assert log_request.payload == expected

    def test_call_without_encoding_passes_raw_message(
        self, mock_publisher: mock.Mock
    ) -> None:
        """Tests that use_encoding=False passes the raw message as payload."""
        transmitter = LogTransmitter(
            name="log_transmit", publisher=mock_publisher, use_encoding=False
        )
        message = "raw message"
        transmitter(message)

        log_request = mock_publisher.publish.call_args.args[0]
        assert log_request.payload == message
