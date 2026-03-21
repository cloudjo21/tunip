"""Unit tests for log_error decorator."""

import logging
from unittest import mock

import pytest

from tunip.log_decorators import log_error


class TestLogErrorDecorator:
    """Tests for log_error decorator."""

    @pytest.fixture
    def logger(self) -> mock.Mock:
        return mock.Mock(spec=logging.Logger)

    def test_log_error_reraises_type_error(self, logger: mock.Mock) -> None:
        """Tests that TypeError is logged and re-raised."""

        class Operation:
            def sub(self, a, b):
                return a - b

            @log_error(logger=logger)
            def ho(self, a, b):
                return self.sub(a, b)

        with pytest.raises(TypeError):
            Operation().ho(5, "1")
        logger.error.assert_called_once()

    def test_log_error_reraises_key_error(self, logger: mock.Mock) -> None:
        """Tests that KeyError is logged and re-raised."""
        x: dict[int, str] = {}

        @log_error(logger=logger)
        def get(index):
            return x[index]

        with pytest.raises(KeyError):
            get(0)
        logger.error.assert_called_once()

    def test_log_error_calls_sender_on_error(self, logger: mock.Mock) -> None:
        """Tests that sender is called with error message keyword arg when exception occurs."""
        sender = mock.Mock()
        x: dict[int, str] = {}

        @log_error(logger=logger, sender=sender)
        def get(index):
            return x[index]

        with pytest.raises(KeyError):
            get(0)
        sender.assert_called_once()
        _, call_kwargs = sender.call_args
        assert "message" in call_kwargs

    def test_log_error_without_sender_does_not_raise_on_missing_sender(
        self, logger: mock.Mock
    ) -> None:
        """Tests that omitting sender does not cause additional errors."""
        x: dict[int, str] = {}

        @log_error(logger=logger)
        def get(index):
            return x[index]

        with pytest.raises(KeyError):
            get(0)
        logger.error.assert_called_once()
