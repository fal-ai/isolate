import re
from pathlib import Path
from typing import List, Tuple

from isolate.backends.local import LocalPythonEnvironment
from isolate.connections._local._base import PythonExecutionBase
from isolate.logs import LogLevel, LogSource


class MockPythonExecution(PythonExecutionBase):
    """A concrete implementation of PythonExecutionBase for testing."""

    def __init__(self):
        env = LocalPythonEnvironment()
        super().__init__(
            environment=env,
            environment_path=Path("/fake/path"),
            extra_inheritance_paths=[],
        )
        self.logged_messages: List[Tuple[str, LogLevel, LogSource]] = []

    def get_python_cmd(self, executable, connection, log_fd):  # type: ignore[override]
        return ["python", "-c", "pass"]

    def handle_agent_log(self, line: str, *, level: LogLevel, source: LogSource) -> None:
        self.logged_messages.append((line, level, source))


class TestMkPatterns:
    """Tests for _mk_patterns method."""

    def setup_method(self):
        self.executor = MockPythonExecution()

    def test_empty_env(self):
        """Empty environment should return empty pattern list."""
        patterns = self.executor._mk_patterns({})
        assert patterns == []

    def test_short_values_excluded(self):
        """Values with 8 or fewer characters should not create patterns."""
        env = {
            "SHORT": "12345678",  # exactly 8 chars - should be excluded
            "TINY": "abc",  # 3 chars - should be excluded
        }
        patterns = self.executor._mk_patterns(env)
        assert patterns == []

    def test_long_values_included(self):
        """Values with more than 8 characters should create patterns."""
        env = {
            "SECRET_KEY": "123456789",  # 9 chars - should be included
            "API_TOKEN": "abcdefghij",  # 10 chars - should be included
        }
        patterns = self.executor._mk_patterns(env)
        assert len(patterns) == 2

    def test_path_excluded(self):
        """PATH should be excluded regardless of length."""
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",  # long but should be excluded
            "SECRET": "supersecretvalue",  # should be included
        }
        patterns = self.executor._mk_patterns(env)
        assert len(patterns) == 1
        # Verify the pattern matches the secret, not PATH
        assert patterns[0].search("supersecretvalue")

    def test_special_regex_chars_escaped(self):
        """Special regex characters in values should be escaped."""
        env = {
            "REGEX_VAL": "secret.value+test*",  # contains regex special chars
        }
        patterns = self.executor._mk_patterns(env)
        assert len(patterns) == 1
        # Should match literal string, not as regex
        assert patterns[0].search("secret.value+test*")
        # Should not match regex interpretation
        assert not patterns[0].search("secretXvalueXtestXXX")

    def test_mixed_values(self):
        """Mix of includable and excludable values."""
        env = {
            "PATH": "/very/long/path/value",  # excluded (PATH)
            "SHORT": "abc",  # excluded (too short)
            "EXACT8": "12345678",  # excluded (exactly 8)
            "SECRET1": "123456789",  # included (9 chars)
            "SECRET2": "abcdefghijk",  # included (11 chars)
        }
        patterns = self.executor._mk_patterns(env)
        assert len(patterns) == 2


class TestMaskAgentLog:
    """Tests for _mask_agent_log method."""

    def setup_method(self):
        self.executor = MockPythonExecution()

    def test_masks_secret_in_line(self):
        """Secrets should be replaced with asterisks."""
        secret = "my_secret_api_key_12345"
        patterns = [re.compile(re.escape(secret))]

        self.executor._mask_agent_log(
            f"Connecting with key: {secret}",
            patterns=patterns,
            level=LogLevel.STDOUT,
            source=LogSource.USER,
        )

        assert len(self.executor.logged_messages) == 1
        line, level, source = self.executor.logged_messages[0]
        assert secret not in line
        assert "********" in line
        assert "Connecting with key:" in line
        assert level == LogLevel.STDOUT
        assert source == LogSource.USER

    def test_masks_multiple_secrets(self):
        """Multiple secrets in the same line should all be masked."""
        secret1 = "first_secret_value"
        secret2 = "second_secret_value"
        patterns = [
            re.compile(re.escape(secret1)),
            re.compile(re.escape(secret2)),
        ]

        self.executor._mask_agent_log(
            f"Using {secret1} and {secret2}",
            patterns=patterns,
            level=LogLevel.STDERR,
            source=LogSource.BRIDGE,
        )

        line, level, source = self.executor.logged_messages[0]
        assert secret1 not in line
        assert secret2 not in line
        assert line.count("********") == 2

    def test_short_lines_passed_through(self):
        """Lines with 8 or fewer chars skip pattern matching but still log."""
        patterns = [re.compile(re.escape("longpattern"))]

        self.executor._mask_agent_log(
            "short",
            patterns=patterns,
            level=LogLevel.TRACE,
            source=LogSource.USER,
        )

        line, _, _ = self.executor.logged_messages[0]
        assert line == "short"

    def test_no_patterns_passes_through(self):
        """Lines with no matching patterns should pass through unchanged."""
        patterns = [re.compile(re.escape("not_in_message"))]

        original = "This is a normal log message without secrets"
        self.executor._mask_agent_log(
            original,
            patterns=patterns,
            level=LogLevel.STDOUT,
            source=LogSource.USER,
        )

        line, _, _ = self.executor.logged_messages[0]
        assert line == original

    def test_empty_patterns_list(self):
        """Empty patterns list should pass line through unchanged."""
        original = "Log message with no patterns to match"
        self.executor._mask_agent_log(
            original,
            patterns=[],
            level=LogLevel.STDOUT,
            source=LogSource.USER,
        )

        line, _, _ = self.executor.logged_messages[0]
        assert line == original

    def test_repeated_secret_in_line(self):
        """Same secret appearing multiple times should be masked each time."""
        secret = "repeated_secret"
        patterns = [re.compile(re.escape(secret))]

        self.executor._mask_agent_log(
            f"{secret} appears twice: {secret}",
            patterns=patterns,
            level=LogLevel.STDOUT,
            source=LogSource.USER,
        )

        line, _, _ = self.executor.logged_messages[0]
        assert secret not in line
        assert line.count("********") == 2

    def test_preserves_level_and_source(self):
        """Level and source should be passed to handle_agent_log correctly."""
        test_cases = [
            (LogLevel.STDOUT, LogSource.USER),
            (LogLevel.STDERR, LogSource.USER),
            (LogLevel.TRACE, LogSource.BRIDGE),
        ]

        for level, source in test_cases:
            self.executor.logged_messages.clear()
            self.executor._mask_agent_log(
                "test message",
                patterns=[],
                level=level,
                source=source,
            )
            _, logged_level, logged_source = self.executor.logged_messages[0]
            assert logged_level == level
            assert logged_source == source


class TestIntegration:
    """Integration tests for _mk_patterns and _mask_agent_log together."""

    def setup_method(self):
        self.executor = MockPythonExecution()

    def test_end_to_end_masking(self):
        """Test the full flow from env vars to masked log output."""
        env = {
            "PATH": "/usr/bin",
            "API_KEY": "sk-1234567890abcdef",
            "DB_PASSWORD": "super_secret_password",
            "SHORT": "abc",
        }

        patterns = self.executor._mk_patterns(env)

        # Should have patterns for API_KEY and DB_PASSWORD only
        assert len(patterns) == 2

        log_line = f"Connecting to DB with {env['DB_PASSWORD']} and API key {env['API_KEY']}"
        self.executor._mask_agent_log(
            log_line,
            patterns=patterns,
            level=LogLevel.STDOUT,
            source=LogSource.USER,
        )

        masked_line, _, _ = self.executor.logged_messages[0]
        assert env["API_KEY"] not in masked_line
        assert env["DB_PASSWORD"] not in masked_line
        assert "Connecting to DB with" in masked_line
        assert "and API key" in masked_line
        assert masked_line.count("********") == 2
