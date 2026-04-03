#!/usr/bin/env python3
"""
test_plex_qbt_throttle.py
Unit tests for plex_qbt_throttle.py — stdlib only (unittest + unittest.mock).
"""

import os
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch
from pathlib import Path
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Bootstrap: set required env vars before module import so config
# loading doesn't fail or read from a live config.env on disk.
# ---------------------------------------------------------------------------
os.environ["PLEX_TOKEN"] = "test-token"
os.environ["QBT_INSTANCES"] = "localhost:8080:admin:adminadmin"

sys.path.insert(0, str(Path(__file__).parent))
import plex_qbt_throttle as m  # noqa: E402  (import after env setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(body: bytes, status: int = 200, headers: dict = None):
    """Return a mock that behaves as an open urllib response (context manager)."""
    resp = MagicMock()
    resp.read.return_value = body
    resp.status = status
    header_dict = headers or {}
    resp.headers.get = lambda key, default="": header_dict.get(key, default)
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# _parse_qbt_instances
# ---------------------------------------------------------------------------

class TestParseQbtInstances(unittest.TestCase):
    def test_basic_http(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "myhost:9090:admin:secret"}):
            r = m._parse_qbt_instances()
        self.assertEqual(r, [("myhost", 9090, "admin", "secret", "http")])

    def test_https_scheme(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "myhost:8443:admin:secret:https"}):
            r = m._parse_qbt_instances()
        self.assertEqual(r[0], ("myhost", 8443, "admin", "secret", "https"))

    def test_multiple_instances(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "h1:8080:u:p,h2:8081:u2:p2"}):
            r = m._parse_qbt_instances()
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0][0], "h1")
        self.assertEqual(r[1][0], "h2")

    def test_invalid_entry_skipped(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "badentry"}):
            r = m._parse_qbt_instances()
        self.assertEqual(r, [])

    def test_invalid_scheme_falls_back_to_http(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "h:80:u:p:ftp"}):
            r = m._parse_qbt_instances()
        self.assertEqual(r[0][4], "http")

    def test_mixed_valid_and_invalid(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "good:8080:u:p,bad"}):
            r = m._parse_qbt_instances()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0][0], "good")

    def test_password_with_colon(self):
        # password field consumes everything after 3rd colon
        with patch.dict(os.environ, {"QBT_INSTANCES": "h:8080:user:pa:ss:word"}):
            r = m._parse_qbt_instances()
        # parts[3] = "pa", parts[4] = "ss:word" — scheme is invalid, falls back to http
        self.assertEqual(len(r), 1)

    def test_trailing_comma_ignored(self):
        with patch.dict(os.environ, {"QBT_INSTANCES": "h1:8080:u:p,"}):
            r = m._parse_qbt_instances()
        self.assertEqual(len(r), 1)


# ---------------------------------------------------------------------------
# calculate_limits
# ---------------------------------------------------------------------------

class TestCalculateLimits(unittest.TestCase):
    def setUp(self):
        m.TOTAL_BANDWIDTH_BPS   = 1_000_000_000   # 1 Gbps download
        m.TOTAL_UPLOAD_BPS      = 1_000_000_000   # same upload by default
        m.QBT_HEADROOM_FRACTION = 0.8
        m.QBT_UPLOAD_FRACTION   = 0.9
        m.PLEX_OVERHEAD_FACTOR  = 1.25
        m.MIN_QBT_DL_BYTES = 10 * 1024 * 1024   # 10 MB/s
        m.MIN_QBT_UL_BYTES =  5 * 1024 * 1024   #  5 MB/s

    def test_zero_plex_bitrate_full_bandwidth(self):
        dl, ul, _ = m.calculate_limits(1, 0)
        self.assertEqual(dl, int(1_000_000_000 * 0.8 / 8))
        self.assertEqual(ul, int(1_000_000_000 * 0.9 / 8))

    def test_plex_overhead_applied(self):
        plex_bps = 100_000_000  # 100 Mbps
        dl, ul, _ = m.calculate_limits(1, plex_bps)
        reserved = 100_000_000 * 1.25   # 125 Mbps
        remaining = 1_000_000_000 - reserved
        self.assertEqual(dl, int(remaining * 0.8 / 8))

    def test_min_floor_applied_on_massive_stream(self):
        dl, ul, _ = m.calculate_limits(1, 10_000_000_000_000)  # absurdly huge
        self.assertEqual(dl, m.MIN_QBT_DL_BYTES)
        self.assertEqual(ul, m.MIN_QBT_UL_BYTES)

    def test_asymmetric_bandwidth(self):
        m.TOTAL_UPLOAD_BPS = 50_000_000   # 50 Mbps up, 1 Gbps down
        dl, ul, _ = m.calculate_limits(1, 0)
        self.assertEqual(dl, int(1_000_000_000 * 0.8 / 8))
        self.assertEqual(ul, int(50_000_000 * 0.9 / 8))

    def test_detail_contains_stream_count(self):
        _, _, detail = m.calculate_limits(3, 50_000_000)
        self.assertIn("3 stream(s)", detail)

    def test_detail_contains_remaining_bandwidth(self):
        _, _, detail = m.calculate_limits(1, 0)
        self.assertIn("remaining DL", detail)
        self.assertIn("UL", detail)

    def test_multiple_streams_bitrates_sum(self):
        # calculate_limits receives already-summed bps from get_plex_sessions
        dl1, _, _ = m.calculate_limits(2, 200_000_000)
        dl2, _, _ = m.calculate_limits(1, 200_000_000)
        self.assertEqual(dl1, dl2)  # sum is the same regardless of stream count passed


# ---------------------------------------------------------------------------
# apply_limits
# ---------------------------------------------------------------------------

class TestApplyLimits(unittest.TestCase):
    def setUp(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.DRY_RUN = False
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        m.MIN_QBT_DL_BYTES = 10 * 1024 * 1024
        m.MIN_QBT_UL_BYTES =  5 * 1024 * 1024

    def tearDown(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.DRY_RUN = False

    def _make_client(self, success=True):
        client = MagicMock()
        client.ensure_logged_in.return_value = True
        client.set_speed_limits.return_value = success
        client.base = "http://localhost:8080"
        client.cookie = "SID=test"
        return client

    def test_sets_limits_on_single_client(self):
        client = self._make_client()
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        client.set_speed_limits.assert_called_once_with(50 * 1024 * 1024, 25 * 1024 * 1024)

    def test_skips_when_limits_unchanged(self):
        client = self._make_client()
        m.last_dl_limit = 50 * 1024 * 1024
        m.last_ul_limit = 25 * 1024 * 1024
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        client.set_speed_limits.assert_not_called()

    def test_applies_when_limits_change_by_more_than_1_pct(self):
        client = self._make_client()
        m.last_dl_limit = 50 * 1024 * 1024
        m.last_ul_limit = 25 * 1024 * 1024
        # 5% change — should trigger update
        new_dl = int(50 * 1024 * 1024 * 1.05)
        with patch.object(m, 'clients', [client]):
            m.apply_limits(new_dl, 25 * 1024 * 1024, "THROTTLE")
        client.set_speed_limits.assert_called_once()

    def test_force_bypasses_tolerance_check(self):
        client = self._make_client()
        m.last_dl_limit = 50 * 1024 * 1024
        m.last_ul_limit = 25 * 1024 * 1024
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "SHUTDOWN", force=True)
        client.set_speed_limits.assert_called_once()

    def test_split_between_two_instances(self):
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        c1, c2 = self._make_client(), self._make_client()
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        c1.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)
        c2.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)

    def test_no_split_when_disabled(self):
        m.QBT_SPLIT_BETWEEN_INSTANCES = False
        c1, c2 = self._make_client(), self._make_client()
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        c1.set_speed_limits.assert_called_once_with(dl, ul)
        c2.set_speed_limits.assert_called_once_with(dl, ul)

    def test_no_split_for_single_instance(self):
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        c = self._make_client()
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [c]):
            m.apply_limits(dl, ul, "THROTTLE")
        c.set_speed_limits.assert_called_once_with(dl, ul)

    def test_dry_run_skips_api_calls(self):
        m.DRY_RUN = True
        client = self._make_client()
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        client.set_speed_limits.assert_not_called()
        client.ensure_logged_in.assert_not_called()

    def test_dry_run_zero_shows_unlimited(self):
        m.DRY_RUN = True
        client = self._make_client()
        # Should not raise — just log
        with patch.object(m, 'clients', [client]):
            m.apply_limits(0, 0, "NORMAL")

    def test_failed_set_clears_cookie(self):
        client = self._make_client(success=False)
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        self.assertIsNone(client.cookie)

    def test_login_failure_skips_client(self):
        client = self._make_client()
        client.ensure_logged_in.return_value = False
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        client.set_speed_limits.assert_not_called()

    def test_last_limits_updated_after_apply(self):
        client = self._make_client()
        dl = 50 * 1024 * 1024
        ul = 25 * 1024 * 1024
        with patch.object(m, 'clients', [client]):
            m.apply_limits(dl, ul, "THROTTLE")
        self.assertEqual(m.last_dl_limit, dl)
        self.assertEqual(m.last_ul_limit, ul)


# ---------------------------------------------------------------------------
# QbtClient.login
# ---------------------------------------------------------------------------

class TestQbtClientLogin(unittest.TestCase):
    def _make_client(self):
        return m.QbtClient("localhost", 8080, "admin", "password")

    def test_login_extracts_sid_cookie(self):
        resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=abc123; Path=/"})
        with patch("plex_qbt_throttle.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.login()
        self.assertTrue(ok)
        self.assertEqual(c.cookie, "SID=abc123")

    def test_login_fails_when_no_sid_in_response(self):
        resp = _make_response(b"Ok", headers={"Set-Cookie": "Path=/"})
        with patch("plex_qbt_throttle.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.login()
        self.assertFalse(ok)
        self.assertIsNone(c.cookie)

    def test_login_fails_on_urlerror(self):
        with patch("plex_qbt_throttle.urlopen", side_effect=URLError("refused")):
            c = self._make_client()
            ok = c.login()
        self.assertFalse(ok)
        self.assertIsNone(c.cookie)

    def test_login_fails_on_httperror(self):
        with patch("plex_qbt_throttle.urlopen", side_effect=HTTPError(None, 401, "Unauthorized", {}, None)):
            c = self._make_client()
            ok = c.login()
        self.assertFalse(ok)

    def test_ensure_logged_in_calls_login_when_no_cookie(self):
        c = self._make_client()
        c.cookie = None
        with patch.object(c, "login", return_value=True) as mock_login:
            result = c.ensure_logged_in()
        mock_login.assert_called_once()
        self.assertTrue(result)

    def test_ensure_logged_in_skips_login_when_cookie_exists(self):
        c = self._make_client()
        c.cookie = "SID=existing"
        with patch.object(c, "login") as mock_login:
            result = c.ensure_logged_in()
        mock_login.assert_not_called()
        self.assertTrue(result)

    def test_https_base_url(self):
        c = m.QbtClient("myserver", 8443, "admin", "pass", "https")
        self.assertEqual(c.base, "https://myserver:8443")

    def test_http_base_url(self):
        c = m.QbtClient("myserver", 8080, "admin", "pass", "http")
        self.assertEqual(c.base, "http://myserver:8080")


# ---------------------------------------------------------------------------
# QbtClient.set_speed_limits (re-login retry)
# ---------------------------------------------------------------------------

class TestQbtClientSetSpeedLimits(unittest.TestCase):
    def _make_client(self):
        c = m.QbtClient("localhost", 8080, "admin", "password")
        c.cookie = "SID=existing"
        return c

    def test_success_returns_true(self):
        resp = _make_response(b"", status=200)
        with patch("plex_qbt_throttle.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertTrue(ok)

    def test_retry_after_auth_expiry(self):
        """Simulates: 2 POST fails → re-login → 2 POST succeed."""
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        success_resp = _make_response(b"", status=200)
        side = [URLError("auth"), URLError("auth"), login_resp, success_resp, success_resp]
        with patch("plex_qbt_throttle.urlopen", side_effect=side):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertTrue(ok)
        self.assertEqual(c.cookie, "SID=newtoken")

    def test_no_retry_when_no_cookie(self):
        """If there's no cookie, don't attempt re-login on failure."""
        with patch("plex_qbt_throttle.urlopen", side_effect=URLError("error")):
            c = self._make_client()
            c.cookie = None
            ok = c.set_speed_limits(1024, 512)
        self.assertFalse(ok)

    def test_returns_false_when_retry_also_fails(self):
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        side = [URLError("auth"), URLError("auth"), login_resp, URLError("still broken"), URLError("still broken")]
        with patch("plex_qbt_throttle.urlopen", side_effect=side):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# get_plex_sessions
# ---------------------------------------------------------------------------

class TestGetPlexSessions(unittest.TestCase):
    def setUp(self):
        m.PLEX_URL   = "http://localhost:32400"
        m.PLEX_TOKEN = "test-token"

    def test_no_sessions(self):
        xml = b'<?xml version="1.0"?><MediaContainer size="0"></MediaContainer>'
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 0)
        self.assertEqual(bps, 0)

    def test_single_active_video_stream(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="1">
  <Video bitrate="20000" key="/library/metadata/1">
    <Player state="playing" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 20_000_000)   # 20000 kbps × 1000

    def test_paused_session_excluded(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="1">
  <Video bitrate="20000" key="/library/metadata/1">
    <Player state="paused" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 0)
        self.assertEqual(bps, 0)

    def test_stopped_session_excluded(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="1">
  <Video bitrate="20000" key="/library/metadata/1">
    <Player state="stopped" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 0)
        self.assertEqual(bps, 0)

    def test_multiple_streams_bitrate_summed(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="2">
  <Video bitrate="10000" key="/library/metadata/1">
    <Player state="playing" />
  </Video>
  <Video bitrate="15000" key="/library/metadata/2">
    <Player state="playing" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 2)
        self.assertEqual(bps, 25_000_000)

    def test_audio_track_session_counted(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="1">
  <Track bitrate="320" key="/library/metadata/music/1">
    <Player state="playing" />
  </Track>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 320_000)

    def test_bitrate_fallback_to_media_element(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="1">
  <Video key="/library/metadata/1">
    <Player state="playing" />
    <Media bitrate="8000" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 8_000_000)

    def test_mixed_active_and_paused(self):
        xml = b"""<?xml version="1.0"?>
<MediaContainer size="2">
  <Video bitrate="10000" key="/library/metadata/1">
    <Player state="playing" />
  </Video>
  <Video bitrate="10000" key="/library/metadata/2">
    <Player state="paused" />
  </Video>
</MediaContainer>"""
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 10_000_000)

    def test_urlerror_returns_minus_one(self):
        with patch("plex_qbt_throttle.urlopen", side_effect=URLError("timeout")):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)

    def test_httperror_returns_minus_one(self):
        with patch("plex_qbt_throttle.urlopen", side_effect=HTTPError(None, 401, "Unauthorized", {}, None)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)

    def test_malformed_xml_returns_minus_one(self):
        with patch("plex_qbt_throttle.urlopen", return_value=_make_response(b"not xml at all <>")):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig(unittest.TestCase):
    def setUp(self):
        self._orig_token = m.PLEX_TOKEN
        self._orig_inst  = m.QBT_INSTANCES

    def tearDown(self):
        m.PLEX_TOKEN    = self._orig_token
        m.QBT_INSTANCES = self._orig_inst

    def test_missing_token_exits(self):
        m.PLEX_TOKEN = ""
        with self.assertRaises(SystemExit):
            m._validate_config()

    def test_empty_instances_exits(self):
        m.QBT_INSTANCES = []
        with self.assertRaises(SystemExit):
            m._validate_config()

    def test_valid_config_passes(self):
        m.PLEX_TOKEN    = "valid-token"
        m.QBT_INSTANCES = [("host", 8080, "u", "p", "http")]
        m._validate_config()   # must not raise


# ---------------------------------------------------------------------------
# Signal / stop_event
# ---------------------------------------------------------------------------

class TestStopEvent(unittest.TestCase):
    def setUp(self):
        m.stop_event.clear()

    def tearDown(self):
        m.stop_event.clear()

    def test_stop_event_is_threading_event(self):
        self.assertIsInstance(m.stop_event, threading.Event)

    def test_signal_handler_sets_stop_event(self):
        self.assertFalse(m.stop_event.is_set())
        m.handle_signal(15, None)
        self.assertTrue(m.stop_event.is_set())

    def test_stop_event_initially_clear(self):
        self.assertFalse(m.stop_event.is_set())


# ---------------------------------------------------------------------------
# Config env helpers
# ---------------------------------------------------------------------------

class TestEnvHelpers(unittest.TestCase):
    def test_env_returns_value(self):
        with patch.dict(os.environ, {"_TEST_KEY": "hello"}):
            self.assertEqual(m._env("_TEST_KEY"), "hello")

    def test_env_returns_default(self):
        os.environ.pop("_TEST_MISSING", None)
        self.assertEqual(m._env("_TEST_MISSING", "default"), "default")

    def test_env_int_parses(self):
        with patch.dict(os.environ, {"_TEST_INT": "42"}):
            self.assertEqual(m._env_int("_TEST_INT", 0), 42)

    def test_env_int_returns_default_on_bad_value(self):
        with patch.dict(os.environ, {"_TEST_INT": "notanumber"}):
            self.assertEqual(m._env_int("_TEST_INT", 99), 99)

    def test_env_float_parses(self):
        with patch.dict(os.environ, {"_TEST_FLOAT": "1.5"}):
            self.assertAlmostEqual(m._env_float("_TEST_FLOAT", 0.0), 1.5)

    def test_env_float_returns_default_on_bad_value(self):
        with patch.dict(os.environ, {"_TEST_FLOAT": "abc"}):
            self.assertAlmostEqual(m._env_float("_TEST_FLOAT", 3.14), 3.14)


if __name__ == "__main__":
    unittest.main(verbosity=2)
