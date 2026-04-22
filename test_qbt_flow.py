#!/usr/bin/env python3
"""
test_qbt_flow.py
Unit tests for qbt_flow.py — stdlib only (unittest + unittest.mock).
"""

import json
import os
import sys
import threading
import unittest
from email.message import Message
from typing import Optional
from unittest.mock import MagicMock, patch
from pathlib import Path
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Bootstrap: set required env vars before module import so config
# loading doesn't fail or read from a live config.env on disk.
# ---------------------------------------------------------------------------
os.environ["PLEX_TOKEN"] = "test-token"
os.environ["QBT_INSTANCES"] = "localhost:8080:admin:adminadmin"
os.environ["LOG_FILE"] = os.devnull  # keep test noise out of throttle.log

sys.path.insert(0, str(Path(__file__).parent))
import qbt_flow as m  # noqa: E402  (import after env setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(body: bytes, status: int = 200, headers: Optional[dict] = None):
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
# _parse_speed
# ---------------------------------------------------------------------------

class TestParseSpeed(unittest.TestCase):
    # -- Bits per second (ISP-style) --
    def test_gbps(self):
        self.assertEqual(m._parse_speed("1Gbps"), 1_000_000_000)

    def test_mbps(self):
        self.assertEqual(m._parse_speed("500Mbps"), 500_000_000)

    def test_kbps(self):
        self.assertEqual(m._parse_speed("8000Kbps"), 8_000_000)

    def test_bps(self):
        self.assertEqual(m._parse_speed("1000bps"), 1000)

    # -- Bytes per second (app-style) --
    def test_mb_per_sec(self):
        self.assertEqual(m._parse_speed("10MB/s"), 10 * 1024 * 1024)

    def test_kb_per_sec(self):
        self.assertEqual(m._parse_speed("512KB/s"), 512 * 1024)

    def test_gb_per_sec(self):
        self.assertEqual(m._parse_speed("1GB/s"), 1024**3)

    # -- Case insensitivity --
    def test_case_insensitive(self):
        self.assertEqual(m._parse_speed("1gbps"), 1_000_000_000)
        self.assertEqual(m._parse_speed("10mb/s"), 10 * 1024 * 1024)

    # -- Plain numbers (backward compat) --
    def test_plain_int(self):
        self.assertEqual(m._parse_speed("1000000000"), 1_000_000_000)

    def test_plain_float(self):
        self.assertEqual(m._parse_speed("1.5"), 1.5)

    def test_numeric_passthrough(self):
        self.assertEqual(m._parse_speed(500_000_000), 500_000_000)

    # -- Edge cases --
    def test_empty_string(self):
        self.assertEqual(m._parse_speed(""), 0)

    def test_whitespace(self):
        self.assertEqual(m._parse_speed("  500Mbps  "), 500_000_000)

    def test_fractional_with_suffix(self):
        self.assertEqual(m._parse_speed("1.5Gbps"), 1_500_000_000)

    def test_unrecognised_suffix_returns_zero(self):
        self.assertEqual(m._parse_speed("10xyz"), 0)


class TestEnvSpeed(unittest.TestCase):
    """Verify _env_speed reads key and falls back to default."""

    def test_env_var_with_suffix(self):
        with patch.dict(os.environ, {"TOTAL_BANDWIDTH": "500Mbps"}, clear=False):
            val = m._env_speed("TOTAL_BANDWIDTH", 1_000_000_000)
        self.assertEqual(val, 500_000_000)

    def test_env_var_plain_number(self):
        with patch.dict(os.environ, {"TOTAL_BANDWIDTH": "100000000"}, clear=False):
            val = m._env_speed("TOTAL_BANDWIDTH", 1_000_000_000)
        self.assertEqual(val, 100_000_000)

    def test_default_used(self):
        env = os.environ.copy()
        env.pop("TOTAL_BANDWIDTH", None)
        with patch.dict(os.environ, env, clear=True):
            val = m._env_speed("TOTAL_BANDWIDTH", 1_000_000_000)
        self.assertEqual(val, 1_000_000_000)


# ---------------------------------------------------------------------------
# calculate_limits
# ---------------------------------------------------------------------------

class TestCalculateLimits(unittest.TestCase):
    def setUp(self):
        m.TOTAL_BANDWIDTH_BPS   = 1_000_000_000   # 1 Gbps download
        m.TOTAL_UPLOAD_BPS      = 1_000_000_000   # same upload by default
        m.QBT_HEADROOM_FRACTION = 0.8
        m.QBT_UPLOAD_FRACTION   = 0.9
        m.STREAM_OVERHEAD_FACTOR  = 1.25
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
        m.last_racing_active = None
        m.last_detail = None
        m.last_activity = {}
        m.DRY_RUN = False
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        m.QBT_DYNAMIC_SPLIT = False
        m.RACING_WINDOW_ENABLED = False
        m.MIN_QBT_DL_BYTES = 10 * 1024 * 1024
        m.MIN_QBT_UL_BYTES =  5 * 1024 * 1024

    def tearDown(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.last_detail = None
        m.last_activity = {}
        m.DRY_RUN = False
        m.QBT_DYNAMIC_SPLIT = False
        m.RACING_WINDOW_ENABLED = False

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
        m.last_racing_active = False  # same state as current (racing disabled)
        m.last_detail = "1 stream(s), using ~5 Mbps"
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE",
                           detail="1 stream(s), using ~5 Mbps")
        client.set_speed_limits.assert_not_called()

    def test_reapplies_when_detail_changes(self):
        """Stream count change should trigger re-apply even if byte limits are similar."""
        client = self._make_client()
        m.last_dl_limit = 50 * 1024 * 1024
        m.last_ul_limit = 25 * 1024 * 1024
        m.last_racing_active = False
        m.last_detail = "1 stream(s), using ~5 Mbps"
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE",
                           detail="2 stream(s), using ~5 Mbps")
        client.set_speed_limits.assert_called_once()

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

    def test_split_ul_when_dl_is_unlimited(self):
        """When DL is unlimited (0) but UL has a budget, UL must still be split."""
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        c1, c2 = self._make_client(), self._make_client()
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(0, ul, "THROTTLE")
        # DL stays 0 (unlimited) for each; UL is halved
        c1.set_speed_limits.assert_called_once_with(0, ul // 2)
        c2.set_speed_limits.assert_called_once_with(0, ul // 2)

    def test_split_both_unlimited_stays_unlimited(self):
        """When both DL and UL are 0 (NORMAL), each instance gets (0, 0)."""
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        c1, c2 = self._make_client(), self._make_client()
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(0, 0, "NORMAL")
        c1.set_speed_limits.assert_called_once_with(0, 0)
        c2.set_speed_limits.assert_called_once_with(0, 0)

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
        with patch("qbt_flow.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.login()
        self.assertTrue(ok)
        self.assertEqual(c.cookie, "SID=abc123")

    def test_login_fails_when_no_sid_in_response(self):
        resp = _make_response(b"Ok", headers={"Set-Cookie": "Path=/"})
        with patch("qbt_flow.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.login()
        self.assertFalse(ok)
        self.assertIsNone(c.cookie)

    def test_login_fails_on_urlerror(self):
        with patch("qbt_flow.urlopen", side_effect=URLError("refused")):
            c = self._make_client()
            ok = c.login()
        self.assertFalse(ok)
        self.assertIsNone(c.cookie)

    def test_login_fails_on_httperror(self):
        with patch("qbt_flow.urlopen", side_effect=HTTPError("", 401, "Unauthorized", Message(), None)):
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
        with patch("qbt_flow.urlopen", return_value=resp):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertTrue(ok)

    def test_retry_after_auth_expiry(self):
        """Simulates: 2 POST fails → re-login → 2 POST succeed."""
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        success_resp = _make_response(b"", status=200)
        side = [URLError("auth"), URLError("auth"), login_resp, success_resp, success_resp]
        with patch("qbt_flow.urlopen", side_effect=side):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertTrue(ok)
        self.assertEqual(c.cookie, "SID=newtoken")

    def test_no_retry_when_no_cookie(self):
        """If there's no cookie, don't attempt re-login on failure."""
        with patch("qbt_flow.urlopen", side_effect=URLError("error")):
            c = self._make_client()
            c.cookie = None
            ok = c.set_speed_limits(1024, 512)
        self.assertFalse(ok)

    def test_returns_false_when_retry_also_fails(self):
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        side = [URLError("auth"), URLError("auth"), login_resp, URLError("still broken"), URLError("still broken")]
        with patch("qbt_flow.urlopen", side_effect=side):
            c = self._make_client()
            ok = c.set_speed_limits(1024, 512)
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# QbtClient._get_json  /  get_torrent_activity
# ---------------------------------------------------------------------------

class TestQbtClientGetJson(unittest.TestCase):
    def _make_client(self):
        c = m.QbtClient("localhost", 8080, "admin", "password")
        c.cookie = "SID=existing"
        return c

    def test_success_returns_parsed_json(self):
        body = json.dumps([{"name": "torrent1"}, {"name": "torrent2"}]).encode()
        with patch("qbt_flow.urlopen", return_value=_make_response(body)):
            c = self._make_client()
            result = c._get_json("/api/v2/torrents/info?filter=downloading")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "torrent1")

    def test_empty_list_returns_empty_list(self):
        body = json.dumps([]).encode()
        with patch("qbt_flow.urlopen", return_value=_make_response(body)):
            c = self._make_client()
            result = c._get_json("/api/v2/torrents/info?filter=downloading")
        self.assertEqual(result, [])

    def test_urlerror_returns_none(self):
        with patch("qbt_flow.urlopen", side_effect=URLError("timeout")):
            c = self._make_client()
            result = c._get_json("/api/v2/torrents/info?filter=downloading")
        self.assertIsNone(result)

    def test_auth_expired_relogs_and_retries(self):
        """403 HTTPError triggers re-login and a second attempt."""
        err_403 = HTTPError("", 403, "Forbidden", Message(), None)
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        body = json.dumps([{"name": "t"}]).encode()
        success_resp = _make_response(body)
        with patch("qbt_flow.urlopen", side_effect=[err_403, login_resp, success_resp]):
            c = self._make_client()
            result = c._get_json("/api/v2/torrents/info?filter=downloading")
        self.assertEqual(len(result), 1)
        self.assertEqual(c.cookie, "SID=newtoken")

    def test_403_retry_also_fails_returns_none(self):
        err_403 = HTTPError("", 403, "Forbidden", Message(), None)
        login_resp = _make_response(b"Ok", headers={"Set-Cookie": "SID=newtoken; Path=/"})
        with patch("qbt_flow.urlopen", side_effect=[err_403, login_resp, URLError("still broken")]):
            c = self._make_client()
            result = c._get_json("/api/v2/torrents/info?filter=downloading")
        self.assertIsNone(result)


class TestQbtClientGetTorrentActivity(unittest.TestCase):
    def _make_client(self):
        c = m.QbtClient("localhost", 8080, "admin", "password")
        c.cookie = "SID=existing"
        return c

    def test_returns_counts_when_both_queries_succeed(self):
        dl_body = json.dumps([{"name": "a"}, {"name": "b"}]).encode()
        ul_body = json.dumps([{"name": "c"}]).encode()
        responses = [_make_response(dl_body), _make_response(ul_body)]
        with patch("qbt_flow.urlopen", side_effect=responses):
            c = self._make_client()
            result = c.get_torrent_activity()
        self.assertEqual(result, (2, 1))

    def test_returns_none_when_dl_query_fails(self):
        with patch("qbt_flow.urlopen", side_effect=URLError("timeout")):
            c = self._make_client()
            result = c.get_torrent_activity()
        self.assertIsNone(result)

    def test_returns_none_when_ul_query_fails(self):
        dl_body = json.dumps([]).encode()
        dl_resp = _make_response(dl_body)
        with patch("qbt_flow.urlopen", side_effect=[dl_resp, URLError("timeout")]):
            c = self._make_client()
            result = c.get_torrent_activity()
        self.assertIsNone(result)

    def test_returns_zero_zero_when_no_active_torrents(self):
        dl_body = json.dumps([]).encode()
        ul_body = json.dumps([]).encode()
        with patch("qbt_flow.urlopen", side_effect=[_make_response(dl_body), _make_response(ul_body)]):
            c = self._make_client()
            result = c.get_torrent_activity()
        self.assertEqual(result, (0, 0))


# ---------------------------------------------------------------------------
# apply_limits — dynamic split
# ---------------------------------------------------------------------------

class TestApplyLimitsDynamic(unittest.TestCase):
    """Tests for QBT_DYNAMIC_SPLIT behaviour."""

    def setUp(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.last_detail = None
        m.last_activity = {}
        m.DRY_RUN = False
        m.QBT_SPLIT_BETWEEN_INSTANCES = True
        m.QBT_DYNAMIC_SPLIT = True
        m.RACING_WINDOW_ENABLED = False
        m.MIN_QBT_DL_BYTES = 10 * 1024 * 1024
        m.MIN_QBT_UL_BYTES =  5 * 1024 * 1024

    def tearDown(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.last_detail = None
        m.last_activity = {}
        m.DRY_RUN = False
        m.QBT_DYNAMIC_SPLIT = False
        m.RACING_WINDOW_ENABLED = False

    def _make_client(self, port=8080, dl_count=0, ul_count=0, activity_ok=True):
        client = MagicMock()
        client.ensure_logged_in.return_value = True
        client.set_speed_limits.return_value = True
        client.base = f"http://localhost:{port}"
        client.cookie = "SID=test"
        if activity_ok:
            client.get_torrent_activity.return_value = (dl_count, ul_count)
        else:
            client.get_torrent_activity.return_value = None  # simulates failure
        return client

    def test_active_instance_gets_full_dl_budget(self):
        """When one instance is downloading and the other is idle,
        the active one should get the full DL budget."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        active = self._make_client(8080, dl_count=3, ul_count=1)
        idle   = self._make_client(8081, dl_count=0, ul_count=0)
        with patch.object(m, 'clients', [active, idle]):
            m.apply_limits(dl, ul, "THROTTLE")
        # Active instance gets full DL budget (1 active instance)
        active.set_speed_limits.assert_called_once_with(dl, ul)
        # Idle instance gets the MIN floor
        idle.set_speed_limits.assert_called_once_with(m.MIN_QBT_DL_BYTES, m.MIN_QBT_UL_BYTES)

    def test_both_active_splits_equally(self):
        """When both instances have active downloads, budget is split 50/50."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        c1 = self._make_client(8080, dl_count=2, ul_count=2)
        c2 = self._make_client(8081, dl_count=1, ul_count=1)
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        c1.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)
        c2.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)

    def test_dl_idle_ul_active_handled_independently(self):
        """DL and UL activity are independent: an instance can be idle for DL
        but still seeding (active UL) and vice-versa."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        # c1: only downloading, not seeding
        c1 = self._make_client(8080, dl_count=1, ul_count=0)
        # c2: only seeding, not downloading
        c2 = self._make_client(8081, dl_count=0, ul_count=3)
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        # DL: only c1 is active → c1 gets full DL, c2 gets MIN
        # UL: only c2 is active → c2 gets full UL, c1 gets MIN
        c1_args = c1.set_speed_limits.call_args[0]
        c2_args = c2.set_speed_limits.call_args[0]
        self.assertEqual(c1_args[0], dl)              # c1 DL = full budget
        self.assertEqual(c1_args[1], m.MIN_QBT_UL_BYTES)  # c1 UL = floor
        self.assertEqual(c2_args[0], m.MIN_QBT_DL_BYTES)  # c2 DL = floor
        self.assertEqual(c2_args[1], ul)              # c2 UL = full budget

    def test_all_idle_falls_back_to_equal_split(self):
        """When all instances are idle (0 torrents), treat all as active so
        limits are still applied without locking everyone at the floor."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        c1 = self._make_client(8080, dl_count=0, ul_count=0)
        c2 = self._make_client(8081, dl_count=0, ul_count=0)
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        # Falls back to equal split
        c1.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)
        c2.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)

    def test_query_failure_treats_instance_as_active(self):
        """If get_torrent_activity returns None (API error), the instance is
        treated as active (fail-open) and gets its share of the budget."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        c1 = self._make_client(8080, activity_ok=False)  # query fails
        c2 = self._make_client(8081, dl_count=1, ul_count=1)
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")
        # Both treated as active → 50/50 split
        c1.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)
        c2.set_speed_limits.assert_called_once_with(dl // 2, ul // 2)

    def test_activity_change_triggers_reapply_when_budget_unchanged(self):
        """If the budget doesn't change but an instance transitions from idle
        to active, limits must be re-applied (not skipped by tolerance check)."""
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        c1 = self._make_client(8080, dl_count=1, ul_count=1)
        c2 = self._make_client(8081, dl_count=0, ul_count=0)

        # First call: both active=True for c1, idle for c2
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")

        # Reset call counts; now c2 becomes active
        c1.set_speed_limits.reset_mock()
        c2.set_speed_limits.reset_mock()
        c2.get_torrent_activity.return_value = (2, 2)  # c2 now active
        m.last_dl_limit = dl
        m.last_ul_limit = ul
        m.last_detail = "THROTTLE"

        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(dl, ul, "THROTTLE")

        # Re-apply should have happened (both now 50/50)
        c1.set_speed_limits.assert_called_once()
        c2.set_speed_limits.assert_called_once()

    def test_unlimited_dl_stays_unlimited_for_active_instance(self):
        """0 DL budget (unlimited) should pass through as 0 for active instances."""
        ul = 50 * 1024 * 1024
        c1 = self._make_client(8080, dl_count=2, ul_count=1)
        c2 = self._make_client(8081, dl_count=0, ul_count=0)
        with patch.object(m, 'clients', [c1, c2]):
            m.apply_limits(0, ul, "THROTTLE")
        # c1 active: DL=0 (unlimited), UL=full budget
        c1.set_speed_limits.assert_called_once_with(0, ul)
        # c2 idle: DL=0 (unlimited, 0 passes through), UL=MIN
        c2.set_speed_limits.assert_called_once_with(0, m.MIN_QBT_UL_BYTES)


# ---------------------------------------------------------------------------
# get_plex_sessions
# ---------------------------------------------------------------------------

class TestGetPlexSessions(unittest.TestCase):
    def setUp(self):
        m.PLEX_URL   = "http://localhost:32400"
        m.PLEX_TOKEN = "test-token"

    def test_no_sessions(self):
        xml = b'<?xml version="1.0"?><MediaContainer size="0"></MediaContainer>'
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
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
        with patch("qbt_flow.urlopen", return_value=_make_response(xml)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 10_000_000)

    def test_urlerror_returns_minus_one(self):
        with patch("qbt_flow.urlopen", side_effect=URLError("timeout")):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)

    def test_httperror_returns_minus_one(self):
        with patch("qbt_flow.urlopen", side_effect=HTTPError("", 401, "Unauthorized", Message(), None)):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)

    def test_malformed_xml_returns_minus_one(self):
        with patch("qbt_flow.urlopen", return_value=_make_response(b"not xml at all <>")):
            count, bps = m.get_plex_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)


# ---------------------------------------------------------------------------
# _validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig(unittest.TestCase):
    def setUp(self):
        self._orig_servers = m._configured_servers[:]
        self._orig_inst  = m.QBT_INSTANCES

    def tearDown(self):
        m._configured_servers[:] = self._orig_servers
        m.QBT_INSTANCES = self._orig_inst

    def test_no_servers_exits(self):
        m._configured_servers[:] = []
        with self.assertRaises(SystemExit):
            m._validate_config()

    def test_empty_instances_exits(self):
        m.QBT_INSTANCES = []
        with self.assertRaises(SystemExit):
            m._validate_config()

    def test_valid_config_passes(self):
        m._configured_servers[:] = [("plex", "url", "tok", m.get_plex_sessions)]
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

    def test_env_int_returns_default_when_missing(self):
        os.environ.pop("_TEST_MISSING_INT", None)
        self.assertEqual(m._env_int("_TEST_MISSING_INT", 7), 7)

    def test_env_float_returns_default_when_missing(self):
        os.environ.pop("_TEST_MISSING_FLOAT", None)
        self.assertAlmostEqual(m._env_float("_TEST_MISSING_FLOAT", 2.5), 2.5)


# ---------------------------------------------------------------------------
# Racing window — _is_racing_window
# ---------------------------------------------------------------------------

class TestIsRacingWindow(unittest.TestCase):
    def setUp(self):
        self._orig_enabled = m.RACING_WINDOW_ENABLED
        self._orig_start   = m.RACING_WINDOW_START
        self._orig_end     = m.RACING_WINDOW_END

    def tearDown(self):
        m.RACING_WINDOW_ENABLED = self._orig_enabled
        m.RACING_WINDOW_START   = self._orig_start
        m.RACING_WINDOW_END     = self._orig_end

    def test_disabled_always_false(self):
        m.RACING_WINDOW_ENABLED = False
        self.assertFalse(m._is_racing_window())

    def test_inside_normal_window(self):
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 0
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 3
            self.assertTrue(m._is_racing_window())

    def test_outside_normal_window(self):
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 0
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 12
            self.assertFalse(m._is_racing_window())

    def test_at_start_boundary(self):
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 0
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 0
            self.assertTrue(m._is_racing_window())

    def test_at_end_boundary_excluded(self):
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 0
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 7
            self.assertFalse(m._is_racing_window())

    def test_wrap_midnight_inside_late(self):
        """22:00–07:00 window, hour=23 → inside."""
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 22
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 23
            self.assertTrue(m._is_racing_window())

    def test_wrap_midnight_inside_early(self):
        """22:00–07:00 window, hour=3 → inside."""
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 22
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 3
            self.assertTrue(m._is_racing_window())

    def test_wrap_midnight_outside(self):
        """22:00–07:00 window, hour=15 → outside."""
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START = 22
        m.RACING_WINDOW_END   = 7
        with patch("qbt_flow.datetime") as mock_dt:
            mock_dt.now.return_value.hour = 15
            self.assertFalse(m._is_racing_window())


# ---------------------------------------------------------------------------
# Racing window — apply_limits integration
# ---------------------------------------------------------------------------

class TestApplyLimitsRacing(unittest.TestCase):
    def setUp(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.DRY_RUN = False
        m.RACING_WINDOW_ENABLED = True
        m.RACING_INSTANCE_PORT  = 39001
        m.RACING_NON_RACING_DL_LIMIT = 5 * 1024 * 1024
        m.RACING_NON_RACING_UL_LIMIT = 5 * 1024 * 1024
        m.MIN_QBT_DL_BYTES = 10 * 1024 * 1024
        m.MIN_QBT_UL_BYTES =  5 * 1024 * 1024

    def tearDown(self):
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.DRY_RUN = False
        m.RACING_WINDOW_ENABLED = False

    def _make_client(self, port, success=True):
        client = MagicMock()
        client.ensure_logged_in.return_value = True
        client.set_speed_limits.return_value = success
        client.base = f"http://localhost:{port}"
        client.cookie = "SID=test"
        return client

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_caps_media_instance(self, _mock):
        media  = self._make_client(39000)
        racing = self._make_client(39001)
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [media, racing]):
            m.apply_limits(dl, ul, "THROTTLE")
        # Media instance (39000) should be capped
        media.set_speed_limits.assert_called_once_with(
            m.RACING_NON_RACING_DL_LIMIT,
            m.RACING_NON_RACING_UL_LIMIT,
        )
        # Racing instance (39001) should get remainder
        expected_dl = max(dl - m.RACING_NON_RACING_DL_LIMIT, m.MIN_QBT_DL_BYTES)
        expected_ul = max(ul - m.RACING_NON_RACING_UL_LIMIT, m.MIN_QBT_UL_BYTES)
        racing.set_speed_limits.assert_called_once_with(expected_dl, expected_ul)

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_unlimited_gives_both_unlimited(self, _mock):
        """When no streams (dl=0, ul=0 NORMAL), racing instance gets unlimited."""
        media  = self._make_client(39000)
        racing = self._make_client(39001)
        with patch.object(m, 'clients', [media, racing]):
            m.apply_limits(0, 0, "NORMAL")
        # Racing instance gets 0, 0 (both unlimited — no streams)
        racing.set_speed_limits.assert_called_once_with(0, 0)
        # Media still capped
        media.set_speed_limits.assert_called_once_with(
            m.RACING_NON_RACING_DL_LIMIT,
            m.RACING_NON_RACING_UL_LIMIT,
        )

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_unlimited_dl_still_throttles_ul(self, _mock):
        """When DL is unlimited (0) but UL has a budget (streams active),
        the racing instance should get throttled UL, not unlimited."""
        media  = self._make_client(39000)
        racing = self._make_client(39001)
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [media, racing]):
            m.apply_limits(0, ul, "THROTTLE")
        # Racing instance: DL unlimited, UL = ul - cap
        expected_ul = max(ul - m.RACING_NON_RACING_UL_LIMIT, m.MIN_QBT_UL_BYTES)
        racing.set_speed_limits.assert_called_once_with(0, expected_ul)
        # Media instance: still hard-capped on both
        media.set_speed_limits.assert_called_once_with(
            m.RACING_NON_RACING_DL_LIMIT,
            m.RACING_NON_RACING_UL_LIMIT,
        )

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_enforces_min_floor_on_racing_instance(self, _mock):
        """If total minus cap < MIN floor, racing instance gets MIN."""
        media  = self._make_client(39000)
        racing = self._make_client(39001)
        # dl barely above the non-racing cap → racing gets MIN
        dl = m.RACING_NON_RACING_DL_LIMIT + 1
        ul = m.RACING_NON_RACING_UL_LIMIT + 1
        with patch.object(m, 'clients', [media, racing]):
            m.apply_limits(dl, ul, "THROTTLE")
        racing.set_speed_limits.assert_called_once_with(
            m.MIN_QBT_DL_BYTES,
            m.MIN_QBT_UL_BYTES,
        )

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_dry_run_logs_labels(self, _mock):
        m.DRY_RUN = True
        media  = self._make_client(39000)
        racing = self._make_client(39001)
        with patch.object(m, 'clients', [media, racing]):
            # Should not raise; should not call set_speed_limits
            m.apply_limits(100 * 1024 * 1024, 50 * 1024 * 1024, "THROTTLE")
        media.set_speed_limits.assert_not_called()
        racing.set_speed_limits.assert_not_called()

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_always_reapplies_even_if_unchanged(self, _mock):
        """During racing window, tolerance skip is disabled."""
        client = self._make_client(39001)
        m.last_dl_limit = 50 * 1024 * 1024
        m.last_ul_limit = 25 * 1024 * 1024
        with patch.object(m, 'clients', [client]):
            m.apply_limits(50 * 1024 * 1024, 25 * 1024 * 1024, "THROTTLE")
        # Single instance → no racing split (needs >1), but tolerance IS skipped
        client.set_speed_limits.assert_called_once()

    @patch("qbt_flow._is_racing_window", return_value=True)
    def test_racing_single_instance_no_racing_split(self, _mock):
        """With a single instance, racing window doesn't do special split."""
        client = self._make_client(39001)
        dl = 100 * 1024 * 1024
        ul = 50 * 1024 * 1024
        with patch.object(m, 'clients', [client]):
            m.apply_limits(dl, ul, "THROTTLE")
        # Single instance → normal path (no split, no racing logic)
        client.set_speed_limits.assert_called_once_with(dl, ul)

    def test_racing_to_normal_transition_reapplies(self):
        """When racing ends but limits are unchanged, must still re-apply to uncap media."""
        media  = self._make_client(39000)
        racing = self._make_client(39001)

        # First call: racing active, dl=0 (no Plex streams)
        with patch("qbt_flow._is_racing_window", return_value=True), \
             patch.object(m, 'clients', [media, racing]):
            m.apply_limits(0, 0, "NORMAL")
        # Media was capped during racing
        media.set_speed_limits.assert_called_once_with(
            m.RACING_NON_RACING_DL_LIMIT,
            m.RACING_NON_RACING_UL_LIMIT,
        )
        media.reset_mock()
        racing.reset_mock()

        # Second call: racing ended, same dl=0 ul=0 — must NOT skip
        with patch("qbt_flow._is_racing_window", return_value=False), \
             patch.object(m, 'clients', [media, racing]):
            m.apply_limits(0, 0, "NORMAL")
        # Both should now be unlimited (0, 0)
        media.set_speed_limits.assert_called_once_with(0, 0)
        racing.set_speed_limits.assert_called_once_with(0, 0)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

class TestMain(unittest.TestCase):
    def setUp(self):
        self._orig_token = m.PLEX_TOKEN
        self._orig_inst  = m.QBT_INSTANCES
        self._orig_dry   = m.DRY_RUN

    def tearDown(self):
        m.PLEX_TOKEN    = self._orig_token
        m.QBT_INSTANCES = self._orig_inst
        m.DRY_RUN       = self._orig_dry
        m.stop_event.clear()

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(0, 0))
    def test_main_loop_normal_no_streams(self, mock_plex, mock_apply):
        """One iteration: no streams → NORMAL limits."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]

        # Stop after one iteration
        def stop_after_first(*args, **kwargs):
            m.stop_event.set()

        mock_apply.side_effect = stop_after_first

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()
        mock_apply.assert_called()

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(2, 50_000_000))
    def test_main_loop_throttle_with_streams(self, mock_plex, mock_apply):
        """One iteration: active streams → THROTTLE limits."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]

        call_count = [0]
        def stop_on_second_call(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 1:
                m.stop_event.set()

        mock_apply.side_effect = stop_on_second_call

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        # First apply was THROTTLE (with detail), second was SHUTDOWN cleanup
        calls = mock_apply.call_args_list
        self.assertTrue(any("THROTTLE" in str(c) for c in calls))

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(-1, 0))
    def test_main_loop_plex_unreachable_keep(self, mock_plex, mock_apply):
        """Plex unreachable with keep action → no apply_limits call in loop."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.UNREACHABLE_ACTION = "keep"

        # Run one iteration then stop
        original_wait = m.stop_event.wait
        call_count = [0]
        def wait_and_stop(timeout):
            call_count[0] += 1
            if call_count[0] >= 1:
                m.stop_event.set()
            return original_wait(0)

        with patch.object(m.stop_event, "wait", side_effect=wait_and_stop), \
             patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(-1, 0))
    def test_main_loop_plex_unreachable_unlimited(self, mock_plex, mock_apply):
        """Plex unreachable with unlimited action → apply unlimited."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.UNREACHABLE_ACTION = "unlimited"

        def stop_after_first(*args, **kwargs):
            m.stop_event.set()

        mock_apply.side_effect = stop_after_first

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()
        mock_apply.assert_called()

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(0, 0))
    def test_main_non_dry_run_cleanup(self, mock_plex, mock_apply):
        """Non-dry-run shutdown calls apply_limits(0, 0, SHUTDOWN, force=True)."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.stop_event.set()  # stop immediately

        with patch("sys.argv", ["prog"]):
            m.main()
        # Should have called apply_limits for SHUTDOWN with force=True
        calls = mock_apply.call_args_list
        shutdown_calls = [c for c in calls if "SHUTDOWN" in str(c)]
        self.assertTrue(len(shutdown_calls) > 0)

    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(0, 0))
    def test_main_racing_window_log(self, mock_plex, mock_apply):
        """Main logs racing window config when enabled."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RACING_WINDOW_ENABLED = True
        m.RACING_WINDOW_START   = 0
        m.RACING_WINDOW_END     = 7
        m.stop_event.set()

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()
        m.RACING_WINDOW_ENABLED = False


# ---------------------------------------------------------------------------
# load_env
# ---------------------------------------------------------------------------

class TestLoadEnv(unittest.TestCase):
    def test_load_env_skips_comments_and_blanks(self):
        from unittest.mock import mock_open
        fake_content = "# comment\n\nFOO_TEST_VAR=hello\nexport BAR_TEST_VAR=world\n"
        with patch("pathlib.Path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data=fake_content)):
            # Clear to ensure setdefault can write
            os.environ.pop("FOO_TEST_VAR", None)
            os.environ.pop("BAR_TEST_VAR", None)
            m._load_env()
        self.assertEqual(os.environ.get("FOO_TEST_VAR"), "hello")
        self.assertEqual(os.environ.get("BAR_TEST_VAR"), "world")
        # Cleanup
        os.environ.pop("FOO_TEST_VAR", None)
        os.environ.pop("BAR_TEST_VAR", None)

    def test_load_env_does_not_overwrite_existing(self):
        from unittest.mock import mock_open
        os.environ["EXISTING_VAR"] = "original"
        fake_content = "EXISTING_VAR=overwritten\n"
        with patch("pathlib.Path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data=fake_content)):
            m._load_env()
        self.assertEqual(os.environ["EXISTING_VAR"], "original")
        os.environ.pop("EXISTING_VAR", None)

    def test_load_env_no_file(self):
        with patch("pathlib.Path.exists", return_value=False):
            m._load_env()  # should not raise


# ---------------------------------------------------------------------------
# BackoffTracker
# ---------------------------------------------------------------------------

class TestBackoffTracker(unittest.TestCase):
    def test_no_failures_skip_returns_false(self):
        bt = m.BackoffTracker()
        self.assertFalse(bt.should_skip())

    def test_record_failure_increases_delay(self):
        bt = m.BackoffTracker(max_interval=300)
        d1 = bt.record_failure()
        d2 = bt.record_failure()
        self.assertEqual(d1, 2)
        self.assertEqual(d2, 4)

    def test_record_success_resets(self):
        bt = m.BackoffTracker()
        bt.record_failure()
        bt.record_failure()
        bt.record_success()
        self.assertEqual(bt.failures, 0)
        self.assertFalse(bt.should_skip())

    def test_max_interval_cap(self):
        bt = m.BackoffTracker(max_interval=10)
        for _ in range(20):
            bt.record_failure()
        self.assertEqual(bt.current_delay(), 10)

    def test_should_skip_during_backoff(self):
        bt = m.BackoffTracker()
        bt.record_failure()
        # Just recorded a failure — should skip because _next_retry is in the future
        self.assertTrue(bt.should_skip())

    def test_current_delay_zero_when_no_failures(self):
        bt = m.BackoffTracker()
        self.assertEqual(bt.current_delay(), 0)


# ---------------------------------------------------------------------------
# Jellyfin / Emby sessions
# ---------------------------------------------------------------------------

class TestJellyfinEmbySessions(unittest.TestCase):
    def test_jellyfin_active_session(self):
        data = json.dumps([
            {
                "NowPlayingItem": {"Bitrate": 20_000_000},
                "PlayState": {"IsPaused": False},
            }
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 20_000_000)

    def test_jellyfin_paused_excluded(self):
        data = json.dumps([
            {
                "NowPlayingItem": {"Bitrate": 20_000_000},
                "PlayState": {"IsPaused": True},
            }
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 0)
        self.assertEqual(bps, 0)

    def test_jellyfin_no_nowplayingitem_skipped(self):
        data = json.dumps([{"Id": "abc"}]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 0)

    def test_jellyfin_error_returns_minus_one(self):
        with patch("qbt_flow.urlopen", side_effect=URLError("timeout")):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, -1)

    def test_emby_uses_emby_path_prefix(self):
        data = json.dumps([
            {
                "NowPlayingItem": {"Bitrate": 8_000_000},
                "PlayState": {"IsPaused": False},
            }
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp) as mock_url:
            count, bps = m.get_emby_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 8_000_000)
        # Verify the URL contains /emby/Sessions
        call_args = mock_url.call_args
        req = call_args[0][0]
        self.assertIn("/emby/Sessions", req.full_url)

    def test_jellyfin_multiple_streams_summed(self):
        data = json.dumps([
            {"NowPlayingItem": {"Bitrate": 10_000_000}, "PlayState": {"IsPaused": False}},
            {"NowPlayingItem": {"Bitrate": 15_000_000}, "PlayState": {"IsPaused": False}},
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 2)
        self.assertEqual(bps, 25_000_000)

    def test_jellyfin_missing_bitrate_defaults_to_zero(self):
        data = json.dumps([
            {"NowPlayingItem": {"Name": "Some Movie"}, "PlayState": {"IsPaused": False}},
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 0)

    def test_jellyfin_transcoding_info_bitrate_fallback(self):
        """When NowPlayingItem has no Bitrate, use TranscodingInfo.Bitrate."""
        data = json.dumps([
            {
                "NowPlayingItem": {"Name": "Some Movie"},
                "PlayState": {"IsPaused": False},
                "TranscodingInfo": {"Bitrate": 9_872_000},
            }
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 9_872_000)

    def test_jellyfin_media_sources_bitrate_fallback(self):
        """When NowPlayingItem and TranscodingInfo have no Bitrate, use MediaSources."""
        data = json.dumps([
            {
                "NowPlayingItem": {
                    "Name": "Some Movie",
                    "MediaSources": [{"Bitrate": 15_000_000}],
                },
                "PlayState": {"IsPaused": False},
            }
        ]).encode()
        resp = _make_response(data)
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 15_000_000)

    def test_jellyfin_malformed_json_returns_minus_one(self):
        resp = _make_response(b"not json")
        with patch("qbt_flow.urlopen", return_value=resp):
            count, bps = m.get_jellyfin_sessions()
        self.assertEqual(count, -1)


# ---------------------------------------------------------------------------
# get_sessions dispatcher
# ---------------------------------------------------------------------------

class TestGetSessions(unittest.TestCase):
    def setUp(self):
        self._orig_servers = m._configured_servers[:]
        self._orig_backoffs = dict(m._server_backoffs)
        m._server_backoffs.clear()

    def tearDown(self):
        m._configured_servers[:] = self._orig_servers
        m._server_backoffs.clear()
        m._server_backoffs.update(self._orig_backoffs)

    def test_single_server(self):
        mock_fn = MagicMock(return_value=(2, 50_000_000))
        m._configured_servers[:] = [("plex", "url", "tok", mock_fn)]
        count, bps = m.get_sessions()
        self.assertEqual(count, 2)
        self.assertEqual(bps, 50_000_000)
        mock_fn.assert_called_once_with("url", "tok")

    def test_multiple_servers_aggregated(self):
        plex_fn = MagicMock(return_value=(1, 20_000_000))
        jf_fn = MagicMock(return_value=(2, 30_000_000))
        m._configured_servers[:] = [("plex", "u1", "t1", plex_fn), ("jellyfin", "u2", "t2", jf_fn)]
        count, bps = m.get_sessions()
        self.assertEqual(count, 3)
        self.assertEqual(bps, 50_000_000)

    def test_one_fails_other_succeeds(self):
        fail_fn = MagicMock(return_value=(-1, 0))
        ok_fn = MagicMock(return_value=(1, 10_000_000))
        m._configured_servers[:] = [("plex", "u1", "t1", fail_fn), ("jellyfin", "u2", "t2", ok_fn)]
        count, bps = m.get_sessions()
        self.assertEqual(count, 1)
        self.assertEqual(bps, 10_000_000)

    def test_all_fail_returns_minus_one(self):
        fail_fn = MagicMock(return_value=(-1, 0))
        m._configured_servers[:] = [("plex", "u1", "t1", fail_fn)]
        count, bps = m.get_sessions()
        self.assertEqual(count, -1)
        self.assertEqual(bps, 0)

    def test_backoff_skips_server(self):
        bt = m.BackoffTracker(300)
        bt.record_failure()
        m._server_backoffs["plex"] = bt
        plex_fn = MagicMock(return_value=(99, 99))
        ok_fn = MagicMock(return_value=(1, 10_000_000))
        m._configured_servers[:] = [("plex", "u1", "t1", plex_fn), ("jellyfin", "u2", "t2", ok_fn)]
        count, bps = m.get_sessions()
        self.assertEqual(count, 1)
        plex_fn.assert_not_called()

    def test_all_in_backoff_returns_minus_one(self):
        bt = m.BackoffTracker(300)
        bt.record_failure()
        m._server_backoffs["plex"] = bt
        m._configured_servers[:] = [("plex", "u1", "t1", MagicMock())]
        count, bps = m.get_sessions()
        self.assertEqual(count, -1)


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------

class TestStatusEndpoint(unittest.TestCase):
    def test_status_handler_json(self):
        handler = MagicMock(spec=m._StatusHandler)
        handler.path = "/status"
        handler.wfile = MagicMock()
        m._StatusHandler.do_GET(handler)
        handler.send_response.assert_called_with(200)
        handler.send_header.assert_called_with("Content-Type", "application/json")

    def test_status_handler_metrics(self):
        handler = MagicMock(spec=m._StatusHandler)
        handler.path = "/metrics"
        handler.wfile = MagicMock()
        m._StatusHandler.do_GET(handler)
        handler.send_response.assert_called_with(200)
        handler.send_header.assert_called_with("Content-Type", "text/plain; version=0.0.4")

    def test_status_handler_root(self):
        handler = MagicMock(spec=m._StatusHandler)
        handler.path = "/"
        handler.wfile = MagicMock()
        m._StatusHandler.do_GET(handler)
        handler.send_response.assert_called_with(200)

    def test_status_handler_404(self):
        handler = MagicMock(spec=m._StatusHandler)
        handler.path = "/unknown"
        handler.wfile = MagicMock()
        m._StatusHandler.do_GET(handler)
        handler.send_response.assert_called_with(404)

    def test_start_status_server_disabled(self):
        orig = m.STATUS_PORT
        m.STATUS_PORT = 0
        result = m._start_status_server()
        self.assertIsNone(result)
        m.STATUS_PORT = orig

    def test_start_status_server_success(self):
        """Status server binds and returns a server object."""
        orig = m.STATUS_PORT
        m.STATUS_PORT = 19876  # unlikely to be in use
        try:
            server = m._start_status_server()
            self.assertIsNotNone(server)
            server.shutdown()
        finally:
            m.STATUS_PORT = orig

    def test_start_status_server_port_in_use(self):
        """OSError when port is already bound returns None."""
        orig = m.STATUS_PORT
        m.STATUS_PORT = 19877
        try:
            with patch("qbt_flow.HTTPServer", side_effect=OSError("Address already in use")):
                result = m._start_status_server()
            self.assertIsNone(result)
        finally:
            m.STATUS_PORT = orig

    def test_log_message_suppressed(self):
        handler = MagicMock(spec=m._StatusHandler)
        m._StatusHandler.log_message(handler, "test %s", "arg")
        # Should not raise — just suppresses output


# ---------------------------------------------------------------------------
# Ramp-up (integration via main loop)
# ---------------------------------------------------------------------------

class TestRampUp(unittest.TestCase):
    def setUp(self):
        self._orig_token = m.PLEX_TOKEN
        self._orig_inst  = m.QBT_INSTANCES
        self._orig_dry   = m.DRY_RUN
        self._orig_ramp  = m.RAMP_UP_STEPS
        self._orig_poll  = m.POLL_INTERVAL
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.POLL_INTERVAL = 0

    def tearDown(self):
        m.PLEX_TOKEN    = self._orig_token
        m.QBT_INSTANCES = self._orig_inst
        m.DRY_RUN       = self._orig_dry
        m.RAMP_UP_STEPS = self._orig_ramp
        m.POLL_INTERVAL = self._orig_poll
        m.last_dl_limit = None
        m.last_ul_limit = None
        m.last_racing_active = None
        m.stop_event.clear()

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions")
    def test_ramp_up_starts_on_stream_drop(self, mock_sessions, mock_apply, _srv):
        """Streams → no streams with RAMP_UP_STEPS=3 triggers ramp-up."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RAMP_UP_STEPS = 3

        # Cycle 1: 2 streams → throttle
        # Cycle 2: 0 streams → start ramp (step 1/3)
        # Cycle 3: 0 streams → ramp (step 2/3)
        # Cycle 4: 0 streams → unlimited
        call_count = [0]
        def sessions_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return (2, 50_000_000)
            return (0, 0)

        mock_sessions.side_effect = lambda: sessions_side_effect()

        apply_count = [0]
        def apply_side_effect(*args, **kwargs):
            apply_count[0] += 1
            # Set last_dl_limit to a throttled value on first call
            if apply_count[0] == 1:
                m.last_dl_limit = 50 * 1024 * 1024
                m.last_ul_limit = 25 * 1024 * 1024
            if apply_count[0] >= 5:  # enough iterations
                m.stop_event.set()

        mock_apply.side_effect = apply_side_effect

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        # Check that RAMP-UP label was used
        labels = [c.args[2] for c in mock_apply.call_args_list]
        self.assertIn("RAMP-UP", labels)

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions")
    def test_ramp_cancelled_by_new_stream(self, mock_sessions, mock_apply, _srv):
        """New streams during ramp cancel the ramp."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RAMP_UP_STEPS = 3

        call_count = [0]
        def sessions_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return (2, 50_000_000)  # streams active
            if call_count[0] == 2:
                return (0, 0)  # streams stop → ramp begins
            if call_count[0] == 3:
                return (1, 30_000_000)  # stream comes back → cancel ramp
            return (0, 0)

        mock_sessions.side_effect = lambda: sessions_side_effect()

        apply_count = [0]
        def apply_side_effect(*args, **kwargs):
            apply_count[0] += 1
            if apply_count[0] == 1:
                m.last_dl_limit = 50 * 1024 * 1024
                m.last_ul_limit = 25 * 1024 * 1024
            if apply_count[0] >= 5:
                m.stop_event.set()

        mock_apply.side_effect = apply_side_effect

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        labels = [c.args[2] for c in mock_apply.call_args_list]
        self.assertIn("THROTTLE", labels)

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(0, 0))
    def test_ramp_disabled_when_zero_steps(self, mock_sessions, mock_apply, _srv):
        """RAMP_UP_STEPS=0 means instant unlimited."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RAMP_UP_STEPS = 0
        m.stop_event.set()

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        # Should NOT have any RAMP-UP labels
        labels = [c.args[2] for c in mock_apply.call_args_list if len(c.args) > 2]
        self.assertNotIn("RAMP-UP", labels)

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions")
    def test_ramp_overflow_goes_unlimited(self, mock_sessions, mock_apply, _srv):
        """Ramp-up doubling past max bandwidth jumps straight to NORMAL."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RAMP_UP_STEPS = 10  # many steps so doubling overflows quickly
        orig_bw = m.TOTAL_BANDWIDTH_BPS
        m.TOTAL_BANDWIDTH_BPS = 100_000_000  # 100 Mbps → max_bw = 12.5 MB/s

        call_count = [0]
        def sessions_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return (1, 50_000_000)
            return (0, 0)

        mock_sessions.side_effect = lambda: sessions_side_effect()

        apply_count = [0]
        def apply_side_effect(*args, **kwargs):
            apply_count[0] += 1
            if apply_count[0] == 1:
                # Set high enough so doubling overflows max_bw (12.5 MB/s)
                m.last_dl_limit = 10_000_000  # 10 MB/s → *2 = 20 MB/s > 12.5
                m.last_ul_limit = 10_000_000
            if apply_count[0] >= 4:
                m.stop_event.set()

        mock_apply.side_effect = apply_side_effect

        try:
            with patch("sys.argv", ["prog", "--dry-run"]):
                m.main()
        finally:
            m.TOTAL_BANDWIDTH_BPS = orig_bw

        labels = [c.args[2] for c in mock_apply.call_args_list]
        # Overflow triggers immediate NORMAL instead of RAMP-UP
        self.assertIn("NORMAL", labels)

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions")
    def test_ramp_completes_naturally(self, mock_sessions, mock_apply, _srv):
        """Ramp with small limit completes all steps without overflowing."""
        m.PLEX_TOKEN    = "test-token"
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.RAMP_UP_STEPS = 2  # 2 steps: one RAMP-UP, final → NORMAL

        call_count = [0]
        def sessions_side_effect():
            call_count[0] += 1
            if call_count[0] == 1:
                return (1, 50_000_000)
            return (0, 0)

        mock_sessions.side_effect = lambda: sessions_side_effect()

        apply_count = [0]
        def apply_side_effect(*args, **kwargs):
            apply_count[0] += 1
            if apply_count[0] == 1:
                # Small value so doubling stays well under max_bw
                m.last_dl_limit = 1_000_000   # 1 MB/s → *2 = 2 MB/s << 125 MB/s
                m.last_ul_limit = 500_000
            if apply_count[0] >= 5:
                m.stop_event.set()

        mock_apply.side_effect = apply_side_effect

        with patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        labels = [c.args[2] for c in mock_apply.call_args_list]
        # Should see RAMP-UP then NORMAL when ramp_remaining hits 0
        self.assertIn("RAMP-UP", labels)
        self.assertIn("NORMAL", labels)


# ---------------------------------------------------------------------------
# Main loop with backoff
# ---------------------------------------------------------------------------

class TestMainBackoff(unittest.TestCase):
    def setUp(self):
        self._orig_inst  = m.QBT_INSTANCES
        self._orig_dry   = m.DRY_RUN
        self._orig_ramp  = m.RAMP_UP_STEPS
        self._orig_poll  = m.POLL_INTERVAL
        m.POLL_INTERVAL = 0

    def tearDown(self):
        m.QBT_INSTANCES = self._orig_inst
        m.DRY_RUN       = self._orig_dry
        m.RAMP_UP_STEPS = self._orig_ramp
        m.POLL_INTERVAL = self._orig_poll
        m.stop_event.clear()

    @patch("qbt_flow._start_status_server")
    @patch("qbt_flow.apply_limits")
    @patch("qbt_flow.get_sessions", return_value=(-1, 0))
    def test_unreachable_keep_no_apply(self, mock_sessions, mock_apply, _srv):
        """When all servers unreachable with 'keep', apply_limits is not called."""
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m.UNREACHABLE_ACTION = "keep"
        m.RAMP_UP_STEPS = 0

        call_count = [0]
        original_wait = m.stop_event.wait
        def wait_and_stop(timeout):
            call_count[0] += 1
            if call_count[0] >= 2:
                m.stop_event.set()
            return original_wait(0)

        with patch.object(m.stop_event, "wait", side_effect=wait_and_stop), \
             patch("sys.argv", ["prog", "--dry-run"]):
            m.main()

        # get_sessions was polled
        self.assertTrue(mock_sessions.call_count >= 1)
        # With keep + dry-run, apply_limits should not be called
        mock_apply.assert_not_called()


# ---------------------------------------------------------------------------
# _validate_config — media server type
# ---------------------------------------------------------------------------

class TestValidateConfigServers(unittest.TestCase):
    def setUp(self):
        self._orig_servers = m._configured_servers[:]
        self._orig_inst = m.QBT_INSTANCES

    def tearDown(self):
        m._configured_servers[:] = self._orig_servers
        m.QBT_INSTANCES = self._orig_inst

    def test_no_servers_exits(self):
        m._configured_servers[:] = []
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        with self.assertRaises(SystemExit):
            m._validate_config()

    def test_plex_only_passes(self):
        m._configured_servers[:] = [("plex", "url", "tok", m.get_plex_sessions)]
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m._validate_config()  # must not raise

    def test_multiple_servers_pass(self):
        m._configured_servers[:] = [
            ("plex", "u1", "t1", m.get_plex_sessions),
            ("jellyfin", "u2", "t2", m.get_jellyfin_sessions),
        ]
        m.QBT_INSTANCES = [("h", 8080, "u", "p", "http")]
        m._validate_config()  # must not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
