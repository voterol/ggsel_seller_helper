import json
import os
import tempfile
import unittest
from unittest.mock import Mock

import requests

from autoresponder import AutoResponder
from config import Config
from ggsel_api import APIFailure, GGSelAPI


def make_config(**overrides):
    values = {
        "ggsel_seller_id": 1,
        "ggsel_api_key": "secret-key",
        "telegram_bot_token": "telegram-token",
        "telegram_group_id": -100,
    }
    values.update(overrides)
    return Config(**values)


def response(status=200, payload=None):
    result = Mock(spec=requests.Response)
    result.status_code = status
    if isinstance(payload, Exception):
        result.json.side_effect = payload
    else:
        result.json.return_value = payload
    return result


class GGSelAPISecurityTests(unittest.TestCase):
    def test_origin_is_normalized_and_used_for_reviews(self):
        api = GGSelAPI(make_config(ggsel_base_url="https://EXAMPLE.com/custom/"))
        api.token = "opaque"
        api.session.request = Mock(return_value=response(payload={"retval": 0, "reviews": []}))

        self.assertEqual(api.get_reviews(), {"retval": 0, "reviews": []})
        _, url = api.session.request.call_args.args
        self.assertEqual(url, "https://example.com/custom/reviews")

    def test_rejects_unsafe_or_ambiguous_origins(self):
        invalid = (
            "http://seller.example/api",
            "https://user:password@seller.example/api",
            "https://seller.example/api?token=leak",
            "https://seller.example/api#fragment",
            "//seller.example/api",
        )
        for origin in invalid:
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                GGSelAPI(make_config(ggsel_base_url=origin))

    def test_every_request_has_explicit_timeout_and_login_uses_session(self):
        api = GGSelAPI(make_config(ggsel_connect_timeout=2, ggsel_read_timeout=9))
        api.session.request = Mock(return_value=response(payload={"token": "opaque"}))

        self.assertTrue(api.login())
        self.assertEqual(api.session.request.call_args.kwargs["timeout"], (2.0, 9.0))

    def test_token_is_parameter_not_interpolated_into_url(self):
        api = GGSelAPI(make_config())
        api.token = "secret-token"
        api.session.request = Mock(return_value=response(payload={"retval": 0, "content": {}}))

        api.get_purchase_info(42)

        _, url = api.session.request.call_args.args
        self.assertNotIn("secret-token", url)
        self.assertEqual(api.session.request.call_args.kwargs["params"]["token"], "secret-token")

    def test_send_uses_documented_http_success(self):
        api = GGSelAPI(make_config())
        api.token = "opaque"
        api.session.request = Mock(return_value=response(payload=ValueError("not json")))
        self.assertTrue(api.send_message(7, "hello"))
        self.assertIsNone(api.last_failure)

        api.session.request.return_value = response(payload={"retval": 0})
        self.assertTrue(api.send_message(7, "hello"))
        self.assertIsNone(api.last_failure)

    def test_purchase_info_sends_required_locale_header(self):
        api = GGSelAPI(make_config())
        api.token = "opaque"
        api.session.request = Mock(return_value=response(payload={"retval": 0, "content": {}}))

        self.assertIsNotNone(api.get_purchase_info(42))
        self.assertEqual(api.session.request.call_args.kwargs["headers"]["locale"], "ru")

    def test_balance_token_is_sent_as_parameter_not_url_text(self):
        api = GGSelAPI(make_config())
        api.token = "secret-token"
        api.session.request = Mock(return_value=response(payload={"retval": 0, "content": {}}))

        self.assertIsNotNone(api.get_balance_info())
        _, url = api.session.request.call_args.args
        self.assertNotIn("secret-token", url)
        self.assertEqual(api.session.request.call_args.kwargs["params"]["token"], "secret-token")

    def test_transport_and_status_failures_are_classified(self):
        api = GGSelAPI(make_config())
        api.token = "opaque"
        api.session.request = Mock(side_effect=requests.Timeout())
        self.assertIsNone(api.get_last_sales())
        self.assertEqual(api.last_failure, APIFailure.RETRYABLE)

        api.session.request = Mock(return_value=response(status=400))
        self.assertIsNone(api.get_last_sales())
        self.assertEqual(api.last_failure, APIFailure.PERMANENT)

        api.session.request = Mock(return_value=response(status=401))
        api.login = Mock(return_value=False)
        self.assertIsNone(api.get_last_sales())
        self.assertEqual(api.last_failure, APIFailure.AUTHENTICATION)

    def test_invalid_message_never_crosses_http_boundary(self):
        api = GGSelAPI(make_config())
        api.token = "opaque"
        api.session.request = Mock()
        self.assertFalse(api.send_message(7, "\x00\x01"))
        self.assertFalse(api.send_message(-1, "hello"))
        api.session.request.assert_not_called()

    def test_post_is_not_configured_for_automatic_retry(self):
        api = GGSelAPI(make_config())
        retry = api.session.get_adapter("https://").max_retries
        self.assertNotIn("POST", retry.allowed_methods)


class AutoResponderBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tempdir.name, "autoresponder.json")
        self.responder = AutoResponder(self.path)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_text_is_bounded_and_controls_are_removed(self):
        index = self.responder.add_trigger("HeLLo\x00", "ok\x01" + "x" * 5000)
        trigger = self.responder.get_trigger(index)
        self.assertEqual(trigger["phrase"], "hello")
        self.assertNotIn("\x01", trigger["response"])
        self.assertEqual(len(trigger["response"]), 4000)
        self.assertEqual(self.responder.find_response("HELLO\x00")["response"], trigger["response"])

    def test_update_does_not_allow_arbitrary_config_keys(self):
        index = self.responder.add_trigger("hello", "world")
        self.assertTrue(self.responder.update_trigger(index, response="new", injected="value"))
        self.assertNotIn("injected", self.responder.get_trigger(index))

    def test_csv_options_normalize_untrusted_values(self):
        self.responder.config["csv_mode"]["enabled"] = True
        self.responder.add_csv_rule("Amount", match_type="contains", option_value="10")
        matches = self.responder.check_csv_options([
            None,
            {"name": "Amount\x00", "user_data": "100\x01"},
        ])
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["option_name"], "Amount")
        self.assertEqual(matches[0]["option_value"], "100")
        self.assertEqual(self.responder.check_csv_options("not-a-list"), [])

    def test_malformed_loaded_collections_fail_closed(self):
        self.responder.config["triggers"] = [None, "bad"]
        self.responder.config["csv_mode"] = {"enabled": True, "rules": "bad"}
        self.assertIsNone(self.responder.find_response("anything"))
        self.assertEqual(self.responder.check_csv_options([{"name": "x"}]), [])

    def test_saved_config_is_private_and_valid_json(self):
        self.responder.set_notify_text("notice")
        with open(self.path, encoding="utf-8") as config_file:
            self.assertEqual(json.load(config_file)["notify_text"], "notice")
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_instances_do_not_share_nested_defaults(self):
        other_path = os.path.join(self.tempdir.name, "other.json")
        self.responder.config["review_responses"]["enabled"] = True
        self.assertFalse(AutoResponder(other_path).is_review_responses_enabled())


if __name__ == "__main__":
    unittest.main()
