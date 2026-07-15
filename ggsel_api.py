import hashlib
import logging
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Config
from database import Chat


class APIFailure(str, Enum):
    """Machine-readable classification for the most recent API failure."""

    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    AUTHENTICATION = "authentication"


class GGSelAPI:
    """Small synchronous client for the GGSel seller API.

    Existing return contracts are retained (``None``/``False`` on failure).
    ``last_failure`` adds failure classification for callers which need to
    decide whether an operation should be retried.
    """

    DEFAULT_TIMEOUT: Tuple[float, float] = (5.0, 30.0)
    MAX_MESSAGE_LENGTH = 4000
    RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

    def __init__(self, config: Config):
        self.config = config
        self.base_url = self._validated_base_url(config.ggsel_base_url)
        self.timeout = self._validated_timeout(
            getattr(config, "ggsel_connect_timeout", self.DEFAULT_TIMEOUT[0]),
            getattr(config, "ggsel_read_timeout", self.DEFAULT_TIMEOUT[1]),
        )
        self.token: Optional[str] = None
        self.last_failure: Optional[APIFailure] = None
        self.session = requests.Session()

        # Retry idempotent reads only. Retrying message POSTs can duplicate them.
        retry = Retry(
            total=max(0, int(getattr(config, "max_retries", 2))),
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=self.RETRYABLE_STATUS_CODES,
            allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.headers.update({"Accept": "application/json"})

    @staticmethod
    def _validated_base_url(value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("ggsel_base_url must be a non-empty HTTPS URL")
        parsed = urlsplit(value.strip())
        if (
            parsed.scheme.lower() != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "ggsel_base_url must be an HTTPS URL without credentials, query, or fragment"
            )
        # urlsplit validates ports lazily.
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("ggsel_base_url contains an invalid port") from exc
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        rendered_host = f"[{host}]" if ":" in host else host
        netloc = f"{rendered_host}:{port}" if port is not None else rendered_host
        path = parsed.path.rstrip("/")
        return urlunsplit(("https", netloc, path, "", ""))

    @staticmethod
    def _validated_timeout(connect: Any, read: Any) -> Tuple[float, float]:
        try:
            timeout = (float(connect), float(read))
        except (TypeError, ValueError) as exc:
            raise ValueError("GGSel HTTP timeouts must be numbers") from exc
        if any(item <= 0 or item > 300 for item in timeout):
            raise ValueError("GGSel HTTP timeouts must be greater than 0 and at most 300 seconds")
        return timeout

    def _url(self, path: str) -> str:
        # Endpoints are constants controlled by this client, not caller input.
        return f"{self.base_url}/{path.lstrip('/')}"

    def _generate_sign(self, timestamp: str) -> str:
        data = f"{self.config.ggsel_api_key}{timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()

    def _set_http_failure(self, status_code: int) -> None:
        if status_code in (401, 403):
            self.last_failure = APIFailure.AUTHENTICATION
        elif status_code in self.RETRYABLE_STATUS_CODES:
            self.last_failure = APIFailure.RETRYABLE
        else:
            self.last_failure = APIFailure.PERMANENT

    def _request(self, method: str, path: str, **kwargs: Any) -> Optional[requests.Response]:
        kwargs["timeout"] = self.timeout
        try:
            response = self.session.request(method, self._url(path), **kwargs)
        except (requests.Timeout, requests.ConnectionError):
            self.last_failure = APIFailure.RETRYABLE
            logging.warning("GGSel API request failed due to a temporary transport error")
            return None
        except requests.RequestException:
            self.last_failure = APIFailure.PERMANENT
            logging.warning("GGSel API request failed before receiving a response")
            return None
        if not 200 <= response.status_code < 300:
            self._set_http_failure(response.status_code)
            return None
        return response

    def _json(self, response: requests.Response) -> Optional[Any]:
        try:
            return response.json()
        except (ValueError, TypeError):
            self.last_failure = APIFailure.PERMANENT
            return None

    def login(self) -> bool:
        timestamp = str(int(time.time()))
        payload = {
            "seller_id": self.config.ggsel_seller_id,
            "timestamp": timestamp,
            "sign": self._generate_sign(timestamp),
        }
        response = self._request(
            "POST",
            "apilogin",
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        if response is None:
            self.token = None
            return False
        data = self._json(response)
        token = data.get("token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token.strip():
            self.token = None
            self.last_failure = APIFailure.PERMANENT
            return False
        self.token = token
        self.last_failure = None
        return True

    def _authenticated_request(
        self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> Optional[requests.Response]:
        if not self.token and not self.login():
            return None
        request_params = dict(params or {})
        request_params["token"] = self.token
        response = self._request(method, path, params=request_params, **kwargs)
        # Refresh credentials only when the server actually rejects them. A
        # transport/5xx failure must not trigger an unrelated login request.
        if response is None and self.last_failure == APIFailure.AUTHENTICATION and self.login():
            request_params["token"] = self.token
            response = self._request(method, path, params=request_params, **kwargs)
        return response

    def get_chats(
        self, filter_new: Optional[int] = None, email: Optional[str] = None,
        id_ds: Optional[str] = None, pagesize: int = 100, page: int = 1,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(pagesize, int) or not 1 <= pagesize <= 1000 or not isinstance(page, int) or page < 1:
            self.last_failure = APIFailure.PERMANENT
            return None
        params: Dict[str, Any] = {"pagesize": pagesize, "page": page}
        if filter_new is not None:
            params["filter_new"] = filter_new
        if email:
            params["email"] = email
        if id_ds:
            params["id_ds"] = id_ds
        response = self._authenticated_request("GET", "debates/v2/chats", params=params)
        data = self._json(response) if response is not None else None
        if isinstance(data, dict):
            self.last_failure = None
            return data
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return None

    def get_chat_messages(self, chat_id: int) -> Optional[List[Dict[str, Any]]]:
        if not isinstance(chat_id, int) or isinstance(chat_id, bool) or chat_id <= 0:
            self.last_failure = APIFailure.PERMANENT
            return None
        response = self._authenticated_request("GET", "debates/v2", params={"id_i": chat_id})
        data = self._json(response) if response is not None else None
        messages = data if isinstance(data, list) else data.get("messages") if isinstance(data, dict) else None
        if isinstance(messages, list) and all(isinstance(item, dict) for item in messages):
            self.last_failure = None
            return messages
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return None

    @staticmethod
    def _clean_message(message: Any) -> Optional[str]:
        if not isinstance(message, str):
            return None
        # Preserve newlines/tabs but remove transport control characters.
        cleaned = "".join(ch for ch in message if ch in "\n\r\t" or ord(ch) >= 32)
        if not cleaned.strip():
            return None
        return cleaned[:GGSelAPI.MAX_MESSAGE_LENGTH]

    def send_message(self, chat_id: int, message: str) -> bool:
        if not isinstance(chat_id, int) or isinstance(chat_id, bool) or chat_id <= 0:
            self.last_failure = APIFailure.PERMANENT
            return False
        cleaned = self._clean_message(message)
        if cleaned is None:
            self.last_failure = APIFailure.PERMANENT
            return False
        response = self._authenticated_request(
            "POST",
            "debates/v2",
            params={"id_i": chat_id},
            json={"message": cleaned},
            headers={"Content-Type": "application/json"},
        )
        data = self._json(response) if response is not None else None
        # A 2xx response without the documented success marker is not proof of
        # delivery and must not be reported as successful.
        if isinstance(data, dict) and data.get("retval") == 0:
            self.last_failure = None
            return True
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return False

    def get_last_sales(self, top: int = 10) -> Optional[Dict[str, Any]]:
        if not isinstance(top, int) or isinstance(top, bool) or not 1 <= top <= 1000:
            self.last_failure = APIFailure.PERMANENT
            return None
        response = self._authenticated_request(
            "GET", "seller-last-sales", params={"top": top}, headers={"locale": "ru"}
        )
        data = self._json(response) if response is not None else None
        if isinstance(data, dict):
            self.last_failure = None
            return data
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return None

    def get_purchase_info(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        if not isinstance(invoice_id, int) or isinstance(invoice_id, bool) or invoice_id <= 0:
            self.last_failure = APIFailure.PERMANENT
            return None
        response = self._authenticated_request("GET", f"purchase/info/{invoice_id}")
        data = self._json(response) if response is not None else None
        if isinstance(data, dict) and data.get("retval") == 0:
            self.last_failure = None
            return data
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return None

    def get_chats_by_email(self, email: str, pagesize: int = 100, page: int = 1) -> Optional[Dict[str, Any]]:
        if not isinstance(email, str) or not email.strip() or len(email) > 320:
            self.last_failure = APIFailure.PERMANENT
            return None
        return self.get_chats(email=email.strip(), pagesize=pagesize, page=page)

    def parse_chats_response(self, response_data: Dict[str, Any]) -> List[Chat]:
        chats: List[Chat] = []
        if not isinstance(response_data, dict) or not isinstance(response_data.get("items"), list):
            return chats
        for item in response_data["items"]:
            if not isinstance(item, dict) or item.get("id_i") is None:
                continue
            chats.append(Chat(
                id_i=item["id_i"],
                email=item.get("email") or None,
                product=item.get("product", 0),
                last_message=item.get("last_message", ""),
                cnt_msg=item.get("cnt_msg", 0),
                cnt_new=item.get("cnt_new", 0),
            ))
        return chats

    def get_reviews(
        self, count: int = 20, review_type: str = "all", page: int = 1,
        product_id: int = None,
    ) -> Optional[Dict[str, Any]]:
        if (
            not isinstance(count, int) or not 1 <= count <= 100
            or review_type not in {"good", "bad", "all"}
            or not isinstance(page, int) or page < 1
            or product_id is not None and (not isinstance(product_id, int) or product_id <= 0)
        ):
            self.last_failure = APIFailure.PERMANENT
            return None
        params: Dict[str, Any] = {"type": review_type, "page": page, "count": count}
        if product_id is not None:
            params["product_id"] = product_id
        response = self._authenticated_request(
            "GET", "reviews", params=params, headers={"locale": "ru-RU"}
        )
        data = self._json(response) if response is not None else None
        if isinstance(data, dict) and data.get("retval") == 0:
            self.last_failure = None
            return data
        if response is not None:
            self.last_failure = APIFailure.PERMANENT
        return None

    def get_review_by_invoice(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        if not isinstance(invoice_id, int) or isinstance(invoice_id, bool) or invoice_id <= 0:
            self.last_failure = APIFailure.PERMANENT
            return None
        for page in range(1, 20):
            data = self.get_reviews(count=50, page=page)
            if not data:
                break
            reviews = data.get("reviews", [])
            if not isinstance(reviews, list) or not reviews:
                break
            for review in reviews:
                if isinstance(review, dict) and review.get("invoice_id") == invoice_id:
                    return review
        return None
