import asyncio
import base64
import logging
from typing import List

import aiohttp
import aiohttp.http_exceptions
import websockets

logging.getLogger().setLevel(logging.INFO)


class SignalAPI:
    def __init__(
        self, signal_service: str, phone_number: str, exit_gracefully: asyncio.Event
    ):
        self.signal_service = signal_service
        self.phone_number = phone_number
        self.exit_gracefully = exit_gracefully

        # self.session = aiohttp.ClientSession()

    async def receive(self):
        try:
            uri = self._receive_ws_uri()
            self.connection = websockets.connect(uri, ping_interval=None)
            async with self.connection as websocket:
                while not self.exit_gracefully.is_set():
                    try:
                        # Use asyncio.wait_for to add a timeout to the async for loop
                        raw_message = await asyncio.wait_for(
                            websocket.recv(), timeout=0.2
                        )
                        # TODO: set timeout with config
                        yield raw_message
                    except asyncio.TimeoutError:
                        # no message
                        continue
                logging.info("Signalbot API: Graceful exit, stop receiving")

        except Exception as e:
            raise ReceiveMessagesError(e)

    async def get_attachment(self, attachment_id: str) -> str:
        """Fetch attachment by given id and encode to base64."""
        uri = f"{self._attachment_rest_uri()}/{attachment_id}"
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(uri)
                resp.raise_for_status()
                content = await resp.content.read()
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
        ):
            raise GetAttachmentError

        base64_bytes = base64.b64encode(content)
        base64_string = str(base64_bytes, encoding="utf-8")

        return base64_string

    def _attachment_rest_uri(self):
        return f"http://{self.signal_service}/v1/attachments"

    async def send(
        self,
        receiver: str,
        message: str,
        base64_attachments: list = None,
        text_mode: str = None,
    ) -> aiohttp.ClientResponse:
        uri = self._send_rest_uri()
        if base64_attachments is None:
            base64_attachments = []
        if text_mode is None:
            text_mode = "styled"
        payload = {
            "base64_attachments": base64_attachments,
            "message": message,
            "number": self.phone_number,
            "recipients": [receiver],
            "text_mode": text_mode,
        }
        logging.info(f"Send \n'{message}'\nto '{receiver}'")
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                await self.raise_for_status(resp)
                return resp
        except SignalClientResponseError as exc:
            logging.exception("Sending failed")
            if exc.text and "RateLimitException" in exc.text:
                raise RateLimitError(exc.status_code, exc.message, exc.url, exc.text)
            raise
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            logging.exception("Sending failed")
            raise SendMessageError

    async def react(
        self, recipient: str, reaction: str, target_author: str, timestamp: int
    ) -> aiohttp.ClientResponse:
        uri = self._react_rest_uri()
        payload = {
            "recipient": recipient,
            "reaction": reaction,
            "target_author": target_author,
            "timestamp": timestamp,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
        ):
            raise ReactionError

    async def start_typing(self, receiver: str):
        uri = self._typing_indicator_uri()
        payload = {
            "recipient": receiver,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.put(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
        ):
            raise StartTypingError

    async def stop_typing(self, receiver: str):
        uri = self._typing_indicator_uri()
        payload = {
            "recipient": receiver,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.delete(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
        ):
            raise StopTypingError

    async def send_receipt(self, receiver: str, timestamp: int, receipt_type: str):
        if receipt_type not in ["read", "viewed"]:
            raise SendReceiptError(f"Invalid receipt_type: '{receipt_type}'")
        uri = self._receipt_uri()
        payload = {
            "recipient": receiver,
            "receipt_type": receipt_type,
            "timestamp": timestamp,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
        ):
            raise SendReceiptError

    async def list_group_members(self, group: str) -> aiohttp.ClientResponse:
        uri = self._list_group_members_uri(group)
        payload = {
            "number": self.phone_number,
            "group": group,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def list_groups(self) -> aiohttp.ClientResponse:
        uri = self._list_groups_uri()
        payload = {
            "number": self.phone_number,
        }

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(uri, json=payload)
                resp.raise_for_status()
                return await resp.json()
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def create_group(
        self, name: str, description: str, members: List
    ) -> aiohttp.ClientResponse:
        uri = self._create_group_uri()
        payload = {
            "description": description,
            "group_link": "enabled",
            "members": [self.phone_number].extend(members),
            "name": name,
            "permissions": {"add_members": "only-admins", "edit_group": "only-admins"},
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def delete_group(self, group_id: str) -> aiohttp.ClientResponse:
        uri = self._delete_group_uri(group_id)
        payload = {
            "number": self.phone_number,
            "group_id": group_id,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.delete(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def add_to_group(self, group_id: str, member: str) -> aiohttp.ClientResponse:
        uri = self._add_to_group_uri(group_id)
        payload = {
            "members": [member],
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def list_group(self, group_id: str) -> aiohttp.ClientResponse:
        uri = self._list_group_uri(group_id)
        payload = {
            "number": self.phone_number,
            "group_id": group_id,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def update_group(
        self, group_id: str, base64_avatar: str, description: str
    ) -> aiohttp.ClientResponse:
        uri = self._update_group_members_uri(group_id)
        payload = {
            "number": self.phone_number,
            "group_id": group_id,
            "base64_avatar": base64_avatar,
            "description": description,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.put(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def remove_group_members(
        self, group_id: str, members: List[str]
    ) -> aiohttp.ClientResponse:
        uri = self._remove_group_members_uri(group_id)
        payload = {
            "number": self.phone_number,
            "group_id": group_id,
            "members": members,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.delete(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def quit_group(self, group_id: str) -> aiohttp.ClientResponse:
        uri = self._quit_group_uri(group_id)
        payload = {
            "number": self.phone_number,
            "group_id": group_id,
        }
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(uri, json=payload)
                resp.raise_for_status()
                return resp
        except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
            KeyError,
        ):
            raise SendMessageError

    async def raise_for_status(self, resp):
        if not resp.ok:
            # reason should always be not None for a started response
            assert resp.reason is not None
            resp_json = await resp.json()
            resp.release()
            error_text = resp_json.get("error", "")
            raise SignalClientResponseError(
                resp.status, resp.reason, resp.request_info.real_url, error_text
            )

    def _receive_ws_uri(self):
        return f"ws://{self.signal_service}/v1/receive/{self.phone_number}"

    def _send_rest_uri(self):
        return f"http://{self.signal_service}/v2/send"

    def _react_rest_uri(self):
        return f"http://{self.signal_service}/v1/reactions/{self.phone_number}"

    def _typing_indicator_uri(self):
        return f"http://{self.signal_service}/v1/typing-indicator/{self.phone_number}"

    def _receipt_uri(self):
        return f"http://{self.signal_service}/v1/receipts/{self.phone_number}"

    def _list_group_members_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}"

    def _update_group_members_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}"

    def _list_groups_uri(self):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}"

    def _create_group_uri(self):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}"

    def _add_to_group_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}/members"

    def _list_group_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}"

    def _delete_group_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}"

    def _remove_group_members_uri(self, group_id: str):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}/members"

    def _quit_group_uri(self, group_id):
        return f"http://{self.signal_service}/v1/groups/{self.phone_number}/{group_id}/quit"


class ReceiveMessagesError(Exception):
    pass


class SendMessageError(Exception):
    pass


class SignalClientResponseError(aiohttp.ClientError):
    text = ""

    def __init__(self, status_code=None, message=None, url=None, text=None):
        self.status_code = status_code
        self.message = message
        self.url = url
        self.text = text

    def __str__(self) -> str:
        return "{}, message={!r} ({!r}), url={!r}".format(
            self.status_code,
            self.message,
            self.text,
            self.url,
        )


class TypingError(Exception):
    pass


class StartTypingError(TypingError):
    pass


class StopTypingError(TypingError):
    pass


class SendReceiptError(Exception):
    pass


class ReactionError(Exception):
    pass


class GetAttachmentError(Exception):
    pass


class RateLimitError(SendMessageError):
    def __init__(self, status_code=None, message=None, url=None, text=None):
        self.status_code = status_code
        self.message = message
        self.url = url
        self.text = text

    def __str__(self) -> str:
        return "{}, message={!r} ({!r}), url={!r}".format(
            self.status_code,
            self.message,
            self.text,
            self.url,
        )
