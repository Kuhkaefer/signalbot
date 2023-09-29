import logging
from typing import List

import aiohttp
import websockets


class SignalAPI:
    def __init__(self, signal_service: str, phone_number: str):
        self.signal_service = signal_service
        self.phone_number = phone_number

        # self.session = aiohttp.ClientSession()

    async def receive(self):
        try:
            uri = self._receive_ws_uri()
            self.connection = websockets.connect(uri, ping_interval=None)
            async with self.connection as websocket:
                async for raw_message in websocket:
                    yield raw_message

        except Exception as e:
            raise ReceiveMessagesError(e)

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
        logging.info(f"Create group: \n{payload=}")
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
        logging.info(f"Add to group: \n{payload=}")
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

    def _receive_ws_uri(self):
        return f"ws://{self.signal_service}/v1/receive/{self.phone_number}"

    def _send_rest_uri(self):
        return f"http://{self.signal_service}/v2/send"

    def _react_rest_uri(self):
        return f"http://{self.signal_service}/v1/reactions/{self.phone_number}"

    def _typing_indicator_uri(self):
        return f"http://{self.signal_service}/v1/typing-indicator/{self.phone_number}"

    def _list_group_members_uri(self, group_id: str):
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


class TypingError(Exception):
    pass


class StartTypingError(TypingError):
    pass


class StopTypingError(TypingError):
    pass


class ReactionError(Exception):
    pass
