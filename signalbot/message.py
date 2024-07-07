import json
import logging
from enum import Enum
from typing import List, Optional, Tuple

from signalbot.api import SignalAPI


class MessageType(Enum):
    SYNC_MESSAGE = 1
    DATA_MESSAGE = 2


class Message:
    def __init__(
        self,
        source: str,
        timestamp: int,
        type_: MessageType,
        text: str,
        base64_attachments: list = None,
        attachment_types: list = None,
        group: str = None,
        reaction: str = None,
        mentions: list = None,
        quote: dir = None,
        contacts: list = None,
        raw_message: str = None,
    ):
        # required
        self.source = source
        self.timestamp = timestamp
        self.type = type_
        self.text = text

        # optional
        self.base64_attachments = base64_attachments
        if self.base64_attachments is None:
            self.base64_attachments = []
        self.attachment_types = attachment_types
        if self.attachment_types is None:
            self.attachment_types = []

        self.group = group

        self.reaction = reaction

        self.mentions = mentions
        if self.mentions is None:
            self.mentions = []

        self.quote = quote

        self.contacts = contacts

        self.raw_message = raw_message

    def recipient(self) -> str:
        # Case 1: Group chat
        if self.group:
            logging.info(f"group: {self.group}")
            return self.group

        # Case 2: User chat
        logging.info(f"chat: {self.source}")
        return self.source

    @classmethod
    async def parse(cls, signal: SignalAPI, raw_message: str):
        try:
            raw_message = json.loads(raw_message)
        except Exception:
            raise UnknownMessageFormatError
        logging.info(f"{raw_message=}")

        # General attributes
        try:
            source = raw_message["envelope"]["source"]
            timestamp = raw_message["envelope"]["timestamp"]
        except Exception:
            raise UnknownMessageFormatError

        # Option 1: syncMessage
        if "syncMessage" in raw_message["envelope"]:
            type_ = MessageType.SYNC_MESSAGE
            text = cls._parse_sync_message(raw_message["envelope"]["syncMessage"])
            group = cls._parse_group_information(
                raw_message["envelope"]["syncMessage"]["sentMessage"]
            )
            reaction = cls._parse_reaction(
                raw_message["envelope"]["syncMessage"]["sentMessage"]
            )
            mentions = cls._parse_mentions(
                raw_message["envelope"]["syncMessage"]["sentMessage"]
            )
            quote = cls._parse_quote(
                raw_message["envelope"]["syncMessage"]["sentMessage"]
            )
            # base64_attachments = None
            attachment_types, base64_attachments = await cls._parse_attachments(
                signal, raw_message["envelope"]["syncMessage"]["sentMessage"]
            )
            contacts = cls._parse_contacts(
                raw_message["envelope"]["syncMessage"]["sentMessage"]
            )

        # Option 2: dataMessage
        elif "dataMessage" in raw_message["envelope"]:
            type_ = MessageType.DATA_MESSAGE
            text = cls._parse_data_message(raw_message["envelope"]["dataMessage"])
            group = cls._parse_group_information(raw_message["envelope"]["dataMessage"])
            reaction = cls._parse_reaction(raw_message["envelope"]["dataMessage"])
            mentions = cls._parse_mentions(raw_message["envelope"]["dataMessage"])
            quote = cls._parse_quote(raw_message["envelope"]["dataMessage"])
            attachment_types, base64_attachments = await cls._parse_attachments(
                signal, raw_message["envelope"]["dataMessage"]
            )
            contacts = cls._parse_contacts(raw_message["envelope"]["dataMessage"])
        elif "typingMessage" in raw_message["envelope"]:
            # logging.info("typing message")
            return
        elif "receiptMessage" in raw_message["envelope"]:
            # logging.info("receipt message")
            return
        else:
            raise UnknownMessageFormatError

        # logging.info(f"{source=}")

        return cls(
            source,
            timestamp,
            type_,
            text,
            base64_attachments,
            attachment_types,
            group,
            reaction,
            mentions,
            quote,
            contacts,
            raw_message,
        )

    @classmethod
    async def _parse_attachments(
        cls, signal: SignalAPI, data_message: dict
    ) -> Tuple[List[str], List[str]]:
        if "attachments" not in data_message:
            return [], []

        attachment_types = []
        attachments = []
        for attachment in data_message["attachments"]:
            attachments.append(await signal.get_attachment(attachment["id"]))
            attachment_types.append(attachment["contentType"])

        return attachment_types, attachments

    @classmethod
    def _parse_sync_message(cls, sync_message: dict) -> str:
        try:
            text = sync_message["sentMessage"]["message"]
            return text
        except Exception:
            raise UnknownMessageFormatError

    @classmethod
    def _parse_data_message(cls, data_message: dict) -> str:
        try:
            text = data_message["message"]
            return text
        except Exception:
            raise UnknownMessageFormatError

    @classmethod
    def _parse_group_information(cls, message: dict) -> Optional[List]:
        try:
            group = message["groupInfo"]["groupId"]
            return group
        except Exception:
            return None

    @classmethod
    def _parse_mentions(cls, data_message: dict) -> List:
        try:
            mentions = data_message["mentions"]
            return mentions
        except Exception:
            return []

    @classmethod
    def _parse_quote(cls, data_message: dict) -> dir:
        try:
            quote = data_message["quote"]
            logging.info(f"{quote=}")
            return quote
        except Exception:
            return None

    @classmethod
    def _parse_reaction(cls, message: dict) -> str:
        try:
            reaction = message["reaction"]["emoji"]
            return reaction
        except Exception:
            return None

    @classmethod
    def _parse_contacts(cls, message: dict) -> List:
        try:
            contacts = message["contacts"]
            return contacts
        except Exception:
            return []

    def __str__(self):
        if self.text is None:
            return ""
        return self.text


class UnknownMessageFormatError(Exception):
    pass
