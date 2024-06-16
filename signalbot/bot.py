import asyncio
import logging
import time
import traceback
from typing import List

import mysql.connector
from aiohttp.client_exceptions import ClientResponseError
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .api import SignalAPI, ReceiveMessagesError
from .command import Command
from .context import Context
from .message import Message, UnknownMessageFormatError, MessageType
from .storage import RedisStorage, InMemoryStorage

logging.getLogger().setLevel(logging.INFO)


class SignalBot:
    def __init__(self, config: dict):
        """SignalBot

        Example Config:
        ===============
        signal_service: "127.0.0.1:8080"
        phone_number: "+49123456789"
        storage:
            redis_host: "redis"
            redis_port: 6379
        """
        self.config = config

        self.commands = []  # populated by .register()

        self.listen_all_users = False
        self.listen_all_groups = False
        self.groups_to_listen = {}  # populated by .listenGroup()
        self.users_to_listen = set()  # populated by .listenUser()
        self.blocked_users = set()
        self.blocked_groups = set()

        # Required
        self._init_api()
        self._init_event_loop()
        self._init_scheduler()
        self._init_db()
        self._init_status()

        # Optional
        self._init_storage()

    def _init_api(self):
        try:
            self.phone_number = self.config["phone_number"]
            self._signal_service = self.config["signal_service"]
            self._signal = SignalAPI(self._signal_service, self.phone_number)
        except KeyError:
            raise SignalBotError("Could not initialize SignalAPI with given config")

    def _init_event_loop(self):
        self._event_loop = asyncio.get_event_loop()
        self._q = asyncio.Queue()

    def _init_storage(self):
        try:
            config_storage = self.config["storage"]
            self._redis_host = config_storage["redis_host"]
            self._redis_port = config_storage["redis_port"]
            self.storage = RedisStorage(self._redis_host, self._redis_port)
        except Exception:
            self.storage = InMemoryStorage()
            logging.warning(
                "[Bot] Could not initialize Redis. In-memory storage will be used. "
                "Restarting will delete the storage!"
            )

    def _init_scheduler(self):
        try:
            self.scheduler = AsyncIOScheduler(event_loop=self._event_loop)
        except Exception as e:
            raise SignalBotError(f"Could not initialize scheduler: {e}")

    def _init_db(self):
        self.db_config = self.config["db_config"]

    def _init_status(self):
        self.test_mode = self.config.get("test_mode", False)
        self.maintenance = self.config.get("maintenance", False)

    def listen(self, required_id: str, optional_id: str = None):
        # Case 1: required id is a phone number, optional_id is not being used
        if self.is_phone_number(required_id):
            phone_number = required_id
            self.listenUser(phone_number)
            return

        # TODO: is uuid

        # Case 2: required id is a group id
        if self._is_group_id(required_id) and self._is_internal_id(optional_id):
            group_id = required_id
            internal_id = optional_id
            self.listenGroup(group_id, internal_id)
            return

        # Case 3: optional_id is a group id (Case 2 swapped)
        if self._is_internal_id(required_id) and self._is_group_id(optional_id):
            group_id = optional_id
            internal_id = required_id
            self.listenGroup(group_id, internal_id)
            return

        logging.warning(
            "[Bot] Can't listen for user/group because input does not look valid"
        )

    def listenUser(self, phone_number: str):
        if not self.is_phone_number(phone_number):
            logging.warning(
                "[Bot] Can't listen for user because phone number does not look valid"
            )
            return

        self.unblock_user(phone_number)
        self.users_to_listen.add(phone_number)

    def unlistenUser(self, phone_number: str):
        if not self.is_phone_number(phone_number):
            logging.warning(
                "[Bot] Can't unlisten user because phone number does not look valid"
            )
            return

        if phone_number not in self.users_to_listen:
            logging.warning(
                "Cant unlisten user because its not in self.users_to_listen"
            )
            return

        self.users_to_listen.remove(phone_number)

    def listenAllUsers(self):
        self.listen_all_users = True

    def listenAllGroups(self):
        self.listen_all_groups = True

    def listenGroup(self, group_id: str, internal_id: str):
        if not (self._is_group_id(group_id) and self._is_internal_id(internal_id)):
            logging.warning(
                "[Bot] Can't listen for group because group id and "
                "internal id do not look valid"
            )
            return

        self.unblock_group(internal_id)
        self.groups_to_listen[internal_id] = group_id

    def unlistenGroup(self, internal_id: str):
        if not (self._is_internal_id(internal_id)):
            logging.warning(
                "[Bot] Can't unlisten group because " "internal id does not look valid"
            )
            return

        if internal_id not in self.groups_to_listen:
            logging.warning(
                "Cant unlisten group because its not in self.groups_to_listen"
            )
            return

        self.groups_to_listen.pop(internal_id)

    def block_group(self, internal_id: str):
        if not self._is_internal_id(internal_id):
            logging.warning(f"'{internal_id}' is not an internal id")
            return

        self.unlistenGroup(internal_id)
        self.blocked_groups.add(internal_id)

    def unblock_group(self, internal_id: str):
        if not self._is_internal_id(internal_id):
            logging.warning(f"'{internal_id}' is not an internal id")
            return

        if internal_id in self.blocked_groups:
            self.blocked_groups.remove(internal_id)

    def block_user(self, user: str):
        if not (self.is_phone_number(user) or self.is_uuid(user)):
            logging.warning(f"'{user}' is not a number or uuid")
            return

        self.unlistenUser(user)
        self.blocked_users.add(user)

    def unblock_user(self, user: str):
        if not (self.is_phone_number(user) or self.is_uuid(user)):
            logging.warning(f"'{user}' is not a number or uuid")
            return

        if user in self.blocked_users:
            self.blocked_users.remove(user)

    def is_phone_number(self, phone_number: str) -> bool:
        if phone_number is None:
            return False
        if phone_number[0] != "+":
            return False
        if len(phone_number[1:]) > 15:
            return False
        return True

    def is_uuid(self, uuid: str) -> bool:
        if not uuid:
            return False
        uuid_format = [8, 4, 4, 4, 12]
        uuid_lengths = [len(part) for part in uuid.split("-")]
        is_uuid = uuid_lengths == uuid_format
        return is_uuid

    def _is_group_id(self, group_id: str) -> bool:
        if group_id is None:
            return False
        prefix = "group."
        if group_id[: len(prefix)] != prefix:
            return False
        if group_id[-1] != "=":
            return False
        return True

    def _is_internal_id(self, internal_id: str) -> bool:
        if internal_id is None:
            return False
        return internal_id[-1] == "="

    def register(self, command: Command):
        command.bot = self
        command.setup()
        self.commands.append(command)

    def start(self, producers=1, consumers=3):
        self._event_loop.create_task(
            self._produce_consume_messages(producers=producers, consumers=consumers)
        )

        # Add more scheduler tasks here
        # self.scheduler.add_job(...)
        self.scheduler.start()

        # Run event loop
        self._event_loop.run_forever()

    async def send(
        self,
        receiver: str,
        text: str,
        base64_attachments: list = None,
        listen: bool = False,
        text_mode: str = None,
    ) -> int:
        resolved_receiver = await self._resolve_receiver(receiver)
        resp = await self._signal.send(
            resolved_receiver,
            text,
            base64_attachments=base64_attachments,
            text_mode=text_mode,
        )
        resp_payload = await resp.json()
        timestamp = resp_payload["timestamp"]
        # logging.info(f"[Bot] New message {timestamp} sent:\n{text}")

        if listen:
            if self.is_phone_number(receiver):
                sent_message = Message(
                    source=receiver,  # otherwise we can't respond in the right chat
                    timestamp=timestamp,
                    type_=MessageType.SYNC_MESSAGE,
                    text=text,
                    base64_attachments=base64_attachments,
                    group=None,
                )
            else:
                sent_message = Message(
                    source=self.phone_number,  # no need to pretend
                    timestamp=timestamp,
                    type_=MessageType.SYNC_MESSAGE,
                    text=text,
                    base64_attachments=base64_attachments,
                    group=receiver,
                )
            await self._ask_commands_to_handle(sent_message)

        return timestamp

    async def react(self, message: Message, emoji: str):
        # TODO: check that emoji is really an emoji
        receiver = await self._resolve_receiver(message.recipient())
        target_author = message.source
        logging.info(f"{receiver=}. {target_author=}")
        if target_author == receiver:
            logging.warning("Can't react to own message yet")
            return
        timestamp = message.timestamp
        await self._signal.react(receiver, emoji, target_author, timestamp)
        # logging.info(f"[Bot] New reaction: {emoji}")

    async def start_typing(self, receiver: str):
        receiver = await self._resolve_receiver(receiver)
        await self._signal.start_typing(receiver)

    async def stop_typing(self, receiver: str):
        receiver = await self._resolve_receiver(receiver)
        await self._signal.stop_typing(receiver)

    async def send_receipt(self, receiver: str, timestamp: int, receipt_type: str):
        receiver = await self._resolve_receiver(receiver)
        await self._signal.send_receipt(receiver, timestamp, receipt_type)

    async def list_group_members(self, group: str):
        return await self._signal.list_group_members(group)

    async def list_groups(self):
        return await self._signal.list_groups()

    async def create_group(self, name, description: str = " ", members: List = []):
        return await self._signal.create_group(name, description, members)

    async def add_to_group(self, group_id: str, member: str):
        return await self._signal.add_to_group(group_id, member)

    async def delete_group(self, group_id: str):
        return await self._signal.delete_group(group_id)

    async def update_group(
        self, group_id, base64_avatar: str = None, description: str = None
    ):
        return await self._signal.update_group(group_id, base64_avatar, description)

    async def list_group(self, group_id: str):
        return await self._signal.list_group(group_id)

    async def remove_group_members(self, group_id: str, members: List[str]):
        return await self._signal.remove_group_members(group_id, members)

    async def quit_group(self, group_id: str):
        return await self._signal.quit_group(group_id)

    async def _resolve_receiver(self, receiver: str) -> str:
        # Blocked receiver
        if receiver in [*self.blocked_users, *self.blocked_groups]:
            raise InvalidReceiverError(
                f"Tried to send to blocked group. This should not happen. {receiver=}"
            )

        # Number
        if self.is_phone_number(receiver):
            return receiver

        # uuid
        if self.is_uuid(receiver):
            return receiver

        # Not an internal id
        if not self._is_internal_id(receiver):
            raise InvalidReceiverError(
                f"Receiver could not be resolved. '{receiver}' is neither a number, "
                f"uuid or internal group id"
            )

        # Known internal id
        if receiver in self.groups_to_listen:
            internal_id = receiver
            group_id = self.groups_to_listen[internal_id]
            return group_id

        # Unknown internal id -> try to get it
        logging.warning(
            f"Group ID for internal ID '{receiver}' is not known. List groups to get it"
        )
        try:
            resp = await self.list_groups()
        except ClientResponseError:
            logging.exception(
                "Can't find group_id. Is sharebot not a member of that group?"
            )
            raise InvalidReceiverError(f"Can't find group_id for {receiver=}")

        for group in resp:
            if group["internal_id"] == receiver:
                break
        group_id = group["id"]
        logging.info(f"Got {group_id=}")
        return group_id

    # see https://stackoverflow.com/questions/55184226/catching-exceptions-in-individual-tasks-and-restarting-them
    @classmethod
    async def _rerun_on_exception(cls, coro, *args, **kwargs):
        """Restart coroutine by waiting an exponential time deplay"""
        max_sleep = 5 * 60  # sleep for at most 5 mins until rerun
        reset = 3 * 60  # reset after 3 minutes running successfully
        init_sleep = 1  # always start with sleeping for 1 second

        next_sleep = init_sleep
        while True:
            start_t = int(time.monotonic())  # seconds

            try:
                await coro(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:
                traceback.print_exc()
            except SystemExit as sexc:
                logging.info(f"System Exit with code {sexc.code}")
                if sexc.code and sexc.code in [0, 1, 2]:
                    logging.info("wait for tasks to finish")
                    await asyncio.gather(*asyncio.all_tasks())
                raise

            end_t = int(time.monotonic())  # seconds

            if end_t - start_t < reset:
                sleep_t = next_sleep
                next_sleep = min(max_sleep, next_sleep * 2)  # double sleep time
            else:
                next_sleep = init_sleep  # reset sleep time
                sleep_t = next_sleep

            logging.warning(f"Restarting coroutine in {sleep_t} seconds")
            await asyncio.sleep(sleep_t)

    async def _produce_consume_messages(self, producers=1, consumers=3) -> None:
        for n in range(1, producers + 1):
            produce_task = self._rerun_on_exception(self._produce, n)
            asyncio.create_task(produce_task)

        for n in range(1, consumers + 1):
            consume_task = self._rerun_on_exception(self._consume, n)
            asyncio.create_task(consume_task)

    async def _produce(self, name: int) -> None:
        logging.info(f"[Bot] Producer #{name} started")
        try:
            async for raw_message in self._signal.receive():
                # logging.info(f"[Raw Message] {raw_message}")

                try:
                    message = await Message.parse(self._signal, raw_message)
                except UnknownMessageFormatError:
                    continue

                if not message:
                    continue

                if not self._should_react(message):
                    continue

                await self._ask_commands_to_handle(message)

        except ReceiveMessagesError as e:
            # TODO: retry strategy
            raise SignalBotError(f"Cannot receive messages: {e}")

    def _should_react(self, message: Message) -> bool:
        source = message.recipient()
        logging.info(f"{source=}")

        # Source is blacklisted
        if source in [*self.blocked_users, *self.blocked_groups]:
            logging.info("user is blocked. don't react")
            return False

        # Listen all numbers or number is whitelisted
        if (self.is_phone_number(source) or self.is_uuid(source)) and (
            self.listen_all_users or source in self.users_to_listen
        ):
            logging.info("allow user")
            return True

        # Listen all groups or group is whitelisted
        if self._is_internal_id(source) and (
            self.listen_all_groups or source in self.groups_to_listen
        ):
            logging.info("allow group")
            return True

        # Unknown format
        if not (
            self.is_phone_number(source)
            or self.is_uuid(source)
            or self._is_internal_id(source)
        ):
            logging.warning(
                f"Source '{source}' neither a phone number, uuid or internal group id"
            )

        logging.info("don't react")
        return False

    async def _ask_commands_to_handle(self, message: Message):
        for command in self.commands:
            await self._q.put((command, message, time.perf_counter()))

    async def _consume(self, name: int) -> None:
        logging.info(f"[Bot] Consumer #{name} started")
        db_connection = mysql.connector.connect(**self.db_config)
        db_cursor = db_connection.cursor()
        db_cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        db_cursor.execute("SET @@SESSION.interactive_timeout=2700000")
        db_cursor.execute("SET @@SESSION.wait_timeout=2700000")
        db_connection.commit()
        while True:
            try:
                await self._consume_new_item(name, db_connection, db_cursor)
            except Exception:
                continue
            except SystemExit:
                db_cursor.close()
                db_connection.close()
                raise

    async def _consume_new_item(self, name: int, db_connection, db_cursor) -> None:
        command, message, t = await self._q.get()
        now = time.perf_counter()
        # logging.info(f"[Bot] Consumer #{name} got new job in {now-t:0.5f} seconds")

        # handle Command
        try:
            context = Context(self, message)
            await command.handle(context, db_connection, db_cursor)
        except Exception as e:
            logging.error(f"[{command.__class__.__name__}] Error: {e}")
            raise e

        # done
        self._q.task_done()


class SignalBotError(Exception):
    pass


class InvalidReceiverError(SignalBotError):
    pass
