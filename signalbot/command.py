import functools
from typing import List, Tuple, Optional

from .context import Context
from .message import Message


def triggered(*by, case_sensitive=False):
    def decorator_triggered(func):
        @functools.wraps(func)
        async def wrapper_triggered(*args, **kwargs):
            c = args[1]
            text = c.message.text
            if not isinstance(text, str):
                return

            by_words = by
            if not case_sensitive:
                text = text.lower()
                by_words = [t.lower() for t in by_words]
            if text not in by_words:
                return

            return await func(*args, **kwargs)

        return wrapper_triggered

    return decorator_triggered


class Command:
    # optional
    def setup(self):
        pass

    # optional
    def describe(self) -> str:
        return None

    # overwrite
    async def handle(self, context: Context):
        raise NotImplementedError

    async def firewall(self, c: Context) -> Optional[Tuple[str, str]]:
        # Check User
        self.db_cursor.execute(
            f"SELECT name, id FROM Person WHERE number = {c.message.source}"
        )
        user_name = None
        user_id = None
        for x in self.db_cursor:
            user_name = x[0]
            user_id = x[1]
        if user_name:
            return user_name, user_id
        c.send(
            f"You are not registered yet and cannot invite people. Register with 'register NAME'"
        )
        return None, None

    # helper method
    # deprecated: please use @triggered
    @classmethod
    def triggered(cls, message: Message, trigger_words: List[str]) -> bool:
        # Message needs to be text
        text = message.text
        if not isinstance(text, str):
            return False

        # Text must match trigger words without capitalization
        text = text.lower()
        if text in trigger_words:
            return True

        return False


class CommandError(Exception):
    pass
