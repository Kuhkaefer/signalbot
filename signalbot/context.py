# from .bot import Signalbot # TODO: figure out how to enable this for typing
from .message import Message


class Context:
    def __init__(self, bot, message: Message):
        self.bot = bot
        self.message = message

    async def send(
        self, text: str, base64_attachments: list = None, listen: bool = False
    ):
        await self.bot.send(
            self.message.recipient(),
            text,
            base64_attachments=base64_attachments,
            listen=listen,
        )

    async def react(self, emoji: str):
        await self.bot.react(self.message, emoji)

    async def start_typing(self):
        await self.bot.start_typing(self.message.recipient())

    async def stop_typing(self):
        await self.bot.stop_typing(self.message.recipient())

    async def list_group_members(self, group_id):
        resp = await self.bot.list_group_members(group_id)
        return resp

    async def list_groups(self):
        resp = await self.bot.list_groups()
        return resp
