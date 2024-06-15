import aiohttp
import asyncio
import json

from datetime import datetime, timedelta
from typing import Optional, Dict, Callable
from urllib.parse import urlencode


class BotCommandBase:
    """
    This class represents a command object in Telegram Bot API.
    """
    _description = ""
    callback: Optional[Callable] = None

    def __init__(self, bot: "BotBase", callback=None, description=None):
        self.bot = bot
        if callback:
            self.register_callback(callback)
        self.description = description

    @property
    def command(self):
        return self.__class__.__name__.lower()

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, desc):
        try:
            self._description = desc.capitalize()[:25]
        except TypeError:
            self._description = self.__class__.__name__

    def register_callback(self, callback):
        """
        Register callback to make instance callable.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        self.callback = callback

    def __call__(self, *args, **kwargs):
        res = self.callback(*args, **kwargs)
        try:
            # this makes __call__ effectively awaitable
            # when self.callback is a coroutine function.
            return self.bot._loop.create_task(res)
        except TypeError:
            return res


class BotBase:
    """
    This class manages telegram bot runtime and allows making
    get and post requests.
    """
    base_url = "https://api.telegram.org"
    token = "/bot%s"
    method = "/%s"

    def __init__(self, token):
        if not token or not isinstance(token, str):
            raise ValueError("token must be a non-empty string")
        self.token %= token
        self.session = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self.is_running = False
        self._tasks = set()
        self._work_complete: Optional[asyncio.Future] = None
        self._stop_session: Optional[asyncio.Future] = None

    async def run(self):
        async with aiohttp.ClientSession(self.base_url) as session:
            self.session = session
            self._loop = asyncio.get_running_loop()
            self.is_running = True
            self._stop_session = self._loop.create_future()
            self._work_complete = self._loop.create_future()
            await self._stop_session
            self.is_running = False
            await self._work_complete
        self._stop_session = None
        self._work_complete = None

    def add_tasks(self, *tasks):
        for task in tasks:
            task: asyncio.Task
            self._tasks.add(task)
            task.add_done_callback(self.complete_work)

    def complete_work(self, task):
        self._tasks.discard(task)
        if not self._tasks:
            self._work_complete.set_result(None)

    def stop_session(self):
        self._stop_session.set_result(None)

    async def get(self, method, data={}):
        data = "?" + urlencode(data) if data else ""
        url = self.token + self.method % method + data
        async with self.session.get(url) as response:
            contents = await response.read()
            contents = json.loads(contents)
        return contents

    async def post(self, method, data, headers={}, as_json=True):
        url = self.token + self.method % method
        if as_json:
            data = json.dumps(data)
            hdrs = {"Content-Type": "application/json; charset=utf-8"}
        else:
            data = urlencode(data).encode()
            hdrs = {
                "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            }
        hdrs.update(headers)
        async with self.session.post(url, data=data, headers=hdrs) as response:
            contents = await response.read()
            contents = json.loads(contents)
        return contents


class BotCommandManagerMixin:
    """
    This mixin adds command managing features to BotBase subclass.
    """
    commands: Dict[str, BotCommandBase] = {}
    _get_commands = "getMyCommands"
    _set_commands = "setMyCommands"
    has_unset_commands = False

    def __init__(self):
        # Can't initiate this class on its own.
        # Use as base of BotBase subclass.
        # e.g. `class Bot(BotBase, BotCommandManagerMixin):...`
        raise NotImplementedError

    def add_start(self, callback):
        self.add_commands(start=callback)

    def add_help(self, callback):
        self.add_commands(help=callback)

    def add_settings(self, callback):
        self.add_commands(settings=callback)

    def add_commands(self, **commands_callbacks):
        for command, callback in commands_callbacks.items():
            if self.commands.get(command, False):
                continue
            cmd_class = type(
                command.capitalize(),
                (BotCommandBase,),
                {},
            )
            cmd_instance = cmd_class(self, callback)
            self.__class__.commands[command] = cmd_instance
            self.__class__.has_unset_commands = True

    async def get_commands(self):
        res = await self.get(self._get_commands)
        if res["ok"]:
            return res["result"]

    async def set_commands(self):
        """
        Sets telegram bot commands taken from self.commands.
        Commands already set but missing from self.commands will be unset.
        """
        cmds = {
            "commands": [],
        }
        for name, command in self.commands.items():
            cmd_dict = {
                "command": name,
                "description": command.description,
            }
            cmds["commands"].append(cmd_dict)
        res = await self.post(self._set_commands, cmds)
        if res["ok"]:
            self.__class__.has_unset_commands = False
        return res


class BotUpdateHandlerMixin:
    """
    This mixin adds update polling feature to BotBase subclass.
    """
    _get_updates = "getUpdates"
    allowed_updates = ["message", "edited_message"]
    limit = 100
    offset = 1
    timeout = 3600

    # if no update retrieved for 7 days, id of the next update is set randomly
    _reset_period = timedelta(days=7)
    last_update_date = None
    last_update_id = 0
    updates: asyncio.Queue = asyncio.Queue()
    queries: asyncio.Queue = asyncio.Queue()
    cmds_pending: asyncio.Queue = asyncio.Queue()

    def __init__(self):
        # Can't initiate this class on its own.
        # Use as base of BotBase subclass.
        # e.g. `class Bot(BotBase, BotUpdateHandlerMixin):...`
        raise NotImplementedError

    @classmethod
    def recalculate_lud(cls, date):
        # `date` is param of Update object from Telegram Bot API.
        cls.last_update_date = datetime.fromtimestamp(date)

    @classmethod
    def recalculate_luid(cls, luid):
        if cls.reset_period_expired():
            cls.last_update_id = luid
        else:
            cls.last_update_id = max(cls.last_update_id, luid)

    @classmethod
    def recalculate_offset(cls):
        cls.offset = cls.last_update_id + 1

    @classmethod
    def reset_period_expired(cls):
        try:
            return datetime.today() - cls.last_update_date > cls._reset_period
        except TypeError:
            return False

    async def get_updates(self):
        data = self._get_request_data()
        res = await self.get(self._get_updates, data)
        if res["ok"]:
            for update in res["result"]:
                self.updates.put_nowait(update)

    def _get_request_data(self):
        return {
            "offset": self.offset,
            "limit": self.limit,
            "timeout": self.timeout,
            "allowed_updates": self.allowed_updates,
        }

    async def process_updates(self):
        """
        Process updates and recalculate offset param.
        """
        update_id = 0
        date = self.last_update_date
        # while stmt references attr of BotBase.
        while not self.updates.empty():
            update = await self.updates.get()
            update_id = max(update["update_id"], update_id)
            self.process_update(update)
        # Update class attributes
        date = msg_obj.get("date") or msg_obj.get("edit_date") or date
        self.__class__.recalculate_lud(date)
        self.__class__.recalculate_luid(update_id)
        self.__class__.recalculate_offset()

    def process_update(self, update):
        """
        Process update if 'message' in Update or 'edited_message' in Update
        """
        msg_obj = update.get("message") or update.get("edited_message")
        if msg_obj:
            self.process_message(msg_obj)

    def process_message(self, msg_obj):
        """
        Parse message for command or query and put processed data into
        the corresponding queue.
        """
        try:
            chat_id = msg_obj["chat"]["id"]
            message = msg_obj["text"]
            if message.startswith("/"):
                # only commands in the beginning of the message are supported.
                for entity in msg_obj["entities"]:
                    if entity["type"] == "bot_command":
                        cmd_end = entity["length"]
                command, params = message[1:cmd_end], message[cmd_end:]
                cmd_pending = chat_id, command, params
                self.cmds_pending.put_nowait(cmd_pending)
            else:
                params = self._deserialize(message)
                if isinstance(params, dict):
                    query = chat_id, params
                    self.queries.put_nowait(query)
        except (
            json.JSONDecodeError,
            TypeError,
            KeyError,
        ):
            pass

    def _deserialize(self, msg):
        try:
            return json.loads(msg)
        except json.JSONDecodeError:
            # next JSONDecodeError will be propagated.
            return json.loads(msg.replace("'", "\""))

    async def run_polling(self):
        while self.is_running:
            getter = asyncio.create_task(self.get_updates())
            stopper = self._stop_session
            done, pending = await asyncio.wait(
                [getter, stopper], return_when=asyncio.FIRST_COMPLETED,
            )
            first = done.pop()
            if getter is not first:
                getter.cancel()
                break
            await self.process_updates()


class Bot(BotBase, BotCommandManagerMixin, BotUpdateHandlerMixin):
    pass
