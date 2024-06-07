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

    def __init__(self, callback=None, description=None):
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
        if not desc:
            self._description = self.__class__.__name__
        elif isinstance(desc, str):
            self._description = desc.capitalize()[:25]

    def register_callback(self, callback):
        """
        Register callback to make instance callable.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")
        self.callback = callback

    def __call__(self, *args, **kwargs):
        return self.callback(*args, **kwargs)


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
        self._stop_session: Optional[asyncio.Future] = None

    async def run(self):
        async with aiohttp.ClientSession(self.base_url) as session:
            self.session = session
            self._loop = asyncio.get_running_loop()
            self.is_running = True
            self._stop_session = self._loop.create_future()
            await self._stop_session
        self.is_running = False
        self._stop_session = None

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
            cmd_instance = cmd_class(callback)
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
        # TODO:
        #   Consider making BotComandBase JSON serializable.
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


class BotUpdateManagerMixin:
    """
    This mixin adds update polling feature to BotBase subclass.
    """
    allowed_updates = ["message", "edited_message"]
    _get_updates = "getUpdates"
    last_update_date = None
    last_update_id = 0
    limit = 100
    offset = 1
    reset_period = timedelta(days=7)
    timeout = 10
    updates = []
    messages = []

    def __init__(self):
        # Can't initiate this class on its own.
        # Use as base of BotBase subclass.
        # e.g. `class Bot(BotBase, BotUpdateManagerMixin):...`
        raise NotImplementedError

    @classmethod
    def recalculate_lud(cls, date):
        """date is param of update object from Telegram Bot API"""
        cls.last_update_date = date

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
            lud = datetime.fromtimestamp(cls.last_update_date)
            return datetime.today() - lud > cls.reset_period
        except TypeError:
            return False

    def get_updates(self):
        data = self._get_request_data()
        try:
            res = self.get(self._get_updates, data)
        except AttributeError:
            raise TypeError(
                f"BotBase must be base of this instance type for this method to work"
            )
        if res["ok"]:
            self.updates = res["result"]

    def _get_request_data(self):
        return {
            "offset": self.offset,
            "limit": self.limit,
            "timeout": self.timeout,
            "allowed_updates": self.allowed_updates,
        }

    def process_updates(self):
        update_id = 0
        for update in self.updates:
            update_id = max(update["update_id"], update_id)
            try:
                message = update["message"]
            except KeyError:
                message = update["edited_message"]
            user_input = json.loads(message["text"])
            if isinstance(user_input, dict):
                self.messages.append(message)
        self.updates = []
        # Update class attributes
        date = update.get("date", update.get("edit_date"))
        self.__class__.recalculate_lud(date)
        self.__class__.recalculate_luid(update_id)
        self.__class__.recalculate_offset()
