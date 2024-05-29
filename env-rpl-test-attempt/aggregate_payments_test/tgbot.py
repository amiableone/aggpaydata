import json
import urllib.parse
import urllib.request

from datetime import datetime, timedelta
from typing import Optional, Dict, Callable


class BotCommandBase:
    _description = ""
    callback: Optional[Callable] = None

    @property
    def command(self):
        return self.__class__.__name__.lower()

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, desc):
        if isinstance(desc, str):
            self._description = desc.capitalize()[:25]

    def register_callback(self, callback):
        """
        Register callback to make instance callable.
        """
        if not isinstance(callback, Callable):
            raise ValueError("callback must be callable")
        self.callback = callback

    def __call__(self, *args, **kwargs):
        return self.callback(*args, **kwargs)


class Start(BotCommandBase):
    pass


class Help(BotCommandBase):
    pass


class Settings(BotCommandBase):
    pass


start = Start()
print(start.command)


class BotBase:
    token = ""
    url = "https://api.telegram.org/bot%s/%s"

    def __init__(self, token):
        if not token or not isinstance(token, str):
            raise ValueError("token must be a non-empty string")
        self.token = token

    def get(self, method):
        url = self.get_url(method)
        with urllib.request.urlopen(url) as response:
            contents = response.read()
            contents = json.loads(contents)
        return contents

    def post(self, method, data, headers={}):
        if not isinstance(data, dict):
            raise ValueError("data must be dict")
        if not isinstance(headers, dict):
            raise ValueError("headers must be dict")
        url = self.get_url(method)
        bdata = urllib.parse.urlencode(data).encode()
        initial_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        }
        headers.update(initial_headers)
        request = urllib.request.Request(url, bdata, headers)
        with urllib.request.urlopen(request) as response:
            contents = json.loads(response.read())
        return contents

    def get_url(self, method):
        return self.url % (self.token, method)



class BotCommandManagerMixin:
    """
    Adds commands start, help (optional), and
    settings (optional).
    """
    commands: Dict[str, BotCommandBase] = {}
    get_my_commands = "getMyCommands"

    def add_start(self, callback):
        self.add_commands(start=callback)

    def add_help(self, callback):
        self.add_commands(help=callback)

    def add_settings(self, callback):
        self.add_commands(settings=callback)

    def add_commands(self, **commands_callbacks):
        res = []
        for command, callback in commands_callbacks.items():
            try:
                cmd_class = type(
                    command.capitalize(),
                    (BotCommandBase,),
                    {},
                )
                cmd_instance = cmd_class()
                value = self.commands.setdefault(command, cmd_instance)
                if value == cmd_instance:
                    cmd_instance.register_callback(callback)
                    res.append(f"succes: /{command}")
                else:
                    res.append(f"failed: command /{command} already exists")
            except ValueError:
                # register_callback raised ValueError.
                res.append(f"failed: callback must be callable, not {type(callback)}")
        # Return success status of each command.
        return res

    @property
    def commands_are_set(self):
        try:
            return self._commands_are_set, None
        except AttributeError:
            try:
                res = self.get(self.get_my_commands)
            except AttributeError:
                raise TypeError(
                    f"BotBase must be base of this instance type for this method to work"
                )
            if res["ok"]:
                # check if command list is not empty
                self._commands_are_set = bool(res["result"])
                return self._commands_are_set, res["result"]


class BotUpdateManagerMixin:
    allowed_updates = ["message", "edited_message"]
    _get_updates = "getUpdates"
    last_update_date = None
    limit = 100
    last_update_id = 0
    offset = 1
    reset_period = timedelta(days=7)
    timeout = 10
    updates = []

    @classmethod
    def recalculate_lud(cls, date):
        cls.last_update_date = date

    @classmethod
    def recalculate_luid(cls, luid):
        if datetime.today() - cls.last_update_date <= cls.reset_period:
            cls.last_update_id = max(cls.last_update_id, luid)
        else:
            cls.last_update_id = luid

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
        bdata = urllib.parse.urlencode(data).encode()
        try:
            res = self.get(self._get_updates)
        except AttributeError:
            raise TypeError(
                f"BotBase must be base of this instance type for this method to work"
            )
        if res["ok"]:
            self.updates = res["result"][::-1]

    def _get_request_data(self):
        return {
            "offset": self.offset,
            "limit": self.limit,
            "timeout": self.timeout,
            "allowed_updates": self.allowed_updates,
        }

    def process_updates(self):
        for update in updates:
            # Retrive relevant data from update
            update_id = update["update_id"]
            try:
                message = update["message"]
            except KeyError:
                message = update["edited_message"]
