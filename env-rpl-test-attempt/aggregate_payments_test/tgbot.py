import json
import urllib.parse
import urllib.request

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
                    f"Probably forgot to use BotBase as base for this instance type"
                )
            if res["ok"]:
                # check if command list is not empty
                self._commands_are_set = bool(res["result"])
                return self._commands_are_set, res["result"]
