import sys
from typing import Any, cast

import InquirerPy.inquirer
from InquirerPy.base.control import Choice, Separator # pyright: ignore[reportPrivateImportUsage]

if sys.stdin.isatty():
    async def text(message: str) -> str:
        result = await InquirerPy.inquirer.text( # pyright: ignore[reportPrivateImportUsage]
            message=message
        ).execute_async()
        return cast(str, result)
        
    async def choices(message: str, choices: list[Choice | Separator]):
        choice = await InquirerPy.inquirer.rawlist( # pyright: ignore[reportPrivateImportUsage]
            message=message,
            choices=choices,
        ).execute_async() 
        return cast(Any, choice)

    async def confirm(message: str, default: bool = False) -> bool:
        result = await InquirerPy.inquirer.confirm( # pyright: ignore[reportPrivateImportUsage]
            message=message,
            default=default
        ).execute_async()
        return cast(bool, result)

    async def secret(message: str) -> str:
        result = await InquirerPy.inquirer.secret( # pyright: ignore[reportPrivateImportUsage]
            message=message
        ).execute_async()
        return cast(str, result)

    async def filepath(message: str) -> str:
        result = await InquirerPy.inquirer.filepath( # pyright: ignore[reportPrivateImportUsage]
            message=message
        ).execute_async()
        return cast(str, result)
else:
    import getpass
    
    async def text(message: str) -> str:
        return input(f"{message}: ")

    async def choices(message: str, choices: list[Choice | Separator]):
        print(message)
        for i, choice in enumerate(choices):
            if isinstance(choice, Separator):
                print(f"{choice._line}")
            else:
                print(f"{i + 1}) {choice.name}")

        selection = int(input())
        choice = choices[selection-1]
        return cast(Choice, choice).value


    async def confirm(message: str, default: bool = False) -> bool:
        response = input(f"{message} ({'Y/n' if default else 'y/N'}): ").strip().lower()
        if not response:
            return default
        return response in ["y", "yes"]

    async def secret(message: str) -> str:
        return getpass.getpass(f"{message}: ")

    async def filepath(message: str) -> str:
        return input(f"{message}: ")