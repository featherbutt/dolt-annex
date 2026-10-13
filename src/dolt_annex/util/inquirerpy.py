from collections.abc import Callable
from typing import Any, cast

import InquirerPy.inquirer
from InquirerPy.base.control import Choice, Separator # pyright: ignore[reportPrivateImportUsage]

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