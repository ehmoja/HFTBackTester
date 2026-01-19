"""Command-line entrypoints for compile and run."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mm_bt.cli.bt_compile import main as compile_main
    from mm_bt.cli.bt_run import main as run_main
    from mm_bt.cli.tardis_download import main as tardis_download_main


def __getattr__(name: str):
    if name == "compile_main":
        from mm_bt.cli.bt_compile import main as _main

        return _main
    if name == "run_main":
        from mm_bt.cli.bt_run import main as _main

        return _main
    if name == "tardis_download_main":
        from mm_bt.cli.tardis_download import main as _main

        return _main
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "compile_main",
    "run_main",
    "tardis_download_main",
]
