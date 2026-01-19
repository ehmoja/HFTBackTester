"""Tardis IO utilities (CSV read, metadata, file location)."""

from __future__ import annotations

from mm_bt.io.instrument_meta import (
    InstrumentMeta,
    InstrumentMetaProvider,
    StaticJsonProvider,
    TardisInstrumentMetaApiProvider,
)
from mm_bt.io.infer_increments import infer_l2_increments
from mm_bt.io.tardis_csv import L2_HEADER, L2Row, iter_l2_rows
from mm_bt.io.tardis_download import (
    DownloadNotFound,
    DownloadPlan,
    build_download_plan,
    canonical_tardis_path,
    download_tardis_csv_gz,
)
from mm_bt.io.tardis_locator import TardisLocator, locate_tardis_files

__all__ = [
    "DownloadNotFound",
    "DownloadPlan",
    "InstrumentMeta",
    "InstrumentMetaProvider",
    "L2_HEADER",
    "L2Row",
    "StaticJsonProvider",
    "TardisInstrumentMetaApiProvider",
    "infer_l2_increments",
    "TardisLocator",
    "build_download_plan",
    "canonical_tardis_path",
    "download_tardis_csv_gz",
    "iter_l2_rows",
    "locate_tardis_files",
]
