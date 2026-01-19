import json

from mm_bt.core import Side
from mm_bt.ingest import JsonlQuarantineSink, QuarantineRecord


def test_jsonl_quarantine_sink(tmp_path) -> None:
    path = tmp_path / "quarantine.jsonl"
    record = QuarantineRecord(
        reason="bad row",
        source="test.csv",
        line_number=3,
        payload={"side": Side.BID, "value": 1},
    )
    sink = JsonlQuarantineSink(path)
    with sink:
        sink.record(record)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["reason"] == "bad row"
    assert data["payload"]["side"] == "bid"
