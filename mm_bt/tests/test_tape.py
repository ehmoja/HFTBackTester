import json

from mm_bt.core import Lots, QuoteAtoms, Side, Ticks, TsNs
from mm_bt.sim import TapeWriter


def test_tape_writer_records(tmp_path) -> None:
    path = tmp_path / "tape.jsonl"
    with TapeWriter(path, run_meta={"run_id": "test"}) as tape:
        tape.record_action(
            ts_recv_ns=TsNs(1000),
            action_id=1,
            side=Side.BID,
            qty_lots=Lots(2),
        )
        tape.record_fill(
            ts_recv_ns=TsNs(1000),
            fill_id=1,
            action_id=1,
            side=Side.BID,
            price_ticks=Ticks(10),
            qty_lots=Lots(2),
            notional=QuoteAtoms(20),
            fee_atoms=QuoteAtoms(1),
        )
        tape.record_equity(
            ts_recv_ns=TsNs(1000),
            cash=QuoteAtoms(100),
            position=Lots(2),
            equity=QuoteAtoms(120),
        )

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    header = json.loads(lines[0])
    assert header["type"] == "header"
    assert header["run_id"] == "test"
    action = json.loads(lines[1])
    assert action["type"] == "action"
    assert action["action_id"] == 1
    fill = json.loads(lines[2])
    assert fill["type"] == "fill"
    assert fill["fill_id"] == 1
    equity = json.loads(lines[3])
    assert equity["type"] == "equity"
