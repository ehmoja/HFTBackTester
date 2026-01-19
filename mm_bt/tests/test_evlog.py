from mm_bt.core import hash_json_bytes, hash_text_u64
from mm_bt.core import Lots, Side, Ticks, TsNs
from mm_bt.evlog import IndexEntry, read_index, write_index
from mm_bt.evlog import EvlogReader
from mm_bt.evlog import L2Batch, L2Update
from mm_bt.evlog import EvlogWriter


def _batch(ts_recv: int, ts_exch: int, resets: bool, updates) -> L2Batch:
    return L2Batch(
        ts_recv_ns=TsNs(ts_recv),
        ts_exch_ns=TsNs(ts_exch),
        resets_book=resets,
        updates=tuple(updates),
    )


def test_evlog_roundtrip(tmp_path) -> None:
    path = tmp_path / "test.evlog"
    batch = _batch(
        1000,
        900,
        True,
        [
            L2Update(Side.BID, Ticks(10), Lots(1), True),
            L2Update(Side.ASK, Ticks(11), Lots(2), True),
        ],
    )
    quantizer_hash = hash_json_bytes(
        {"price_increment": "1", "amount_increment": "1"}
    )
    with EvlogWriter(
        path,
        exchange_id=hash_text_u64("binance"),
        symbol_id=hash_text_u64("BTCUSDT"),
        quantizer_hash=quantizer_hash,
    ) as writer:
        writer.write_l2_batch(batch)

    with EvlogReader(path) as reader:
        batches = list(reader.iter_l2_batches())
    assert len(batches) == 1
    out = batches[0]
    assert int(out.ts_recv_ns) == 1000
    assert int(out.ts_exch_ns) == 900
    assert out.resets_book is True
    assert [u.side for u in out.updates] == [Side.BID, Side.ASK]
    assert [int(u.price_ticks) for u in out.updates] == [10, 11]
    assert [int(u.amount_lots) for u in out.updates] == [1, 2]
    assert [u.is_snapshot for u in out.updates] == [True, True]


def test_evlog_index_seek(tmp_path) -> None:
    path = tmp_path / "test.evlog"
    idx_path = tmp_path / "test.idx"
    batches = [
        _batch(
            1000,
            900,
            True,
            [L2Update(Side.BID, Ticks(10), Lots(1), True)],
        ),
        _batch(
            2000,
            1900,
            False,
            [L2Update(Side.ASK, Ticks(11), Lots(2), False)],
        ),
    ]
    entries = []
    quantizer_hash = hash_json_bytes(
        {"price_increment": "1", "amount_increment": "1"}
    )
    with EvlogWriter(
        path,
        exchange_id=hash_text_u64("binance"),
        symbol_id=hash_text_u64("BTCUSDT"),
        quantizer_hash=quantizer_hash,
    ) as writer:
        for batch in batches:
            offset = writer.tell()
            writer.write_l2_batch(batch)
            entries.append(IndexEntry(int(batch.ts_recv_ns), offset))
    write_index(idx_path, entries)

    with EvlogReader(path, index_path=idx_path) as reader:
        reader.seek_time(2000)
        out = list(reader.iter_l2_batches())
    assert len(out) == 1
    assert int(out[0].ts_recv_ns) == 2000


def test_index_roundtrip(tmp_path) -> None:
    idx_path = tmp_path / "test.idx"
    entries = [
        IndexEntry(1000, 16),
        IndexEntry(2000, 64),
    ]
    write_index(idx_path, entries)
    out = read_index(idx_path)
    assert out == entries
