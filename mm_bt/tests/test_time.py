from mm_bt.core import OrderingKey, compare_ordering_key


def test_ordering_key_comparison() -> None:
    a = OrderingKey(1, 0, 0)
    b = OrderingKey(1, 1, 0)
    c = OrderingKey(2, 0, 0)

    assert a < b < c
    assert compare_ordering_key(a, a) == 0
    assert compare_ordering_key(a, b) == -1
    assert compare_ordering_key(c, b) == 1
