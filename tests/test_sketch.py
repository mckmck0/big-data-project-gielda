"""Testy szkicu kwantylowego.

Uruchomienie z katalogu shared_workspace:
    python3 tests/test_sketch.py
"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gielda.sketch import LogHistogram

ALPHA = 0.01


def exact_quantile(values, q):
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


def test_median_within_alpha():
    random.seed(42)
    vals = [random.uniform(0.01, 50.0) for _ in range(20001)]
    h = LogHistogram(alpha=ALPHA)
    for v in vals:
        h.add(v)
    true_med = exact_quantile(vals, 0.5)
    approx = h.quantile(0.5)
    rel_err = abs(approx - true_med) / true_med
    assert rel_err <= ALPHA * 1.05, f"blad {rel_err:.4%} > alpha ({approx} vs {true_med})"


def test_merge_equals_union():
    random.seed(7)
    a_vals = [random.uniform(0.1, 5.0) for _ in range(5000)]
    b_vals = [random.uniform(2.0, 80.0) for _ in range(5000)]
    a, b, u = LogHistogram(ALPHA), LogHistogram(ALPHA), LogHistogram(ALPHA)
    for v in a_vals:
        a.add(v)
        u.add(v)
    for v in b_vals:
        b.add(v)
        u.add(v)
    a.merge(b)
    for q in (0.25, 0.5, 0.9):
        assert abs(a.quantile(q) - u.quantile(q)) < 1e-12, \
            f"merge != union dla q={q}"


def test_serialization_roundtrip():
    h = LogHistogram(ALPHA)
    for v in (0.5, 1.0, 2.0, 4.0, 8.0):
        h.add(v)
    restored = LogHistogram.from_json(h.to_json())
    assert restored.quantile(0.5) == h.quantile(0.5)
    assert restored.zero_count == h.zero_count
    assert restored.buckets == h.buckets


def test_zeros():
    h = LogHistogram(ALPHA)
    for _ in range(10):
        h.add(0.0)
    h.add(5.0)
    assert h.quantile(0.5) == 0.0, "mediana samych zer (poza jedna wartoscia) to 0"
    assert h.zero_count == 10


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"OK    {name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL  {name}: {e}")
    sys.exit(1 if failures else 0)
