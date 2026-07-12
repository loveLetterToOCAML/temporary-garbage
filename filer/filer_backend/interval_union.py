from __future__ import annotations

from typing_extensions import TypeVar, Generic
from sortedcontainers import SortedList


T = TypeVar('T')


class IntervalUnion(Generic[T]):
    def __init__(self):
        self.intervals: SortedList = SortedList()

    def add(self, start: T, end: T) -> list[tuple[T, T]]:
        if start > end:
            raise ValueError("start must be <= end")

        lo = self.intervals.bisect_left((start,))
        if lo > 0 and self.intervals[lo - 1][1] >= start:
            lo -= 1

        hi = lo
        merged_start, merged_end = start, end
        overlapping = []
        n = len(self.intervals)
        while hi < n and self.intervals[hi][0] <= merged_end:
            s, e = self.intervals[hi]
            overlapping.append((s, e))
            merged_start = min(merged_start, s)
            merged_end = max(merged_end, e)
            hi += 1

        del self.intervals[lo:hi]
        self.intervals.add((merged_start, merged_end))
        return overlapping

    def delete(self, start: T, end: T) -> list[tuple[T, T]]:
        if start > end:
            raise ValueError("start must be <= end")

        lo = self.intervals.bisect_left((start,))
        if lo > 0 and self.intervals[lo - 1][1] >= start:
            lo -= 1

        i = lo
        n = len(self.intervals)
        to_remove = []
        to_insert = []
        removed = []

        while i < n and self.intervals[i][0] <= end:
            s, e = self.intervals[i]
            to_remove.append((s, e))

            if s < start:
                to_insert.append((s, start))

            if e > end:
                to_insert.append((end, e))

            removed.append((max(s, start), min(e, end)))
            i += 1

        if to_remove:
            del self.intervals[lo:lo + len(to_remove)]
            for frag in to_insert:
                self.intervals.add(frag)

        return removed

    def intersect(self, start: T, end: T) -> IntervalUnion[T]:
        result = IntervalUnion[T]()
        lo = self.intervals.bisect_left((start,))
        if lo > 0 and self.intervals[lo - 1][1] >= start:
            lo -= 1
        i = lo
        n = len(self.intervals)
        while i < n and self.intervals[i][0] <= end:
            s, e = self.intervals[i]
            clipped = (max(s, start), min(e, end))
            if clipped[0] <= clipped[1]:
                result.add(*clipped)
            i += 1
        return result

    def intersect_difference(self, start: T, end: T) -> IntervalUnion[T]:
        if start > end:
            raise ValueError("start must be <= end")

        holes = IntervalUnion[T]()
        cursor = start

        lo = self.intervals.bisect_left((start,))
        if lo > 0 and self.intervals[lo - 1][1] >= start:
            lo -= 1

        i = lo
        n = len(self.intervals)
        while i < n and self.intervals[i][0] <= end:
            s, e = self.intervals[i]
            if s > cursor:
                holes.add(cursor, min(s, end))
            cursor = max(cursor, e)
            if cursor >= end:
                return holes
            i += 1

        if cursor < end:
            holes.add(cursor, end)
        return holes

    @property
    def number_parts(self) -> int:
        return len(self.intervals)

    @property
    def actual_filled(self) -> int:
        return sum(map(lambda x: (x[1] - x[0]), self.intervals))

    def __repr__(self):
        return f"{self.intervals}"


if __name__ == '__main__':
    i = IntervalUnion[int]()
    i.add(10, 20)
    i.add(20, 30)
    i.add(25, 32)
    i.add(1, 8)
    print(i.intersect(5, 20))
    print(i)

    i = IntervalUnion[int]()
    i.add(10, 11)
    i.add(12, 13)
    i.add(14, 15)
    i.add(16, 17)
    print(i.intersect_difference(11, 32))
    print(i)

    i = IntervalUnion[int]()
    i.add(10, 11)
    print(i.intersect_difference(11, 32))
    print(i.intersect_difference(12, 32))
    print(i.intersect_difference(10, 32))
    print(i.intersect_difference(9, 32))
    print(i.intersect_difference(5, 10))
    print(i.intersect_difference(5, 9))
    print(i.intersect_difference(5, 12))
    print(i.intersect_difference(5, 11))

    i = IntervalUnion[int]()
    i.add(10, 11)
    i.add(12, 13)
    for j in range(8, 13):
        print("====", j, 32)
        print(i.intersect_difference(j, 32))
        print(i.intersect_difference(j, 32).actual_filled)
    for j in range(8, 12):
        print("====", j, 12)
        print(i.intersect_difference(j, 12))
    for j in range(8, 11):
        print("====", j, 11)
        print(i.intersect_difference(j, 11))
    for j in range(8, 15):
        print("====", 5, j)
        print(i.intersect_difference(5, j))
    for j in range(11, 15):
        print("====", 10, j)
        print(i.intersect_difference(10, j))
    for j in range(12, 15):
        print("====", 11, j)
        print(i.intersect_difference(11, j))

    i = IntervalUnion[int]()
    i.add(10, 20)
    print(i.delete(19, 30))
    print(i.delete(14, 16))
    print(i)
