from filer.filer_backend.interval_union import IntervalUnion

from sortedcontainers import SortedDict

from typing import Iterator


class BytesIntervalUnion:

    def __init__(self, total_size: int, store_data: bool = True):
        self._total_size = total_size
        self._current_interval = IntervalUnion()
        self._data_slices = SortedDict()
        self._store_data = store_data

    def union_from(self, offset: int, data: bytes, slice_index: int | None = None) -> int:
        interval_tuple = (offset, min(offset + len(data), self._total_size))
        intersection: IntervalUnion = self._current_interval.intersect(*interval_tuple)
        bytes_updated = 0
        for start, end in intersection.intervals:
            if (start, end) in self._data_slices:
                del self._data_slices[(start, end)]
                self._current_interval.delete(start, end)
                bytes_updated += start - end
        intersection_diff: IntervalUnion = self._current_interval.intersect_difference(*interval_tuple)
        for start, end in intersection_diff.intervals:
            if self._store_data:
                self._data_slices[(start, end)] = data[start-offset: end-offset]
            elif slice_index is not None:
                self._data_slices[(start, end)] = (slice_index, end - start)
            else:
                self._data_slices[(start, end)] = (end - start, end - start)
        self._current_interval.add(*interval_tuple)
        return intersection_diff.actual_filled + bytes_updated

    @property
    def is_complete(self) -> bool:
        return self._current_interval.actual_filled == self._total_size

    def complete_data_exn(self) -> bytes | list[int]:
        result = b'' if self._store_data else []
        cur = 0
        for data_slice in self._data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            result += self._data_slices[data_slice] if self._store_data else [self._data_slices[data_slice][0]]
            cur += len(self._data_slices[data_slice]) if self._store_data else self._data_slices[data_slice][1]
        return result

    def complete_data_gen_exn(self) -> Iterator[bytes | int]:
        cur = 0
        for data_slice in self._data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            yield self._data_slices[data_slice] if self._store_data else self._data_slices[data_slice][0]
            cur += len(self._data_slices[data_slice]) if self._store_data else self._data_slices[data_slice][1]

    @property
    def expected_size(self):
        return self._total_size

    @property
    def number_parts(self):
        return self._current_interval.number_parts


if __name__ == '__main__':
    u = BytesIntervalUnion(0x100)
    print(u.expected_size)
    print(u.is_complete)
    print(u.union_from(0x10, b'a'*0x10))
    print(u.union_from(0x18, b'b' * 0x10))
    print(u.union_from(0x29, b'c' * 0x10))
    print(u.union_from(0x59, b'd' * 0x20))
    print(u.union_from(41, b'e' * (121-41)))
    print(u.union_from(42, b'd' * (122-42)))
    print(u.union_from(38, b'c' * 4))
    print(u.union_from(0, b'd' * 0x18))
    print(u.is_complete)
    print(u.union_from(121, b'x' * 0x100))
    print(u.is_complete)
    print(u.complete_data_exn(), len(u.complete_data_exn()))

    u = BytesIntervalUnion(0x100, store_data=False)
    print(u.expected_size)
    print(u.is_complete)
    print(u.union_from(0x10, b'a' * 0x10))
    print(u.union_from(0x18, b'b' * 0x10))
    print(u.union_from(0x29, b'c' * 0x10))
    print(u.union_from(0x59, b'd' * 0x20))
    print(u.union_from(41, b'e' * (121 - 41)))
    print(u.union_from(42, b'd' * (122 - 42)))
    print(u.union_from(38, b'c' * 4))
    print(u.union_from(0, b'd' * 0x18))
    print(u.is_complete)
    print(u.union_from(121, b'x' * 0x100))
    print(u.is_complete)
    print(u.complete_data_exn(), len(u.complete_data_exn()))

    u = BytesIntervalUnion(0x100, store_data=False)
    print(u.expected_size)
    print(u.is_complete)
    print(u.union_from(0x10, b'a' * 0x10, slice_index=0))
    print(u.union_from(0x18, b'b' * 0x10, slice_index=1))
    print(u.union_from(0x29, b'c' * 0x10, slice_index=2))
    print(u.union_from(0x59, b'd' * 0x20, slice_index=3))
    print(u.union_from(41, b'e' * (121 - 41), slice_index=4))
    print(u.union_from(42, b'd' * (122 - 42), slice_index=5))
    print(u.union_from(38, b'c' * 4, slice_index=6))
    print(u.union_from(0, b'd' * 0x18, slice_index=7))
    print(u.is_complete)
    print(u.union_from(121, b'x' * 0x100, slice_index=8))
    print(u.is_complete)
    print(u.complete_data_exn(), len(u.complete_data_exn()))
