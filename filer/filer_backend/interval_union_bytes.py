from filer.filer_backend.interval_union import IntervalUnion

from sortedcontainers import SortedDict

from typing import Iterator


class BytesIntervalUnion:

    def __init__(self, total_size: int):
        self._total_size = total_size
        self._current_interval = IntervalUnion()
        self._data_slices = SortedDict()

    def union_from(self, offset: int, data: bytes) -> int:
        interval_tuple = (offset, min(offset + len(data), self._total_size))
        intersection: IntervalUnion = self._current_interval.intersect(*interval_tuple)
        for start, end in intersection.intervals:
            if (start, end) in self._data_slices:
                del self._data_slices[(start, end)]
                self._current_interval.delete(start, end)
        intersection_diff: IntervalUnion = self._current_interval.intersect_difference(*interval_tuple)
        for start, end in intersection_diff.intervals:
            self._data_slices[(start, end)] = data[start-offset: end-offset]
        self._current_interval.add(*interval_tuple)
        return intersection_diff.actual_filled

    @property
    def is_complete(self) -> bool:
        return self._current_interval.actual_filled == self._total_size

    def complete_data_exn(self) -> bytes:
        result = b''
        cur = 0
        for data_slice in self._data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            result += self._data_slices[data_slice]
            cur += len(self._data_slices[data_slice])
        return result

    def complete_data_gen_exn(self) -> Iterator[bytes]:
        cur = 0
        for data_slice in self._data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            yield self._data_slices[data_slice]
            cur += len(self._data_slices[data_slice])

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
