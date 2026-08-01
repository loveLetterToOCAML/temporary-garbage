from filer.base_exceptions import FilerSerialException, OutOfConstraints, FilerConstraintType, ExpectedAgainstReality, \
    PredicateType


def ensure_compatible_chunk_exn(offset: int, size: int, fixed_chunk_size: int, expected_total_size: int):
    if offset + size > expected_total_size:
        raise FilerSerialException(
            OutOfConstraints(
                failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                details=ExpectedAgainstReality[int](expectationType=PredicateType.INFERIOR,
                                                    reference=expected_total_size, got=offset + size)
            )
        )

    if offset % fixed_chunk_size != 0:
        raise FilerSerialException(
            OutOfConstraints(
                failedConstraint=FilerConstraintType.ALIGNED_CHUNK_EXPECTED,
                details=ExpectedAgainstReality[int](expectationType=PredicateType.EQUALS,
                                                    reference=offset - offset % fixed_chunk_size, got=offset)
            )
        )
    if size != fixed_chunk_size and expected_total_size - offset >= fixed_chunk_size:
        raise FilerSerialException(
            OutOfConstraints(
                failedConstraint=FilerConstraintType.FIXED_CHUNK_SIZE_EXPECTED,
                details=ExpectedAgainstReality[int](expectationType=PredicateType.EQUALS,
                                                    reference=fixed_chunk_size, got=size)
            )
        )
