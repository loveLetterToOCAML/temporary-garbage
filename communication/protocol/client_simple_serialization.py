from basetypes.a_root import SerializationNode
from basetypes.a_root_params import RootSerial
from basetypes.aa_optimized import Optimized, OptimizedDataType
from basetypes.ab_basetypes import BaseTypes

from typing import Tuple, List, Iterable
from datetime import datetime, timedelta, time, date
from types import NoneType
from enum import Enum
import struct

from basetypes.implementation.basetypes_constraints import StringWithConstraint, BytesWithConstraint, IntWithConstraint
from basetypes.implementation.basetypes_match import TypeLengthValue, RawSparseObject


def p16(data):
    return struct.pack('<H', data)

def p32(data):
    return struct.pack('<I', data)

def serialize_none(_):
    return Optimized.optimized_path_until(OptimizedDataType.NONE)

def serialize_bool(v: bool):
    return Optimized.optimized_path_until(OptimizedDataType.BOOL) + bytes([1 if v else 0])

def serialize_int(i: int):
    sign = True
    if i < 0:
        sign = False
        i = -i
    s = [i & 0xff]
    i >>= 8
    while i:
        s.append(i & 0xff)
        i >>= 8
    if len(s) == 1:
        return (Optimized.optimized_path_until(OptimizedDataType.UINT8) if sign else
                Optimized.optimized_path_until(OptimizedDataType.NEG_UINT8)) + bytes(s)
    elif len(s) == 2:
        return (Optimized.optimized_path_until(OptimizedDataType.UINT16) if sign else
                Optimized.optimized_path_until(OptimizedDataType.NEG_UINT16)) + bytes(s)
    elif len(s) <= 4:
        return (Optimized.optimized_path_until(OptimizedDataType.UINT32) if sign else
                Optimized.optimized_path_until(OptimizedDataType.NEG_UINT32)) + bytes(s) + b'\x00' * (4 - len(s))
    elif len(s) <= 8:
        return (Optimized.optimized_path_until(OptimizedDataType.UINT64) if sign else
                Optimized.optimized_path_until(OptimizedDataType.NEG_UINT64)) + bytes(s) + b'\x00' * (8 - len(s))
    return Optimized.optimized_path_until(OptimizedDataType.INT) + bytes([sign]) + serialize_int(len(s)) + bytes(s)

def serialize_float(f: float):
    return Optimized.optimized_path_until(OptimizedDataType.FLOAT) + struct.pack('<f', f)

# TODO: maybe handle max size here (or we may consider if it reaches serialization it means sizes are ok?)
def serialize_string(s: str):
    encoded = s.encode()
    return serialize_bytes(encoded)

def serialize_bytes(b: bytes):
    return Optimized.optimized_path_until(OptimizedDataType.BYTES) + serialize_int(len(b)) + b

def serialize_type(n: SerializationNode):
    return Optimized.optimized_path_until(OptimizedDataType.TYPE) + serialize_bytes(n.path_until())


def object_to_sparse(obj: RootSerial):
    object_type = obj.Type
    params_map = param_map_for(object_type)
    serialized_tlv = []
    for attr in obj.model_dump():
        if attr == 'Type':
            continue
        resolve_attribute_type = params_map[attr]
        value = serialize_one(getattr(object_type, attr))
        serialized_tlv.append(TypeLengthValue(
            type=resolve_attribute_type.value,
            length=len(value),
            value=value
        ))

    return RawSparseObject(
        objectType=object_type,
        attributes=serialized_tlv
    )

def serialize_tlv(tlv: TypeLengthValue):
    assert len(tlv.value) == tlv.length
    assert len(tlv.value) < 0xff0000  # TODO: check max here
    # TODO: handle attribute value > 0xff. Maybe put on 2 bytes and allows max 0xffff ?
    return bytes([tlv.type]) + serialize_int(tlv.length) + tlv.value

def serialize_object(obj: RootSerial):
    sparse_object: RawSparseObject = object_to_sparse(obj)
    assert len(sparse_object.attributes) < 256
    return Optimized.optimized_path_until(OptimizedDataType.SPARSE_OBJECT) + \
        serialize_bytes(sparse_object.objectType) + \
        bytes([len(sparse_object.attributes)]) + \
        b''.join(map(serialize_tlv, sparse_object.attributes))


def serialize_datetime(dt: datetime):
    packed = struct.pack(
        '<HBBBBB', dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    )
    return Optimized.optimized_path_until(OptimizedDataType.DATETIME) + packed

def serialize_date(d: date):
    packed = struct.pack(
        '<HBB', d.year, d.month, d.day
    )
    return Optimized.optimized_path_until(OptimizedDataType.DATETIME) + packed

def serialize_time(t: time):
    packed = struct.pack(
        '<BBB', t.hour, t.minute, t.second
    )
    return Optimized.optimized_path_until(OptimizedDataType.TIME) + packed

def serialize_timedelta(td: timedelta):
    packed = struct.pack(
        '<HH', td.seconds, td.microseconds
    )
    return Optimized.optimized_path_until(OptimizedDataType.DATETIME) + serialize_int(td.days) + packed

def serialize_str_with_constraint(swc: StringWithConstraint):
    return Optimized.optimized_path_until(OptimizedDataType.STRING_WITH_CONSTRAINT) + serialize_int(swc.constraint.value) + serialize_string(swc.data)

def serialize_bytes_with_constraint(swc: BytesWithConstraint):
    return Optimized.optimized_path_until(OptimizedDataType.BYTES_WITH_CONSTRAINT) + serialize_int(swc.constraint.value) + serialize_bytes(swc.data)

def serialize_int_with_constraint(swc: IntWithConstraint):
    return Optimized.optimized_path_until(OptimizedDataType.INT_WITH_CONSTRAINT) + serialize_int(swc.constraint.value) + serialize_int(swc.data)


class SerializationMethods:
    BaseTypes = serialize_none
    BOOL = serialize_bool
    INT = serialize_int
    FLOAT = serialize_float
    DECIMAL = serialize_decimal
    STRING = serialize_string
    BYTES = serialize_bytes

    TYPE = serialize_type
    SPARSE_OBJECT = serialize_object

    DATETIME = serialize_datetime
    DATE = serialize_date
    TIME = serialize_time
    TIMEDELTA = serialize_timedelta

    STRING_WITH_CONSTRAINT = serialize_str_with_constraint
    BYTES_WITH_CONSTRAINT = serialize_bytes_with_constraint
    INT_WITH_CONSTRAINT = serialize_int_with_constraint

"""
To be correctly serialized, value must be of any type within DefaultBaseType or DefaultGenericType
"""
def serialize_one(value) -> bytes:
    match value:
        case bool():
            return bytes([BaseTypes.BOOL, 1 if value else 0])
        case int():
            if value < 0:
                return bytes([7]) + serialize_int(-value)
            return bytes([1]) + serialize_int(value)
        case Enum():
            return bytes([1]) + serialize_int(value.value)
        case bytes():
            if len(value) < 0x10000 or len(value) >= 0x1000000:  # intentionally fails if too big
                return bytes([2, *p16(len(value))]) + value
            else:
                return bytes([10, *p32(len(value))[:3]]) + value
        case str():
            encoded = value.encode()
            if len(encoded) < 0x10000 or len(value) >= 0x1000000:  # intentionally fails if too big
                return bytes([3, *p16(len(encoded))]) + encoded
            else:
                return bytes([11, *p32(len(encoded))[:3]]) + encoded
        case datetime():
            packed = struct.pack(
                '!HBBBBB',value.year, value.month, value.day, value.hour, value.minute, value.second
            )
            return bytes([4]) + packed
        case list():
            packed = b''.join(map(serialize_one, value))
            return bytes([5, *p16(len(value))]) + packed
        case NoneType():
            return bytes([6])
        case dict():
            packed = b''
            for k, v in value.items():
                packed += serialize_one(k)
                packed += serialize_one(v)
            return bytes([8, *p16(len(value))]) + packed
        case float():
            return bytes([9]) + struct.pack('<f', value)
        case _:
            return bytes([12]) + serialize_parsed_command(serialize_payload(value))




"""
The following function is intended for use to handle multiple packet serialization as well as automatic fragmentation
within Fragment packets to split packet bytes on the network
"""
def merge_and_fragment_packets_max_size(payloads: list, max_size: int = 1490) -> Iterable[Fragment]:
    current = b''
    chunks = []
    for payload in payloads:
        serialized = serialize_parsed_command(serialize_payload(payload))
        current += serialized
        while len(current) > max_size:
            chunks.append(current[:max_size])
            current = current[max_size:]
    if current:
        chunks.append(current)
    return map(
        lambda ib: Fragment(
            ChunkTotal = len(chunks),
            ChunkIndex = ib[0],
            Chunk = ib[1],
        ),
        enumerate(chunks)
    )


def deserialize_mfd_packet(data: bytes) -> Tuple[BasePacket | None, int]:
    """
    This returns only the next packet deserialized (and number of bytes read), if any
    (if there are sufficient bytes to read)
    """
    if len(data) < 5:
        return None, 0

    magic = data[:3]
    if magic != b'MFD':
        raise Exception('Packet not starting with right magic bytes')

    length = u16(data[3:5])
    if len(data) < length:
        return None, 0

    if length < 13:
        raise Exception('Length too small for mfd packet')

    pkt_number = u32(data[5:9])
    response_to = u32(data[9:13])

    constructed, read = unserialize_parsed_command(data[13: length])
    assert read == length - 13

    return BasePacket(
        PacketNumber = pkt_number,
        InResponseTo = response_to,
        Payload = constructed
    ), length

def reassemble_fragments(fragments: List[Fragment]) -> Iterable:
    """
    This returns the list of packets matching reassembled fragments or raises exception if not all fragments are given
    """
    if not fragments:
        return

    n_fragments = len(fragments)
    per_index = {
        fragment.ChunkIndex: fragment
        for fragment in fragments
    }

    data = b''
    for fragment_idx in range(len(fragments)):
        if fragment_idx not in per_index:
            raise Exception(f"Fragment {fragment_idx} not given")
        if per_index[fragment_idx].ChunkTotal != n_fragments:
            raise Exception(f"Wrong number of chunks given (gave {per_index[fragment_idx].ChunkTotal}, "
                            f"expected {n_fragments})")
        data += per_index[fragment_idx].Chunk

    while data:
        constructed, read = unserialize_parsed_command(data)
        yield constructed
        if not read:
            return
        data = data[read:]


if __name__ == '__main__':
    assert serialize_int(unserialize_int(b'\x03\x44\x01\x10')) == b'\x03\x44\x01\x10'

    from common_network.protocol.per_type.router import RouterReset

    bp = BasePacket(
        PacketNumber = 0x88,
        InResponseTo = 0x77,
        Payload = RouterReset(ClientID=0xffeeffdd)
    )
    ser = serialize_mfd_packet(bp)
    print(ser)
    unser = deserialize_mfd_packet(ser)
    print(unser)

    pkts = [
        RouterReset(ClientID=0xffeeffddaaaa+i*0x100000000000111)
        for i in range(0x30)
    ]

    frags = list(merge_and_fragment_packets_max_size(pkts, 0xdd))
    for x in frags:
        print(x)

    for pkt in reassemble_fragments(frags):
        print(pkt)
