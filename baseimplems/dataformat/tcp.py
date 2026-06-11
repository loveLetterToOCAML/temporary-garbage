"""
Sliding-window flow control over UDP with anyio.

Key properties
--------------
* Window of N "in-flight" slots — a packet only leaves in-flight when its ACK
  arrives (or it is declared lost after max retries).
* Each slot has an independent retransmit timer; expiry triggers resend, not
  window release.  The slot is released only on ACK or final give-up.
* ACK loss is handled: the receiver is idempotent (deduplicates by seq) and
  always re-ACKs a duplicate without reprocessing.
* The sender never sends more than WINDOW_SIZE unacknowledged packets at once
  (back-pressure on the producer).
* RTT is estimated with EWMA; the retransmit timeout (RTO) adapts accordingly
  (simplified Jacobson/Karels).

Topology
--------
  Producer  →  SlidingWindowSender  ──UDP──►  UDPReceiver
                      ▲                            │
                      └──────────── ACKs ──────────┘

Run demo
--------
  python sliding_window.py
  # Opens two UDP sockets on localhost and pumps 200 packets through them.

Dependencies: anyio  (pip install anyio)
"""

from __future__ import annotations

import time
import struct
import random
import logging
from contextvars import ContextVar

import anyio
import anyio.abc
from dataclasses import dataclass, field
from collections import OrderedDict
from enum import Enum, auto

from pydantic import BaseModel

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s",
)

# ---------------------------------------------------------------------------
# Wire format  (tiny fixed header, no framing needed over UDP datagrams)
# ---------------------------------------------------------------------------
#
#  DATA packet:   [ 0x01 | seq: u32 | payload ]
#  ACK  packet:   [ 0x02 | seq: u32 ]
#
TYPE_DATA = 0x01
TYPE_ACK  = 0x02
HEADER_DATA = struct.Struct("!BL")      # type + seq  (5 bytes)
HEADER_ACK  = struct.Struct("!BL")      # same layout


def encode_data(seq: int, payload: bytes) -> bytes:
    return HEADER_DATA.pack(TYPE_DATA, seq) + payload


def encode_ack(seq: int) -> bytes:
    return HEADER_ACK.pack(TYPE_ACK, seq)


def decode(raw: bytes) -> tuple[int, int, bytes]:
    """Returns (pkt_type, seq, payload_or_empty)."""
    pkt_type, seq = HEADER_ACK.unpack_from(raw)
    payload = raw[HEADER_DATA.size:] if pkt_type == TYPE_DATA else b""
    return pkt_type, seq, payload



class RTTEstimatorParameters(BaseModel):
    ALPHA: float = 0.125
    BETA: float = 0.125
    K: float = 4
    MIN_RTO: float = 0.1
    MAX_RTO: float = 20.0

rtt_estimator_parameters = ContextVar[RTTEstimatorParameters]('rtt_estimator', default=RTTEstimatorParameters())


# ---------------------------------------------------------------------------
# RTT estimator  (Jacobson/Karels simplified)
# See https://tcpcc.systemsapproach.org/algorithm.html
# ---------------------------------------------------------------------------

class RTTEstimator:
    # srtt = sample rtt
    # rttvar = variance of srtt
    # rto = next computed timeout

    def __init__(self, initial_rtt: float = 0.2):
        self.parameters = rtt_estimator_parameters.get()
        self.srtt    = initial_rtt
        self.rttvar  = initial_rtt / 2
        self.rto     = initial_rtt * 2

    def update(self, sample: float) -> None:
        diff = self.srtt - sample
        self.rttvar = (1 - self.parameters.BETA) * self.rttvar + self.parameters.BETA * abs(diff)
        self.srtt   = (1 - self.parameters.ALPHA) * self.srtt   + self.parameters.ALPHA * sample
        self.rto    = max(self.parameters.MIN_RTO, min(self.parameters.MAX_RTO, self.srtt + self.parameters.K * self.rttvar))

    @property
    def timeout(self) -> float:
        return self.rto


class SlotState(Enum):
    IN_FLIGHT = 1
    ACKED     = 2
    LOST      = 3


class Slot(BaseModel):
    seq: int
    payload: bytes
    sentAt: float
    state: SlotState = SlotState.IN_FLIGHT
    attempts: int = 1
    doneEvent: anyio.Event = anyio.Event()


class SlidingWindowSender:

    def __init__(
        self,
        socket: anyio.abc.UDPSocket,
        peer_addr: tuple[str, int],
        window_size: int = 8,
        max_attempts: int = 5,
        sim_loss: float = 0.0,
    ) -> None:
        self._sock        = socket
        self._peer        = peer_addr
        self._window      = anyio.Semaphore(window_size)
        self._max_attempts = max_attempts
        self._sim_loss    = sim_loss
        self._rtt         = RTTEstimator()
        self._seq         = 0
        # seq → Slot  (only in-flight slots live here)
        self._in_flight: dict[int, Slot] = {}
        self._lock        = anyio.Lock()


    async def send(self, payload: bytes) -> None:
        await self._window.acquire()        # block until a slot is free
        async with self._lock:
            seq = self._seq
            self._seq += 1
            slot = Slot(seq=seq, payload=payload, sent_at=time.monotonic())
            self._in_flight[seq] = slot
        await self._transmit(seq, payload)

    async def send_with_nursery(self, payload: bytes, nursery: anyio.abc.TaskGroup) -> None:
        await self._window.acquire()
        async with self._lock:
            seq = self._seq
            self._seq += 1
            slot = Slot(seq=seq, payload=payload, sent_at=time.monotonic())
            self._in_flight[seq] = slot

        await self._transmit(seq, payload)
        nursery.start_soon(self._watchdog, seq)

    async def handle_ack(self, seq: int) -> None:
        async with self._lock:
            slot = self._in_flight.pop(seq, None)

        if slot is None:
            logger.debug("ACK %d — duplicate or unknown, ignoring", seq)
            return

        if slot.attempts == 1:
            rtt_sample = time.monotonic() - slot.sentAt
            self._rtt.update(rtt_sample)
        slot.state = SlotState.ACKED
        slot.doneEvent.set()
        self._window.release()
        if slot.attempts == 1:
            logger.debug(
                "ACK %d  rtt=%.3fs  rto=%.3fs  in_flight=%d",
                seq, rtt_sample, self._rtt.timeout, len(self._in_flight),
            )

    async def drain(self) -> None:
        while True:
            async with self._lock:
                slots = list(self._in_flight.values())
            if not slots:
                break
            async with anyio.create_task_group() as tg:
                for slot in slots:
                    tg.start_soon(slot.done_event.wait)


    async def _transmit(self, seq: int, payload: bytes) -> None:
        """Raw UDP send, with optional simulated loss."""
        if self._sim_loss > 0 and random.random() < self._sim_loss:
            logger.debug("SIM LOSS  seq=%d", seq)
            return
        await self._sock.sendto(encode_data(seq, payload), *self._peer)

    async def _watchdog(self, seq: int) -> None:
        while True:
            rto = self._rtt.timeout
            with anyio.move_on_after(rto):
                async with self._lock:
                    slot = self._in_flight.get(seq)
                if slot:
                    await slot.done_event.wait()
                return  # ACK arrived in time — done

            async with self._lock:
                slot = self._in_flight.get(seq)

            if slot is None:
                return

            if slot.attempts >= self._max_attempts:
                logger.warning("GIVE UP  seq=%d  after %d attempts", seq, slot.attempts)
                async with self._lock:
                    self._in_flight.pop(seq, None)
                slot.state = SlotState.LOST
                slot.done_event.set()
                self._window.release()
                return

            slot.attempts += 1
            slot.sentAt = time.monotonic()
            logger.debug(
                "RETRANSMIT  seq=%d  attempt=%d  rto=%.3fs",
                seq, slot.attempts, rto,
            )
            await self._transmit(seq, slot.payload)


# ---------------------------------------------------------------------------
# Receiver  (idempotent, always re-ACKs duplicates)
# ---------------------------------------------------------------------------

class SlidingWindowReceiver:
    """
    Listens on a UDP socket, delivers payloads in-order to a handler,
    always ACKs (including duplicates) so the sender can free slots.

    Parameters
    ----------
    socket        anyio UDP socket (already bound)
    handler       async callable (seq, payload) → None
    dedup_window  how many recent seq numbers to remember for dedup
    """

    def __init__(
        self,
        socket: anyio.abc.UDPSocket,
        handler,
        dedup_window: int = 1024,
    ) -> None:
        self._sock         = socket
        self._handler      = handler
        self._dedup_window = dedup_window
        self._seen: OrderedDict[int, bool] = OrderedDict()

    async def run(self) -> None:
        """Receive loop — runs until cancelled."""
        async with self._sock:
            async for raw, (host, port) in self._sock:
                try:
                    pkt_type, seq, payload = decode(raw)
                except struct.error:
                    logger.warning("Malformed packet from %s:%d", host, port)
                    continue

                if pkt_type != TYPE_DATA:
                    continue

                # Always ACK — even duplicates (ACK could have been lost)
                ack = encode_ack(seq)
                await self._sock.sendto(ack, host, port)

                if seq in self._seen:
                    logger.debug("DUP  seq=%d  re-ACKed, skipping handler", seq)
                    continue

                # Evict oldest if dedup window is full
                while len(self._seen) >= self._dedup_window:
                    self._seen.popitem(last=False)
                self._seen[seq] = True

                logger.debug("RECV  seq=%d  %d bytes", seq, len(payload))
                await self._handler(seq, payload)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

SENDER_PORT   = 15000
RECEIVER_PORT = 15001
HOST          = "127.0.0.1"
NUM_PACKETS   = 60
WINDOW_SIZE   = 8
SIM_LOSS      = 0.20    # 20% artificial packet loss to exercise retransmits


async def demo_receiver_handler(seq: int, payload: bytes) -> None:
    text = payload.decode(errors="replace")
    logger.info("  📦 delivered  seq=%-4d  payload=%r", seq, text[:40])
    # Simulate a slow receiver — won't overflow it
    await anyio.sleep(0.02)


async def run_receiver() -> None:
    async with await anyio.create_udp_socket(
        local_host=HOST, local_port=RECEIVER_PORT
    ) as sock:
        receiver = SlidingWindowReceiver(sock, demo_receiver_handler)
        await receiver.run()


async def run_sender() -> None:
    # Give receiver a moment to bind
    await anyio.sleep(0.1)

    async with await anyio.create_udp_socket(
        local_host=HOST, local_port=SENDER_PORT
    ) as data_sock:
        # Separate socket for ACK reception on sender side
        async with await anyio.create_udp_socket(
            local_host=HOST, local_port=SENDER_PORT + 100
        ) as ack_sock:

            sender = SlidingWindowSender(
                data_sock,
                peer_addr=(HOST, RECEIVER_PORT),
                window_size=WINDOW_SIZE,
                max_attempts=5,
                sim_loss=SIM_LOSS,
            )

            async def ack_loop() -> None:
                """Receive ACKs and hand them to the sender."""
                async for raw, _ in ack_sock:
                    try:
                        pkt_type, seq, _ = decode(raw)
                    except struct.error:
                        continue
                    if pkt_type == TYPE_ACK:
                        await sender.handle_ack(seq)

            async with anyio.create_task_group() as tg:
                tg.start_soon(ack_loop)

                # Producer: send NUM_PACKETS as fast as the window allows
                t0 = time.monotonic()
                for i in range(NUM_PACKETS):
                    payload = f"packet-{i:04d} hello world!".encode()
                    await sender.send_with_nursery(payload, tg)

                # Wait for all in-flight to settle
                await sender.drain()
                elapsed = time.monotonic() - t0

                lost = sum(
                    1 for s in sender._in_flight.values()
                    if s.state == SlotState.LOST
                )
                logger.info(
                    "Done — %d packets in %.2fs  (window=%d  sim_loss=%.0f%%  given_up=%d)",
                    NUM_PACKETS, elapsed, WINDOW_SIZE, SIM_LOSS * 100, lost,
                )
                tg.cancel_scope.cancel()    # stop ack_loop


async def main() -> None:
    async with anyio.create_task_group() as tg:
        tg.start_soon(run_receiver)
        tg.start_soon(run_sender)


if __name__ == "__main__":
    anyio.run(main)