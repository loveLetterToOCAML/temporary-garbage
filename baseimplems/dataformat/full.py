"""
Sliding-window flow control over UDP with anyio.

Key properties
--------------
* Dynamic effective window = min(CWND, RWND):
    - CWND  (congestion window) â€” sender-side, grows via slow-start /
      congestion-avoidance, shrinks on loss.
    - RWND  (receiver window)  â€” advertised by the receiver in every ACK,
      reflecting its remaining processing capacity.
* Each in-flight slot has an independent retransmit watchdog; the slot is
  released only on ACK receipt or after max_attempts (Karn's rule: RTT
  samples are skipped for retransmitted packets).
* ACK loss is handled: the receiver is idempotent (dedup by seq) and always
  re-ACKs duplicates without reprocessing.
* RTTEstimator follows RFC 6298 exactly: bootstrap on first sample, EWMA
  thereafter, RTTVAR updated with old SRTT before SRTT absorbs new sample.

Wire format
-----------
  DATA packet:  [ 0x01 | seq: u32 | payload ]
  ACK  packet:  [ 0x02 | seq: u32 | rwnd: u16 ]   â† rwnd added

Topology
--------
  Producer  â†’  SlidingWindowSender  â”€â”€UDPâ”€â”€â–º  SlidingWindowReceiver
                      â–²                            â”‚
                      â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ ACKs â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
                                 (seq + rwnd)

Run demo
--------
  python sliding_window.py

Dependencies: anyio  (pip install anyio)
"""

from __future__ import annotations

import time
import struct
import random
import logging
from contextlib import asynccontextmanager
import anyio
import anyio.abc
from dataclasses import dataclass, field
from collections import OrderedDict
from enum import Enum, auto

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s",
)

# ---------------------------------------------------------------------------
# Wire format
# ---------------------------------------------------------------------------
#
#  DATA:  [ type:u8=0x01 | seq:u32 | payloadâ€¦ ]
#  ACK:   [ type:u8=0x02 | seq:u32 | rwnd:u16 ]
#
TYPE_DATA = 0x01
TYPE_ACK = 0x02
HEADER_DATA = struct.Struct("!BL")  # 5 bytes
HEADER_ACK = struct.Struct("!BLH")  # 7 bytes  (added rwnd field)


def encode_data(seq: int, payload: bytes) -> bytes:
    return HEADER_DATA.pack(TYPE_DATA, seq) + payload


def encode_ack(seq: int, rwnd: int) -> bytes:
    return HEADER_ACK.pack(TYPE_ACK, seq, rwnd)


def decode(raw: bytes) -> tuple[int, int, bytes, int]:
    """
    Returns (pkt_type, seq, payload_or_empty, rwnd_or_0).
    rwnd is only meaningful when pkt_type == TYPE_ACK.
    """
    pkt_type = raw[0]
    if pkt_type == TYPE_DATA:
        _, seq = HEADER_DATA.unpack_from(raw)
        return TYPE_DATA, seq, raw[HEADER_DATA.size:], 0
    elif pkt_type == TYPE_ACK:
        _, seq, rwnd = HEADER_ACK.unpack_from(raw)
        return TYPE_ACK, seq, b"", rwnd
    else:
        raise struct.error(f"Unknown packet type 0x{pkt_type:02x}")


# ---------------------------------------------------------------------------
# RTT estimator â€” RFC 6298 compliant
# ---------------------------------------------------------------------------

class RTTEstimator:
    """
    Jacobson/Karels EWMA estimator, following RFC 6298 exactly:
      - Before first sample: RTO = 1 s (Â§2.1)
      - First sample R:  SRTT = R,  RTTVAR = R/2,  RTO = SRTT + 4*RTTVAR  (Â§2.2)
      - Subsequent:      RTTVAR updated with *old* SRTT, then SRTT updated  (Â§2.3)
    Karn's rule: callers must NOT pass samples from retransmitted packets.
    """

    ALPHA = 0.125  # SRTT smoothing  (1/8)
    BETA = 0.25  # RTTVAR smoothing (1/4)
    K = 4  # RTO = SRTT + K * RTTVAR
    MIN_RTO = 0.2  # seconds
    MAX_RTO = 60.0  # RFC 6298 Â§2.5

    def __init__(self) -> None:
        self.srtt: float | None = None  # no estimate before first sample
        self.rttvar: float | None = None
        self.rto: float = 1.0  # RFC 6298 Â§2.1

    def update(self, sample: float) -> None:
        if self.srtt is None:
            # Â§2.2 â€” first measurement bootstrap
            self.srtt = sample
            self.rttvar = sample / 2.0
        else:
            # Â§2.3 â€” RTTVAR uses *old* SRTT; update order matters
            self.rttvar = (1 - self.BETA) * self.rttvar + self.BETA * abs(self.srtt - sample)
            self.srtt = (1 - self.ALPHA) * self.srtt + self.ALPHA * sample

        self.rto = max(self.MIN_RTO,
                       min(self.MAX_RTO, self.srtt + self.K * self.rttvar))

    @property
    def timeout(self) -> float:
        return self.rto


# ---------------------------------------------------------------------------
# Congestion window  (CWND)
# ---------------------------------------------------------------------------

class CongestionWindow:
    """
    Sender-side congestion control: slow start â†’ congestion avoidance.

    Slow start:          cwnd += 1 per ACK  (doubles each RTT)
    Congestion avoidance: cwnd += 1/cwnd per ACK  (+1 per RTT)
    On timeout loss:     ssthresh = cwnd/2,  cwnd = 1  (back to slow start)
    On triple-dup-ACK:   ssthresh = cwnd/2,  cwnd = ssthresh  (fast recovery)

    cwnd is expressed in *packets* (not bytes) for simplicity.
    """

    def __init__(self, initial_window: int = 1, max_window: int = 64) -> None:
        self.cwnd: float = float(initial_window)
        self.ssthresh: float = float(max_window)
        self._max: float = float(max_window)

    # Called for every clean ACK (not a retransmit ACK)
    def on_ack(self) -> None:
        if self.cwnd < self.ssthresh:
            # Slow start: exponential growth
            self.cwnd = min(self.cwnd + 1.0, self._max)
        else:
            # Congestion avoidance: linear growth (+1 per RTT)
            self.cwnd = min(self.cwnd + 1.0 / self.cwnd, self._max)
        logger.debug("CWND  cwnd=%.1f  ssthresh=%.1f  [slow_start=%s]",
                     self.cwnd, self.ssthresh, self.cwnd < self.ssthresh)

    # Called on RTO timeout (hard loss)
    def on_timeout_loss(self) -> None:
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = 1.0
        logger.debug("CWND timeout-loss  cwnd=%.1f  ssthresh=%.1f", self.cwnd, self.ssthresh)

    # Called on triple duplicate ACK (fast retransmit)
    def on_triple_dup_ack(self) -> None:
        self.ssthresh = max(self.cwnd / 2.0, 2.0)
        self.cwnd = self.ssthresh
        logger.debug("CWND triple-dup-ack  cwnd=%.1f  ssthresh=%.1f", self.cwnd, self.ssthresh)

    @property
    def value(self) -> int:
        return max(1, int(self.cwnd))


# ---------------------------------------------------------------------------
# Per-slot state machine
# ---------------------------------------------------------------------------

class SlotState(Enum):
    IN_FLIGHT = auto()
    ACKED = auto()
    LOST = auto()


@dataclass
class Slot:
    seq: int
    payload: bytes
    sent_at: float
    state: SlotState = SlotState.IN_FLIGHT
    attempts: int = 1
    done_event: anyio.Event = field(default_factory=anyio.Event)


# ---------------------------------------------------------------------------
# Dynamic window gate
# ---------------------------------------------------------------------------

class WindowGate:
    """
    Replaces the fixed Semaphore.  Tracks how many slots are in-flight and
    blocks senders when in_flight >= effective_window = min(cwnd, rwnd).

    Unlike a semaphore whose capacity is set at construction, the effective
    window here can change at any time (CWND grows/shrinks, RWND is updated
    from each ACK).  Waiters are woken via an anyio.Event that is reset and
    re-raised as the window opens.
    """

    def __init__(self) -> None:
        self._in_flight: int = 0
        self._cwnd: int = 1  # start at 1 (slow start)
        self._rwnd: int = 1  # conservative until first ACK
        self._lock: anyio.Lock = anyio.Lock()
        self._has_space: anyio.Event = anyio.Event()
        # Set initially so the very first acquire() doesn't block
        self._has_space.set()

    # ------------------------------------------------------------------
    # Called by sender before transmitting
    # ------------------------------------------------------------------

    async def acquire(self) -> None:
        """Block until effective_window > in_flight."""
        while True:
            async with self._lock:
                if self._in_flight < self._effective:
                    self._in_flight += 1
                    return
                # Window full â€” arm a fresh event to wait on
                evt = anyio.Event()
                self._has_space = evt

            # Wait outside the lock
            await evt.wait()

    # ------------------------------------------------------------------
    # Called by ACK handler / watchdog when a slot is freed
    # ------------------------------------------------------------------

    async def release(self) -> None:
        async with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._has_space.set()

    # ------------------------------------------------------------------
    # Called when a new RWND value arrives from the receiver
    # ------------------------------------------------------------------

    async def update_rwnd(self, rwnd: int) -> None:
        async with self._lock:
            self._rwnd = max(1, rwnd)
            self._has_space.set()  # window may have opened

    # ------------------------------------------------------------------
    # Called by CongestionWindow mutations
    # ------------------------------------------------------------------

    async def update_cwnd(self, cwnd: int) -> None:
        async with self._lock:
            self._cwnd = max(1, cwnd)
            self._has_space.set()

    @property
    def _effective(self) -> int:
        """Must be called with _lock held."""
        return min(self._cwnd, self._rwnd)

    @property
    def stats(self) -> dict:
        return {
            "in_flight": self._in_flight,
            "cwnd": self._cwnd,
            "rwnd": self._rwnd,
            "effective": min(self._cwnd, self._rwnd),
        }


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

class SlidingWindowSender:
    """
    Sends payloads over UDP with a dynamic sliding window.

    Effective window = min(CWND, RWND):
      CWND â€” grows via slow-start / congestion-avoidance, shrinks on loss.
      RWND â€” updated from the rwnd field carried in every ACK datagram.

    The sender owns its watchdog nursery internally.  Use as an async
    context manager so the nursery is alive for the full session:

        async with SlidingWindowSender(...) as sender:
            await sender.send(payload)
            await sender.drain()
    """

    def __init__(
            self,
            socket: anyio.abc.UDPSocket,
            peer_addr: tuple[str, int],
            max_cwnd: int = 64,
            max_attempts: int = 5,
            sim_loss: float = 0.0,
    ) -> None:
        self._sock = socket
        self._peer = peer_addr
        self._max_attempts = max_attempts
        self._sim_loss = sim_loss

        self._rtt = RTTEstimator()
        self._cwnd = CongestionWindow(initial_window=1, max_window=max_cwnd)
        self._gate = WindowGate()

        self._seq = 0
        self._in_flight: dict[int, Slot] = {}
        self._lock = anyio.Lock()
        self._nursery: anyio.abc.TaskGroup | None = None

    # ------------------------------------------------------------------
    # Async context manager â€” owns the watchdog nursery
    # ------------------------------------------------------------------
    #
    # @asynccontextmanager works best as a factory function, not inside a
    # class, because the generator must stay alive between __aenter__ and
    # __aexit__ and there is no natural place to store it on self without
    # reintroducing the same complexity.
    #
    # The clean class-based idiom is: delegate __aenter__/__aexit__ to a
    # single @asynccontextmanager *method* that yields self.  Python's
    # asynccontextmanager returns an AsyncContextManager object whose
    # __aenter__/__aexit__ drive the generator â€” we just forward to it.

    async def __aenter__(self) -> "SlidingWindowSender":
        self._cm = self._lifespan()
        return await self._cm.__aenter__()

    async def __aexit__(self, *exc_info) -> None:
        await self._cm.__aexit__(*exc_info)
        self._nursery = None

    @asynccontextmanager
    async def _lifespan(self):
        """Owns the watchdog task group for the full sender lifetime."""
        async with anyio.create_task_group() as tg:
            self._nursery = tg
            yield self
            await self.drain()
            tg.cancel_scope.cancel()  # stop watchdogs that are still waiting

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send(self, payload: bytes) -> None:
        """Block until window has space, then transmit and start watchdog."""
        if self._nursery is None:
            raise RuntimeError("Use 'async with SlidingWindowSender(...) as sender'")
        await self._gate.acquire()

        async with self._lock:
            seq = self._seq
            self._seq += 1
            slot = Slot(seq=seq, payload=payload, sent_at=time.monotonic())
            self._in_flight[seq] = slot

        await self._transmit(seq, payload)
        self._nursery.start_soon(self._watchdog, seq)

    async def handle_ack(self, seq: int, rwnd: int) -> None:
        """
        Called when an ACK arrives.
        Updates RWND, RTT (Karn: only for first-attempt packets), and CWND.
        """
        # Always update RWND from every ACK, even duplicates
        await self._gate.update_rwnd(rwnd)

        async with self._lock:
            slot = self._in_flight.pop(seq, None)

        if slot is None:
            logger.debug("ACK %d â€” duplicate or unknown", seq)
            return

        # Karn's rule: only sample RTT for packets that were not retransmitted
        if slot.attempts == 1:
            rtt_sample = time.monotonic() - slot.sent_at
            self._rtt.update(rtt_sample)

        # Grow CWND on clean ACK
        self._cwnd.on_ack()
        await self._gate.update_cwnd(self._cwnd.value)

        slot.state = SlotState.ACKED
        slot.done_event.set()
        await self._gate.release()

        logger.debug(
            "ACK %d  rtt=%.3fs  rto=%.3fs  %s",
            seq, time.monotonic() - slot.sent_at,
            self._rtt.timeout, self._gate.stats,
        )

    async def drain(self) -> None:
        """Wait until all in-flight packets have settled."""
        while True:
            async with self._lock:
                slots = list(self._in_flight.values())
            if not slots:
                break
            async with anyio.create_task_group() as tg:
                for slot in slots:
                    tg.start_soon(slot.done_event.wait)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _transmit(self, seq: int, payload: bytes) -> None:
        if self._sim_loss > 0 and random.random() < self._sim_loss:
            logger.debug("SIM LOSS  seq=%d", seq)
            return
        await self._sock.sendto(encode_data(seq, payload), *self._peer)

    async def _watchdog(self, seq: int) -> None:
        """
        Per-slot retransmit watchdog.
        On timeout: retransmit, shrink CWND (on_timeout_loss), loop.
        On give-up:  mark LOST, release gate slot.
        """
        while True:
            rto = self._rtt.timeout

            with anyio.move_on_after(rto):
                async with self._lock:
                    slot = self._in_flight.get(seq)
                if slot:
                    await slot.done_event.wait()
                return  # ACK arrived â€” done

            # RTO fired
            async with self._lock:
                slot = self._in_flight.get(seq)

            if slot is None:
                return  # ACK raced with timeout

            if slot.attempts >= self._max_attempts:
                logger.warning("GIVE UP  seq=%d  after %d attempts", seq, slot.attempts)
                async with self._lock:
                    self._in_flight.pop(seq, None)
                slot.state = SlotState.LOST
                slot.done_event.set()
                self._cwnd.on_timeout_loss()
                await self._gate.update_cwnd(self._cwnd.value)
                await self._gate.release()
                return

            slot.attempts += 1
            # Do NOT update sent_at â€” Karn: we won't sample RTT for this retransmit
            logger.debug("RETRANSMIT  seq=%d  attempt=%d  rto=%.3fs", seq, slot.attempts, rto)
            self._cwnd.on_timeout_loss()
            await self._gate.update_cwnd(self._cwnd.value)
            await self._transmit(seq, slot.payload)
            # loop: wait another RTO


# ---------------------------------------------------------------------------
# Receiver  (idempotent, advertises RWND in every ACK)
# ---------------------------------------------------------------------------

class SlidingWindowReceiver:
    """
    Listens on a UDP socket, delivers payloads to a handler, re-ACKs
    duplicates.  Tracks its own processing queue depth and advertises
    remaining capacity (RWND) in every ACK.

    Parameters
    ----------
    socket        anyio UDP socket (already bound)
    handler       async callable (seq, payload) â†’ None
    max_queue     processing queue capacity (drives RWND advertisement)
    dedup_window  how many recent seq numbers to remember for dedup
    """

    def __init__(
            self,
            socket: anyio.abc.UDPSocket,
            handler,
            max_queue: int = 16,
            dedup_window: int = 1024,
    ) -> None:
        self._sock = socket
        self._handler = handler
        self._max_queue = max_queue
        self._dedup_window = dedup_window
        self._seen: OrderedDict[int, bool] = OrderedDict()
        # Semaphore limits concurrent handler calls = processing queue depth
        self._proc_sem = anyio.Semaphore(max_queue)

    def _rwnd(self) -> int:
        """Remaining processing slots = what we're willing to receive."""
        return self._proc_sem._value  # slots not yet acquired

    async def run(self) -> None:
        """Receive loop â€” runs until cancelled."""
        async with anyio.create_task_group() as tg:
            async with self._sock:
                async for raw, (host, port) in self._sock:
                    try:
                        pkt_type, seq, payload, _ = decode(raw)
                    except struct.error:
                        logger.warning("Malformed packet from %s:%d", host, port)
                        continue

                    if pkt_type != TYPE_DATA:
                        continue

                    # ACK immediately with current RWND
                    await self._sock.sendto(encode_ack(seq, self._rwnd()), host, port)

                    if seq in self._seen:
                        logger.debug("DUP  seq=%d  re-ACKed", seq)
                        continue

                    while len(self._seen) >= self._dedup_window:
                        self._seen.popitem(last=False)
                    self._seen[seq] = True

                    logger.debug("RECV  seq=%d  %d bytes  rwnd=%d",
                                 seq, len(payload), self._rwnd())

                    # Dispatch handler as a concurrent task, bounded by _proc_sem
                    tg.start_soon(self._handle, seq, payload, host, port)

    async def _handle(self, seq: int, payload: bytes, host: str, port: int) -> None:
        """Run handler under processing semaphore; re-ACK with updated RWND after."""
        async with self._proc_sem:
            await self._handler(seq, payload)
        # After releasing the slot, send an updated ACK so the sender can open its window
        await self._sock.sendto(encode_ack(seq, self._rwnd()), host, port)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

SENDER_PORT = 15000
RECEIVER_PORT = 15001
ACK_PORT = 15002
HOST = "127.0.0.1"
NUM_PACKETS = 60
MAX_CWND = 16
RECEIVER_QUEUE = 6  # small queue â†’ RWND pressure visible in logs
SIM_LOSS = 0.15


async def demo_receiver_handler(seq: int, payload: bytes) -> None:
    text = payload.decode(errors="replace")
    logger.info("  ðŸ“¦ delivered  seq=%-4d  %r", seq, text[:40])
    # Simulate slow processing â€” backs up the queue, shrinks RWND
    await anyio.sleep(random.uniform(0.02, 0.08))


async def run_receiver() -> None:
    async with await anyio.create_udp_socket(
            local_host=HOST, local_port=RECEIVER_PORT
    ) as sock:
        receiver = SlidingWindowReceiver(
            sock,
            demo_receiver_handler,
            max_queue=RECEIVER_QUEUE,
        )
        await receiver.run()


async def run_sender() -> None:
    await anyio.sleep(0.1)  # let receiver bind first

    async with await anyio.create_udp_socket(
            local_host=HOST, local_port=SENDER_PORT
    ) as data_sock:
        async with await anyio.create_udp_socket(
                local_host=HOST, local_port=ACK_PORT
        ) as ack_sock:

            async with SlidingWindowSender(
                    data_sock,
                    peer_addr=(HOST, RECEIVER_PORT),
                    max_cwnd=MAX_CWND,
                    max_attempts=5,
                    sim_loss=SIM_LOSS,
            ) as sender:

                async def ack_loop() -> None:
                    async for raw, _ in ack_sock:
                        try:
                            pkt_type, seq, _, rwnd = decode(raw)
                        except struct.error:
                            continue
                        if pkt_type == TYPE_ACK:
                            await sender.handle_ack(seq, rwnd)

                async with anyio.create_task_group() as tg:
                    tg.start_soon(ack_loop)

                    t0 = time.monotonic()
                    for i in range(NUM_PACKETS):
                        payload = f"packet-{i:04d} hello!".encode()
                        await sender.send(payload)

                    await sender.drain()
                    elapsed = time.monotonic() - t0

                    logger.info(
                        "Done â€” %d packets in %.2fs  (max_cwnd=%d  rx_queue=%d  sim_loss=%.0f%%)",
                        NUM_PACKETS, elapsed, MAX_CWND, RECEIVER_QUEUE, SIM_LOSS * 100,
                    )
                    tg.cancel_scope.cancel()


async def main() -> None:
    async with anyio.create_task_group() as tg:
        tg.start_soon(run_receiver)
        tg.start_soon(run_sender)


if __name__ == "__main__":
    anyio.run(main)