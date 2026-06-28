from anyio import AsyncContextManagerMixin


# Simplified version of StatsForStreamProcessing to only get simple time or space related information

class StatsForStreamProcessing(AsyncContextManagerMixin):

    def process_intent(self, intent):
        stream_infos = stream_stats = stats_per_type = global_stats = None

        if intent.intentType & StatsIntentType.STREAM_INFOS.value == StatsIntentType.STREAM_INFOS.value:
            stream_infos = list(self._stats_stream._stream_statuses.values())

        if intent.intentType & StatsIntentType.STREAM_STATS.value == StatsIntentType.STREAM_STATS.value:
            stream_stats = {
                self._stats_stream._stream_infos[k]: {
                    k2: convert_to_public(v2) for k2, v2 in v.items()
                } for k, v in self._stats_stream._stream_stats.items()
            }

        if intent.intentType & StatsIntentType.GLOBAL_STATS_PER_TYPE.value == StatsIntentType.GLOBAL_STATS_PER_TYPE.value:
            stats_per_type = {
                k: convert_to_public(v) for k, v in self._stats_stream._global_stats_per_type.items()
            }

        if intent.intentType & StatsIntentType.GLOBAL_STATS.value == StatsIntentType.GLOBAL_STATS.value:
            global_stats = GlobalStreamStats(
                **{
                    **self._stats_stream._global_stats.dict(),
                    'timeStats': self._stats_stream._global_stats.timeStats.public
                }
            )

        return StatsOutput(
            streamInfos=stream_infos,
            streamStats=stream_stats,
            statsPerType=stats_per_type,
            globalStats=global_stats
        )

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._stats_stream = current_stats_stream.get()

        remote_send_orders, local_receive_orders = create_memory_object_stream[StatsIntent](max_buffer_size=0)
        local_send_stats, remote_receive_stats = create_memory_object_stream[StatsPerStream](max_buffer_size=0)

        async def process():
            async with (
                local_receive_orders,
                local_send_stats
            ):
                async for intent in local_receive_orders:
                    await local_send_stats.send(self.process_intent(intent))

        async with create_task_group() as tg:
            tg.start_soon(process)
            yield remote_send_orders, remote_receive_stats

def run(self, data: bytes) -> CompressionResult:
    compressed, c_ms = self.compress(data)
    decompressed, d_ms = self.decompress(compressed)
    orig = len(data)
    comp = len(compressed)
    return CompressionResult(
        algorithm=f"gzip (level={self.params.compresslevel})",
        original_size=orig,
        compressed_size=comp,
        compression_ratio=round(orig / comp, 4) if comp else float("inf"),
        space_saving_pct=round((1 - comp / orig) * 100, 2) if orig else 0.0,
        compress_time_ms=round(c_ms, 4),
        decompress_time_ms=round(d_ms, 4),
        lossless_verified=decompressed == data,
    )