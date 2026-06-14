from anyio import AsyncContextManagerMixin, create_memory_object_stream, create_task_group
from anyio.abc import ObjectReceiveStream

from contextlib import asynccontextmanager


class ExpectingHash(AsyncContextManagerMixin):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], hash_parameters: HashParameters, expected_hash: bytes):
        self._upper_stream = upper_stream
        self._hash_parameters = hash_parameters
        self._expected_hash = expected_hash

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        parrot_stream_send, parrot_stream = create_memory_object_stream[bytes]()
        hash_instance = hash_instance_for(self._hash_parameters)

        async def hash_data_on_the_fly():
            async with self._upper_stream:
                async for data in self._upper_stream:
                    hash_instance.update(data)
                    parrot_stream_send.send(data)

        async with create_task_group() as tg:
            tg.start_soon(hash_data_on_the_fly)
            yield parrot_stream

            dgst = hash_instance.digest()
            if dgst != self._expected_hash:
                raise HashIntegrityError(expectedHash=self._expected_hash, computedHash=dgst)
