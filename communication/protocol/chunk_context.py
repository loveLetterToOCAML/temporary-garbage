from pydantic import BaseModel


class ChunkInteractorLimits(BaseModel):
    millisecondsTimeoutPerPacket: int = 120000

class ChunkConfig(BaseModel):
    allowChunks: bool = True
    maxChunkSize: int = 1490
    # maxPacketSize represents the maximum memory amount some component can hold for a given interactor, so that it avoids
    # denial of service condition; If more data must be transmitted in one packet, it needs using the continuation workflow
    # which means the serialized object must support chunking or sufficient persistence capacity is remaining
    maxPacketSize: int = 0x1000000  # 16 Mb of data at most by default per interactor, this can be restricted in case of memory limit


class PersistenceLimit(BaseModel):
    maxBytesAllowed: int = 0x100000000  # 4 Gb of data available (strongly depends on the conditions)
    maxBytesAllowedPerInteractor: int = 0x10000000  # divided by 256

class EffectfulInternalInteraction(BaseModel):
    pass

"""
Model is
[Default RAM persistence] < [Default Filer persistence]  < [Additional persistence sorted by increasing size]
           maxPacketSize  < maxBytesAllowedPerInteractor < maxBytesAllowedPerInteractor for each configured persistence

So the serialization process take into account a sorted list of persistences
"""