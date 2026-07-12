from abc import ABC, abstractmethod


class ReprEnforced(ABC):
    @abstractmethod
    def __repr__(self) -> str:
        ...
