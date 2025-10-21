from abc import ABC,abstractmethod

class Consumer(ABC):
    def __init__(self):
        pass
    @abstractmethod
    def poll(self,timeout_ms):
        pass
    @abstractmethod
    def close(self):
        pass
    @abstractmethod
    def subscribe(self,topics):
        pass