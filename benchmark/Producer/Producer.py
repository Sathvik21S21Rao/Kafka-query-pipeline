from abc import ABC,abstractmethod

class Producer(ABC):
    def __init__(self):
        pass
    @abstractmethod
    def send(self,topic,value):
        pass
    @abstractmethod
    def close(self):
        pass
    @abstractmethod
    def flush(self):
        pass
    