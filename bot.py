from dataclasses import dataclass
from abc import ABC, abstractclassmethod
from typing import Optional


@dataclass
class Strategy(ABC):

    short_name: str

    @abstractclassmethod
    def get_decision(self, ticker: str) -> bool:
        """Get the decision from a strategy"""
        return True


class AverageStrategy(Strategy):

    def get_decision(self, ticker: str) -> bool:
        return False


@dataclass
class Bot:

    ticker: str
    strategy: Optional[Strategy]

    def take_decision(self):
        if self.strategy:
            return self.strategy.get_decision(self.ticker)


class Void:

    def __init__(self) -> None:
        pass

    @classmethod
    def void_method(cls):
        return super().__init__(cls)


def main():

    strategy = AverageStrategy("media")
    bot = Bot("BTC", strategy=strategy)

    print(bot.take_decision())
    v = Void()
    print(v)
    v2 = v.void_method()
    print(v2)


if __name__ == "__main__":
    main()
