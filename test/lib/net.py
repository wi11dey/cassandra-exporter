import typing


class SocketAddress(typing.NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        return f'{self.host}:{self.port}'
