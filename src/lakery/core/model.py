from typing import LiteralString


class BaseModel:
    def __init_subclass__(cls, model_id: LiteralString | None, **kwargs) -> None:
        pass
