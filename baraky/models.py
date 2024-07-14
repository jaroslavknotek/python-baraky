from pydantic import ConfigDict, BaseModel

from typing import Tuple


class EstateOverview(BaseModel, extra="allow"):
    model_config = ConfigDict(extra="allow")
    link: str
    price: int
    id: str
    gps: Tuple[float, float]
