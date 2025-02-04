from pydantic import BaseModel, UUID4


class FloorMeasureBase(BaseModel):
    id: UUID4
    storey: int
    height: float
    accuracy_measure: float
    aux_info: dict | None = None


class FloorMeasureResponse(FloorMeasureBase):
    method: str
    datasets: list[str]
