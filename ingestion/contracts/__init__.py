"""Pydantic data contracts for inbound API payloads — T1-6.

These contracts are validated at ingress before bronze is written.
Any field drift raises a ValidationError and halts the load.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class EIADemandRecord(BaseModel):
    period: str
    respondent: str
    respondent_name: Optional[str] = None
    type: str
    type_name: Optional[str] = None
    value: float
    value_units: str = "megawatthours"


class EIAGenerationRecord(BaseModel):
    period: str
    respondent: str
    fuel_type: str = Field(alias="fueltype")
    fuel_type_description: Optional[str] = Field(None, alias="fueltypeid")
    value: float
    value_units: str = "megawatthours"

    model_config = {"populate_by_name": True}


class CarbonIntensityRecord(BaseModel):
    from_: str = Field(alias="from")
    to: str
    intensity_index: str
    intensity_actual: Optional[float] = None
    intensity_forecast: float

    model_config = {"populate_by_name": True}


class WeatherRecord(BaseModel):
    time: str
    temperature_2m: float
    wind_speed_10m: float
    precipitation: float
    weather_code: Optional[int] = None
