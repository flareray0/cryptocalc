from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import SettingsUpdateRequest
from app.storage.settings import load_settings, save_settings


router = APIRouter(prefix="/api/v1", tags=["settings"])


@router.get("/settings")
def get_settings():
    return load_settings()


@router.post("/settings")
def update_settings(request: SettingsUpdateRequest):
    current = load_settings()
    current.update(request.model_dump())
    save_settings(current)
    return current
