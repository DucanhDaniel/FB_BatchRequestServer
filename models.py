import json
import requests
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, HTTPException, Body
from pydantic import BaseModel, Field, field_validator

class BatchRequest(BaseModel):
    access_token: str = Field(..., description="Facebook Graph API Access Token", examples=["EAAB..."])
    relative_urls: List[str] = Field(
        ...,
        description="Danh sách URL tương đối (tối đa 50), KHÔNG chứa version (v23.0).",
        examples=[
            "act_123456789/ads?fields=id,name&limit=5",
            "act_123456789/campaigns?fields=id,name,objective&limit=5",
            "act_123456789/adsets?fields=id,name&limit=5"
        ]
    )

    @field_validator("relative_urls")
    def validate_urls(cls, v):
        if not 1 <= len(v) <= 50:
            raise ValueError(f"Số lượng URL phải từ 1 đến 50. Hiện tại là {len(v)}.")
        return v
    
class RateLimitResponse(BaseModel):
    summary: Dict[str, Any] = Field(..., description="Dict chứa các thông số insight limit và BUC")
    # app_id_util_pct: Optional[float] = Field(None, description="Phần trăm giới hạn Insights đã sử dụng của ứng dụng (lấy giá trị cao nhất).")
    # acc_id_util_pct: Dict[str, float] = Field(..., description="Phần trăm giới hạn Insights đã sử dụng của từng tài khoản.")
    message: str = Field(..., description="Thông báo kết quả.")
