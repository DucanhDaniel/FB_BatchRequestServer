import json
import requests
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, HTTPException, Body
from pydantic import BaseModel, Field, field_validator

class BatchRequest(BaseModel):
    access_token: str = Field(..., description="Facebook Graph API Access Token", examples=["EAAB..."])
    relative_urls: List[str] = Field(
        ...,
        description="Danh sách URL tương đối, KHÔNG chứa version (v23.0).",
        examples=[
            "act_123456789/ads?fields=id,name&limit=5",
            "act_123456789/campaigns?fields=id,name,objective&limit=5",
            "act_123456789/adsets?fields=id,name&limit=5"
        ]
    )
    
    email: Optional[str] = Field(
        "None",
        description="Email liên hệ (tuỳ chọn).",
        examples=["user@example.com"]
    )
    
class RateLimitRequest(BaseModel):
    access_token: str = Field(..., description="Access Token Facebook.", examples=["EAAB..."])
    ad_account_ids: List[str] = Field(..., description="Danh sách ID tài khoản quảng cáo.", examples=["act_123456789", "act_987654321"])
    
class RateLimitResponse(BaseModel):
    summary: Dict[str, Any] = Field(..., description="Dict chứa các thông số insight limit và BUC")
    message: str = Field(..., description="Thông báo kết quả.")

class CleanupPayload(BaseModel):
    days_to_keep: int = Field(7, ge=0, description="Số ngày log muốn giữ lại. Các file cũ hơn sẽ bị xóa.")