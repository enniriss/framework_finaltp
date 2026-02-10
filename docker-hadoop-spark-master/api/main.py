
import os
from datetime import datetime, timedelta
from functools import lru_cache
from typing import List, Dict, Any

import jwt
import pyarrow.dataset as ds
import pyarrow.fs as fs
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel

APP_TITLE = "Datamart API"
JWT_SECRET = os.getenv("JWT_SECRET", "change_me")
JWT_ALG = os.getenv("JWT_ALG", "HS256")

DATAMART_PATH = os.getenv(
    "DATAMART_PATH",
    "hdfs://namenode:9000/data/gold/predictive_returns"
)

PAGE_SIZE_MAX = int(os.getenv("PAGE_SIZE_MAX", "200"))

app = FastAPI(title=APP_TITLE)

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

def _create_token(username: str) -> str:
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(hours=8)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALG)

def _verify_token(token: str) -> str:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
        return payload.get("sub", "")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

def _bearer_dep(authorization: str = Query(None, alias="Authorization")):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    token = authorization.split(" ", 1)[1].strip()
    return _verify_token(token)

@lru_cache(maxsize=1)
def _get_dataset():
    if DATAMART_PATH.startswith("hdfs://"):
        hdfs, path = fs.HadoopFileSystem.from_uri(DATAMART_PATH)
        return ds.dataset(path, filesystem=hdfs, format="parquet")
    return ds.dataset(DATAMART_PATH, format="parquet")

@app.post("/token", response_model=TokenResponse)
def login(body: LoginRequest):
    # ✅ minimal auth (remplace par DB si besoin)
    if not (body.username == "admin" and body.password == "admin"):
        raise HTTPException(status_code=401, detail="Bad credentials")
    return {"access_token": _create_token(body.username)}

@app.get("/datamarts")
def list_datamarts(user: str = Depends(_bearer_dep)):
    return [{"name": "predictive_returns", "path": DATAMART_PATH}]

@app.get("/datamarts/predictive_returns")
def get_predictive_returns(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=PAGE_SIZE_MAX),
    user: str = Depends(_bearer_dep)
) -> Dict[str, Any]:
    dataset = _get_dataset()
    table = dataset.to_table()  # simple (OK pour TP)
    total = table.num_rows

    start = (page - 1) * page_size
    end = min(start + page_size, total)

    if start >= total:
        return {"page": page, "page_size": page_size, "total": total, "items": []}

    batch = table.slice(start, end - start).to_pydict()
    items = [dict(zip(batch.keys(), vals)) for vals in zip(*batch.values())]

    return {"page": page, "page_size": page_size, "total": total, "items": items}