import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="AI stub service", version="1.0.0")


class ProcessIn(BaseModel):
    job_id: str = Field(min_length=1)
    query: str = Field(min_length=1)


class ProcessOut(BaseModel):
    result: str


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/v1/process", response_model=ProcessOut)
def process(body: ProcessIn):
    if "fail" in body.query.lower():
        raise HTTPException(status_code=503, detail="simulated_ai_failure")
    text = body.query.strip()
    return ProcessOut(result=f"[stub-ai] processed job={body.job_id!r}: {text[:500]}")


def main():
    port = int(os.environ.get("PORT", "8000"))
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()
