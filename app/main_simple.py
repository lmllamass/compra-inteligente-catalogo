from fastapi import FastAPI

app = FastAPI(title="Test API")

@app.get("/")
def root():
    return {"message": "API funcionando", "status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}
