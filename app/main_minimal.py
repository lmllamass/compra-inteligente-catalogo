from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "API funcionando correctamente"}

@app.get("/health") 
def health():
    return {"status": "ok", "service": "ferretero-api"}
