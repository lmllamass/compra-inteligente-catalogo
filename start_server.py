import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"=== STARTING SERVER ON PORT {port} ===")
    from app.main import app
    uvicorn.run(app, host="0.0.0.0", port=port)
