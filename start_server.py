import os
import uvicorn

if __name__ == "__main__":
    # Obtener puerto de la variable de entorno o usar 8080 por defecto
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting server on port {port}")
    
    # Importar la app
    from app.main import app
    
    # Iniciar servidor
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info"
    )
