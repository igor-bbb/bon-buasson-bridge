from fastapi import FastAPI

app = FastAPI()

# Проверка что API живо
@app.get("/")
def root():
    return {"status": "ok"}

# Тестовый endpoint
@app.get("/test")
def test():
    return {"message": "Bon Buasson API работает"}

# DATA endpoint (пока простой)
@app.get("/data")
def get_data():
    return {"message": "DATA endpoint works"}
