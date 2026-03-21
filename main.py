from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test")
def test():
    return {"message": "Bon Buasson API работает"}

@app.get("/data")
def get_data():
    return {"message": "DATA endpoint works"}
