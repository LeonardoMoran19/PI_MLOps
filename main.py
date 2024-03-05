from fastapi import FastAPI

app = FastAPI()

@app.get("/pruebaGet")
def mostrar():
    return 'Prueba funcionamiento'

app.post("/pruebaPost")
def conseguir(str :str):
    return {'message':f'str: {str} '}