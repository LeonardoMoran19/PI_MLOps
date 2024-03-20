from fastapi import FastAPI
import pandas as pd
pd.options.mode.chained_assignment = None

app = FastAPI()

playtime_genre = pd.read_parquet('playtimegenre.parquet')
user_genre = pd.read_parquet('userforgenre.parquet')
users_recommend = pd.read_parquet('usersrecommend.parquet')
users_nrecommend = pd.read_parquet('usersnotrecommend.parquet')
sentiment = pd.read_parquet('sentiment_analysis.parquet')
recomendations = pd.read_parquet('recommendations.parquet')

@app.get('/PlayTimeGenre')
def PlayTimeGenre(genero: str):
    try:
        return playtime_genre.loc[genero.lower()]['return']
    except KeyError:
        return 'Genero no se encuentra en los registros'
    
@app.get("/UserForGenre")
def UserForGenre(genero: str):
    try:
        return user_genre.loc[genero.lower()]['return']
    except KeyError:
        return 'Genero no se encuentra en los registros'

@app.get("/UsersRecommend")
def UsersRecommend( year : int ):
    try:
        return users_recommend.loc[year]['return']
    except KeyError:
        return 'Año no se encuentra en los registros'

@app.get("/UsersNotRecommend")
def UsersNotRecommend( year : int ):
    try:
        return users_nrecommend.loc[year]['return']
    except KeyError:
        return 'Año no se encuentra en los registros'

@app.get("/SentimentAnalysis")
def sentiment_analysis( year : int ):
    try:
        return sentiment.loc[year]['return']
    except KeyError:
        return 'Año no se encuentra en los registros'

@app.get("/GameRecommendation")
def recomendacion_juego( id :int):
    try:
        return recomendations.loc[id]['return']
    except KeyError:
        return 'Juego no se encuentra en los registros'

