from fastapi import FastAPI
import pandas as pd
pd.options.mode.chained_assignment = None
import pickle

app = FastAPI()

@app.get("/UsersRecommend")
def UsersRecommend( year : int ):
    user_reviews = pd.read_parquet('user_reviews.parquet')
    steam_games = pd.read_parquet('steam_games.parquet')
    df = user_reviews[(user_reviews['date'].dt.year == year) & 
                      (user_reviews['sentiment_analysis'] > 0)].drop(columns=['date','sentiment_analysis'])
    df = df.groupby('item_id')['recommend'].sum().reset_index()
    df = df.sort_values(by='recommend', ascending=False)
    df = df.head(3)
    ids = df['item_id'].values
    names =[]
    for id in ids:
        id = int(id)
        names.append(steam_games.loc[id]['name'])
    return 'Puesto 1: '+names[0]+', Puesto 2: '+names[1]+', Puesto 3: '+names[2]

@app.get("/UsersNotRecommend")
def UsersNotRecommend( year : int ):
    user_reviews = pd.read_parquet('user_reviews.parquet')
    steam_games = pd.read_parquet('steam_games.parquet')
    df = user_reviews[(user_reviews['date'].dt.year == year) & 
                      (user_reviews['sentiment_analysis'] == 0)&
                      (user_reviews['recommend'] == 0) ].drop(columns=['recommend','date','sentiment_analysis'])
    df = df['item_id'].value_counts().head(10).reset_index()
    ids = df['item_id'].values
    names =[]
    for id in ids:
        id = int(id)
        try:
            names.append(steam_games.loc[id]['name'])
        except KeyError:
            pass
    return 'Puesto 1: '+names[0]+', Puesto 2: '+names[1]+', Puesto 3: '+names[2]

@app.get("/SentimentAnalysis")
def sentiment_analysis( year : int ):
    df = pd.read_parquet('user_reviews.parquet')
    df = df[(df['date'].dt.year == year)].drop(columns={'date','recommend','item_id'})
    df = df.value_counts().reset_index().set_index('sentiment_analysis')
    return 'Negativo = '+str(df.loc[0]['count'])+' Neutral = '+str(df.loc[1]['count'])+' Positivo = '+str(df.loc[2]['count'])

@app.get('/PlayTimeGenre')
def PlayTimeGenre( genero : str ):
    steam_games = pd.read_parquet('steam_games.parquet')
    steam_games = steam_games[steam_games['genres'].apply(lambda x: genero in x)]
    user_items = pd.read_parquet('users_items.parquet')
    user_items = user_items[['item_id','playtime']]
    user_items = user_items.groupby('item_id')['playtime'].sum().reset_index().set_index('item_id')
    merged_df = steam_games.merge(user_items, how='left', left_index=True, right_index=True)
    merged_df['playtime']=merged_df['playtime'].fillna(0)
    merged_df['date']=merged_df['date'].dt.year
    merged_df= merged_df.drop(columns={'genres','name'})
    merged_df= merged_df.groupby('date')['playtime'].sum().reset_index()
    merged_df = merged_df.sort_values(by='playtime', ascending=False)
    return 'Año de lanzamiento con mas horas jugadas para el genero '+genero+' es el: '+str(int(merged_df['date'].values[0]))
    
@app.get("/UserForGenre")
def UserForGenre( genero : str ): 
    steam_games = pd.read_parquet('steam_games.parquet')
    user_items = pd.read_parquet('users_items.parquet')
    steam_games = steam_games[steam_games['genres'].apply(lambda x: genero in x)]
    steam_games['date']=steam_games['date'].dt.year
    indices= steam_games.index.tolist()
    user_items = user_items[user_items['item_id'].isin(indices)]
    user_top = user_items.drop(columns={'item_name','item_id'})
    user_top = user_top.groupby('user_id')['playtime'].sum().reset_index().sort_values(by='playtime', ascending=False)
    user_top = user_top['user_id'].iloc[0]
    user = user_items[user_items['user_id']==user_top]
    user = user.set_index('item_id')
    game_date = steam_games.drop(columns={'name','genres'})
    user = user.merge(game_date, how='left', left_index=True, right_index=True)
    user = user.drop(columns={'item_name','user_id'})
    user = user.groupby('date')['playtime'].sum().reset_index()
    user = user[user['playtime']>0]
    texto = 'Usuario con mas horas jugadas para el genero '+genero+': '+str(user_top)+'. Horas jugadas por año de lanzamiento: '
    for x in user.index.tolist():
        texto = texto+'Año '+str(int(user.loc[x]['date']))+': '+str(int(user.loc[x]['playtime']))+' Hora(s). '
    return texto

@app.get("/GameRecommendation")
def recomendacion_juego( id :int):
    steam_games=pd.read_parquet('steam_games.parquet').drop(columns={'date'}) 
    with open('similarity_matrix.pkl', 'rb') as f:
        similarity_matrix = pickle.load(f)
    game_index = steam_games.index.get_loc(id)
    similar_games_indices = similarity_matrix[game_index].argsort()[::-1][1:6]
    similar_games = steam_games.iloc[similar_games_indices]
    similar_games = similar_games['name'].values
    
    return 'Juegos recomendados: 1.'+similar_games[0]+', 2.'+similar_games[1]+', 3.'+similar_games[2]+', 4.'+similar_games[3]+', 5.'+similar_games[4]


