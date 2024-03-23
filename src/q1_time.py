from typing import List, Tuple
from datetime import datetime
import pandas as pd
from typing import List, Tuple
from datetime import datetime

file_path = "farmers-protest-tweets-2021-2-4.json"

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    df = pd.read_json(file_path, lines=True)
    # Se crea funcion para aplanar columnas
    def aplanar_columnas(df, columnas):
        for col in columnas:
            col_df = pd.json_normalize(df[col])
            # Se renombra columnas
            col_df.columns = [f"{col}_{subcol}" for subcol in col_df.columns] 
            # Concatenacion con DF original
            df = pd.concat([df.drop(col, axis=1), col_df], axis=1)
        return df

    columnas = ['user']
    df = aplanar_columnas(df, columnas)
    # Transformación de formato fecha
    df['date'] = df['date'].dt.date
    # Se obtiene el top 10 de días con más registros
    top_days= df.groupby('date').size().sort_values(ascending=False).head(10)
    # Se crea lista para resultado final
    top_usernames_per_day = []

    # Itera sobre cada uno de los top_days
    for day in top_days.index:
        df_day = df[df['date'] == day]
        # Cantidad de username repetidos para cada dia
        username_counts = df_day['user_username'].value_counts()
        # Encuentra el máximo valor
        max_count = username_counts.max()
        most_frequent_username = username_counts[username_counts == max_count].index[0]    
        # Guarda el dia y el username en una lista
        top_usernames_per_day.append((day, most_frequent_username))

    # Imprime la lista de resultados
    return(top_usernames_per_day)
