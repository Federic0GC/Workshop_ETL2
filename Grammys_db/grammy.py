import pandas as pd

Grammy_awards = 'Data/the_grammy_awards.csv' 
grammy_awards = pd.read_csv(Grammy_awards)  # Elimina el argumento 'delimiter' para usar la coma como delimitador por defecto

print(grammy_awards.head())
print(grammy_awards.info())
