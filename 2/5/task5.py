import pandas as pd
import json
import msgpack
import pickle
import os

# Чтение набора данных
data = pd.read_csv('jena_climate_2009_2016.csv')

# Выбор нужных числовых полей для анализа
numeric_columns = ["p (mbar)", "T (degC)", "Tdew (degC)", "rh (%)", "VPmax (mbar)", "VPact (mbar)", "sh (g/kg)"]

# Оставляем только указанные поля
data = data[numeric_columns]

# Рассчет характеристик для числовых полей
numeric_stats = {}
for column in numeric_columns:
    stats = {
        'min': data[column].min(),
        'max': data[column].max(),
        'mean': data[column].mean(),
        'sum': data[column].sum(),
        'std_dev': data[column].std()
    }
    numeric_stats[column] = stats

# Сохранение полученных расчетов в JSON
with open('numeric_stats.json', 'w') as json_file:
    json.dump(numeric_stats, json_file)

# Сохранение набора данных в различных форматах: CSV, JSON, Pickle

data.to_csv('jena_climate.csv', index=False)  # CSV
data.to_json('jena_climate.json', orient='records')  # JSON
data.to_pickle('jena_climate.pkl')  # Pickle

# Сохранение в формате MessagePack
packed = msgpack.packb(data.to_dict(orient='records'))
with open('jena_climate.msgpack', 'wb') as msgpack_file:
    msgpack_file.write(packed)

# Определение размеров полученных файлов
files = ['jena_climate.csv', 'jena_climate.json', 'jena_climate.msgpack', 'jena_climate.pkl', 'numeric_stats.json']

for file in files:
    size_in_bytes = os.path.getsize(file)
    size_in_megabytes = size_in_bytes / (1024 * 1024)  # перевод в мегабайты
    print(f"File: {file}, Size: {size_in_megabytes:.2f} MB")
