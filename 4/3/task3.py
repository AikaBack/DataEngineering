import sqlite3
import json
import pandas as pd
import pickle



def read_and_insert_data(file_path_csv, file_path_pkl, db_path):
    conn = sqlite3.connect(db_path)

    # Загружаем данные из CSV и PKL в таблицу music_info
    df_csv = pd.read_csv(file_path_csv, delimiter=';',
                         usecols=['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre', 'energy'])
    df_pkl = pd.DataFrame(pd.read_pickle(file_path_pkl),
                          columns=['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre', 'energy'])
    df_combined = pd.concat([df_csv, df_pkl], ignore_index=True)
    df_combined.to_sql('music_info', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()


def execute_queries(db_path='music.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    VAR = 52

    # Вывод первых (VAR+10) отсортированных по году
    cursor.execute("SELECT * FROM music_info ORDER BY year DESC LIMIT ?", (VAR + 10,))
    sorted_rows_year = cursor.fetchall()
    with open('task3_sorted_rows_year.json', 'w') as json_file:
        json.dump([dict(row) for row in sorted_rows_year], json_file, indent=2)

    # Вывод (сумму, мин, макс, среднее) по продолжительности
    cursor.execute(
        "SELECT SUM(duration_ms) as sum_duration, MIN(duration_ms) as min_duration, MAX(duration_ms) as max_duration, AVG(duration_ms) as avg_duration FROM music_info")
    duration_stats_dict = dict(zip(['sum_duration', 'min_duration', 'max_duration', 'avg_duration'], cursor.fetchone()))
    with open('task3_numeric_stats_duration.json', 'w') as json_file:
        json.dump(duration_stats_dict, json_file, indent=2)

    # Вывод частоты встречаемости для жанров
    cursor.execute("SELECT genre FROM music_info")
    all_genres = [genre.strip() for row in cursor.fetchall() for genre in row['genre'].split(',')]
    genre_counts = {genre: all_genres.count(genre) for genre in set(all_genres)}
    with open('task3_categorical_frequency_genre.json', 'w') as json_file:
        json.dump(genre_counts, json_file, indent=2)

    # Вывод первых (VAR+15) отфильтрованных по предикату жанр Dance/Electronic 2015 года и более отсортированных по году
    cursor.execute(
        "SELECT * FROM music_info WHERE genre LIKE '%Dance/Electronic%' AND year > 2014 ORDER BY year DESC LIMIT ?",
        (VAR + 15,))
    filtered_sorted_rows = cursor.fetchall()
    with open('task3_filtered_sorted_rows.json', 'w') as json_file:
        json.dump([dict(row) for row in filtered_sorted_rows], json_file, indent=2)

    conn.close()


# Пример использования функций
file_path_csv = 'task_3_var_52_part_1.csv'
file_path_pkl = 'task_3_var_52_part_2.pkl'
db_path = 'music.db'

read_and_insert_data(file_path_csv, file_path_pkl, db_path)
execute_queries(db_path)
