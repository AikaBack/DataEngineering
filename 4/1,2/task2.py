import sqlite3
import msgpack
import json


# Функция для чтения данных из файла msgpack и записи в таблицу building_feedback
def read_feedback_to_sqlite(file_path, db_path):
    conn = sqlite3.connect(db_path + '.db')
    cursor = conn.cursor()

    with open(file_path, 'rb') as file:
        data = msgpack.unpack(file)
        for row in data:
            values = (
                row['name'],  # Название здания, к которому относится отзыв
                row['rating'],
                row['convenience'],
                row['security'],
                row['functionality'],
                row['comment']
            )
            cursor.execute(
                "INSERT INTO building_feedback (building_name, rating, convenience, security, functionality, comment) VALUES (?, ?, ?, ?, ?, ?)",
                values
            )

    conn.commit()
    conn.close()


# Функция выполнения запросов с использованием связей между таблицами
def execute_queries_with_relations(db_path):
    conn = sqlite3.connect(db_path + '.db')
    cursor = conn.cursor()

    # Пример запроса: выборка отзывов для определенного здания по его названию
    cursor.execute("SELECT * FROM building_feedback WHERE building_name = 'Бункер 37'")
    with open('task2_feedback_specific_building.json', 'w') as file:
        json.dump(cursor.fetchall(), file, ensure_ascii=False)

    # Пример запроса: подсчет средней оценки (рейтинга) здания на основе отзывов
    cursor.execute("SELECT AVG(rating) FROM building_feedback WHERE building_name = 'Бункер 37'")
    with open('task2_average_rating_specific_building.json', 'w', encoding='utf-8') as file:
        json.dump({'Средняя оценка дома Бункер 37': cursor.fetchone()[0]}, file, ensure_ascii=False)

    # Пример запроса: выборка зданий с комментарием о безопасности выше определенного уровня
    cursor.execute("SELECT building.* FROM building JOIN building_feedback ON building.name = building_feedback.building_name WHERE building_feedback.security > 2")
    with open('task2_security2_buildings_comments.json', 'w') as file:
        json.dump(cursor.fetchall(), file, ensure_ascii=False)

    conn.close()


# Вызов функции для чтения данных из msgpack и записи в таблицу
file_path_msgpack_feedback = 'task_2_var_52_subitem.msgpack'
db_path = 'first'
read_feedback_to_sqlite(file_path_msgpack_feedback, db_path)

# Вызов функции для выполнения запросов с использованием связей между таблицами
execute_queries_with_relations(db_path)
