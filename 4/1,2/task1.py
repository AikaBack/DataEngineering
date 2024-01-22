import sqlite3
import msgpack
import json


# Функция чтения файла формата msgpack в базу данных
def read_msgpack_to_sqlite(file_path, db_path):
    conn = sqlite3.connect(db_path + '.db')
    cursor = conn.cursor()

    with open(file_path, 'rb') as file:
        data = msgpack.unpack(file)

        # Формируем список кортежей для вставки
        values_list = [
            (
                row['name'],
                row['street'],
                row['city'],
                row['zipcode'],
                row['floors'],
                row['year'],
                row['parking'],
                row['prob_price'],
                row['views']
            )
            for row in data
        ]

        # Одним запросом вставляем все данные
        cursor.executemany(
            "INSERT INTO building (name, street, city, zipcode, floors, year, parking, prob_price, views) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values_list
        )

    conn.commit()
    conn.close()


# Функция вызова 3 запросов
def execute_queries(db_path):
    conn = sqlite3.connect(db_path + '.db')
    cursor = conn.cursor()

    VAR = 52

    # Вывод первых (VAR+10) отсортированных по полю views строк из таблицы в файл формата json
    cursor.execute("SELECT * FROM building ORDER BY views LIMIT ?", (VAR + 10,))
    with open('task1_sorted_views_var_52.json', 'w') as file:
        # Используем метод fetchall() для получения результатов в виде словарей
        rows = cursor.fetchall()
        # Конвертируем в список словарей перед записью в файл JSON
        json.dump([dict(row) for row in rows], file, ensure_ascii=False)


    # Вывод (сумма, минимум, максимум, среднее) по полю prob_price с именами статистики
    cursor.execute("SELECT SUM(prob_price), MIN(prob_price), MAX(prob_price), AVG(prob_price) FROM building")
    prob_price_stats = cursor.fetchone()

    print("Статистика цен:")
    print(f"Cумма: {prob_price_stats[0]}")
    print(f"Минимальная цена: {prob_price_stats[1]}")
    print(f"Максимальная цена: {prob_price_stats[2]}")
    print(f"Средняя цена: {prob_price_stats[3]}")

    # Вывод частоты встречаемости для поля parking
    cursor.execute("SELECT parking, COUNT(*) FROM building GROUP BY parking")
    parking_frequency = cursor.fetchall()
    print("Частота встречаемости парковок:", parking_frequency)

    # Вывод первых (VAR+10) отфильтрованных по условию этажность >=5, сортированных по убывающей цене
    cursor.execute("SELECT * FROM building WHERE floors >= 5 ORDER BY prob_price DESC LIMIT ?", (VAR + 10,))
    with open('task1_filtered_floors5_var_52.json', 'w') as file:
        # Используем метод fetchall() для получения результатов в виде словарей
        rows = cursor.fetchall()
        # Конвертируем в список словарей перед записью в файл JSON
        json.dump([dict(row) for row in rows], file, ensure_ascii=False)


    conn.close()


# Вызов функций
file_path_msgpack = 'task_1_var_52_item.msgpack'
db_path = 'first'

read_msgpack_to_sqlite(file_path_msgpack, db_path)
execute_queries(db_path)
