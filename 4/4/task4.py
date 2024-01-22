import sqlite3
import json

def add_data(file_path, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

        # Создаем список значений для вставки
        values_list = [(item.get('name'),
                        item.get('price', 0),
                        item.get('quantity', 0),
                        item.get('category', 'unknown'),
                        item.get('fromCity', 'Неизвестно'),
                        item.get('isAvailable', False),
                        item.get('views', 0),
                        0)  # 0 для updates
                       for item in data]

        # Один запрос на вставку всех данных
        cursor.executemany('''
            INSERT INTO items_info (name, price, quantity, category, fromCity, isAvailable, views, updates)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', values_list)

    conn.commit()
    conn.close()


def execute_change(file_path, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(file_path, 'r', encoding='utf-8') as file:
        current_name = None
        try:
            conn.execute('BEGIN TRANSACTION')  # Начинаем транзакцию
            for line in file:
                if line.startswith("name::"):
                    current_name = line[len("name::"):].strip()
                    continue

                if current_name:
                    components = line.strip().split('::')

                    # Получаем текущую версию записи
                    cursor.execute('SELECT updates, quantity, price FROM items_info WHERE name = ?', (current_name,))
                    result = cursor.fetchone()

                    if result:
                        current_version, current_quantity, current_price = result

                        # Определение типа изменения и выполнение соответствующего действия
                        if components[0] == 'method':
                            method, param = components[1], next(file).strip('param::').strip()

                            if method == 'quantity_add':
                                new_quantity = current_quantity + int(param)
                                if new_quantity >= 0:
                                    cursor.execute('''
                                        UPDATE items_info 
                                        SET quantity = ?, updates = updates + 1
                                        WHERE name = ?
                                    ''', (new_quantity, current_name))
                                    print(f"Updated: Quantity added to {current_name}")
                                else:
                                    print(f"Warning: Invalid update for {current_name}. Negative quantity.")
                            elif method == 'remove':
                                cursor.execute('''
                                    DELETE FROM items_info 
                                    WHERE name = ?
                                ''', (current_name,))
                                print(f"Updated: Removed {current_name}")
                            elif method == 'price_abs':
                                new_price = current_price + float(param)
                                if new_price >= 0:
                                    cursor.execute('''
                                        UPDATE items_info 
                                        SET price = ?, updates = updates + 1
                                        WHERE name = ?
                                    ''', (new_price, current_name))
                                    print(f"Updated: Price changed for {current_name}")
                                else:
                                    print(f"Warning: Invalid update for {current_name}. Negative price.")
                            elif method == 'available':
                                cursor.execute('''
                                    UPDATE items_info 
                                    SET isAvailable = ?, updates = updates + 1
                                    WHERE name = ?
                                ''', (param.lower() == 'true', current_name))
                                print(f"Updated: Availability changed for {current_name}")
                            elif method == 'quantity_sub':
                                new_quantity = current_quantity - int(param)
                                if new_quantity >= 0:
                                    cursor.execute('''
                                        UPDATE items_info 
                                        SET quantity = ?, updates = updates + 1
                                        WHERE name = ?
                                    ''', (new_quantity, current_name))
                                    print(f"Updated: Quantity subtracted from {current_name}")
                                else:
                                    print(f"Warning: Invalid update for {current_name}. Negative quantity.")
                            elif method == 'price_percent':
                                new_price = current_price * (1 + float(param))
                                if new_price >= 0:
                                    cursor.execute('''
                                        UPDATE items_info 
                                        SET price = ?, updates = updates + 1
                                        WHERE name = ?
                                    ''', (new_price, current_name))
                                    print(f"Updated: Price percent changed for {current_name}")
                                else:
                                    print(f"Warning: Invalid update for {current_name}. Negative price.")
                    else:
                        print(f"Warning: No record found for name '{current_name}'")
        finally:
            conn.execute('COMMIT')  # Завершаем транзакцию
            conn.close()


def perform_queries(db_path):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Запрос 1: топ-10 самых обновляемых товаров
        cursor.execute('''
            SELECT name, updates 
            FROM items_info 
            ORDER BY updates DESC 
            LIMIT 10
        ''')
        top_updates = cursor.fetchall()
        with open('task4_top_updates.json', 'w', encoding='utf-8') as json_file:
            json.dump([{'name': name, 'updates': updates} for name, updates in top_updates], json_file,
                      ensure_ascii=False, indent=2)
        # Запрос 2: Анализ цен товаров
        cursor.execute('''
                SELECT category, SUM(price) AS total_price, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) 
                AS avg_price, COUNT(*) AS count
                FROM items_info
                GROUP BY category
            ''')
        price_analysis_result = cursor.fetchall()
        price_analysis_result_with_names = [{
            "category": category,
            "total_price": total_price,
            "min_price": min_price,
            "max_price": max_price,
            "avg_price": avg_price,
            "count": count
        } for category, total_price, min_price, max_price, avg_price, count in price_analysis_result]

        with open('task4_price_analysis.json', 'w', encoding='utf-8') as json_file:
            json.dump(price_analysis_result_with_names, json_file, ensure_ascii=False, indent=2)

        # Запрос 3: Анализ остатков товаров
        cursor.execute('''
                SELECT category, SUM(quantity) AS total_quantity, MIN(quantity) AS min_quantity, MAX(quantity) 
                AS max_quantity, AVG(quantity) AS avg_quantity, COUNT(*) AS count
                FROM items_info
                GROUP BY category
            ''')
        quantity_analysis_result = cursor.fetchall()
        quantity_analysis_result_with_names = [{
            "category": category,
            "total_quantity": total_quantity,
            "min_quantity": min_quantity,
            "max_quantity": max_quantity,
            "avg_quantity": avg_quantity,
            "count": count
        } for category, total_quantity, min_quantity, max_quantity, avg_quantity, count in quantity_analysis_result]

        with open('task4_quantity_analysis.json', 'w', encoding='utf-8') as json_file:
            json.dump(quantity_analysis_result_with_names, json_file, ensure_ascii=False, indent=2)

        # Запрос 4: Произвольный запрос: цена больше 1000, доступен, количество просмотров больше 50000 и сорт. по цене
        cursor.execute(
            'SELECT name, price, isAvailable, views FROM items_info WHERE price >= 1000 AND isAvailable = 1 '
            'AND views > 50000 ORDER BY price')
        custom_query_result = cursor.fetchall()
        custom_query_result_with_names = [{
            "name": name,
            "price": price,
            "isAvailable": isAvailable,
            "views": views
        } for name, price, isAvailable, views in custom_query_result]

        with open('task4_custom_query.json', 'w', encoding='utf-8') as json_file:
            json.dump(custom_query_result_with_names, json_file, ensure_ascii=False, indent=2)


product_data_json = 'task_4_var_52_product_data.json'
update_data_text = 'task_4_var_52_update_data.text'
db_path = 'items.db'
#add_data(product_data_json, db_path)
#execute_change(update_data_text, db_path)
perform_queries(db_path)
