import sqlite3
import msgpack
import pandas as pd
import json
from bs4 import BeautifulSoup


def create_tables(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            address TEXT,
            phone TEXT,
            website TEXT,
            company TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            body TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER,
            name TEXT,
            email TEXT,
            body TEXT,
            FOREIGN KEY (post_id) REFERENCES posts (id)
        )
    ''')

    conn.commit()
    conn.close()


def insert_data_into_tables(db_name, users_msgpack_file, posts_csv_file, comments_html_file):
    with open(users_msgpack_file, 'rb') as file:
        users_data = msgpack.unpack(file, raw=False)

    posts_data = pd.read_csv(posts_csv_file)

    with open(comments_html_file, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')
        table = soup.find('table')
        rows = table.find_all('tr')[1:]
        comments_data = []

        for row in rows:
            columns = row.find_all('td')
            comment_data = {
                'postId': int(columns[0].text),
                'id': int(columns[1].text),
                'name': columns[2].text,
                'email': columns[3].text,
                'body': columns[4].text
            }
            comments_data.append(comment_data)

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.executemany('''
        INSERT INTO users (id, name, username, email, address, phone, website, company)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', [(user['id'], user['name'], user['username'], user['email'],
           f"{user['address']['street']} {user['address']['suite']} {user['address']['city']} {user['address']['zipcode']}",
           user['phone'], user['website'],
           f"{user['company']['name']} {user['company']['catchPhrase']} {user['company']['bs']}") for user in users_data])

    cursor.executemany('INSERT INTO posts (id, user_id, title, body) VALUES (?, ?, ?, ?)',
                       [(post['id'], post['userId'], post['title'], post['body']) for _, post in posts_data.iterrows()])

    cursor.executemany('INSERT INTO comments (id, post_id, name, email, body) VALUES (?, ?, ?, ?, ?)',
                       [(comment['id'], comment['postId'], comment['name'], comment['email'], comment['body'])
                        for comment in comments_data])

    conn.commit()
    conn.close()


def update_posts_comments(db_name, posts_file, comments_file):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Загружаем обновленные данные из файлов
    df_posts_updated = pd.read_csv(posts_file)
    df_comments_updated = pd.read_html(comments_file)[0]

    # Обновление таблицы posts
    for index, row in df_posts_updated.iterrows():
        cursor.execute('''
            UPDATE posts
            SET user_id = ?
            WHERE id = ?
        ''', (row['userId'], row['id']))

    # Обновление таблицы comments
    for index, row in df_comments_updated.iterrows():
        try:
            cursor.execute('''
                UPDATE comments
                SET name = ?, email = ?
                WHERE post_id = ?
            ''', (row['Name'], row['Email'], row['Post ID']))
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")

    conn.commit()
    conn.close()


def execute_queries(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Запрос 1
    cursor.execute('''
        SELECT p.id, p.title, p.body, COUNT(c.id) as comment_count
        FROM posts p
        LEFT JOIN comments c ON p.id = c.post_id
        GROUP BY p.id, p.title, p.body
        HAVING comment_count > 3
        ORDER BY comment_count DESC
        LIMIT 10
    ''')
    query_1_result = cursor.fetchall()
    save_to_json(query_1_result, 'task5_query_1_posts_more_than_3_comms.json',
                 keys=['post_id', 'title', 'body', 'comment_count'])

    # Запрос 2
    cursor.execute('''
        SELECT p.id, p.title, COUNT(c.id) as comment_count
        FROM posts p
        LEFT JOIN comments c ON p.id = c.post_id
        GROUP BY p.id, p.title
        ORDER BY comment_count DESC
    ''')
    query_2_result = cursor.fetchall()
    save_to_json(query_2_result, 'task5_query_2_top_posts_comms_count.json', keys=['post_id', 'title', 'comment_count'])

    # Запрос 3
    cursor.execute('''
        SELECT name, email, COUNT(id) as comment_count
        FROM comments
        GROUP BY name, email
        ORDER BY comment_count DESC
    ''')
    query_3_result = cursor.fetchall()
    save_to_json(query_3_result, 'task5_query_3_top_commenters.json', keys=['name', 'email', 'comment_count'])

    # Запрос 4
    cursor.execute('''
        SELECT id, post_id, name, email, body, LENGTH(body) as char_count
        FROM comments
        ORDER BY char_count DESC
        LIMIT 50
    ''')
    query_4_result = cursor.fetchall()
    save_to_json(query_4_result, 'task5_query_4_50_biggest_comms.json',
                 keys=['id', 'post_id', 'name', 'email', 'body', 'char_count'])

    # Запрос 5
    cursor.execute('''
        SELECT u.id as user_id, u.name as user_name, COUNT(p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON u.id = p.user_id
        GROUP BY u.id, u.name
        ORDER BY user_name, user_id
    ''')
    query_5_result = cursor.fetchall()
    save_to_json(query_5_result, 'task5_query_5_users_posts.json', keys=['user_id', 'user_name', 'post_count'])

    # Запрос 6
    cursor.execute('''
        SELECT p.title as post_title, p.body as post_body, c.name as comment_name, c.body as comment_body
        FROM posts p
        LEFT JOIN comments c ON p.id = c.post_id
        ORDER BY post_title, comment_name
        LIMIT 100
    ''')
    query_6_result = cursor.fetchall()
    save_to_json(query_6_result, 'task5_query_6_100_posts_comments.json',
                 keys=['post_title', 'post_body', 'comment_name', 'comment_body'])

    # Запрос 7
    # Анализ количества комментариев у постов
    cursor.execute('''
        SELECT MIN(comment_count) as min_comments, MAX(comment_count) as max_comments, AVG(comment_count) as avg_comments
        FROM (
            SELECT p.id, COUNT(c.id) as comment_count
            FROM posts p
            LEFT JOIN comments c ON p.id = c.post_id
            GROUP BY p.id
        )
    ''')
    query_7_result_comments = cursor.fetchone()
    save_to_json([query_7_result_comments], 'task5_query_7_posts_analysis.json',
                 keys=['min_comments', 'max_comments', 'avg_comments'])

    # Запрос 8
    # Анализ количества символов в комментариях
    cursor.execute('''
        SELECT MIN(char_count) as min_chars, MAX(char_count) as max_chars, AVG(char_count) as avg_chars
        FROM (
            SELECT LENGTH(body) as char_count
            FROM comments
        )
    ''')
    query_8_result_chars = cursor.fetchone()
    save_to_json([query_8_result_chars], 'task5_query_8_comms_analysis.json',
                 keys=['min_chars', 'max_chars', 'avg_chars'])

    conn.close()


def save_to_json(data, file_name, keys=None):
    # Преобразование кортежей в словари с явным указанием ключей
    result_dict = [dict(zip(keys, row)) for row in data]

    # Сохранение в JSON-файл
    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(result_dict, json_file, ensure_ascii=False, indent=2)


# Имена базы данных и файлов
db_name = 'posts.db'
users_msgpack_file = 'users.msgpack'
posts_csv_file = 'posts.csv'
comments_html_file = 'comments.html'

# Создание таблиц
create_tables(db_name)

# Запись данных в таблицы
insert_data_into_tables(db_name, users_msgpack_file, posts_csv_file, comments_html_file)

# Выполнение запросов по обновлению данных в таблицах posts и comments из файлов для обновления.
update_posts_comments('posts.db', 'posts_updated.csv', 'comments_updated.html')

# Вызов метода с передачей базы данных
execute_queries(db_name)
