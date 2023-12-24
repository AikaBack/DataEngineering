from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import collections
import zipfile


def handle_zip(zip_file_path):
        books = []
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            html_files = [file for file in zip_ref.namelist() if file.endswith('.html')]

            for html_file in html_files:
                with zip_ref.open(html_file) as file:
                    html_content = file.read()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    book = {}

                    category_tag = soup.find('span', string='Категория:')
                    if category_tag:
                        book['Категория'] = category_tag.find_next_sibling().get_text(strip=True)

                    book_title_tag = soup.find('h1', class_='book-title')
                    if book_title_tag:
                        book['Название книги'] = book_title_tag.get_text(strip=True)
                    else:
                        book['Название книги'] = "Нет информации о названии книги"

                    author_tag = soup.find('p', class_='author-p')
                    if author_tag:
                        book['Автор'] = author_tag.get_text(strip=True)
                    else:
                        book['Автор'] = "Нет информации об авторе"

                    pages_tag = soup.find('span', class_='pages')
                    if pages_tag:
                        pages_text = pages_tag.get_text(strip=True)
                        volume = re.search(r'\d+', pages_text)
                        book['Объем'] = int(volume.group()) if volume else "Нет информации об объеме"
                    else:
                        book['Объем'] = "Нет информации об объеме"

                    year_tag = soup.find('span', class_='year')
                    if year_tag:
                        year_text = year_tag.get_text(strip=True)
                        year = re.search(r'\d{4}', year_text)
                        book['Год издания'] = int(year.group()) if year else "Нет информации о годе издания"
                    else:
                        book['Год издания'] = "Нет информации о годе издания"

                    # Добавляем данные каждой книги в список books
                    books.append(book)
        return books


file_path = "zip_var_52.zip"

books = handle_zip(file_path)

books = sorted(books, key=lambda x: x['Автор'], reverse=True)

with open("result_all_1_json.json", "w", encoding="utf-8") as f:
    json.dump(books, f, ensure_ascii=False)

# Фильтруем книги с объемом >= 250 страниц
filtered_books = [book for book in books if int(book['Объем']) >= 250]

# Сохраняем отфильтрованные книги в JSON-файл
with open("result_filter_1.json", "w", encoding="utf-8") as f:
    json.dump(filtered_books, f, ensure_ascii=False)

result = []

df = pd.DataFrame(books)
pd.set_option('display.float_format', '{:.2f}'.format)
data1 = df['Объем']
print(data1.describe())  #  статистические х-ки

words = df['Автор']
data2 = collections.Counter(words)
print(data2)  # частота категорий