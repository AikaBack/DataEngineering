from bs4 import BeautifulSoup
import zipfile
import json
from collections import Counter
import re

def convert_to_number(string):
    multipliers = {'K': 1000, 'M': 1000000}
    if string:
        string = string.replace('.', '')  # Удаляем точки
        if string[-1] in multipliers:
            return float(string[:-1]) * multipliers[string[-1]]
        return float(string)
    return 0  # Возвращаем 0, если строка пустая или None


def handle_zip(zip_file_path):
    products = []
    with (zipfile.ZipFile(zip_file_path, 'r') as zip_ref):
        for file_name in zip_ref.namelist():
            if file_name.endswith('.html'):
                with zip_ref.open(file_name) as file:
                    html_content = file.read()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    dataset_info = {}

                    # Извлечение данных для article_name
                    dataset_info['article_name'] = soup.find('h1', class_='sc-iAEyYk').text.strip()

                    # Извлечение данных для licence
                    licence_elements = soup.find_all('p', class_='sc-dKfzgJ')
                    licence_text = ''
                    for element in licence_elements:
                        if element.find('a', class_='sc-jegxcv'):
                            licence_text = element.text.strip()
                            break
                        elif 'Other' in element.text:  # Проверка на случай, если нет ссылки
                            licence_text = element.text.strip()
                            break
                    dataset_info['licence'] = licence_text

                    # Извлечение данных для download_size
                    download_size_element = soup.find('span', class_='sc-gjTGSA', string=re.compile(r'\bDownload\b'))
                    download_size = int(
                        re.search(r'\d+', download_size_element.text).group()) if download_size_element else 0
                    dataset_info['download_size'] = download_size

                    # Извлечение данных для code_topics
                    code_topics_element = soup.find('span', class_='sc-fLQRDB', string=re.compile(r'Code \(\d+\)'))
                    dataset_info['code_topics'] = int(re.search(r'\d+', code_topics_element.text).group())

                    # Извлечение данных для files_count
                    files_count = ''
                    found_element = soup.find('p', class_='sc-dKfzgJ', string=re.compile(r'Count', flags=re.IGNORECASE))
                    if found_element:
                        files_count_text = found_element.get_text(strip=True)
                        digit_match = re.search(r'\d+', files_count_text)
                        if digit_match:
                            files_count = digit_match.group()

                    dataset_info['files_count'] = files_count

                    # Извлечение данных для views
                    h5_element = soup.find('h5', class_='sc-brKeYL sc-ktEKTO sc-jvkuxH izsmej JpJng fXjUHb')
                    dataset_info['views'] = convert_to_number(
                        h5_element.text.strip().replace(',', '')) if h5_element else 0

                    products.append(dataset_info)
    return products


# Путь к zip-файлу
zip_file_path = "objects.zip"

# Обработка zip-файла и извлечение данных
products_data = handle_zip(zip_file_path)

# JSON-файл с обработанными данными
with open('processed_data.json', 'w', encoding='utf-8') as json_file:
    json.dump(products_data, json_file, ensure_ascii=False)

# Сортировка по просмотрам (views)
sorted_by_views = sorted(products_data, key=lambda x: x['views'], reverse=True)
with open('sorted_by_views.json', 'w', encoding='utf-8') as json_file:
    json.dump(sorted_by_views, json_file, ensure_ascii=False)

# Фильтрация по размеру загрузки (download_size) больше 50 МБ
filtered_by_size = [product for product in products_data if isinstance(product['download_size'], int) and product['download_size'] > 50]
with open('filtered_by_size.json', 'w', encoding='utf-8') as json_file:
    json.dump(filtered_by_size, json_file, ensure_ascii=False)

# Выбор числового поля 'views' и вычисление статистических характеристик
views = [product['views'] for product in products_data]
views_statistics = {
    'mean': sum(views) / len(views),
    'median': sorted(views)[len(views) // 2],
    'max': max(views),
    'min': min(views),
}
with open('views_statistics.json', 'w', encoding='utf-8') as json_file:
    json.dump(views_statistics, json_file, ensure_ascii=False)

# Выбор текстового поля 'licence' и подсчет частоты меток
licence_labels = [product['licence'] for product in products_data]
licence_frequency = Counter(licence_labels)
with open('licence_frequency.json', 'w', encoding='utf-8') as json_file:
    json.dump(licence_frequency, json_file, ensure_ascii=False)
