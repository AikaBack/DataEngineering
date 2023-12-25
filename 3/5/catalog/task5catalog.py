from bs4 import BeautifulSoup
import json
from collections import Counter
import statistics

# Чтение HTML-кода из файла
file_path = 'search result.html'

with open(file_path, 'r', encoding='utf-8') as file:
    html_code = file.read()

soup = BeautifulSoup(html_code, 'html.parser')

results = []
downloads = []  # Добавлен список для хранения значений downloads

items = soup.find_all('li', class_='sc-caslwl')

# Собираем информацию о каждом результате и сохраняем в отдельный файл
for idx, item in enumerate(items):
    dataset_info = {}

    title = item.find('div', class_='sc-beqWaB').text.strip().lower()  # Приводим к нижнему регистру
    dataset_info['title'] = title

    # Удаляем запятые из чисел перед преобразованием их в int
    downloads_value = int(item.find('p', class_='sc-dnwKUv').text.strip().split()[0].replace(',', ''))
    dataset_info['downloads'] = downloads_value
    downloads.append(downloads_value)  # Добавляем значение downloads в список

    upvotes = int(item.find('span', class_='sc-gjTGSA').text.strip())
    dataset_info['upvotes'] = upvotes

    date_element = item.find('span', class_='sc-lnAgIa')
    date_ago = date_element.find(title=True)['title']
    dataset_info['date_ago'] = date_ago

    results.append(dataset_info)


# Фильтрация по upvotes
filtered_by_upvotes = [result for result in results if result['upvotes'] >= 100]

# Сортировка по downloads
sorted_by_downloads = sorted(results, key=lambda x: x['downloads'], reverse=True)

# Статистика по downloads
downloads_statistics = {
    'mean': statistics.mean(downloads),
    'median': statistics.median(downloads),
    'max': max(downloads),
    'min': min(downloads),
}

# Частота по заголовкам (title)
title_frequency = Counter(result['title'] for result in results)

# Сохранение результатов в отдельные JSON файлы
with open('search_results.json', 'w', encoding='utf-8') as file:
    json.dump(results, file, ensure_ascii=False, indent=4)

with open('sorted_by_downloads.json', 'w', encoding='utf-8') as file:
    json.dump(sorted_by_downloads, file, ensure_ascii=False, indent=4)

with open('filtered_by_upvotes.json', 'w', encoding='utf-8') as file:
    json.dump(filtered_by_upvotes, file, ensure_ascii=False, indent=4)

with open('downloads_statistics.json', 'w', encoding='utf-8') as file:
    json.dump(downloads_statistics, file, ensure_ascii=False, indent=4)

with open('title_frequency.json', 'w', encoding='utf-8') as file:
    json.dump(title_frequency, file, ensure_ascii=False, indent=4)
