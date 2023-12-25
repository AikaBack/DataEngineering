from bs4 import BeautifulSoup
import zipfile
import json
from collections import Counter
import re


def handle_zip(zip_file_path):
    products = []

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        html_files = [file for file in zip_ref.namelist() if file.endswith('.html')]

        for html_file in html_files:
            with zip_ref.open(html_file) as file:
                html_content = file.read()
                soup = BeautifulSoup(html_content, 'html.parser')

                # Извлекаем информацию о продуктах
                product_items = soup.find_all('div', class_='product-item')

                for item in product_items:
                    product = {}

                    # Извлекаем различные характеристики продукта
                    product['Название'] = item.find('span').get_text(strip=True)
                    product['Цена'] = item.find('price').get_text(strip=True)

                    # Найдем количество бонусов, если они есть
                    bonus_tag = item.find('strong', string=re.compile(r'начислим \d+ бонусов', re.IGNORECASE))
                    if bonus_tag:
                        bonus_text = bonus_tag.get_text(strip=True)
                        bonus_amount = re.search(r'\d+', bonus_text)
                        if bonus_amount:
                            product['Количество бонусов'] = int(bonus_amount.group())
                        else:
                            product['Количество бонусов'] = "Нет информации о бонусах"
                    else:
                        product['Количество бонусов'] = "Нет информации о бонусах"

                    # Извлекаем остальные характеристики продукта
                    product['Processor'] = item.find('li', type='processor').get_text(strip=True) if item.find('li', type='processor') else "Нет информации"
                    product['Sim'] = item.find('li', type='sim').get_text(strip=True) if item.find('li', type='sim') else "Нет информации"
                    product['Resolution'] = item.find('li', type='resolution').get_text(strip=True) if item.find('li', type='resolution') else "Нет информации"
                    product['Acc'] = item.find('li', type='acc').get_text(strip=True) if item.find('li', type='acc') else "Нет информации"
                    product['Camera'] = item.find('li', type='camera').get_text(strip=True) if item.find('li', type='camera') else "Нет информации"
                    product['RAM'] = item.find('li', type='ram').get_text(strip=True) if item.find('li', type='ram') else "Нет информации"
                    product['Matrix'] = item.find('li', type='matrix').get_text(strip=True) if item.find('li', type='matrix') else "Нет информации"

                    # Добавляем информацию о продукте в список
                    products.append(product)

    return products

file_path = "zip_var_52.zip"

products_data = handle_zip(file_path)

# Сохраняем все полученные данные в JSON файл
with open('all_products_data.json', 'w', encoding='utf-8') as json_file:
    json.dump(products_data, json_file, ensure_ascii=False)

# Сортировка по полю 'RAM'
sorted_by_ram = sorted(products_data, key=lambda x: int(re.search(r'\d+', x['RAM']).group()) if re.search(r'\d+', x['RAM']) else 0)

# Сохраняем отсортированные по RAM продукты в JSON файл
with open('sorted_by_ram.json', 'w', encoding='utf-8') as json_file:
    json.dump(sorted_by_ram, json_file, ensure_ascii=False)

# Фильтрация по полю 'SIM' (где количество SIM больше 2)
filtered_by_sim = [product for product in products_data if re.search(r'\d+', product['Sim']) and int(re.search(r'\d+', product['Sim']).group()) > 2]

# Сохраняем продукты, где количество SIM больше 2, в JSON файл
with open('filtered_by_sim.json', 'w', encoding='utf-8') as json_file:
    json.dump(filtered_by_sim, json_file, ensure_ascii=False)

# Выбор числового поля 'Количество бонусов' и вычисление статистических характеристик
bonus_amounts = [product['Количество бонусов'] for product in products_data if isinstance(product['Количество бонусов'], int)]
bonus_statistics = {
    'Сумма': sum(bonus_amounts),
    'Минимальное значение': min(bonus_amounts),
    'Максимальное значение': max(bonus_amounts),
    'Среднее значение': sum(bonus_amounts) / len(bonus_amounts) if bonus_amounts else 0
}

# Сохраняем статистические характеристики по количеству бонусов в JSON файл
with open('bonus_statistics.json', 'w', encoding='utf-8') as json_file:
    json.dump(bonus_statistics, json_file, ensure_ascii=False)

# Выбор текстового поля 'Matrix' и подсчет частоты меток
matrix_labels = [product['Matrix'] for product in products_data]
matrix_frequency = Counter(matrix_labels)

# Сохраняем частоту меток в поле Matrix в JSON файл
with open('matrix_frequency.json', 'w', encoding='utf-8') as json_file:
    json.dump(matrix_frequency, json_file, ensure_ascii=False)