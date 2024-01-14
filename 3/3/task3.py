import zipfile
import xml.etree.ElementTree as ET
import json
from collections import defaultdict
import statistics


def handle_zip(zip_file_path):
    objects_data = []

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        xml_files = [file for file in zip_ref.namelist() if file.endswith('.xml')]

        for xml_file in xml_files:
            with zip_ref.open(xml_file) as file:
                tree = ET.parse(file)
                root = tree.getroot()

                object_data = {}
                for child in root:
                    object_data[child.tag] = child.text.strip() if child.text else None

                objects_data.append(object_data)

    return objects_data

# Обработка данных из XML
objects_data = handle_zip('zip_var_52.zip')

# Записываем все результаты в один JSON-файл
with open('all_results.json', 'w', encoding='utf-8') as all_results_file:
    json.dump(objects_data, all_results_file, indent=2, ensure_ascii=False)

# Отсортировать значения по одному из доступных полей (radius)
sorted_by_radius = sorted(objects_data, key=lambda x: int(x.get('radius', 0) or 0), reverse=True)
with open('sorted_by_radius.json', 'w', encoding='utf-8') as sorted_radius_file:
    json.dump(sorted_by_radius, sorted_radius_file, indent=2, ensure_ascii=False)

# Выполнить фильтрацию по другому полю (constellation)
filtered_by_constellation = [obj for obj in objects_data if obj.get('constellation') == 'Рыбы']
with open('filtered_by_constellation.json', 'w', encoding='utf-8') as filtered_constellation_file:
    json.dump(filtered_by_constellation, filtered_constellation_file, indent=2, ensure_ascii=False)

# Для числового поля 'radius' вычисляем статистические характеристики
radius_values = [float(obj.get('radius', 0)) for obj in objects_data if obj.get('radius')]
radius_statistics = {
    'sum': sum(radius_values),
    'min': min(radius_values),
    'max': max(radius_values),
    'average': statistics.mean(radius_values) if radius_values else None,
    'median': statistics.median(radius_values) if radius_values else None
}
with open('radius_statistics.json', 'w') as radius_statistics_file:
    json.dump(radius_statistics, radius_statistics_file, indent=2, ensure_ascii=False)

# Для текстового поля 'constellation' вычисляем частоту меток
constellation_frequency = defaultdict(int)
for obj in objects_data:
    constellation_frequency[obj.get('constellation', 'Unknown')] += 1
with open('constellation_frequency.json', 'w') as constellation_frequency_file:
    json.dump(dict(constellation_frequency), constellation_frequency_file, indent=2, ensure_ascii=False)