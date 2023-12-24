import zipfile
import xml.etree.ElementTree as ET
import json
import io


def handle_zip(zip_file_path):
    objects_data = []

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        xml_files = [name for name in zip_ref.namelist() if name.endswith('.xml')]

        for xml_file in xml_files:
            with zip_ref.open(xml_file) as xml_data:
                xml_content = xml_data.read()
                tree = ET.parse(io.BytesIO(xml_content))
                root = tree.getroot()

                for clothing_item in root.findall('clothing'):
                    item_data = {}
                    for child in clothing_item:
                        item_data[child.tag] = child.text.strip() if child.text else None
                    objects_data.append(item_data)

    return objects_data


# Обработка данных из XML
xml_file_path = 'zip_var_52.zip'
objects_data = handle_zip(xml_file_path)

# Записываем все данные в один JSON-файл
with open('all_data.json', 'w') as all_data_file:
    json.dump(objects_data, all_data_file, indent=2, ensure_ascii=False)

# Остальные операции

# Отсортировать значения по одному из доступных полей (например, 'price')
sorted_by_price = sorted(objects_data, key=lambda x: int(x.get('price', 0) or 0))
with open('sorted_by_price.json', 'w') as sorted_price_file:
    json.dump(sorted_by_price, sorted_price_file, indent=2, ensure_ascii=False)

# Выполнить фильтрацию по другому полю (например, 'color')
filtered_by_color = [obj for obj in objects_data if obj.get('color') == 'Желтый']
with open('filtered_by_color.json', 'w') as filtered_color_file:
    json.dump(filtered_by_color, filtered_color_file, indent=2, ensure_ascii=False)

# Для числового поля 'price' вычисляем статистические характеристики
price_values = [float(obj.get('price', 0)) for obj in objects_data if obj.get('price')]
price_statistics = {
    'sum': sum(price_values),
    'min': min(price_values),
    'max': max(price_values),
    'average': sum(price_values) / len(price_values) if price_values else None,
}

with open('price_statistics.json', 'w') as price_statistics_file:
    json.dump(price_statistics, price_statistics_file, indent=2, ensure_ascii=False)

# Для текстового поля 'material' вычисляем частоту материалов
material_frequency = {}
for obj in objects_data:
    material = obj.get('material')
    if material:
        material_frequency[material] = material_frequency.get(material, 0) + 1

with open('material_frequency.json', 'w') as material_frequency_file:
    json.dump(material_frequency, material_frequency_file, indent=2, ensure_ascii=False)
