import pandas as pd

def count_unique_name_email_pairs(comments_updated_file):
    # Загружаем обновленные данные из файла
    updated_data = pd.read_html(comments_updated_file)[0]

    # Подсчитываем количество уникальных пар Name и Email
    unique_name_email_pairs_count = updated_data[['Name', 'Email']].drop_duplicates().shape[0]

    return unique_name_email_pairs_count

# Пример использования
unique_pairs_count = count_unique_name_email_pairs('comments_updated.html')
print(f'Количество уникальных пар Name и Email после обновления: {unique_pairs_count}')
