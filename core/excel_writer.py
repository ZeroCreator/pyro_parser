import xlsxwriter
import os
from typing import List, Dict


def create_excel_report(data: List[Dict], filename: str = "результаты.xlsx") -> str:
    """
    Создает Excel файл с результатами парсинга
    Args:
        data: Список словарей с данными
        filename: Имя файла для сохранения
    Returns:
        str: Путь к созданному файлу
    """
    if not data:
        return ""

    # Создаем папку results если ее нет
    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    # Полный путь к файлу
    full_path = os.path.join(results_dir, filename)

    try:
        # Создаем книгу Excel
        workbook = xlsxwriter.Workbook(full_path)
        worksheet = workbook.add_worksheet('Магазины')

        # Настраиваем форматы
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4F81BD',
            'font_color': 'white',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        cell_format = workbook.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        url_format = workbook.add_format({
            'font_color': 'blue',
            'underline': 1,
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        # Определяем заголовки
        headers = ['№', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка', 'Дата сбора']

        # Записываем заголовки
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)

        # Записываем данные
        for row, item in enumerate(data, 1):
            # Номер
            worksheet.write(row, 0, row, cell_format)

            # Название магазина
            worksheet.write(row, 1, item.get('Название магазина', ''), cell_format)

            # Адрес
            worksheet.write(row, 2, item.get('Адрес', ''), cell_format)

            # Телефон
            worksheet.write(row, 3, item.get('Телефон', ''), cell_format)

            # Сайт
            website = item.get('Сайт', '')
            if website:
                worksheet.write_url(row, 4, website, url_format, website)
            else:
                worksheet.write(row, 4, '', cell_format)

            # Ссылка
            url = item.get('Ссылка', '')
            if url:
                worksheet.write_url(row, 5, url, url_format, url)
            else:
                worksheet.write(row, 5, '', cell_format)

            # Дата сбора
            worksheet.write(row, 6, item.get('Дата сбора', ''), cell_format)

        # Настраиваем ширину колонок
        worksheet.set_column('A:A', 5)  # №
        worksheet.set_column('B:B', 30)  # Название
        worksheet.set_column('C:C', 40)  # Адрес
        worksheet.set_column('D:D', 25)  # Телефон
        worksheet.set_column('E:E', 30)  # Сайт
        worksheet.set_column('F:F', 60)  # Ссылка
        worksheet.set_column('G:G', 20)  # Дата сбора

        # Добавляем фильтр
        worksheet.autofilter(0, 0, len(data), len(headers) - 1)

        # Замораживаем заголовок
        worksheet.freeze_panes(1, 0)

        # Закрываем книгу
        workbook.close()

        return full_path

    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        return ""
