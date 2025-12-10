import xlsxwriter
import os
from typing import List, Dict


def create_excel_report(new_shops: List[Dict],
                                 parsed_shops: List[Dict],
                                 all_shops: List[Dict],
                                 filename: str = "результаты.xlsx") -> str:
    """
    Создает Excel файл с четырьмя вкладками:
    1. Новые магазины
    2. Спарсенные магазины (текущий парсинг)
    3. Все магазины (из базы данных)
    4. Статистика

    Args:
        new_shops: Список новых магазинов
        parsed_shops: Список магазинов из текущего парсинга
        all_shops: Список всех магазинов из базы данных
        filename: Имя файла для сохранения

    Returns:
        str: Путь к созданному файлу
    """
    if not parsed_shops:
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

        # ========== СОЗДАНИЕ ФОРМАТОВ ==========
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

        # Формат для выделения новых магазинов (зеленый)
        new_shop_format = workbook.add_format({
            'bold': True,
            'bg_color': '#C6EFCE',  # Светло-зеленый
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        # Формат для ссылок в новых магазинах
        new_shop_url_format = workbook.add_format({
            'font_color': 'blue',
            'underline': 1,
            'bg_color': '#C6EFCE',  # Светло-зеленый фон
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        # Формат для отсутствующих магазинов
        missing_format = workbook.add_format({
            'bg_color': '#FFC7CE',  # Светло-красный
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        # Формат для неактивных магазинов
        inactive_format = workbook.add_format({
            'font_color': '#999999',
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        # ========== ВКЛАДКА 1: НОВЫЕ МАГАЗИНЫ ==========
        headers_basic = ['№', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка', 'Дата сбора']

        if new_shops:
            worksheet1 = workbook.add_worksheet('Новые магазины')

            # Записываем заголовки
            for col, header in enumerate(headers_basic):
                worksheet1.write(0, col, header, header_format)

            # Записываем данные
            for row, shop in enumerate(new_shops, 1):
                # Номер
                worksheet1.write(row, 0, row, new_shop_format)

                # Название магазина
                worksheet1.write(row, 1, shop.get('Название магазина', ''), new_shop_format)

                # Адрес
                worksheet1.write(row, 2, shop.get('Адрес', ''), new_shop_format)

                # Телефон
                worksheet1.write(row, 3, shop.get('Телефон', ''), new_shop_format)

                # Сайт
                website = shop.get('Сайт', '')
                if website:
                    worksheet1.write_url(row, 4, website, new_shop_url_format, website)
                else:
                    worksheet1.write(row, 4, '', new_shop_format)

                # Ссылка
                url = shop.get('Ссылка', '')
                if url:
                    worksheet1.write_url(row, 5, url, new_shop_url_format, url)
                else:
                    worksheet1.write(row, 5, '', new_shop_format)

                # Дата сбора
                worksheet1.write(row, 6, shop.get('Дата сбора', ''), new_shop_format)

            # Настраиваем ширину колонок
            worksheet1.set_column('A:A', 5)  # №
            worksheet1.set_column('B:B', 30)  # Название
            worksheet1.set_column('C:C', 40)  # Адрес
            worksheet1.set_column('D:D', 25)  # Телефон
            worksheet1.set_column('E:E', 30)  # Сайт
            worksheet1.set_column('F:F', 60)  # Ссылка
            worksheet1.set_column('G:G', 20)  # Дата сбора

            # Добавляем фильтр
            worksheet1.autofilter(0, 0, len(new_shops), len(headers_basic) - 1)
            worksheet1.freeze_panes(1, 0)

            # Добавляем информацию о количестве
            worksheet1.write(len(new_shops) + 2, 0, f"Всего новых магазинов: {len(new_shops)}")

        else:
            # Если новых магазинов нет, создаем вкладку с сообщением
            worksheet1 = workbook.add_worksheet('Новые магазины')

            # Записываем заголовки
            for col, header in enumerate(headers_basic):
                worksheet1.write(0, col, header, header_format)

            # Сообщение об отсутствии новых магазинов
            worksheet1.write(1, 0, 1, cell_format)
            worksheet1.write(1, 1, 'Новых магазинов не обнаружено', cell_format)

            # Настраиваем ширину колонок
            worksheet1.set_column('A:A', 5)
            worksheet1.set_column('B:B', 30)
            worksheet1.set_column('C:C', 40)
            worksheet1.set_column('D:D', 25)
            worksheet1.set_column('E:E', 30)
            worksheet1.set_column('F:F', 60)
            worksheet1.set_column('G:G', 20)

        # ========== ВКЛАДКА 2: СПАРСЕННЫЕ МАГАЗИНЫ ==========
        worksheet2 = workbook.add_worksheet('Спарсенные магазины')

        # Записываем заголовки
        for col, header in enumerate(headers_basic):
            worksheet2.write(0, col, header, header_format)

        # Записываем данные всех спарсенных магазинов
        for row, shop in enumerate(parsed_shops, 1):
            # Номер
            worksheet2.write(row, 0, row, cell_format)

            # Название магазина
            worksheet2.write(row, 1, shop.get('Название магазина', ''), cell_format)

            # Адрес
            worksheet2.write(row, 2, shop.get('Адрес', ''), cell_format)

            # Телефон
            worksheet2.write(row, 3, shop.get('Телефон', ''), cell_format)

            # Сайт
            website = shop.get('Сайт', '')
            if website:
                worksheet2.write_url(row, 4, website, url_format, website)
            else:
                worksheet2.write(row, 4, '', cell_format)

            # Ссылка
            url = shop.get('Ссылка', '')
            if url:
                worksheet2.write_url(row, 5, url, url_format, url)
            else:
                worksheet2.write(row, 5, '', cell_format)

            # Дата сбора
            worksheet2.write(row, 6, shop.get('Дата сбора', ''), cell_format)

        # Настраиваем ширину колонок
        worksheet2.set_column('A:A', 5)  # №
        worksheet2.set_column('B:B', 30)  # Название
        worksheet2.set_column('C:C', 40)  # Адрес
        worksheet2.set_column('D:D', 25)  # Телефон
        worksheet2.set_column('E:E', 30)  # Сайт
        worksheet2.set_column('F:F', 60)  # Ссылка
        worksheet2.set_column('G:G', 20)  # Дата сбора

        # Добавляем фильтр
        worksheet2.autofilter(0, 0, len(parsed_shops), len(headers_basic) - 1)
        worksheet2.freeze_panes(1, 0)

        # Добавляем информацию о количестве
        worksheet2.write(len(parsed_shops) + 2, 0, f"Всего спарсено магазинов: {len(parsed_shops)}")

        # ========== ВКЛАДКА 3: ВСЕ МАГАЗИНЫ ==========
        # Дополнительные заголовки для всех магазинов
        headers_all = ['№', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка',
                       'Дата добавления', 'Дата последнего обнаружения', 'В последнем парсинге', 'Статус']

        worksheet3 = workbook.add_worksheet('Все магазины')

        # Записываем заголовки
        for col, header in enumerate(headers_all):
            worksheet3.write(0, col, header, header_format)

        # Создаем множество ссылок новых магазинов для выделения
        new_shops_links = {shop.get('Ссылка', '') for shop in new_shops}
        parsed_shops_links = {shop.get('Ссылка', '') for shop in parsed_shops}

        # Записываем данные всех магазинов
        for row, shop in enumerate(all_shops, 1):
            # Определяем статус магазина
            shop_link = shop.get('Ссылка', '')
            in_parsed = shop_link in parsed_shops_links
            is_new = shop_link in new_shops_links

            # Выбираем формат в зависимости от статуса
            if not in_parsed:
                row_format = missing_format
                status = "Отсутствует"
            elif is_new:
                row_format = new_shop_format
                status = "Новый"
            else:
                row_format = cell_format
                status = "В базе"

            # Номер
            worksheet3.write(row, 0, row, row_format)

            # Название магазина
            worksheet3.write(row, 1, shop.get('Название магазина', ''), row_format)

            # Адрес
            worksheet3.write(row, 2, shop.get('Адрес', ''), row_format)

            # Телефон
            worksheet3.write(row, 3, shop.get('Телефон', ''), row_format)

            # Сайт
            website = shop.get('Сайт', '')
            if website:
                # Используем соответствующий формат для ссылок
                if not in_parsed:
                    url_fmt = missing_format
                elif is_new:
                    url_fmt = new_shop_url_format
                else:
                    url_fmt = url_format
                worksheet3.write_url(row, 4, website, url_fmt, website)
            else:
                worksheet3.write(row, 4, '', row_format)

            # Ссылка
            url = shop.get('Ссылка', '')
            if url:
                worksheet3.write_url(row, 5, url, url_format, url)
            else:
                worksheet3.write(row, 5, '', row_format)

            # Дата добавления
            worksheet3.write(row, 6, shop.get('Дата добавления', ''), row_format)

            # Дата последнего обнаружения
            worksheet3.write(row, 7, shop.get('Дата последнего обнаружения', ''), row_format)

            # В последнем парсинге
            in_last = "Да" if shop.get('обнаружен_в_последнем_парсинге') else "Нет"
            worksheet3.write(row, 8, in_last, row_format)

            # Статус
            worksheet3.write(row, 9, status, row_format)

        # Настраиваем ширину колонок
        worksheet3.set_column('A:A', 5)  # №
        worksheet3.set_column('B:B', 30)  # Название
        worksheet3.set_column('C:C', 40)  # Адрес
        worksheet3.set_column('D:D', 25)  # Телефон
        worksheet3.set_column('E:E', 30)  # Сайт
        worksheet3.set_column('F:F', 60)  # Ссылка
        worksheet3.set_column('G:G', 20)  # Дата добавления
        worksheet3.set_column('H:H', 25)  # Дата последнего обнаружения
        worksheet3.set_column('I:I', 20)  # В последнем парсинге
        worksheet3.set_column('J:J', 15)  # Статус

        # Добавляем фильтр
        worksheet3.autofilter(0, 0, len(all_shops), len(headers_all) - 1)
        worksheet3.freeze_panes(1, 0)

        # Добавляем информацию о количестве
        worksheet3.write(len(all_shops) + 2, 0, f"Всего магазинов в базе: {len(all_shops)}")

        # ========== ВКЛАДКА 4: СТАТИСТИКА ==========
        worksheet4 = workbook.add_worksheet('Статистика')

        # Заголовок статистики
        stats_header_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'center',
            'valign': 'vcenter'
        })

        worksheet4.merge_range('A1:C1', 'СТАТИСТИКА ПАРСИНГА', stats_header_format)

        # Данные статистики
        stats_data = [
            ['Показатель', 'Значение'],
            ['Всего в базе данных', len(all_shops)],
            ['Спарсено в текущем запуске', len(parsed_shops)],
            ['Новых магазинов', len(new_shops)],
            ['Магазинов с телефоном', sum(1 for s in parsed_shops if s.get('Телефон'))],
            ['Магазинов с сайтом', sum(1 for s in parsed_shops if s.get('Сайт'))],
            ['', ''],
            ['Магазинов не найдено в этом парсинге',
             sum(1 for s in all_shops if not s.get('обнаружен_в_последнем_парсинге', False))],
            ['Процент покрытия',
             f"{(len(parsed_shops) / len(all_shops) * 100):.1f}%" if all_shops else "0%"],
            ['', ''],
            ['Дата парсинга', parsed_shops[0].get('Дата сбора', '') if parsed_shops else ''],
            ['Город', 'Ростов-на-Дону']
        ]

        for row, (label, value) in enumerate(stats_data, 2):
            worksheet4.write(row, 0, label)
            worksheet4.write(row, 1, value)

        # Настраиваем ширину колонок
        worksheet4.set_column('A:A', 40)
        worksheet4.set_column('B:B', 25)

        # Закрываем книгу
        workbook.close()

        return full_path

    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        import traceback
        traceback.print_exc()
        return ""
