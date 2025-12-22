import xlsxwriter
import os
from typing import List, Dict


def create_excel_report(new_shops: List[Dict],
                        parsed_shops: List[Dict],
                        all_shops: List[Dict],
                        filename: str = "результаты.xlsx") -> str:
    """
    Создает Excel файл с пятью вкладками:
    1. Новые магазины (все новые)
    2. Яндекс магазины (текущий парсинг)
    3. 2GIS магазины (текущий парсинг)
    4. Все магазины (из базы данных)
    5. Статистика

    Если данные отсутствуют - вкладка будет создана с сообщением
    """
    results_dir = "results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    full_path = os.path.join(results_dir, filename)

    try:
        workbook = xlsxwriter.Workbook(full_path)

        # ========== СОЗДАНИЕ ФОРМАТОВ ==========
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

        new_shop_format = workbook.add_format({
            'bold': True,
            'bg_color': '#C6EFCE',
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        yandex_format = workbook.add_format({
            'bg_color': '#FFE699',
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        gis_format = workbook.add_format({
            'bg_color': '#B4C6E7',
            'align': 'left',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True
        })

        empty_format = workbook.add_format({
            'bg_color': '#F2F2F2',
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'text_wrap': True,
            'italic': True
        })

        # ========== ВКЛАДКА 1: НОВЫЕ МАГАЗИНЫ ==========
        headers_new = ['№', 'Источник', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка', 'Дата сбора']
        worksheet1 = workbook.add_worksheet('Новые магазины')

        for col, header in enumerate(headers_new):
            worksheet1.write(0, col, header, header_format)

        if new_shops:
            for row, shop in enumerate(new_shops, 1):
                source = shop.get('Источник', 'unknown')
                # Выбираем формат в зависимости от источника
                if source == 'yandex':
                    row_fmt = yandex_format
                elif source == '2gis':
                    row_fmt = gis_format
                else:
                    row_fmt = new_shop_format

                worksheet1.write(row, 0, row, row_fmt)
                worksheet1.write(row, 1, source, row_fmt)
                worksheet1.write(row, 2, shop.get('Название магазина', ''), row_fmt)
                worksheet1.write(row, 3, shop.get('Адрес', ''), row_fmt)
                worksheet1.write(row, 4, shop.get('Телефон', ''), row_fmt)

                website = shop.get('Сайт', '')
                if website:
                    worksheet1.write_url(row, 5, website, url_format, website)
                else:
                    worksheet1.write(row, 5, '', row_fmt)

                url = shop.get('Ссылка', '')
                if url:
                    worksheet1.write_url(row, 6, url, url_format, url)
                else:
                    worksheet1.write(row, 6, '', row_fmt)

                worksheet1.write(row, 7, shop.get('Дата сбора', ''), row_fmt)
        else:
            # Если нет новых магазинов
            worksheet1.write(1, 0, 1, empty_format)
            worksheet1.write(1, 1, 'Нет новых магазинов', empty_format)
            worksheet1.merge_range(1, 2, 1, 7, 'В текущем парсинге не обнаружено новых магазинов', empty_format)

        # Настройка ширины колонок
        widths_new = [5, 10, 30, 40, 25, 30, 60, 20]
        for i, width in enumerate(widths_new):
            worksheet1.set_column(i, i, width)

        # ========== ВКЛАДКА 2: ЯНДЕКС МАГАЗИНЫ ==========
        headers_basic = ['№', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка', 'Дата сбора']
        worksheet2 = workbook.add_worksheet('Яндекс магазины')

        for col, header in enumerate(headers_basic):
            worksheet2.write(0, col, header, header_format)

        # Разделяем parsed_shops на Яндекс и 2GIS
        yandex_shops = [shop for shop in parsed_shops if shop.get('Источник') == 'yandex']
        two_gis_shops = [shop for shop in parsed_shops if shop.get('Источник') == '2gis']

        if yandex_shops:
            for row, shop in enumerate(yandex_shops, 1):
                worksheet2.write(row, 0, row, yandex_format)
                worksheet2.write(row, 1, shop.get('Название магазина', ''), yandex_format)
                worksheet2.write(row, 2, shop.get('Адрес', ''), yandex_format)
                worksheet2.write(row, 3, shop.get('Телефон', ''), yandex_format)

                website = shop.get('Сайт', '')
                if website:
                    worksheet2.write_url(row, 4, website, url_format, website)
                else:
                    worksheet2.write(row, 4, '', yandex_format)

                url = shop.get('Ссылка', '')
                if url:
                    worksheet2.write_url(row, 5, url, url_format, url)
                else:
                    worksheet2.write(row, 5, '', yandex_format)

                worksheet2.write(row, 6, shop.get('Дата сбора', ''), yandex_format)

            # Добавляем статистику по вкладке
            total_row = len(yandex_shops) + 2
            worksheet2.write(total_row, 0, f"Всего Яндекс магазинов: {len(yandex_shops)}")
        else:
            # Если нет Яндекс магазинов
            worksheet2.write(1, 0, 1, empty_format)
            worksheet2.write(1, 1, 'Нет данных', empty_format)
            worksheet2.merge_range(1, 2, 1, 6, 'В текущем парсинге Яндекс не использовался или не дал результатов',
                                   empty_format)

        # Настройка ширины колонок
        widths_basic = [5, 30, 40, 25, 30, 60, 20]
        for i, width in enumerate(widths_basic):
            worksheet2.set_column(i, i, width)

        # ========== ВКЛАДКА 3: 2GIS МАГАЗИНЫ ==========
        worksheet3 = workbook.add_worksheet('2GIS магазины')

        for col, header in enumerate(headers_basic):
            worksheet3.write(0, col, header, header_format)

        if two_gis_shops:
            for row, shop in enumerate(two_gis_shops, 1):
                worksheet3.write(row, 0, row, gis_format)
                worksheet3.write(row, 1, shop.get('Название магазина', ''), gis_format)
                worksheet3.write(row, 2, shop.get('Адрес', ''), gis_format)
                worksheet3.write(row, 3, shop.get('Телефон', ''), gis_format)

                website = shop.get('Сайт', '')
                if website:
                    worksheet3.write_url(row, 4, website, url_format, website)
                else:
                    worksheet3.write(row, 4, '', gis_format)

                url = shop.get('Ссылка', '')
                if url:
                    worksheet3.write_url(row, 5, url, url_format, url)
                else:
                    worksheet3.write(row, 5, '', gis_format)

                worksheet3.write(row, 6, shop.get('Дата сбора', ''), gis_format)

            # Добавляем статистику по вкладке
            total_row = len(two_gis_shops) + 2
            worksheet3.write(total_row, 0, f"Всего 2GIS магазинов: {len(two_gis_shops)}")
        else:
            # Если нет 2GIS магазинов
            worksheet3.write(1, 0, 1, empty_format)
            worksheet3.write(1, 1, 'Нет данных', empty_format)
            worksheet3.merge_range(1, 2, 1, 6, 'В текущем парсинге 2GIS не использовался или не дал результатов',
                                   empty_format)

        # Настройка ширины колонок
        for i, width in enumerate(widths_basic):
            worksheet3.set_column(i, i, width)

        # ========== ВКЛАДКА 4: ВСЕ МАГАЗИНЫ ==========
        headers_all = ['№', 'Источник', 'Название магазина', 'Адрес', 'Телефон', 'Сайт', 'Ссылка',
                       'Дата добавления', 'Дата последнего обнаружения', 'В последнем парсинге']

        worksheet4 = workbook.add_worksheet('Все магазины')

        for col, header in enumerate(headers_all):
            worksheet4.write(0, col, header, header_format)

        if all_shops:
            for row, shop in enumerate(all_shops, 1):
                source = shop.get('Источник', 'unknown')

                # Выбираем формат в зависимости от источника
                if source == 'yandex':
                    fmt = yandex_format
                elif source == '2gis':
                    fmt = gis_format
                else:
                    fmt = cell_format

                worksheet4.write(row, 0, row, fmt)
                worksheet4.write(row, 1, source, fmt)
                worksheet4.write(row, 2, shop.get('Название магазина', ''), fmt)
                worksheet4.write(row, 3, shop.get('Адрес', ''), fmt)
                worksheet4.write(row, 4, shop.get('Телефон', ''), fmt)

                website = shop.get('Сайт', '')
                if website:
                    worksheet4.write_url(row, 5, website, url_format, website)
                else:
                    worksheet4.write(row, 5, '', fmt)

                url = shop.get('Ссылка', '')
                if url:
                    worksheet4.write_url(row, 6, url, url_format, url)
                else:
                    worksheet4.write(row, 6, '', fmt)

                worksheet4.write(row, 7, shop.get('Дата добавления', ''), fmt)
                worksheet4.write(row, 8, shop.get('Дата последнего обнаружения', ''), fmt)

                # Внимание: ключ должен быть 'обнаружен_в_последнем_парсинге' (строчная "о")
                in_last = shop.get('обнаружен_в_последнем_парсинге', False)
                worksheet4.write(row, 9, 'Да' if in_last else 'Нет', fmt)
        else:
            # Если база пуста
            worksheet4.write(1, 0, 1, empty_format)
            worksheet4.write(1, 1, 'База пуста', empty_format)
            worksheet4.merge_range(1, 2, 1, 9, 'База данных магазинов пуста. Запустите парсер для сбора данных.',
                                   empty_format)

        # Настройка ширины колонок
        widths = [5, 10, 30, 40, 25, 30, 60, 20, 25, 20]
        for i, width in enumerate(widths):
            worksheet4.set_column(i, i, width)

        # ========== ВКЛАДКА 5: СТАТИСТИКА ==========
        worksheet5 = workbook.add_worksheet('Статистика')

        # Подсчет статистики
        all_yandex = sum(1 for s in all_shops if s.get('Источник') == 'yandex') if all_shops else 0
        all_2gis = sum(1 for s in all_shops if s.get('Источник') == '2gis') if all_shops else 0

        # Новые магазины по источникам
        new_yandex = sum(1 for s in new_shops if s.get('Источник') == 'yandex') if new_shops else 0
        new_2gis = sum(1 for s in new_shops if s.get('Источник') == '2gis') if new_shops else 0

        # Телефоны и сайты по источникам
        phones_yandex = sum(1 for s in yandex_shops if s.get('Телефон')) if yandex_shops else 0
        phones_2gis = sum(1 for s in two_gis_shops if s.get('Телефон')) if two_gis_shops else 0

        sites_yandex = sum(1 for s in yandex_shops if s.get('Сайт')) if yandex_shops else 0
        sites_2gis = sum(1 for s in two_gis_shops if s.get('Сайт')) if two_gis_shops else 0

        stats_data = [
            ['Показатель', 'Яндекс', '2GIS', 'Всего'],
            ['Всего в базе данных', all_yandex, all_2gis, len(all_shops) if all_shops else 0],
            ['Спарсено в текущем запуске', len(yandex_shops), len(two_gis_shops),
             len(yandex_shops) + len(two_gis_shops)],
            ['Новых магазинов', new_yandex, new_2gis, len(new_shops) if new_shops else 0],
            ['Магазинов с телефоном', phones_yandex, phones_2gis, phones_yandex + phones_2gis],
            ['Магазинов с сайтом', sites_yandex, sites_2gis, sites_yandex + sites_2gis],
        ]

        for row, data in enumerate(stats_data):
            for col, value in enumerate(data):
                if row == 0:
                    worksheet5.write(row, col, value, header_format)
                else:
                    worksheet5.write(row, col, value)

        worksheet5.set_column('A:A', 30)
        worksheet5.set_column('B:D', 15)

        # Добавляем информацию о запуске
        info_row = len(stats_data) + 2
        worksheet5.write(info_row, 0, 'Информация о запуске:')
        worksheet5.write(info_row + 1, 0, 'Запущенные парсеры:')

        parsers_used = []
        if yandex_shops:
            parsers_used.append('Яндекс')
        if two_gis_shops:
            parsers_used.append('2GIS')

        if parsers_used:
            worksheet5.write(info_row + 1, 1, ', '.join(parsers_used))
        else:
            worksheet5.write(info_row + 1, 1, 'Нет данных (возможно, только экспорт)')

        workbook.close()
        return full_path

    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        import traceback
        traceback.print_exc()
        return ""
