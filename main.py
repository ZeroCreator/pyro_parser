# main.py
import asyncio
import json
import os
import argparse
import re
from datetime import datetime

from parser.yandex_parser import YandexPyroParser
from parser.twogis_parser import TwoGisPyroParser
from core.excel_report import create_excel_report


class PyroDatabase:
    """Простая JSON база данных для магазинов (поддерживает Яндекс и 2GIS)"""

    def __init__(self, db_file="data/database.json"):
        self.db_file = db_file
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        self.db = self._load_db()

    def _load_db(self) -> dict:
        """Загружаем базу или создаем новую"""
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass

        # Создаем новую базу
        return {
            "last_update": None,
            "total_shops": 0,
            "shops": []
        }

    def save_db(self):
        """Сохраняем базу"""
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump(self.db, f, ensure_ascii=False, indent=2)

    def extract_id(self, url: str) -> str:
        """Извлекаем уникальный ID магазина для обоих источников"""
        if not url:
            return ""

        # Для 2GIS: https://2gis.ru/novocherkassk/firm/70000001027412396
        if '2gis.ru' in url:
            match = re.search(r'/firm/(\d+)', url)
            if match:
                return f"2gis_{match.group(1)}"
        # Для Яндекс
        else:
            patterns = [
                r'/org/[^/]+/(\d+)',
                r'businessId=(\d+)',
                r'/(\d+)/details',
            ]

            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return f"yandex_{match.group(1)}"

        return f"hash_{hash(url) & 0xFFFFFFFF:08x}"

    def get_source_from_id(self, shop_id: str) -> str:
        """Определяем источник по ID"""
        if shop_id.startswith('2gis_'):
            return '2gis'
        elif shop_id.startswith('yandex_'):
            return 'yandex'
        return 'unknown'

    def find_shop_by_id(self, shop_id: str) -> dict:
        """Находим магазин по ID"""
        for shop in self.db.get("shops", []):
            if shop.get("id") == shop_id:
                return shop
        return None

    def add_or_update_shop(self, shop_data: dict, source: str = None) -> tuple:
        """
        Добавляем новый магазин или обновляем существующий

        Args:
            shop_data: данные магазина
            source: явно указанный источник ('yandex' или '2gis')

        Returns:
            tuple: (shop, is_new)
        """
        url = shop_data.get("Ссылка", "")
        shop_id = self.extract_id(url)

        # Если источник не указан явно, определяем по ID
        if not source:
            source = self.get_source_from_id(shop_id)

        existing = self.find_shop_by_id(shop_id)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if existing:
            # Обновляем существующий магазин
            existing.update({
                "Название магазина": shop_data.get("Название магазина", existing.get("Название магазина", "")),
                "Адрес": shop_data.get("Адрес", existing.get("Адрес", "")),
                "Телефон": shop_data.get("Телефон", existing.get("Телефон", "")),
                "Сайт": shop_data.get("Сайт", existing.get("Сайт", "")),
                "Дата последнего обнаружения": current_time,
                "обнаружен_в_последнем_парсинге": True
            })
            return existing, False
        else:
            # Добавляем новый магазин
            new_shop = {
                "id": shop_id,
                "Название магазина": shop_data.get("Название магазина", ""),
                "Адрес": shop_data.get("Адрес", ""),
                "Телефон": shop_data.get("Телефон", ""),
                "Сайт": shop_data.get("Сайт", ""),
                "Ссылка": url,
                "Город": shop_data.get("Город", "Ростов-на-Дону"),
                "Дата добавления": current_time,
                "Дата последнего обнаружения": current_time,
                "обнаружен_в_последнем_парсинге": True,
                "Источник": source  # Добавляем поле источник
            }

            self.db["shops"].append(new_shop)
            self.db["total_shops"] += 1
            return new_shop, True

    def mark_all_unfound(self):
        """Помечаем все магазины как не найденные в текущем парсинге"""
        for shop in self.db.get("shops", []):
            shop["обнаружен_в_последнем_парсинге"] = False

    def get_new_shops(self) -> list:
        """Получаем магазины, добавленные в последнем парсинге"""
        new_shops = []
        for shop in self.db.get("shops", []):
            if shop.get("Дата добавления") == shop.get("Дата последнего обнаружения"):
                new_shops.append(shop)
        return new_shops

    def get_all_shops_for_excel(self) -> list:
        """Получаем все магазины в формате для Excel"""
        all_shops = []
        for shop in self.db.get("shops", []):
            excel_shop = {
                "Название магазина": shop.get("Название магазина", ""),
                "Адрес": shop.get("Адрес", ""),
                "Телефон": shop.get("Телефон", ""),
                "Сайт": shop.get("Сайт", ""),
                "Ссылка": shop.get("Ссылка", ""),
                "Дата добавления": shop.get("Дата добавления", ""),
                "Дата последнего обнаружения": shop.get("Дата последнего обнаружения", ""),
                "обнаружен_в_последнем_парсинге": shop.get("обнаружен_в_последнем_парсинге", False),
                "Источник": shop.get("Источник", "unknown")
            }
            all_shops.append(excel_shop)

        # Сортируем по дате последнего обнаружения (новые сверху)
        all_shops.sort(key=lambda x: x.get("Дата последнего обнаружения", ""), reverse=True)
        return all_shops

    def get_stats(self) -> dict:
        """Статистика базы"""
        total = self.db.get("total_shops", 0)
        found_in_last = sum(1 for s in self.db.get("shops", [])
                            if s.get("обнаружен_в_последнем_парсинге", False))

        # Статистика по источникам
        sources = {"yandex": 0, "2gis": 0, "unknown": 0}
        for shop in self.db.get("shops", []):
            source = shop.get("Источник", "unknown")
            sources[source] = sources.get(source, 0) + 1

        return {
            "total_shops": total,
            "found_in_last_parse": found_in_last,
            "missing_in_last_parse": total - found_in_last,
            "last_update": self.db.get("last_update"),
            "sources": sources
        }


async def run_yandex_parser(headless: bool = False) -> list:
    """Запуск парсера Яндекс"""
    print("\n🔍 ЗАПУСК ПАРСЕРА YANDEX")
    print("-" * 50)

    parser = YandexPyroParser(headless=headless)
    results = await parser.parse()

    # Добавляем источник к данным
    for shop in results:
        shop['Источник'] = 'yandex'

    return results


async def run_2gis_parser(headless: bool = False) -> list:
    """Запуск парсера 2GIS"""
    print("\n🔍 ЗАПУСК ПАРСЕРА 2GIS")
    print("-" * 50)

    parser = TwoGisPyroParser(headless=headless)
    results = await parser.parse()

    # Добавляем источник к данным
    for shop in results:
        shop['Источник'] = '2gis'

    return results


async def main():
    """Основная функция парсинга"""
    parser = argparse.ArgumentParser(description='Парсер магазинов пиротехники')
    parser.add_argument('--source', choices=['yandex', '2gis', 'all'], default='all',
                        help='Источник для парсинга: yandex, 2gis или all (по умолчанию)')
    parser.add_argument('--headless', action='store_true',
                        help='Запуск в фоновом режиме (без отображения браузера)')
    parser.add_argument('--export', action='store_true',
                        help='Только экспорт базы в Excel без парсинга')

    args = parser.parse_args()

    print("=" * 80)
    print("🎆 ПАРСЕР МАГАЗИНОВ ПИРОТЕХНИКИ")
    print("=" * 80)

    # 1. Инициализируем базу
    print("\n📂 Загружаем базу данных...")
    db = PyroDatabase()
    stats = db.get_stats()

    print(f"   Всего магазинов в базе: {stats['total_shops']}")
    if stats['sources']:
        print(f"   Яндекс: {stats['sources'].get('yandex', 0)}")
        print(f"   2GIS: {stats['sources'].get('2gis', 0)}")
    print(f"   Последнее обновление: {stats['last_update']}")

    if args.export:
        print("\n📋 Экспорт базы в Excel...")
        await export_to_excel(db)
        return

    # 2. Помечаем все магазины как не найденные
    db.mark_all_unfound()

    # 3. Запускаем парсеры
    current_shops_data = []

    if args.source in ['yandex', 'all']:
        yandex_shops = await run_yandex_parser(headless=args.headless)
        current_shops_data.extend(yandex_shops)

    if args.source in ['2gis', 'all']:
        two_gis_shops = await run_2gis_parser(headless=args.headless)
        current_shops_data.extend(two_gis_shops)

    if not current_shops_data:
        print("❌ Не удалось получить данные")
        return

    print(f"\n✅ Найдено магазинов в текущем парсинге: {len(current_shops_data)}")

    # 4. Обновляем базу данных
    print("\n💾 Обновляем базу данных...")

    # Инициализируем счетчики
    new_shops_count = 0
    updated_shops_count = 0

    for shop_data in current_shops_data:
        # Определяем источник для передачи в add_or_update_shop
        source = shop_data.get('Источник', 'unknown')
        shop, is_new = db.add_or_update_shop(shop_data, source)
        if is_new:
            new_shops_count += 1
        else:
            updated_shops_count += 1

    # Обновляем метаданные базы
    db.db["last_update"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db.save_db()

    print(f"   Новых магазинов: {new_shops_count}")
    print(f"   Обновленных магазинов: {updated_shops_count}")

    # 5. Создаем отчет
    await export_to_excel(db, current_shops_data, new_shops_count, updated_shops_count)


async def export_to_excel(db: PyroDatabase, current_shops_data: list = None,
                         new_shops_count: int = 0, updated_shops_count: int = 0):
    """Экспорт базы данных в Excel"""
    print("\n📊 Подготавливаем данные для отчета...")

    # Получаем данные из базы
    new_shops = db.get_new_shops()
    all_shops_excel = db.get_all_shops_for_excel()

    # Подготавливаем текущие магазины для отчета
    current_shops_for_excel = []
    if current_shops_data:
        for shop_data in current_shops_data:
            current_shops_for_excel.append({
                "Название магазина": shop_data.get("Название магазина", ""),
                "Адрес": shop_data.get("Адрес", ""),
                "Телефон": shop_data.get("Телефон", ""),
                "Сайт": shop_data.get("Сайт", ""),
                "Ссылка": shop_data.get("Ссылка", ""),
                "Дата сбора": shop_data.get("Дата сбора", ""),
                "Город": shop_data.get("Город", ""),
                "Источник": shop_data.get("Источник", "unknown")
            })

    # Подготавливаем новые магазины для отчета
    new_shops_for_excel = []
    for shop in new_shops:
        excel_shop = {
            "Название магазина": shop.get("Название магазина", ""),
            "Адрес": shop.get("Адрес", ""),
            "Телефон": shop.get("Телефон", ""),
            "Сайт": shop.get("Сайт", ""),
            "Ссылка": shop.get("Ссылка", ""),
            "Дата сбора": shop.get("Дата последнего обнаружения", ""),
            "Источник": shop.get("Источник", "unknown")
        }
        new_shops_for_excel.append(excel_shop)

    # Создаем отчет
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"магазины_пиротехники_{timestamp}.xlsx"

    print(f"📄 Создаем отчет: {filename}")

    excel_file = create_excel_report(
        new_shops=new_shops_for_excel,
        parsed_shops=current_shops_for_excel,
        all_shops=all_shops_excel,
        filename=filename
    )

    if excel_file:
        print(f"✅ Отчет успешно создан:")
        print(f"   📍 {os.path.abspath(excel_file)}")
    else:
        print("❌ Не удалось создать отчет")

    # 6. Выводим статистику
    print("\n" + "=" * 80)
    print("📊 СТАТИСТИКА ПАРСИНГА")
    print("=" * 80)

    final_stats = db.get_stats()

    print(f"🏪 Всего магазинов в базе: {final_stats['total_shops']}")
    print(f"🔍 Найдено в этом парсинге: {len(current_shops_data) if current_shops_data else 0}")
    print(f"🆕 Новых магазинов: {new_shops_count}")
    print(f"🔄 Обновленных магазинов: {updated_shops_count}")

    if final_stats['sources']:
        print(f"📊 По источникам:")
        print(f"   Яндекс: {final_stats['sources'].get('yandex', 0)}")
        print(f"   2GIS: {final_stats['sources'].get('2gis', 0)}")

    print(f"📅 Дата парсинга: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    asyncio.run(main())
