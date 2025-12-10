# main.py
import asyncio
import json
import os
import re
from datetime import datetime
from typing import List, Dict, Set
from parser import YandexPyroParser
from core.excel_writer import create_excel_report


class PyroDatabase:
    """Простая JSON база данных для магазинов"""

    def __init__(self, db_file="data/database.json"):
        self.db_file = db_file
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        self.db = self._load_db()

    def _load_db(self) -> Dict:
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
        """Извлекаем уникальный ID магазина"""
        if not url:
            return ""

        patterns = [
            r'/org/[^/]+/(\d+)',
            r'businessId=(\d+)',
            r'/(\d+)/details',
            r'/firm/(\d+)/',
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return f"yandex_{match.group(1)}"

        # Резервный вариант - хеш
        return f"hash_{hash(url) & 0xFFFFFFFF:08x}"

    def find_shop_by_id(self, shop_id: str) -> Dict:
        """Находим магазин по ID"""
        for shop in self.db.get("shops", []):
            if shop.get("id") == shop_id:
                return shop
        return None

    def find_shop_by_url(self, url: str) -> Dict:
        """Находим магазин по URL"""
        shop_id = self.extract_id(url)
        return self.find_shop_by_id(shop_id)

    def add_or_update_shop(self, shop_data: Dict) -> tuple:
        """
        Добавляем новый магазин или обновляем существующий

        Возвращает: (shop, is_new)
        """
        url = shop_data.get("Ссылка", "")
        shop_id = self.extract_id(url)

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
                "обнаружен_в_последнем_парсинге": True
            }

            self.db["shops"].append(new_shop)
            self.db["total_shops"] += 1
            return new_shop, True

    def mark_all_unfound(self):
        """Помечаем все магазины как не найденные в текущем парсинге"""
        for shop in self.db.get("shops", []):
            shop["обнаружен_в_последнем_парсинге"] = False

    def get_new_shops(self) -> List[Dict]:
        """Получаем магазины, добавленные в последнем парсинге"""
        new_shops = []
        for shop in self.db.get("shops", []):
            # Магазин считается новым, если дата добавления = дате последнего обновления
            if shop.get("Дата добавления") == shop.get("Дата последнего обнаружения"):
                new_shops.append(shop)
        return new_shops

    def get_stats(self) -> Dict:
        """Статистика базы"""
        total = self.db.get("total_shops", 0)
        found_in_last = sum(1 for s in self.db.get("shops", [])
                            if s.get("обнаружен_в_последнем_парсинге", False))

        return {
            "total_shops": total,
            "found_in_last_parse": found_in_last,
            "missing_in_last_parse": total - found_in_last,
            "last_update": self.db.get("last_update")
        }


async def main():
    """Основная функция парсинга с базой данных"""
    print("=" * 80)
    print("🎆 ПАРСЕР МАГАЗИНОВ ПИРОТЕХНИКИ - YANDEX MAPS")
    print("=" * 80)

    # 1. Инициализируем базу
    print("\n📂 Загружаем базу данных...")
    db = PyroDatabase()
    stats = db.get_stats()
    print(f"   Всего магазинов в базе: {stats['total_shops']}")
    print(f"   Последнее обновление: {stats['last_update']}")

    # 2. Парсим текущие данные
    print("\n🔍 Начинаем парсинг Яндекс Карт...")
    parser = YandexPyroParser(headless=False)  # False для отладки
    current_shops = await parser.parse()

    if not current_shops:
        print("❌ Не удалось получить данные")
        return

    print(f"✅ Найдено магазинов в текущем парсинге: {len(current_shops)}")

    # 3. Обновляем базу данных
    print("\n💾 Обновляем базу данных...")

    # Помечаем все магазины как не найденные
    db.mark_all_unfound()

    # Добавляем/обновляем магазины
    new_shops_count = 0
    updated_shops_count = 0

    for shop_data in current_shops:
        shop, is_new = db.add_or_update_shop(shop_data)
        if is_new:
            new_shops_count += 1
        else:
            updated_shops_count += 1

    # Обновляем метаданные базы
    db.db["last_update"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db.save_db()

    print(f"   Новых магазинов: {new_shops_count}")
    print(f"   Обновленных магазинов: {updated_shops_count}")

    # 4. Получаем новые магазины для отчета
    new_shops = db.get_new_shops()

    # 5. Создаем отчеты
    print("\n📊 Создаем отчеты...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    # Отчет 1: Новые магазины (всегда создаем)
    if new_shops:
        # Подготавливаем данные для Excel
        excel_data = []
        for shop in new_shops:
            excel_data.append({
                'Название магазина': shop.get('Название магазина', ''),
                'Адрес': shop.get('Адрес', ''),
                'Телефон': shop.get('Телефон', ''),
                'Сайт': shop.get('Сайт', ''),
                'Ссылка': shop.get('Ссылка', ''),
                'Дата добавления': shop.get('Дата добавления', '')
            })

        excel_file = create_excel_report(
            data=excel_data,
            filename=f"новые_магазины_{timestamp}.xlsx"
        )

        print(f"✅ Отчет с новыми магазинами создан:")
        print(f"   📄 {excel_file}")
    else:
        # Создаем пустой отчет
        empty_data = [{
            'Название магазина': 'Новых магазинов не обнаружено',
            'Адрес': '',
            'Телефон': '',
            'Сайт': '',
            'Ссылка': '',
            'Дата добавления': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }]

        excel_file = create_excel_report(
            data=empty_data,
            filename=f"новые_магазины_{timestamp}.xlsx"
        )
        print(f"✅ Отчет создан: {excel_file}")
        print("   ℹ️  Новых магазинов не обнаружено")

    # Отчет 2: Полная выгрузка всех магазинов (опционально)
    print("\n📋 Создаем полный отчет...")
    all_shops_data = []
    for shop in db.db.get("shops", []):
        all_shops_data.append({
            'Название магазина': shop.get('Название магазина', ''),
            'Адрес': shop.get('Адрес', ''),
            'Телефон': shop.get('Телефон', ''),
            'Сайт': shop.get('Сайт', ''),
            'Ссылка': shop.get('Ссылка', ''),
            'Дата добавления': shop.get('Дата добавления', ''),
            'Дата последнего обнаружения': shop.get('Дата последнего обнаружения', ''),
            'В последнем парсинге': 'Да' if shop.get('обнаружен_в_последнем_парсинге') else 'Нет'
        })

    all_excel_file = create_excel_report(
        data=all_shops_data,
        filename=f"все_магазины_{timestamp}.xlsx"
    )
    print(f"✅ Полный отчет создан:")
    print(f"   📄 {all_excel_file}")

    # 6. Выводим статистику
    print("\n" + "=" * 80)
    print("📊 СТАТИСТИКА ПАРСИНГА")
    print("=" * 80)

    final_stats = db.get_stats()

    print(f"🏪 Всего магазинов в базе: {final_stats['total_shops']}")
    print(f"🆕 Новых магазинов в этом парсинге: {new_shops_count}")
    print(f"🔄 Обновленных магазинов: {updated_shops_count}")
    print(f"📅 Дата парсинга: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"📁 База данных: data/database.json")

    if new_shops_count > 0:
        print("\n🎉 Обнаружены новые магазины:")
        for i, shop in enumerate(new_shops, 1):
            name = shop.get('Название магазина', 'Без названия')
            address = shop.get('Адрес', '')
            print(f"   {i}. {name}")
            print(f"      📍 {address}")


if __name__ == "__main__":
    asyncio.run(main())
