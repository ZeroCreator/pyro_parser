import json
from datetime import datetime


def check_database():
    """Проверяем состояние базы данных"""
    try:
        with open("data/database.json", 'r', encoding='utf-8') as f:
            db = json.load(f)

        print("=" * 60)
        print("📊 ПРОВЕРКА БАЗЫ ДАННЫХ")
        print("=" * 60)

        total = db.get("total_shops", 0)
        shops = db.get("shops", [])

        print(f"Всего магазинов: {total}")
        print(f"Последнее обновление: {db.get('last_update')}")

        # Магазины, не найденные в последнем парсинге
        missing = [s for s in shops if not s.get("обнаружен_в_последнем_парсинге", False)]

        if missing:
            print(f"\n⚠️  Магазины, не найденные в последнем парсинге: {len(missing)}")
            for i, shop in enumerate(missing[:3], 1):
                name = shop.get('Название магазина', 'Без названия')[:30]
                last_seen = shop.get('Дата последнего обнаружения', 'неизвестно')
                print(f"   {i}. {name} (последний раз: {last_seen})")

            if len(missing) > 3:
                print(f"      ... и еще {len(missing) - 3}")

        # Последние добавленные магазины
        new_shops = [s for s in shops
                     if s.get("Дата добавления") == s.get("Дата последнего обнаружения")]

        if new_shops:
            print(f"\n🆕 Последние добавленные магазины: {len(new_shops)}")
            for i, shop in enumerate(new_shops[:3], 1):
                name = shop.get('Название магазина', 'Без названия')[:30]
                added = shop.get('Дата добавления', 'неизвестно')
                print(f"   {i}. {name} (добавлен: {added})")

    except FileNotFoundError:
        print("❌ База данных не найдена")
    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    check_database()
