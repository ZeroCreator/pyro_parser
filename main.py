import asyncio
import os
from datetime import datetime
from parser import YandexPyroParser
from core import create_excel_report


async def main():
    """Основная функция запуска"""
    print("=" * 80)
    print("🎆 ПАРСЕР МАГАЗИНОВ ПИРОТЕХНИКИ - YANDEX MAPS")
    print("=" * 80)

    try:
        # Создаем парсер (headless=False для отладки, True для продакшн)
        parser = YandexPyroParser(headless=False)

        # Запускаем парсинг
        results = await parser.parse()

        # Сохраняем результаты
        if results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Сохраняем в Excel
            excel_filename = f"пиротехника_ростов_{timestamp}.xlsx"
            excel_file = create_excel_report(results, excel_filename)

            if excel_file:
                print(f"\n✅ Парсинг успешно завершен!")
                print(f"📊 Найдено магазинов: {len(results)}")
                print(f"📁 Excel файл: {excel_file}")

                # Выводим полный путь
                abs_excel_path = os.path.abspath(excel_file)
                print(f"📍 Полный путь: {abs_excel_path}")
            else:
                print("❌ Не удалось создать Excel файл")
        else:
            print("❌ Не удалось собрать данные")

    except Exception as e:
        print(f"\n❌ Ошибка в основном потоке: {e}")
        import traceback
        traceback.print_exc()


def run():
    """Функция для запуска из командной строки"""
    asyncio.run(main())


if __name__ == "__main__":
    run()
