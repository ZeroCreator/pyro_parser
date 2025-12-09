import asyncio
import random
import re
import time
from datetime import datetime
from typing import List, Dict, Set
from bs4 import BeautifulSoup
import nodriver


class YandexPyroParser:
    """Парсер Яндекс Карт для магазинов пиротехники в Ростове-на-Дону"""

    def __init__(self, headless: bool = False):
        self.headless = headless
        self.browser = None
        self.all_urls: Set[str] = set()
        self.results: List[Dict] = []

        # Области для поиска (разные части города)
        self.search_areas = [
            {
                "name": "Весь город (общий поиск)",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.720451%2C47.232724&sll=39.720451%2C47.232724&sspn=0.672226%2C0.318267&z=11"
            },
            {
                "name": "Центр города детально",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.720451%2C47.232724&sll=39.720451%2C47.232724&sspn=0.336113%2C0.159133&z=12"
            },
            {
                "name": "Северные районы",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.720451%2C47.282724&sll=39.720451%2C47.282724&sspn=0.336113%2C0.159133&z=12"
            },
            {
                "name": "Южные районы",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.720451%2C47.182724&sll=39.720451%2C47.182724&sspn=0.336113%2C0.159133&z=12"
            },
            {
                "name": "Западные районы",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.620451%2C47.232724&sll=39.620451%2C47.232724&sspn=0.336113%2C0.159133&z=12"
            },
            {
                "name": "Восточные районы",
                "url": "https://yandex.ru/maps/39/rostov-na-donu/search/пиротехника/?ll=39.820451%2C47.232724&sll=39.820451%2C47.232724&sspn=0.336113%2C0.159133&z=12"
            }
        ]

    async def init_browser(self) -> bool:
        """Инициализация браузера"""
        try:
            print("🚀 Запуск браузера...")
            self.browser = await nodriver.start(
                headless=self.headless,
                window_size=(1300, 900),
                disable_webgl=True,
                disable_extensions=True
            )
            return True
        except Exception as e:
            print(f"❌ Ошибка инициализации браузера: {e}")
            return False

    async def close(self):
        """Закрытие браузера"""
        try:
            if self.browser:
                await self.browser.stop()
                self.browser = None
        except Exception as e:
            print(f"⚠ Предупреждение при закрытии браузера: {e}")

    async def parse(self) -> List[Dict]:
        """Основной метод парсинга"""
        print("=" * 80)
        print("🔥 ПАРСЕР МАГАЗИНОВ ПИРОТЕХНИКИ - РОСТОВ-НА-ДОНУ")
        print("=" * 80)

        self.start_time = time.time()
        self.results = []
        self.all_urls.clear()

        if not await self.init_browser():
            return []

        try:
            # 1. Парсим все области города
            print(f"\n🎯 НАЧИНАЕМ ПАРСИНГ {len(self.search_areas)} ОБЛАСТЕЙ...")

            for i, area in enumerate(self.search_areas, 1):
                print(f"\n{'=' * 60}")
                print(f"Область {i}/{len(self.search_areas)}: {area['name']}")
                print(f"{'=' * 60}")

                urls_before = len(self.all_urls)

                # Загружаем страницу поиска для этой области
                print(f"🌐 Открываем: {area['name']}")
                page = await self.browser.get(area['url'])
                await asyncio.sleep(4)

                # Скрапим эту область
                await self.smart_area_scroll(page)

                # Собираем ссылки
                await self.collect_store_links(page)

                new_urls = len(self.all_urls) - urls_before
                print(f"✅ В области найдено магазинов: {new_urls}")

                # Пауза между областями
                if i < len(self.search_areas):
                    await asyncio.sleep(random.uniform(5, 8))

            if not self.all_urls:
                print("❌ Не удалось собрать ссылки")
                return []

            print(f"\n✅ Всего собрано ссылок на магазины: {len(self.all_urls)}")

            # 2. Парсим каждый магазин
            print("\n🏪 ПАРСИМ ДАННЫЕ МАГАЗИНОВ...")
            urls_list = list(self.all_urls)

            for i, url in enumerate(urls_list, 1):
                print(f"   {i}/{len(urls_list)}: {url}")
                data = await self.parse_store_page(url)
                if data:
                    self.results.append(data)
                    print(f"      ✅ Получены данные: {data.get('Название магазина', 'Без названия')}")
                else:
                    print(f"      ⚠ Не удалось получить данные")

                # Задержка между запросами
                if i < len(urls_list):
                    await asyncio.sleep(random.uniform(3, 5))

            # 3. Удаляем дубликаты
            self.remove_duplicates()

            # 4. Выводим статистику
            self.print_statistics()

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    async def smart_area_scroll(self, page):
        """Скроллинг для конкретной области"""
        max_scrolls = 30
        no_new_count = 0
        previous_count = 0

        for scroll_num in range(1, max_scrolls + 1):
            print(f"   📍 Скролл {scroll_num}/{max_scrolls}")

            # Сохраняем текущее количество
            current_count_before = len(self.all_urls)

            # Выполняем скролл
            await self.execute_scroll_strategies(page)
            await asyncio.sleep(random.uniform(1.5, 2.5))

            # Собираем ссылки
            await self.collect_store_links(page)

            # Проверяем, появились ли новые ссылки
            new_urls = len(self.all_urls) - current_count_before

            if new_urls > 0:
                print(f"   📥 Новых магазинов: {new_urls}")
                no_new_count = 0
            else:
                no_new_count += 1
                print(f"   📭 Новых магазинов нет ({no_new_count}/3)")

                if no_new_count >= 3:
                    print("   🏁 Завершаем скроллинг этой области")
                    break

            # Если количество ссылок не меняется 3 раза подряд - выходим
            if len(self.all_urls) == previous_count:
                no_new_count += 1
            else:
                no_new_count = 0

            previous_count = len(self.all_urls)

            # Короткая пауза
            await asyncio.sleep(random.uniform(0.5, 1))

    async def execute_scroll_strategies(self, page):
        """Выполнение скролла контейнера"""
        try:
            # Скролл контейнера с результатами
            await page.evaluate("""
                (function() {
                    // Ищем контейнер с результатами
                    const containers = [
                        '.scroll__container',
                        '.scroll__container_width_narrow',
                        '.search-list-view__list-container',
                        '.sidebar-view__panel',
                        '.scrollable-container'
                    ];

                    for (const selector of containers) {
                        const container = document.querySelector(selector);
                        if (container && container.scrollHeight > container.clientHeight) {
                            container.scrollTop = container.scrollHeight;
                            console.log('Скролл контейнера: ' + selector);
                            return true;
                        }
                    }
                    return false;
                })();
            """)

        except Exception as e:
            print(f"   ⚠ Ошибка скролла: {e}")

    async def collect_store_links(self, page):
        """Сбор ссылок на магазины из списка результатов"""
        try:
            # Получаем HTML страницы
            html = await page.get_content()
            soup = BeautifulSoup(html, 'html.parser')

            # Ищем все элементы li с классом search-snippet-view
            store_items = soup.find_all('li', class_='search-snippet-view')

            urls_before = len(self.all_urls)

            for item in store_items:
                # Ищем ссылку внутри data-nosnippet
                nosnippet = item.find('span', attrs={'data-nosnippet': True})
                if nosnippet:
                    # Ищем все ссылки внутри nosnippet
                    links = nosnippet.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if self.is_store_url(href):
                            full_url = self.normalize_url(href)
                            if full_url:
                                self.all_urls.add(full_url)
                                break  # Берем первую подходящую ссылку
                else:
                    # Если нет data-nosnippet, ищем ссылки непосредственно в элементе
                    links = item.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if self.is_store_url(href):
                            full_url = self.normalize_url(href)
                            if full_url:
                                self.all_urls.add(full_url)
                                break

            new_urls = len(self.all_urls) - urls_before
            if new_urls > 0:
                print(f"   📥 Найдено {new_urls} новых магазинов")

        except Exception as e:
            print(f"❌ Ошибка сбора ссылок: {e}")

    def is_store_url(self, url: str) -> bool:
        """Проверка, является ли URL ссылкой на магазин"""
        if not url:
            return False

        # Проверяем паттерны ссылок на организации
        store_patterns = ['/org/', '/firm/', 'businessId=']

        for pattern in store_patterns:
            if pattern in url:
                return True

        return False

    def normalize_url(self, url: str) -> str:
        """Нормализация URL - оставляем только базовую ссылку на магазин"""
        if not url:
            return ""

        # Список вкладок, которые нужно обрезать
        tabs_to_remove = ['/reviews', '/photos', '/gallery', '/menu']

        # Добавляем домен если нужно
        if url.startswith('//'):
            url = f"https:{url}"
        elif url.startswith('/'):
            url = f"https://yandex.ru{url}"
        elif not url.startswith('http'):
            return ""

        # Удаляем параметры запроса и якоря
        url = url.split('?')[0].split('#')[0].strip()

        # Обрезаем вкладки (reviews, photos, gallery, menu)
        for tab in tabs_to_remove:
            if tab in url:
                # Находим позицию вкладки и обрезаем до неё
                tab_index = url.find(tab)
                if tab_index != -1:
                    url = url[:tab_index]

        # Удаляем конечные слеши
        url = url.rstrip('/')

        return url

    async def parse_store_page(self, url: str) -> Dict:
        """Парсинг страницы магазина"""
        try:
            print(f"      📖 Открываем страницу магазина...")
            page = await self.browser.get(url)
            await asyncio.sleep(random.uniform(3, 4))

            # Получаем HTML
            html = await page.get_content()

            # Парсим данные
            data = self.parse_store_data(url, html)

            return data if data else None

        except Exception as e:
            print(f"      ❌ Ошибка парсинга: {e}")
            return None

    def parse_store_data(self, url: str, html: str) -> Dict:
        """Извлечение данных о магазине из HTML"""
        soup = BeautifulSoup(html, 'html.parser')

        data = {
            'Ссылка': url,
            'Город': 'Ростов-на-Дону',
            'Дата сбора': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 1. Название магазина
        title_selectors = [
            'h1.orgpage-header-view__header',
            'h1.business-title-view__title',
            'h1.card-title-view__title',
            'h1[itemprop="name"]',
            '.orgpage-header-view__header',
            '.business-title-view__title',
            '.card-title-view__title'
        ]

        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if text and len(text) > 2:
                    data['Название магазина'] = text
                    break

        # 2. Адрес
        address_selectors = [
            '[itemprop="address"]',
            '.business-contacts-view__address',
            '.card-address-view__address',
            '.orgpage-address-view__address-text',
            '.business-address-view__address',
            'address',
            '.location__description'
        ]

        for selector in address_selectors:
            elem = soup.select_one(selector)
            if elem:
                text = elem.get_text(' ', strip=True)
                if text and len(text) > 5:
                    data['Адрес'] = text
                    break

        # ПРОВЕРКА: Является ли магазин из Ростова-на-Дону
        address = data.get('Адрес', '')
        if address:
            # Приводим адрес к нижнему регистру для проверки
            address_lower = address.lower()

            # Проверяем наличие упоминаний Ростова-на-Дону
            rostov_patterns = [
                'ростов-на-дону',
                'ростов на дону',
                'ростов-на-дону,',
                'г.ростов-на-дону',
                'г. ростов-на-дону',
                'г. ростов',
                'г.ростов',
                'ростов,'
            ]

            # Флаг, что магазин из Ростова
            is_rostov = False

            for pattern in rostov_patterns:
                if pattern in address_lower:
                    is_rostov = True
                    break

            # Если магазин не из Ростова - пропускаем его
            if not is_rostov:
                print(f"      🚫 Пропускаем магазин (не из Ростова-на-Дону): {address}")
                return None
        else:
            # Если адрес не найден, но нам нужна фильтрация по городу - пропускаем
            print(f"      ⚠ Адрес не найден, пропускаем магазин")
            return None

        # 3. Телефон - ищем по классу orgpage-phones-view__phone-number
        phones = []

        # Основной поиск по указанному классу
        phone_elements = soup.find_all(class_='orgpage-phones-view__phone-number')
        for elem in phone_elements:
            phone_text = elem.get_text(strip=True)
            if phone_text:
                # Очищаем номер телефона
                clean_phone = re.sub(r'[^\d\+]', '', phone_text)
                if clean_phone and len(clean_phone) >= 10 and clean_phone not in phones:
                    phones.append(clean_phone)

        # Альтернативный поиск, если основной не сработал
        if not phones:
            # Ищем ссылки с tel:
            for link in soup.find_all('a', href=lambda x: x and x.startswith('tel:')):
                phone = link['href'].replace('tel:', '').strip()
                if phone:
                    clean_phone = re.sub(r'[^\d\+]', '', phone)
                    if clean_phone and clean_phone not in phones:
                        phones.append(clean_phone)

            # Ищем в тексте с помощью регулярных выражений
            text = soup.get_text()
            phone_patterns = [
                r'8\s?[\(\-]?\d{3}[\)\-]?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'\+7\s?[\(\-]?\d{3}[\)\-]?\s?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}',
                r'\(\d{3,4}\)\s?\d{2,3}[\s\-]\d{2}[\s\-]\d{2}'
            ]

            for pattern in phone_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    clean_phone = re.sub(r'[^\d\+]', '', match)
                    if clean_phone and len(clean_phone) >= 10 and clean_phone not in phones:
                        phones.append(clean_phone)

        if phones:
            data['Телефон'] = ', '.join(phones[:3])  # Берем не более 3 номеров

        # 4. Сайт - ищем по классу business-urls-view__text
        site_found = False

        # Основной поиск по указанному классу
        site_elements = soup.find_all(class_='business-urls-view__text')
        for elem in site_elements:
            # Проверяем, есть ли href у элемента или у родительского <a>
            if elem.name == 'a' and elem.get('href'):
                href = elem['href']
            else:
                # Ищем ссылку внутри элемента
                link = elem.find('a')
                if link and link.get('href'):
                    href = link['href']
                else:
                    continue

            if href:
                clean_url = self.clean_website_url(href)
                if clean_url and not self.is_yandex_url(clean_url):
                    data['Сайт'] = clean_url
                    site_found = True
                    break

        # Альтернативный поиск сайта
        if not site_found:
            site_selectors = [
                '.business-urls-view__link',
                '.card-website-view__link',
                '.orgpage-url-view__url',
                '.website-link'
            ]

            for selector in site_selectors:
                elem = soup.select_one(selector)
                if elem and elem.get('href'):
                    href = elem['href']
                    clean_url = self.clean_website_url(href)
                    if clean_url and not self.is_yandex_url(clean_url):
                        data['Сайт'] = clean_url
                        break

        # Проверяем, что собраны ключевые данные
        # Если нет названия, но есть адрес - используем часть адреса как название
        if not data.get('Название магазина') and data.get('Адрес'):
            address_parts = data['Адрес'].split(',')
            if address_parts:
                data['Название магазина'] = address_parts[0].strip()

        # Если все еще нет названия и нет адреса - пропускаем
        if not data.get('Название магазина') and not data.get('Адрес'):
            return None

        return data

    def is_yandex_url(self, url: str) -> bool:
        """Проверка, является ли URL ссылкой на Яндекс"""
        if not url:
            return False

        yandex_domains = ['yandex.ru', 'yandex.com', 'ya.ru', 'yandex.net']
        url_lower = url.lower()

        for domain in yandex_domains:
            if domain in url_lower:
                return True

        return False

    def clean_website_url(self, url: str) -> str:
        """Очистка URL сайта"""
        if not url:
            return ""

        # Очищаем URL
        url = url.split('?')[0].split('#')[0].strip()

        # Добавляем протокол если нужно
        if url.startswith('//'):
            url = f"https:{url}"
        elif not url.startswith('http'):
            url = f"https://{url}"

        return url

    def remove_duplicates(self):
        """Удаление дубликатов"""
        if not self.results:
            return

        unique_results = []
        seen = set()

        for item in self.results:
            # Создаем ключ на основе нормализованной ссылки
            url = item.get('Ссылка', '').lower().strip()

            if url:
                # Извлекаем уникальный ID из URL
                # Паттерн для поиска ID: /org/название/ID/
                match = re.search(r'/(\d+)(?:/|$)', url)
                if match:
                    unique_id = match.group(1)
                    if unique_id not in seen:
                        seen.add(unique_id)
                        unique_results.append(item)
                elif url not in seen:
                    seen.add(url)
                    unique_results.append(item)
            else:
                # Если нет URL, используем название и адрес
                name = item.get('Название магазина', '').lower().strip()
                address = item.get('Адрес', '').lower().strip()

                if name and address:
                    key = f"{name}|{address}"
                    if key not in seen:
                        seen.add(key)
                        unique_results.append(item)
                elif name:
                    if name not in seen:
                        seen.add(name)
                        unique_results.append(item)
                elif address:
                    if address not in seen:
                        seen.add(address)
                        unique_results.append(item)
                else:
                    unique_results.append(item)

        removed = len(self.results) - len(unique_results)
        if removed > 0:
            print(f"🗑 Удалено дубликатов: {removed}")

        self.results = unique_results

    def print_statistics(self):
        """Вывод статистики"""
        print("\n" + "=" * 80)
        print("📊 СТАТИСТИКА СБОРА")
        print("=" * 80)

        elapsed_time = time.time() - self.start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(f"Время выполнения: {minutes} мин {seconds} сек")
        print(f"Всего собрано магазинов: {len(self.results)}")

        # Статистика по данным
        phones_count = sum(1 for r in self.results if r.get('Телефон'))
        sites_count = sum(1 for r in self.results if r.get('Сайт'))

        print(f"📞 Магазинов с телефоном: {phones_count}")
        print(f"🌐 Магазинов с сайтом: {sites_count}")
