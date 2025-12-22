import asyncio
import random
import re
import time
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import nodriver


class TwoGisPyroParser:
    """Парсер 2ГИС для магазинов пиротехники в Ростове-на-Дону."""

    # Константы для конфигурации
    BROWSER_CONFIG = {
        "headless": True,
        "window_size": (1300, 900),
        "disable_webgl": True,
        "disable_extensions": True
    }

    # SVG пути для поиска элементов
    SVG_PATHS = {
        "address": "M5 11v2a6.82 6.82 0 0 1 4.17 1.41C10.75 15.62 11.53 18 11.5 22h1c0-4 .75-6.38 2.33-7.59A6.82 6.82 0 0 1 19 13v-2a7 7 0 0 0-7-7 7 7 0 0 0-7 7z",
        "phone": "M14 14l-1.08 1.45a13.61 13.61 0 0 1-4.37-4.37L10 10a18.47 18.47 0 0 0-.95-5.85L9 4H5.06a1 1 0 0 0-1 1.09 16 16 0 0 0 14.85 14.85 1 1 0 0 0 1.09-1V15h-.15A18.47 18.47 0 0 0 14 14z",
        "website": "M12 4a8 8 0 1 0 8 8 8 8 0 0 0-8-8zm5 9h-6l1-7h1v5.25l4 .75z"
    }

    # Селекторы для поиска элементов
    SELECTORS = {
        "search_results": [
            '.searchResults',
            '.listContainer',
            '.searchResults__list',
            '.searchResults__container',
            '[data-qa="search-results"]',
            '.searchTab__content'
        ],
        "scroll_containers": [
            '.searchResults__list',
            '.listContainer',
            '.searchResults__container',
            '.scroll__container',
            '[data-scroll]'
        ]
    }

    # Паттерны для валидации и фильтрации
    PATTERNS = {
        "phone": [
            r'\+7\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'8\s?\(?\d{3}\)?\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}',
            r'\(\d{3}\)\s?\d{3}[\s-]?\d{2}[\s-]?\d{2}'
        ],
        "domain": [
            r'^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}$',
            r'^[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}\.[a-zA-Z]{2,}$',
            r'^www\.[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}$',
            r'^https?://[a-zA-Z0-9][a-zA-Z0-9-]{0,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}',
            r'^[а-яА-ЯёЁ0-9][а-яА-ЯёЁ0-9-]{0,61}[а-яА-ЯёЁ0-9]\.(ru|рф|su|com|net|org)'
        ],
        "address": [
            r'Ростов-на-Дону[,\s]+[А-Яа-яёЁ0-9\s\-\.]+ул\.[\s]*[А-Яа-яёЁ\-]+[\s]*[,\s]*д\.[\s]*\d+',
            r'г\.\s*Ростов-на-Дону[,\s]+[А-Яа-яёЁ0-9\s\-\.]+',
            r'ул\.\s*[А-Яа-яёЁ\-]+[\s]*[,\s]*д\.\s*\d+[\s]*[,\s]*г\.\s*Ростов-на-Дону'
        ]
    }

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser = None
        self.all_urls: Set[str] = set()
        self.results: List[Dict] = []
        self.start_time = None
        self.processed_ids: Set[str] = set()

        self.search_areas = [
            {
                "name": "Весь город (общий поиск)",
                "url": "https://2gis.ru/rostov-on-don/search/пиротехника"
            }
        ]

    @property
    def source_name(self) -> str:
        return "2gis"

    async def init_browser(self) -> bool:
        """Инициализация браузера"""
        try:
            print("🚀 Запуск браузера 2GIS...")
            config = self.BROWSER_CONFIG.copy()
            config["headless"] = self.headless
            self.browser = await nodriver.start(**config)
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

    async def parse(self) -> List[Dict[str, Any]]:
        """Основной метод парсинга"""
        self._print_header()

        self.start_time = time.time()
        self.results = []
        self.all_urls.clear()
        self.processed_ids.clear()

        if not await self.init_browser():
            return []

        try:
            await self._parse_search_areas()

            if not self.all_urls:
                print("❌ Не удалось собрать ссылки на магазины")
                return []

            urls_list = list(self.all_urls)
            await self._parse_store_pages(urls_list)

            self._remove_duplicates()
            self._print_final_stats()

            return self.results

        except Exception as e:
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
            return self.results
        finally:
            await self.close()

    # ========== ПАРСИНГ ОБЛАСТЕЙ ПОИСКА ==========

    async def _parse_search_areas(self):
        """Парсинг всех областей поиска"""
        print(f"\n🎯 НАЧИНАЕМ ПАРСИНГ {len(self.search_areas)} ОБЛАСТЕЙ")
        print("-" * 50)

        for i, area in enumerate(self.search_areas, 1):
            urls_before = len(self.all_urls)

            print(f"\n📍 Область {i}/{len(self.search_areas)}: {area['name']}")
            print(f"   URL: {area['url']}")

            await self._collect_urls_from_area(area['url'], area['name'])

            new_urls = len(self.all_urls) - urls_before
            print(f"✅ В области найдено магазинов: {new_urls}")
            print(f"📊 Всего собрано ссылок: {len(self.all_urls)}")

            if i < len(self.search_areas):
                await asyncio.sleep(random.uniform(3, 5))

    async def _collect_urls_from_area(self, area_url: str, area_name: str) -> bool:
        """Сбор URL магазинов из конкретной области"""
        try:
            print(f"   🔍 Начинаем сбор ссылок в области: {area_name}")

            tab = await self.browser.get(area_url)
            await asyncio.sleep(random.uniform(4, 6))

            await self._click_search_results_if_needed(tab)

            print("   📥 Собираем ссылки с первой страницы...")
            initial_urls = await self._get_urls_from_current_page(tab)
            if initial_urls:
                self.all_urls.update(initial_urls)
                print(f"   📊 Первая страница: {len(initial_urls)} URL")

            print("   📜 Начинаем прокрутку страницы...")
            await self._scroll_page_to_bottom(tab)

            current_urls = await self._get_urls_from_current_page(tab)
            if current_urls:
                previous_count = len(self.all_urls)
                self.all_urls.update(current_urls)
                new_urls = len(self.all_urls) - previous_count
                print(f"   📎 Всего URL после прокрутки: {len(self.all_urls)} (+{new_urls} новых)")

            print("   🔍 Пробуем найти кнопку пагинации после прокрутки...")
            await self._try_find_pagination_after_scroll(tab, current_page=1)

            print(f"   ✅ Сбор ссылок в области {area_name} завершен")
            return True

        except Exception as e:
            print(f"   ❌ Ошибка сбора в области {area_name}: {str(e)[:100]}")
            return False

    async def _click_search_results_if_needed(self, tab):
        """Кликает по результатам поиска, если они есть"""
        try:
            await asyncio.sleep(2)

            for selector in self.SELECTORS["search_results"]:
                element = await tab.query_selector(selector)
                if element:
                    print("   🖱 Найден контейнер результатов, кликаем...")
                    await element.click()
                    await asyncio.sleep(2)
                    break

            first_card = await tab.query_selector('.minicard')
            if first_card:
                await first_card.click()
                await asyncio.sleep(1)

        except Exception as e:
            print(f"   ⚠ Не удалось кликнуть по результатам: {str(e)[:50]}")

    async def _scroll_page_to_bottom(self, tab):
        """Прокручивает ВСЕ скроллируемые контейнеры"""
        print("   📜 СКРОЛЛИМ ВСЕ КОНТЕЙНЕРЫ...")

        try:
            await self._scroll_main_containers(tab)
            await self._scroll_browser_window(tab)
            await self._scroll_all_scrollable_containers(tab)
            await asyncio.sleep(random.uniform(2, 3))

        except Exception as e:
            print(f"   ❌ Ошибка скроллинга: {str(e)[:100]}")

    async def _scroll_main_containers(self, tab):
        """Прокручивает основные контейнеры"""
        for selector in self.SELECTORS["scroll_containers"]:
            await tab.evaluate(f"""
                (function() {{
                    const container = document.querySelector('{selector}');
                    if (container && container.scrollHeight > container.clientHeight) {{
                        container.scrollTop = container.scrollHeight;
                        return true;
                    }}
                    return false;
                }})()
            """)
        await asyncio.sleep(random.uniform(1, 2))

    async def _scroll_browser_window(self, tab):
        """Прокручивает окно браузера"""
        await tab.evaluate("""
            window.scrollBy({
                top: 800,
                behavior: 'smooth'
            });
        """)
        await asyncio.sleep(random.uniform(1, 2))

    async def _scroll_all_scrollable_containers(self, tab):
        """Прокручивает все скроллируемые контейнеры"""
        container_count = await tab.evaluate("""
            document.querySelectorAll('[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]').length
        """)

        for i in range(container_count):
            await tab.evaluate(f"""
                (function() {{
                    const containers = document.querySelectorAll(
                        '[data-scroll], [tabindex], [overflow="auto"], [overflow="scroll"]'
                    );
                    if (containers[{i}]) {{
                        const container = containers[{i}];
                        if (container.scrollHeight > container.clientHeight) {{
                            container.scrollTop = container.scrollHeight;
                        }}
                    }}
                }})()
            """)
            await asyncio.sleep(0.3)

    async def _try_find_pagination_after_scroll(self, tab, current_page: int = 1):
        """Попытка найти кнопки пагинации после прокрутки"""
        try:
            html = await tab.get_content()
            soup = BeautifulSoup(html, 'lxml')

            next_page_num = current_page + 1
            all_page_links = self._extract_page_links(soup)

            for href in all_page_links:
                page_num = self._extract_page_number(href)
                if page_num == next_page_num:
                    page_url = self._build_full_url(href)
                    print(f"   🖱 Нашли ссылку на страницу {next_page_num}")
                    print(f"   📍 Переходим: {page_url}")

                    await tab.get(page_url)
                    await asyncio.sleep(random.uniform(4, 6))

                    await self._scroll_page_to_bottom(tab)

                    urls_page = await self._get_urls_from_current_page(tab)
                    if urls_page:
                        before = len(self.all_urls)
                        self.all_urls.update(urls_page)
                        new_count = len(self.all_urls) - before
                        print(f"   📊 +{new_count} новых URL")

                    await self._try_find_pagination_after_scroll(tab, next_page_num)
                    return

            print(f"   ⚠ Не найдено ссылок на странице {next_page_num}")

        except Exception as e:
            print(f"   ❌ Ошибка пагинации: {str(e)[:60]}")

    def _extract_page_links(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает все ссылки на страницы из HTML"""
        page_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '/page/' in href or 'page=' in href:
                page_links.append(href)
        return page_links

    def _extract_page_number(self, href: str) -> Optional[int]:
        """Извлекает номер страницы из ссылки"""
        page_match = re.search(r'/page/(\d+)', href) or re.search(r'page=(\d+)', href)
        return int(page_match.group(1)) if page_match else None

    def _build_full_url(self, href: str) -> str:
        """Строит полный URL из относительного пути"""
        if href.startswith('/'):
            return f"https://2gis.ru{href}"
        elif href.startswith('//'):
            return f"https:{href}"
        elif not href.startswith('http'):
            return f"https://2gis.ru{href}"
        return href

    # ========== ОБРАБОТКА URL ==========

    async def _get_urls_from_current_page(self, tab) -> Set[str]:
        """Получение URL магазинов с текущей страницы"""
        try:
            await asyncio.sleep(1)
            html = await tab.get_content()
            urls = self._extract_urls_from_html(html)
            return self._filter_valid_urls(urls)

        except Exception as e:
            print(f"   ❌ Ошибка при извлечении URL: {str(e)[:50]}")
            return set()

    def _extract_urls_from_html(self, html: str) -> List[str]:
        """Извлечение URL магазинов из HTML страницы поиска"""
        soup = BeautifulSoup(html, 'lxml')
        urls = []

        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '/firm/' in href:
                full_url = self._normalize_href(href)
                clean_url = self._clean_url(full_url)
                if clean_url and clean_url not in urls:
                    urls.append(clean_url)

        return list(set(urls))

    def _normalize_href(self, href: str) -> str:
        """Нормализует относительные ссылки в полные URL"""
        if href.startswith('//'):
            return f"https:{href}"
        elif href.startswith('/'):
            return f"https://2gis.ru{href}"
        elif href.startswith('http'):
            return href
        return ""

    def _filter_valid_urls(self, urls: List[str]) -> Set[str]:
        """Фильтрация валидных URL магазинов"""
        filtered_urls = set()
        for url in urls:
            if self._is_valid_store_url(url):
                clean_url = self._clean_url(url)
                if clean_url:
                    filtered_urls.add(clean_url)
        return filtered_urls

    def _is_valid_store_url(self, url: str) -> bool:
        """Проверка валидности URL магазина"""
        if '/firm/' not in url:
            return False

        exclude_patterns = [
            '/reviews', '/gallery', '/photos', '/menu', '/contacts',
            '/search/', 'tab=', '#', 'reviewTab', 'photoTab'
        ]

        return not any(pattern in url for pattern in exclude_patterns)

    def _clean_url(self, url: str) -> str:
        """Очистка URL магазина"""
        url = url.split('?')[0].split('#')[0].rstrip('/')
        return self._normalize_href(url)

    # ========== ПАРСИНГ СТРАНИЦ МАГАЗИНОВ ==========

    async def _parse_store_pages(self, urls_list: List[str]):
        """Парсинг всех собранных страниц магазинов"""
        print(f"\n🏪 Начинаем парсинг {len(urls_list)} магазинов...")

        for i, url in enumerate(urls_list, 1):
            await self._parse_single_store(i, url, len(urls_list))

    async def _parse_single_store(self, index: int, url: str, total: int):
        """Парсинг одного магазина"""
        print(f"\n   {index}/{total}: {url[:80]}...")

        store_id = self._extract_store_id(url)
        print(f"   ID: {store_id}")

        data = await self._parse_store_page(url)
        if data:
            data['ID'] = store_id

            if self._is_rostov_store(data):
                self.results.append(data)
                self._print_store_info(data)
                print(f"      ✅ СОХРАНЯЕМ")
            else:
                print(f"      🚫 ПРОПУСКАЕМ (не из Ростова-на-Дону)")
        else:
            print(f"      ⚠ Не удалось получить данные")

        if index < total:
            await asyncio.sleep(random.uniform(2, 4))

    def _extract_store_id(self, url: str) -> str:
        """Извлечение ID магазина из URL"""
        match = re.search(r'/firm/(\d+)', url)
        return f"2gis_{match.group(1)}" if match else ""

    async def _parse_store_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Парсинг одной страницы магазина"""
        try:
            print(f"      📖 Открываем страницу магазина...")
            page = await self.browser.get(url)
            await asyncio.sleep(random.uniform(3, 5))
            html = await page.get_content()
            soup = BeautifulSoup(html, 'html.parser')
            return self._extract_store_data(url, soup, html)
        except Exception as e:
            print(f"      ❌ Ошибка парсинга: {e}")
            return None

    def _extract_store_data(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """Извлечение данных магазина"""
        data = {
            'Ссылка': url,
            'Город': 'Ростов-на-Дону',
            'Дата сбора': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Источник': '2GIS'
        }

        data.update(self._extract_store_details(soup))
        return data

    def _extract_store_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Извлечение деталей магазина"""
        details = {
            'Адрес': self._find_address(soup),
            'Телефон': self._find_phone(soup),
            'Сайт': self._find_website(soup),
            'Время работы': self._find_opening_hours(soup),
            'Название магазина': self._find_store_name(soup)
        }

        return details

    def _find_address(self, soup: BeautifulSoup) -> str:
        """Поиск адреса магазина"""
        address = self._find_text_near_svg(soup, self.SVG_PATHS["address"])
        return address or self._find_address_alternative(soup)

    def _find_phone(self, soup: BeautifulSoup) -> str:
        """Поиск телефона магазина"""
        phones = self._find_phones_near_svg(soup, self.SVG_PATHS["phone"])
        return ', '.join(phones) if phones else self._find_phones_alternative(soup)

    def _find_website(self, soup: BeautifulSoup) -> str:
        """Поиск сайта магазина"""
        websites = self._find_websites_by_svg(soup, self.SVG_PATHS["website"])
        websites = self._clean_phone_from_website_list(websites)
        return ', '.join(websites[:2]) if websites else ""

    def _find_store_name(self, soup: BeautifulSoup) -> str:
        """Поиск названия магазина"""
        return self._find_store_name_smart(soup)

    def _find_opening_hours(self, soup: BeautifulSoup) -> str:
        """Поиск времени работы"""
        return self._find_opening_hours_smart(soup)

    # ========== МЕТОДЫ ПОИСКА ПО SVG ==========

    def _find_text_near_svg(self, soup: BeautifulSoup, svg_path: str) -> str:
        """Найти текст рядом с SVG-иконкой по пути d"""
        try:
            svg_path_normalized = re.sub(r'\s+', '', svg_path)

            for path in soup.find_all('path'):
                if path.get('d'):
                    path_d_normalized = re.sub(r'\s+', '', path.get('d'))
                    if self._svg_paths_match(path_d_normalized, svg_path_normalized):
                        address_text = self._search_address_in_svg_container(path)
                        if address_text:
                            return self._clean_address(address_text)

        except Exception as e:
            print(f"      ⚠ Ошибка поиска по SVG: {e}")

        return ""

    def _svg_paths_match(self, path1: str, path2: str) -> bool:
        """Проверяет соответствие SVG путей"""
        return (path1[:50] == path2[:50] or path1[:100] == path2[:100])

    def _search_address_in_svg_container(self, path_element) -> str:
        """Поиск адреса в контейнере SVG"""
        parent = path_element
        for _ in range(4):
            parent = parent.parent
            if parent:
                address_text = self._extract_address_from_container(parent)
                if address_text:
                    return address_text
        return ""

    def _extract_address_from_container(self, container) -> str:
        """Извлечь адрес из контейнера"""
        address_parts = []

        for elem in container.find_all(['div', 'span', 'p']):
            text = elem.get_text(' ', strip=True)
            if text and len(text) > 3 and self._looks_like_address(text):
                address_parts.append(text)

        if address_parts:
            full_address = ', '.join(list(dict.fromkeys(address_parts)))
            if self._is_valid_address(full_address):
                return full_address

        container_text = container.get_text('\n', strip=True)
        return self._extract_address_from_text_lines(container_text)

    def _looks_like_address(self, text: str) -> bool:
        """Проверяет, похож ли текст на адрес"""
        address_markers = ['ул.', 'улица', 'пр.', 'проспект', 'д.', 'дом', 'ростов']
        return (any(marker in text.lower() for marker in address_markers) or
                text[0].isupper())

    def _is_valid_address(self, address: str) -> bool:
        """Проверяет валидность адреса"""
        return len(address) > 15 and any(char.isdigit() for char in address)

    def _extract_address_from_text_lines(self, text: str) -> str:
        """Извлекает адрес из строк текста"""
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        address_lines = []

        for line in lines:
            line_lower = line.lower()
            if (any(marker in line_lower for marker in ['ул.', 'улица', 'пр.', 'проспект', 'д.', 'дом', 'ростов']) and
                    len(line) > 10 and not line.startswith('©')):
                address_lines.append(line)

        return ' '.join(address_lines) if address_lines else ""

    def _find_phones_near_svg(self, soup: BeautifulSoup, svg_path: str) -> List[str]:
        """Найти телефоны рядом с SVG-иконкой телефона"""
        phones = []
        try:
            svg_path_normalized = re.sub(r'\s+', '', svg_path)

            for path in soup.find_all('path'):
                if path.get('d'):
                    path_d_normalized = re.sub(r'\s+', '', path.get('d'))
                    if path_d_normalized[:50] == svg_path_normalized[:50]:
                        phones = self._extract_phones_from_svg_container(path)
                        break

        except Exception as e:
            print(f"      ⚠ Ошибка поиска телефонов: {e}")

        return phones

    def _extract_phones_from_svg_container(self, path_element) -> List[str]:
        """Извлечение телефонов из контейнера SVG"""
        phones = []
        parent = path_element

        for _ in range(5):
            parent = parent.parent
            if parent:
                phones.extend(self._extract_tel_links(parent))
                phones.extend(self._extract_phone_text(parent))

        return list(set(phones))

    def _extract_tel_links(self, element) -> List[str]:
        """Извлечение телефонных ссылок"""
        phones = []
        tel_links = element.find_all('a', href=lambda x: x and x.startswith('tel:'))
        for link in tel_links:
            phone = link.get('href', '').replace('tel:', '').strip()
            if phone:
                phones.append(phone)
        return phones

    def _extract_phone_text(self, element) -> List[str]:
        """Извлечение телефонов из текста"""
        phones = []
        text = element.get_text(' ', strip=True)

        for pattern in self.PATTERNS["phone"]:
            for match in re.finditer(pattern, text):
                phone = match.group(0)
                phone_clean = re.sub(r'[^\d\+]', '', phone)
                if len(phone_clean) >= 10:
                    phones.append(phone_clean)

        return phones

    def _find_websites_by_svg(self, soup: BeautifulSoup, svg_path: str) -> List[str]:
        """Найти сайты рядом с SVG-иконкой"""
        websites = []
        try:
            svg_path_normalized = re.sub(r'\s+', '', svg_path)

            for path in soup.find_all('path'):
                if path.get('d'):
                    path_d_normalized = re.sub(r'\s+', '', path.get('d'))
                    if path_d_normalized[:50] == svg_path_normalized[:50]:
                        websites = self._extract_websites_from_svg_container(path)
                        break

        except Exception as e:
            print(f"      ⚠ Ошибка поиска сайтов: {e}")

        return websites

    def _extract_websites_from_svg_container(self, path_element) -> List[str]:
        """Извлечение сайтов из контейнера SVG"""
        websites = []
        parent = path_element

        for _ in range(5):
            parent = parent.parent
            if parent:
                websites.extend(self._extract_websites_from_containers(parent))
                websites.extend(self._extract_websites_from_links(parent))

        return list(set(websites))

    def _extract_websites_from_containers(self, element) -> List[str]:
        """Извлечение сайтов из контейнеров"""
        websites = []
        all_text_containers = element.find_all(['div', 'span', 'a', 'p'])

        for container in all_text_containers:
            container_text = container.get_text(' ', strip=True)
            text_elements = re.split(r'[\s,;]+', container_text)

            for element_text in text_elements:
                element_text = element_text.strip()
                if self._is_website_element(element_text):
                    clean_url = self._normalize_website_text(element_text)
                    if clean_url:
                        websites.append(clean_url)

        return websites

    def _extract_websites_from_links(self, element) -> List[str]:
        """Извлечение сайтов из ссылок"""
        websites = []

        for link in element.find_all('a'):
            link_text = link.get_text(' ', strip=True)
            href = link.get('href', '')

            if href.startswith('tel:'):
                continue

            if self._is_phone_text(link_text):
                continue

            if self._is_website_element(link_text):
                clean_url = self._normalize_website_text(link_text)
                if clean_url:
                    websites.append(clean_url)

        return websites

    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========

    def _clean_phone_from_website_list(self, websites: List[str]) -> List[str]:
        """Очистка списка сайтов от телефонных номеров"""
        return [url for url in websites if not self._is_phone_text(url.replace('https://', '').replace('http://', ''))]

    def _is_phone_text(self, text: str) -> bool:
        """Проверяет, является ли текст телефонным номером"""
        if not text:
            return False

        cleaned = re.sub(r'[^\d\+]', '', text)

        phone_patterns = [
            r'^\+7\d{10}$',
            r'^8\d{10}$',
            r'^7\d{10}$',
            r'^\d{10,11}$',
        ]

        for pattern in phone_patterns:
            if re.match(pattern, cleaned):
                return True

        return text.replace(' ', '').replace('-', '').startswith(('+7', '8(', '(8', '7('))

    def _is_website_element(self, text: str) -> bool:
        """Проверяет, является ли текст сайтом/доменом"""
        if not text or len(text) < 4 or self._is_phone_text(text):
            return False

        for pattern in self.PATTERNS["domain"]:
            if re.match(pattern, text, re.IGNORECASE):
                return True

        if '.' in text:
            parts = text.split('.')
            if len(parts) >= 2:
                last_part = parts[-1].lower()
                common_tlds = ['ru', 'com', 'net', 'org', 'рф', 'su', 'io', 'info', 'biz']
                if last_part in common_tlds:
                    return True

        return False

    def _normalize_website_text(self, text: str) -> str:
        """Нормализация текста в URL"""
        if not text or self._is_phone_text(text):
            return ""

        text = re.sub(r'[\s‒–—]', '', text)

        if text.startswith(('+7', '8(', '7(', '(8', '(7')):
            return ""

        if '.' not in text:
            return ""

        if not text.startswith(('http://', 'https://')):
            if text.startswith('www.'):
                text = 'https://' + text
            else:
                text = 'https://' + text

        if 'redirect.2gis' in text.lower() or '2gis.ru' in text.lower():
            return ""

        text = text.split('?')[0].split('#')[0].rstrip('/')

        try:
            result = urlparse(text)
            if result.scheme and result.netloc:
                return text
        except:
            pass

        return ""

    def _find_address_alternative(self, soup: BeautifulSoup) -> str:
        """Альтернативный поиск адреса"""
        all_text = soup.get_text(' ', strip=True)

        for pattern in self.PATTERNS["address"]:
            match = re.search(pattern, all_text, re.IGNORECASE)
            if match:
                return match.group(0).strip()

        return ""

    def _clean_address(self, address: str) -> str:
        """Очистка адреса от лишних фраз"""
        if not address:
            return ""

        phrases_to_remove = [
            "показать вход", "показать на карте", "показать схему проезда",
            "показать маршрут", "показать здание", "рассмотреть"
        ]

        cleaned_address = address

        for phrase in phrases_to_remove:
            cleaned_address = cleaned_address.replace(phrase, "")

        if "показать" in cleaned_address.lower():
            index = cleaned_address.lower().find("показать")
            if index > 10:
                cleaned_address = cleaned_address[:index]

        cleaned_address = cleaned_address.strip().rstrip(',.; ')
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address)
        cleaned_address = re.sub(r',\s*,', ',', cleaned_address)

        if cleaned_address.endswith(','):
            cleaned_address = cleaned_address[:-1].strip()

        return cleaned_address

    def _find_phones_alternative(self, soup: BeautifulSoup) -> str:
        """Альтернативный поиск телефонов"""
        phones = []
        all_text = soup.get_text(' ', strip=True)

        for pattern in self.PATTERNS["phone"]:
            for match in re.finditer(pattern, all_text):
                phone = match.group(0)
                phone_clean = re.sub(r'[^\d\+]', '', phone)
                if len(phone_clean) >= 10 and phone_clean not in phones:
                    phones.append(phone_clean)

        return ', '.join(phones) if phones else ""

    def _find_store_name_smart(self, soup: BeautifulSoup) -> str:
        """Умный поиск названия магазина"""
        for tag in ['h1', 'h2', 'h3']:
            for elem in soup.find_all(tag):
                text = elem.get_text(' ', strip=True)
                if text and 5 < len(text) < 100:
                    exclude_words = ['перейти', 'назад', 'меню', 'фильтр', 'поиск']
                    if not any(word in text.lower() for word in exclude_words):
                        return text[:150]

        for meta in soup.find_all('meta'):
            if meta.get('property') in ['og:title', 'og:site_name'] and meta.get('content'):
                content = meta.get('content')
                if content and 5 < len(content) < 150:
                    return content

        all_text = soup.get_text('\n', strip=True)
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]

        for line in lines:
            if (len(line) > 10 and len(line) < 100 and
                    line[0].isupper() and
                    not any(word in line.lower() for word in ['поиск', 'каталог', 'фильтр'])):
                return line[:150]

        return ""

    def _find_opening_hours_smart(self, soup: BeautifulSoup) -> str:
        """Умный поиск времени работы"""
        all_text = soup.get_text('\n', strip=True)
        lines = all_text.split('\n')

        keywords = [
            'часы работы', 'время работы', 'режим работы',
            'открыто', 'график работы', 'работаем', 'пн-пт', 'пн–пт',
            'ежедневно', 'круглосуточно'
        ]

        for i, line in enumerate(lines):
            line_lower = line.lower()

            if any(keyword in line_lower for keyword in keywords):
                result_lines = [line.strip()]

                for j in range(1, 3):
                    if i + j < len(lines):
                        next_line = lines[i + j].strip()
                        if len(next_line) < 50 and not next_line.startswith('©'):
                            result_lines.append(next_line)

                return ' '.join(result_lines)[:150]

        return ""

    def _is_rostov_store(self, data: Dict[str, Any]) -> bool:
        """Проверка, что магазин из Ростова-на-Дону"""
        address = data.get('Адрес', '').lower()
        if not address:
            return False

        rostov_patterns = [
            'ростов-на-дону',
            'ростов на дону',
            'г.ростов-на-дону',
            'г. ростов-на-дону',
            'г.ростов',
            'г. ростов'
        ]

        return any(pattern in address for pattern in rostov_patterns)

    # ========== ВЫВОД ИНФОРМАЦИИ ==========

    def _print_store_info(self, data: Dict[str, Any]):
        """Вывод информации о магазине"""
        print(f"      📍 Адрес: {data.get('Адрес', '-')}")
        print(f"      📞 Телефон: {data.get('Телефон', '-')}")
        print(f"      🌐 Сайт: {data.get('Сайт', '-')}")
        print(f"      ⏰ Время работы: {data.get('Время работы', '-')}")

    def _remove_duplicates(self):
        """Удаление дубликатов"""
        if not self.results:
            return

        unique_results = []
        seen = set()

        for item in self.results:
            unique_id = self._generate_unique_id(item)

            if unique_id and unique_id not in seen:
                seen.add(unique_id)
                unique_results.append(item)
            else:
                fallback_key = self._generate_fallback_key(item)
                if fallback_key and fallback_key not in seen:
                    seen.add(fallback_key)
                    unique_results.append(item)
                else:
                    unique_results.append(item)

        removed = len(self.results) - len(unique_results)
        if removed > 0:
            print(f"\n🗑 Удалено дубликатов: {removed}")
        self.results = unique_results

    def _generate_unique_id(self, item: Dict[str, Any]) -> Optional[str]:
        """Генерация уникального ID для элемента"""
        url = item.get('Ссылка', '').lower().strip()
        if url:
            match = re.search(r'/firm/(\d+)', url)
            if match:
                return f"2gis_{match.group(1)}"
            return url
        return None

    def _generate_fallback_key(self, item: Dict[str, Any]) -> Optional[str]:
        """Генерация резервного ключа для уникальности"""
        name = item.get('Название магазина', '').lower().strip()
        address = item.get('Адрес', '').lower().strip()

        if name and address:
            return f"{name}|{address}"
        elif name:
            return name
        elif address:
            return address
        return None

    def _print_header(self):
        """Вывод заголовка парсера"""
        print("=" * 80)
        print("🔥 ПАРСЕР 2GIS: МАГАЗИНЫ ПИРОТЕХНИКИ - РОСТОВ-НА-ДОНУ")
        print("=" * 80)

    def _print_final_stats(self):
        """Вывод статистики сбора"""
        print("\n" + "=" * 80)
        print("📊 СТАТИСТИКА СБОРА 2ГИС (ПИРОТЕХНИКА)")
        print("=" * 80)

        elapsed_time = time.time() - self.start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(f"⏱ Время выполнения: {minutes} мин {seconds} сек")
        print(f"🔗 Всего найдено ссылок: {len(self.all_urls)}")
        print(f"✅ Успешно спарсено: {len(self.results)}")

        phones_count = sum(1 for r in self.results if r.get('Телефон'))
        sites_count = sum(1 for r in self.results if r.get('Сайт'))
        hours_count = sum(1 for r in self.results if r.get('Время работы'))

        print(f"📞 Магазинов с телефоном: {phones_count}")
        print(f"🌐 Магазинов с сайтом: {sites_count}")
        print(f"⏰ Магазинов с графиком работы: {hours_count}")

        print("\n" + "=" * 80)
