import xml.etree.ElementTree as ET
import csv
import os
import re
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Set, Any, Tuple, Optional, Union
import pandas as pd

class XMLAnalyzer:
    """Класс для анализа структуры XML файла"""
    
    def __init__(self, xml_file_path: str, logger: logging.Logger):
        """
        Инициализация анализатора XML
        
        Args:
            xml_file_path (str): Путь к XML файлу
            logger (logging.Logger): Объект логгера
        """
        self.xml_file_path = xml_file_path
        self.logger = logger
        self.schema = {}
        self.root = None
        self.max_depth = 10  # Максимальная глубина рекурсии
        self.tags_count = {}  # Счетчик тегов для определения коллекций
        self.unique_paths = set()  # Уникальные пути в XML
        self.collections = set()  # Пути к коллекциям (спискам элементов)
    
    def analyze(self) -> Dict:
        """
        Анализирует структуру XML файла
        
        Returns:
            Dict: Схема XML файла
        """
        self.logger.info(f"Начинаем анализ файла: {self.xml_file_path}")
        try:
            # Парсим XML файл
            tree = ET.parse(self.xml_file_path)
            self.root = tree.getroot()
            
            # Анализируем структуру
            self.logger.info(f"Корневой элемент: {self.root.tag}")
            self._analyze_element(self.root, [], 0)
            
            # Выявляем коллекции (списки)
            self._identify_collections()
            
            # Создаем схему данных
            self.schema = {
                "root_tag": self.root.tag,
                "paths": list(self.unique_paths),
                "collections": list(self.collections),
                "tags_count": self.tags_count
            }
            
            self.logger.info(f"Анализ завершен. Найдено {len(self.unique_paths)} уникальных путей и {len(self.collections)} коллекций")
            return self.schema
            
        except Exception as e:
            self.logger.error(f"Ошибка при анализе XML: {str(e)}")
            raise
    
    def _analyze_element(self, element: ET.Element, path: List[str], depth: int) -> None:
        """
        Рекурсивно анализирует элемент XML и его потомков
        
        Args:
            element (ET.Element): Элемент XML
            path (List[str]): Текущий путь в дереве XML
            depth (int): Текущая глубина рекурсии
        """
        if depth > self.max_depth:
            self.logger.warning(f"Достигнута максимальная глубина рекурсии ({self.max_depth}) для пути: {'/'.join(path)}")
            return
        
        # Добавляем текущий тег в путь
        current_path = path + [element.tag]
        
        # Увеличиваем счетчик для данного пути
        path_str = '/'.join(current_path)
        if path_str in self.tags_count:
            self.tags_count[path_str] += 1
        else:
            self.tags_count[path_str] = 1
        
        # Добавляем путь и атрибуты в уникальные пути
        self.unique_paths.add(path_str)
        
        # Добавляем атрибуты как специальные пути
        for attr_name in element.attrib:
            attr_path = path_str + f"[@{attr_name}]"
            self.unique_paths.add(attr_path)
        
        # Рекурсивно обрабатываем всех потомков
        for child in element:
            self._analyze_element(child, current_path, depth + 1)
    
    def _identify_collections(self) -> None:
        """
        Определяет, какие пути являются коллекциями (списками элементов)
        """
        # Группируем пути по родительскому пути
        parent_paths = {}
        for path in self.unique_paths:
            parts = path.split('/')
            if len(parts) > 1:
                parent = '/'.join(parts[:-1])
                child = parts[-1]
                
                if parent not in parent_paths:
                    parent_paths[parent] = []
                
                if child not in parent_paths[parent]:
                    parent_paths[parent].append(child)
        
        # Пути с одинаковым именем тега под одним родителем и количеством > 1 считаем коллекциями
        for path, count in self.tags_count.items():
            if count > 1:
                self.collections.add(path)
    
    def get_preview(self, max_elements: int = 5) -> Dict:
        """
        Создает предварительный просмотр данных
        
        Args:
            max_elements (int): Максимальное количество элементов для примера
            
        Returns:
            Dict: Словарь с примерами данных для каждого пути
        """
        preview = {}
        for path in self.unique_paths:
            # Исключаем пути атрибутов
            if "[@" in path:
                continue
                
            # Получаем элементы по XPath
            elements = self.root.findall(path)
            
            # Выбираем первые max_elements элементов
            samples = []
            for i, elem in enumerate(elements[:max_elements]):
                if elem.text and elem.text.strip():
                    samples.append(elem.text.strip())
                elif len(elem.attrib) > 0:
                    samples.append(f"[Атрибуты: {', '.join(elem.attrib.keys())}]")
                else:
                    samples.append("[Пустой элемент]")
            
            if samples:
                preview[path] = samples
        
        return preview


class XMLTransformer:
    """Класс для преобразования XML в табличный формат"""
    
    def __init__(self, xml_file_path: str, schema: Dict, logger: logging.Logger, config: Dict = None):
        """
        Инициализация трансформера
        
        Args:
            xml_file_path (str): Путь к XML файлу
            schema (Dict): Схема XML (из XMLAnalyzer)
            logger (logging.Logger): Объект логгера
            config (Dict, optional): Настройки преобразования
        """
        self.xml_file_path = xml_file_path
        self.schema = schema
        self.logger = logger
        self.config = config or {}
        
        # Устанавливаем настройки по умолчанию, если не указаны
        self.delimiter = self.config.get('delimiter', '||')
        self.max_field_length = self.config.get('max_field_length', 32767)  # Максимальная длина поля в Excel
        self.main_element_path = self.config.get('main_element_path', '')
        self.include_paths = self.config.get('include_paths', [])
        self.exclude_paths = self.config.get('exclude_paths', [])
        
        # Инициализируем дерево XML
        self.tree = ET.parse(xml_file_path)
        self.root = self.tree.getroot()
        
        # Если основной элемент не указан, используем наиболее частый путь из коллекций
        if not self.main_element_path and self.schema['collections']:
            self.main_element_path = sorted(self.schema['collections'], 
                                            key=lambda x: self.schema['tags_count'].get(x, 0), 
                                            reverse=True)[0]
            self.logger.info(f"Автоматически выбран основной элемент: {self.main_element_path}")
    
    def transform(self) -> List[Dict]:
        """
        Преобразует XML в список словарей для последующего экспорта
        
        Returns:
            List[Dict]: Список словарей с данными
        """
        if not self.main_element_path:
            self.logger.error("Не указан основной элемент для обработки")
            raise ValueError("Не указан основной элемент для обработки")
        
        # Находим все основные элементы
        main_elements = self.root.findall(self.main_element_path)
        if not main_elements:
            self.logger.warning(f"Элементы не найдены по пути: {self.main_element_path}")
            return []
        
        self.logger.info(f"Найдено {len(main_elements)} основных элементов по пути {self.main_element_path}")
        
        # Получаем все пути для включения в таблицу
        paths_to_include = self._get_filtered_paths()
        
        # Трансформируем данные
        result = []
        for i, element in enumerate(main_elements):
            if i % 100 == 0 and i > 0:
                self.logger.info(f"Обработано {i} из {len(main_elements)} элементов")
            
            item_data = self._extract_element_data(element, paths_to_include)
            result.append(item_data)
        
        self.logger.info(f"Трансформация завершена. Получено {len(result)} записей")
        return result
    
    def _get_filtered_paths(self) -> List[str]:
        """
        Фильтрует пути согласно настройкам включения/исключения
        
        Returns:
            List[str]: Список путей для обработки
        """
        # Если указаны пути для включения, используем только их
        if self.include_paths:
            paths = [p for p in self.include_paths if p in self.schema['paths']]
        else:
            # Иначе используем все пути, исключая те, что явно указаны
            paths = [p for p in self.schema['paths'] if p not in self.exclude_paths]
        
        # Нормализуем пути относительно основного элемента
        normalized_paths = []
        main_path_parts = self.main_element_path.split('/')
        
        for path in paths:
            path_parts = path.split('/')
            
            # Пропускаем пути, которые не являются потомками основного элемента
            # или равны ему (нам нужны атрибуты и потомки)
            if len(path_parts) <= len(main_path_parts):
                if path != self.main_element_path and not path.startswith(f"{self.main_element_path}[@"):
                    continue
            elif not '/'.join(path_parts[:len(main_path_parts)]) == self.main_element_path:
                continue
            
            normalized_paths.append(path)
        
        self.logger.info(f"Отфильтровано {len(normalized_paths)} путей для включения в таблицу")
        return normalized_paths
    
    def _extract_element_data(self, element: ET.Element, paths: List[str]) -> Dict:
        """
        Извлекает данные из элемента по указанным путям
        
        Args:
            element (ET.Element): Элемент XML
            paths (List[str]): Список путей для извлечения
            
        Returns:
            Dict: Словарь с данными
        """
        data = {}
        
        # Добавляем атрибуты основного элемента
        for attr_name, attr_value in element.attrib.items():
            column_name = f"{element.tag}[@{attr_name}]"
            data[column_name] = self._clean_text(attr_value)
        
        # Обрабатываем вложенные элементы
        main_path_parts = self.main_element_path.split('/')
        main_tag = main_path_parts[-1]
        
        for path in paths:
            # Пропускаем путь самого элемента
            if path == self.main_element_path:
                continue
                
            # Пропускаем атрибуты основного элемента (уже обработаны)
            if path.startswith(f"{self.main_element_path}[@"):
                continue
            
            # Определяем относительный путь для поиска
            if path.startswith(self.main_element_path + '/'):
                rel_path = '.' + path[len(self.main_element_path):]
            else:
                continue
            
            # Обрабатываем атрибуты и элементы
            if '[@' in rel_path:
                # Это путь к атрибуту
                base_path, attr_part = rel_path.split('[@')
                attr_name = attr_part.rstrip(']')
                
                elements = element.findall(base_path)
                if elements:
                    attr_values = [elem.get(attr_name, '') for elem in elements if attr_name in elem.attrib]
                    if attr_values:
                        data[path] = self._join_values(attr_values)
            else:
                # Это путь к элементу
                elements = element.findall(rel_path)
                if elements:
                    # Если это коллекция (путь в списке коллекций)
                    if path in self.schema['collections']:
                        values = [self._clean_text(elem.text) for elem in elements if elem.text]
                        data[path] = self._join_values(values)
                    else:
                        # Берем только первый элемент
                        data[path] = self._clean_text(elements[0].text) if elements[0].text else ""
        
        return data
    
    def _clean_text(self, text: Optional[str]) -> str:
        """
        Очищает текст от HTML-тегов и нормализует
        
        Args:
            text (Optional[str]): Исходный текст
            
        Returns:
            str: Очищенный текст
        """
        if not text:
            return ""
            
        # Удаляем HTML-теги
        clean = re.sub(r'<.*?>', '', text)
        # Заменяем HTML-сущности
        clean = clean.replace('&nbsp;', ' ')
        clean = clean.replace('&laquo;', '"')
        clean = clean.replace('&raquo;', '"')
        clean = clean.replace('&mdash;', '-')
        clean = clean.replace('\n', ' ')
        # Удаляем лишние пробелы
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Проверяем длину
        if len(clean) > self.max_field_length:
            self.logger.warning(f"Текст усечен с {len(clean)} до {self.max_field_length} символов")
            clean = clean[:self.max_field_length]
            
        return clean
    
    def _join_values(self, values: List[str]) -> str:
        """
        Объединяет список значений в строку с разделителем
        
        Args:
            values (List[str]): Список значений
            
        Returns:
            str: Строка с объединенными значениями
        """
        if not values:
            return ""
            
        # Фильтруем пустые значения и объединяем
        return self.delimiter.join(filter(None, values))


class XMLExporter:
    """Класс для экспорта данных в CSV/XLSX"""
    
    def __init__(self, output_dir: str, logger: logging.Logger, config: Dict = None):
        """
        Инициализация экспортера
        
        Args:
            output_dir (str): Директория для сохранения результатов
            logger (logging.Logger): Объект логгера
            config (Dict, optional): Настройки экспорта
        """
        self.output_dir = output_dir
        self.logger = logger
        self.config = config or {}
        
        # Устанавливаем настройки по умолчанию, если не указаны
        self.format = self.config.get('format', 'xlsx')  # 'csv' или 'xlsx'
        self.encoding = self.config.get('encoding', 'utf-8')  # Кодировка для CSV
        self.csv_delimiter = self.config.get('csv_delimiter', ';')  # Разделитель для CSV
        
        # Создаем директорию, если она не существует
        os.makedirs(output_dir, exist_ok=True)
    
    def export(self, data: List[Dict], filename: str) -> str:
        """
        Экспортирует данные в файл
        
        Args:
            data (List[Dict]): Данные для экспорта
            filename (str): Имя файла без расширения
            
        Returns:
            str: Путь к созданному файлу
        """
        if not data:
            self.logger.warning("Нет данных для экспорта")
            return ""
        
        output_file = os.path.join(self.output_dir, f"{filename}.{self.format}")
        
        # Определяем поля (заголовки столбцов)
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())
        
        fields = sorted(list(all_keys))
        
        if self.format == 'csv':
            return self._export_csv(data, fields, output_file)
        else:
            return self._export_xlsx(data, fields, output_file)
    
    def _export_csv(self, data: List[Dict], fields: List[str], output_file: str) -> str:
        """
        Экспортирует данные в CSV
        
        Args:
            data (List[Dict]): Данные для экспорта
            fields (List[str]): Поля (заголовки столбцов)
            output_file (str): Путь к файлу для сохранения
            
        Returns:
            str: Путь к созданному файлу
        """
        try:
            with open(output_file, 'w', newline='', encoding=self.encoding, errors='replace') as f:
                writer = csv.DictWriter(f, fieldnames=fields, delimiter=self.csv_delimiter)
                writer.writeheader()
                writer.writerows(data)
                
            self.logger.info(f"CSV файл успешно создан: {output_file}")
            return output_file
        except Exception as e:
            self.logger.error(f"Ошибка при создании CSV файла: {str(e)}")
            raise
    
    def _export_xlsx(self, data: List[Dict], fields: List[str], output_file: str) -> str:
        """
        Экспортирует данные в XLSX
        
        Args:
            data (List[Dict]): Данные для экспорта
            fields (List[str]): Поля (заголовки столбцов)
            output_file (str): Путь к файлу для сохранения
            
        Returns:
            str: Путь к созданному файлу
        """
        try:
            # Создаем DataFrame
            df = pd.DataFrame(data)
            
            # Переупорядочиваем колонки
            if set(df.columns) == set(fields):
                df = df[fields]
            
            # Сохраняем в Excel
            df.to_excel(output_file, index=False, engine='openpyxl')
            
            self.logger.info(f"XLSX файл успешно создан: {output_file}")
            return output_file
        except Exception as e:
            self.logger.error(f"Ошибка при создании XLSX файла: {str(e)}")
            raise


class XMLProcessor:
    """Основной класс для обработки XML файлов"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Инициализация процессора
        
        Args:
            config_path (Optional[str]): Путь к файлу конфигурации JSON
        """
        # Загружаем конфигурацию
        self.config = self._load_config(config_path) if config_path else {}
        
        # Устанавливаем настройки по умолчанию
        self.input_file = self.config.get('input_file', '')
        self.output_dir = self.config.get('output_dir', 'output')
        
        # Создаем выходную директорию с датой
        current_date = datetime.now().strftime('%Y_%m_%d')
        self.output_dir = os.path.join(self.output_dir, current_date)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Настраиваем логирование
        self.log_file = os.path.join(self.output_dir, 'xml_processor.log')
        self._setup_logger()
        
        # Журналируем начало работы
        self.logger.info(f"XMLProcessor инициализирован. Выходная директория: {self.output_dir}")
        if self.config:
            self.logger.info(f"Загружена конфигурация из файла")
    
    def _load_config(self, config_path: str) -> Dict:
        """
        Загружает конфигурацию из JSON файла
        
        Args:
            config_path (str): Путь к файлу конфигурации
            
        Returns:
            Dict: Словарь с настройками
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка при загрузке конфигурации: {str(e)}")
            return {}
    
    def _setup_logger(self) -> None:
        """
        Настраивает логирование
        """
        self.logger = logging.getLogger('xml_processor')
        self.logger.setLevel(logging.DEBUG)
        
        # Обработчик для записи в файл
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Обработчик для вывода в консоль
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Форматирование
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Добавляем обработчики к логгеру
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def process(self, input_file: Optional[str] = None, config: Optional[Dict] = None) -> Tuple[str, Dict]:
        """
        Обрабатывает XML файл
        
        Args:
            input_file (Optional[str]): Путь к XML файлу (приоритет над конфигурацией)
            config (Optional[Dict]): Дополнительные настройки (приоритет над файлом конфигурации)
            
        Returns:
            Tuple[str, Dict]: Путь к созданному файлу и статистика обработки
        """
        # Определяем входной файл
        xml_file = input_file or self.input_file
        if not xml_file:
            self.logger.error("Не указан входной XML файл")
            raise ValueError("Не указан входной XML файл")
        
        # Объединяем конфигурации
        merged_config = self.config.copy()
        if config:
            merged_config.update(config)
        
        start_time = datetime.now()
        self.logger.info(f"Начало обработки файла: {xml_file}")
        
        try:
            # Анализируем структуру XML
            analyzer = XMLAnalyzer(xml_file, self.logger)
            schema = analyzer.analyze()
            
            # Сохраняем схему для справки
            schema_file = os.path.join(self.output_dir, 'xml_schema.json')
            with open(schema_file, 'w', encoding='utf-8') as f:
                json.dump(schema, f, indent=2, ensure_ascii=False)
            
            # Получаем предпросмотр данных
            preview = analyzer.get_preview()
            preview_file = os.path.join(self.output_dir, 'xml_preview.json')
            with open(preview_file, 'w', encoding='utf-8') as f:
                json.dump(preview, f, indent=2, ensure_ascii=False)
            
            # Трансформируем XML в табличные данные
            transformer = XMLTransformer(xml_file, schema, self.logger, merged_config)
            data = transformer.transform()
            
            # Экспортируем данные
            exporter = XMLExporter(self.output_dir, self.logger, merged_config)
            output_format = merged_config.get('format', 'xlsx')
            output_file = exporter.export(data, f"xml_data_{os.path.basename(xml_file).split('.')[0]}")
            
            # Создаем статистику
            stats = {
                "input_file": xml_file,
                "output_file": output_file,
                "schema_file": schema_file,
                "preview_file": preview_file,
                "total_records": len(data),
                "total_fields": len(schema['paths']),
                "collections_found": len(schema['collections']),
                "processing_time": str(datetime.now() - start_time)
            }
            
            # Сохраняем статистику
            stats_file = os.path.join(self.output_dir, 'processing_stats.json')
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Обработка завершена успешно. Создан файл: {output_file}")
            self.logger.info(f"Общее время обработки: {stats['processing_time']}")
            
            return output_file, stats
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке файла: {str(e)}")
            raise


def main():
    """
    Основная функция запуска
    """
    parser = argparse.ArgumentParser(description='Универсальный парсер XML в табличные форматы')
    parser.add_argument('input_file', help='Путь к XML/YML файлу', nargs='?')
    parser.add_argument('--config', '-c', help='Путь к файлу конфигурации JSON')
    parser.add_argument('--output', '-o', help='Директория для сохранения результатов')
    parser.add_argument('--format', '-f', choices=['csv', 'xlsx'], default='xlsx', 
                        help='Формат выходного файла (по умолчанию: xlsx)')
    parser.add_argument('--encoding', '-e', default='utf-8', 
                        help='Кодировка для CSV файла (по умолчанию: utf-8)')
    parser.add_argument('--delimiter', '-d', default='||', 
                        help='Разделитель для списков (по умолчанию: ||)')
    
    args = parser.parse_args()
    
    # Создаем конфигурацию из аргументов командной строки
    config = {}
    if args.output:
        config['output_dir'] = args.output
    if args.format:
        config['format'] = args.format
    if args.encoding:
        config['encoding'] = args.encoding
    if args.delimiter:
        config['delimiter'] = args.delimiter
    
    try:
        processor = XMLProcessor(args.config)
        output_file, stats = processor.process(args.input_file, config)
        
        print(f"\nОбработка успешно завершена!")
        print(f"Выходной файл: {output_file}")
        print(f"Обработано записей: {stats['total_records']}")
        print(f"Время обработки: {stats['processing_time']}")
        print(f"Дополнительная информация в: {os.path.dirname(output_file)}")
        
    except Exception as e:
        print(f"\nОшибка при обработке: {str(e)}")
        print("Смотрите подробный журнал для получения дополнительной информации.")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())