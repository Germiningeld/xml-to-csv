# Универсальный парсер XML

Мощный инструмент для преобразования XML/YML файлов в табличные форматы (CSV/XLSX) с автоматическим анализом структуры.

## Возможности

- **Автоматический анализ структуры XML** — определяет все поля, атрибуты и коллекции
- **Универсальность** — работает с любыми корректно сформированными XML/YML файлами
- **Гибкая обработка данных** — настраиваемая фильтрация полей и обработка коллекций
- **Несколько форматов экспорта** — CSV и XLSX с настраиваемыми параметрами
- **Подробное логирование** — полная информация о процессе и потенциальных проблемах
- **Статистика и предпросмотр** — для контроля качества обработки данных

## Установка

```bash
# Клонирование репозитория
git clone https://github.com/username/universal-xml-parser.git
cd universal-xml-parser

# Установка зависимостей
pip install -r requirements.txt
```

### Зависимости

- Python 3.6+
- pandas
- openpyxl

## Использование

### Базовое использование

```bash
python xmlparser.py путь_к_файлу.xml
```

Результаты будут сохранены в директорию `output/ГГГГ_ММ_ДД/`.

### Параметры командной строки

```bash
python xmlparser.py [путь_к_файлу.xml] [параметры]
```

Доступные параметры:

| Параметр | Описание |
|----------|----------|
| `--config`, `-c` | Путь к файлу конфигурации JSON |
| `--output`, `-o` | Директория для сохранения результатов |
| `--format`, `-f` | Формат выходного файла: csv или xlsx (по умолчанию: xlsx) |
| `--encoding`, `-e` | Кодировка для CSV файла (по умолчанию: utf-8) |
| `--delimiter`, `-d` | Разделитель для списков (по умолчанию: \|\|) |

### Конфигурационный файл

Для более гибкой настройки процесса парсинга можно использовать конфигурационный файл JSON. Полное описание всех доступных параметров конфигурации доступно в [документации по конфигурации](Config.md).

Пример конфигурационного файла:

```json
{
  "input_file": "example.xml",
  "output_dir": "output/custom",
  "format": "csv",
  "encoding": "cp1251",
  "csv_delimiter": ";",
  "delimiter": "||",
  "main_element_path": "shop/offers/offer",
  "include_paths": [
    "shop/offers/offer[@id]",
    "shop/offers/offer/name",
    "shop/offers/offer/price"
  ],
  "exclude_paths": [
    "shop/offers/offer/description"
  ],
  "max_field_length": 32767
}
```

## Результаты обработки

После обработки XML в указанной директории создаются следующие файлы:

1. **Основной файл данных** — CSV или XLSX с преобразованными данными
2. **xml_schema.json** — структура XML файла в формате JSON
3. **xml_preview.json** — предпросмотр данных для проверки
4. **xml_processor.log** — подробный журнал процесса обработки
5. **processing_stats.json** — статистика обработки (количество записей, время и т.д.)

## Особенности работы

### Обработка списков

Списки (повторяющиеся элементы) автоматически объединяются в одну ячейку с разделителем, указанным в параметре `delimiter`.

Пример:
```xml
<items>
  <item>Значение 1</item>
  <item>Значение 2</item>
  <item>Значение 3</item>
</items>
```

В таблице: `Значение 1||Значение 2||Значение 3`

### Автоматический выбор основного элемента

Если не указан `main_element_path`, скрипт автоматически выбирает наиболее часто встречающийся элемент в коллекциях как основу для таблицы.

### Обработка атрибутов

Атрибуты XML элементов представляются в виде отдельных столбцов с префиксом `[@имя_атрибута]`.

Пример:
```xml
<product id="123" available="true">
  <name>Пример</name>
</product>
```

В таблице будут столбцы: `product[@id]`, `product[@available]`, `product/name`

## Обработка ошибок

Скрипт включает обширную обработку ошибок и предупреждений:

- Проверка корректности XML структуры
- Обнаружение и замена невидимых символов
- Усечение слишком длинных значений
- Обработка неправильной кодировки

Все ошибки и предупреждения записываются в журнал для дальнейшего анализа.

## Производительность

Скрипт оптимизирован для обработки больших XML файлов:

- Поэтапная обработка элементов
- Отслеживание прогресса для длительных операций
- Контроль использования памяти

## Расширение функциональности

Код организован модульно для простоты расширения. Основные классы:

- **XMLAnalyzer** — анализирует структуру XML
- **XMLTransformer** — преобразует XML в табличные данные
- **XMLExporter** — экспортирует данные в выбранный формат
- **XMLProcessor** — координирует весь процесс

## Лицензия

MIT

## Автор

Germininleld

---

*Замечание: для корректной работы скрипта требуется правильно сформированный XML файл.*