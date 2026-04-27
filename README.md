# 🚀 Hadoop & Spark проект для анализа данных автомобилей

Проект для анализа тестовых данных о рынке автомобилей с использованием Hadoop Distributed File System (HDFS) и Apache Spark. Включает Telegram бота для удобного запуска анализа и получения результатов.

## 📋 Описание

Проект реализует распределенную систему обработки данных для анализа цен на автомобили. Система использует:
- **Hadoop HDFS** для хранения данных
- **Apache Spark** для распределенной обработки данных с использованием MapReduce
- **Telegram бот** на aiogram для интерактивного взаимодействия с пользователем

## 🏗️ Архитектура

```
┌─────────────────┐
│  Telegram Bot   │ (aiogram)
│   (Python)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Spark Master   │ (PySpark)
│   + Worker      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  HDFS Cluster   │
│ Namenode +      │
│ Datanode        │
└─────────────────┘
```

## 📁 Структура проекта

```
hasoop-proj/
├── docker-compose.yml          # Конфигурация всех контейнеров
├── Dockerfile.bot               # Dockerfile для Telegram бота
├── hadoop.env                   # Переменные окружения для Hadoop
├── requirements.txt             # Python зависимости для бота
│
├── analyze_data.py              # Основной PySpark скрипт анализа
├── bot_analyze_data.py          # Упрощенная версия для Telegram бота
├── telegram_bot.py              # Telegram бот на aiogram
│
├── all_cars_prices.csv          # Тестовый датасет автомобилей
├── data/                        # Исходные данные (DIM и FACT таблицы)
│   ├── DIM_CPU_PROD.csv
│   ├── DIM_GPU_PROD.csv
│   ├── DIM_RAM_PROD.csv
│   ├── FACT_CPU_PRICE.csv
│   ├── FACT_GPU_PRICE.csv
│   └── FACT_RAM_PRICE.csv
│
└── README.md                    # Этот файл
```

## 🛠️ Технологии

- **Hadoop 3.2.1** - распределенное хранилище данных (HDFS)
- **Apache Spark 3.0.0** - обработка данных (MapReduce)
- **PySpark** - Python API для Spark
- **Docker & Docker Compose** - контейнеризация
- **Python 3.11** - основной язык разработки
- **aiogram 3.1.1** - фреймворк для Telegram ботов
- **Telegram Bot API** - взаимодействие с пользователями

## 🚀 Быстрый старт

### Предварительные требования

- Docker и Docker Compose
- Python 3.11+ (для локальной разработки)
- Telegram аккаунт (для получения токена бота)

### 1. Клонирование и настройка

```bash
# Клонируйте репозиторий (если есть)
git clone <repository-url>
cd hasoop-proj

# Создайте файл .env для токена бота
echo "BOT_TOKEN=your_bot_token_here" > .env
```

### 2. Получение токена Telegram бота

1. Откройте Telegram и найдите [@BotFather](https://t.me/BotFather)
2. Отправьте команду `/newbot`
3. Следуйте инструкциям для создания бота
4. Скопируйте полученный токен в файл `.env`:
   ```
   BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
   ```

### 3. Запуск системы

```bash
# Запуск всех контейнеров
docker-compose up -d

# Проверка статуса
docker-compose ps

# Просмотр логов
docker logs telegram-bot
docker logs spark-master
```

### 4. Использование

#### Через Telegram бота

1. Найдите вашего бота в Telegram
2. Отправьте команду `/start`
3. Дождитесь результатов анализа (обычно 1-3 минуты)
4. Получите детальную статистику по каждому типу компонентов

#### Команды бота

- `/start` - Запустить анализ данных компонентов
- `/help` - Показать справку
- `/status` - Проверить статус системы

#### Прямой запуск через Spark

```bash
# Запуск анализа напрямую
docker exec -e PYTHONIOENCODING=utf-8 spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/analyze_data.py
```

## 📊 Что анализируется

Проект анализирует данные о ценах на автомобили:

### Типы автомобилей
- **Sedan**
- **SUV**
- **Pickup**
- **EV**

### Метрики для каждого типа

1. **Общая статистика**
   - Количество записей
   - Средняя, минимальная, максимальная цена
   - Стандартное отклонение

2. **Топ-5 производителей**
   - Количество записей по каждому производителю
   - Средняя цена

3. **Статистика по годам**
   - Динамика цен по годам
   - Количество записей по годам

4. **Топ-5 мерчантов**
   - Популярные магазины
   - Средние цены у каждого мерчанта

5. **Корреляция**
   - Связь между годом выпуска и ценой
   - Интерпретация силы связи

## 🔧 Компоненты системы

### 1. Hadoop HDFS

**Namenode** (порт 9870, 9000)
- Управление файловой системой
- Web UI: http://localhost:9870

**Datanode** (порт 9864)
- Хранение данных
- Репликация блоков

### 2. Apache Spark

**Spark Master** (порты 8080, 7077)
- Координация задач
- Web UI: http://localhost:8080

**Spark Worker**
- Выполнение задач
- Web UI: http://localhost:8081

### 3. Telegram Bot

**Контейнер telegram-bot**
- Интерактивный интерфейс
- Прогресс-бар во время анализа
- Форматированный вывод результатов

## 📈 Особенности

### Автоматическая загрузка данных

Скрипт `analyze_data.py` автоматически:
- Проверяет наличие данных в HDFS
- Загружает данные, если их нет
- Использует PySpark для загрузки, если HDFS команды недоступны

### Красивый вывод в Telegram

- Эмодзи для визуализации
- Markdown форматирование
- Разделение по категориям компонентов
- Прогресс-бар с анимацией

### Обработка больших данных

- Поддержка датасетов с сотнями тысяч записей
- Распределенная обработка через Spark
- Оптимизированные запросы MapReduce

## 🔍 Web интерфейсы

После запуска доступны:

- **Hadoop Namenode**: http://localhost:9870
- **Spark Master**: http://localhost:8080
- **Spark Worker**: http://localhost:8081

## 🛠️ Разработка

### Локальная разработка

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск бота локально (требует запущенных контейнеров)
python telegram_bot.py
```

### Структура данных

Датасет содержит следующие поля:
- `Vehicle_Type` - тип автомобиля
- `Brand` - бренд
- `Model` - модель
- `Fuel_Type`, `Transmission`, `Drive_Type` - характеристики
- `Year`, `Month`, `Day`, `Week` - временные метки
- `Dealer` - дилер
- `Region_Code` - регион
- `Currency` - валюта
- `Price_USD` - цена в долларах
- `Mileage_KM` - пробег

## 🐛 Устранение неполадок

### Бот не отвечает

```bash
# Проверьте логи
docker logs telegram-bot

# Проверьте токен
docker exec telegram-bot env | grep BOT_TOKEN

# Перезапустите бота
docker-compose restart telegram-bot
```

### Ошибки при анализе

```bash
# Проверьте статус Spark
docker logs spark-master

# Проверьте статус HDFS
docker logs namenode

# Проверьте наличие данных
docker exec namenode hdfs dfs -ls /data/
```

### Проблемы с кодировкой

Все контейнеры настроены на UTF-8. Если видите кракозябры:
```bash
# Убедитесь, что переменная окружения установлена
docker exec spark-master env | grep PYTHONIOENCODING
```

## 📝 Команды управления

```bash
# Запуск всех сервисов
docker-compose up -d

# Остановка всех сервисов
docker-compose down

# Перезапуск конкретного сервиса
docker-compose restart telegram-bot

# Просмотр логов
docker-compose logs -f telegram-bot

# Пересборка контейнера
docker-compose up -d --build telegram-bot

# Очистка данных HDFS (осторожно!)
docker-compose down -v
```

## 🔐 Безопасность

⚠️ **Важно**: 
- Не коммитьте файл `.env` с токеном бота в Git
- Токен бота должен храниться в переменных окружения
- В продакшене используйте секреты Docker или внешние системы управления секретами

## 📄 Лицензия

Этот проект создан в образовательных целях для демонстрации работы с Hadoop и Spark.

## 👤 Автор

Проект создан для изучения распределенных систем обработки больших данных.

## 🙏 Благодарности

- [Big Data Europe](https://github.com/big-data-europe) за Docker образы Hadoop и Spark
- [aiogram](https://github.com/aiogram/aiogram) за отличный фреймворк для Telegram ботов

---

**Примечание**: Для работы проекта требуется минимум 4GB свободной RAM и Docker с поддержкой минимум 2GB памяти для контейнеров.
