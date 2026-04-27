#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram bot for running car market analysis through Hadoop/Spark.
"""

import asyncio
import logging
import os
import subprocess
import sys

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

if not BOT_TOKEN:
    logger.error("BOT_TOKEN is not set. Put BOT_TOKEN into .env before запуск.")
    sys.exit(1)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def run_analysis():
    """Run Spark analysis inside the spark-master container."""
    try:
        check_result = subprocess.run(
            ["docker", "exec", "spark-master", "test", "-f", "/opt/spark/work-dir/bot_analyze_data.py"],
            capture_output=True,
            timeout=5,
        )

        script_path = (
            "/opt/spark/work-dir/bot_analyze_data.py"
            if check_result.returncode == 0
            else "/opt/spark/work-dir/analyze_data.py"
        )

        result = subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                "PYTHONIOENCODING=utf-8",
                "spark-master",
                "/spark/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                script_path,
            ],
            capture_output=True,
            text=True,
            timeout=300,
            encoding="utf-8",
            errors="replace",
        )

        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "stdout": "",
            "stderr": "Таймаут анализа: выполнение заняло больше 5 минут.",
            "returncode": -1,
        }
    except Exception as exc:
        return {
            "success": False,
            "stdout": "",
            "stderr": str(exc),
            "returncode": -1,
        }


def format_results_for_telegram(output_text):
    """Strip Spark noise and keep the useful part of the report."""
    if not output_text:
        return "Результаты анализа не получены."

    skip_fragments = [
        "INFO",
        "WARN",
        "ERROR",
        "log4j:",
        "BlockManager",
        "TaskSetManager",
        "DAGScheduler",
        "SparkContext",
        "MemoryStore",
        "FileSourceScanExec",
        "CodeGenerator",
        "ShutdownHookManager",
        "DeprecationWarning",
        "pyspark",
        "py4j",
        "/spark/python/lib",
    ]

    formatted_lines = []
    for line in output_text.splitlines():
        if any(fragment in line for fragment in skip_fragments):
            continue
        if not formatted_lines and not line.strip():
            continue
        if line.strip() or (formatted_lines and formatted_lines[-1].strip()):
            formatted_lines.append(line)

    result = "\n".join(formatted_lines).strip()
    if len(result) > 4000:
        result = result[:4000] + "\n\n... (результат обрезан)"

    return result or "Анализ выполнен, но полезный вывод не удалось извлечь из логов."


async def update_progress(status_msg, stage, progress_step=0, elapsed_time=0):
    stages = {
        "init": ("Подготовка анализа", "Проверяем Spark и подключение к данным"),
        "loading": ("Загрузка данных", "Читаем датасет автомобилей"),
        "processing": ("Обработка данных", "Считаем агрегаты и группировки"),
        "calculating": ("Расчет метрик", "Собираем сводку по ценам, брендам и регионам"),
        "finalizing": ("Формирование ответа", "Подготавливаем отчет для Telegram"),
    }

    stage_name, stage_desc = stages.get(stage, ("Обработка", "Идет анализ данных"))
    filled = min(progress_step % 11, 10)
    progress_bar = "#" * filled + "-" * (10 - filled)
    time_suffix = f" | {elapsed_time}с" if elapsed_time else ""

    progress_text = (
        f"*{stage_name}*\n"
        f"`{stage_desc}`\n\n"
        f"`[{progress_bar}]`{time_suffix}\n\n"
        f"Пожалуйста, подождите."
    )

    try:
        await status_msg.edit_text(progress_text, parse_mode="Markdown")
    except Exception as exc:
        logger.warning("Не удалось обновить прогресс: %s", exc)


async def run_analysis_with_progress(status_msg):
    import time

    start_time = time.time()
    try:
        await update_progress(status_msg, "init", 0, 0)
        await asyncio.sleep(0.8)

        loop = asyncio.get_event_loop()
        analysis_task = loop.run_in_executor(None, run_analysis)

        progress_counter = 1
        stage_sequence = ["loading", "processing", "calculating", "finalizing"]

        while not analysis_task.done():
            await asyncio.sleep(1.5)
            elapsed = int(time.time() - start_time)
            stage = stage_sequence[min(progress_counter // 4, len(stage_sequence) - 1)]
            await update_progress(status_msg, stage, progress_counter, elapsed)
            progress_counter += 1

        elapsed = int(time.time() - start_time)
        await update_progress(status_msg, "finalizing", progress_counter, elapsed)
        await asyncio.sleep(0.5)
        return await analysis_task
    except Exception as exc:
        logger.error("Ошибка при анализе с прогрессом: %s", exc)
        return {"success": False, "stdout": "", "stderr": str(exc), "returncode": -1}


@dp.message(Command("start"))
async def cmd_start(message: Message):
    status_msg = await message.answer(
        "*Анализ рынка автомобилей*\n\n`Инициализация...`",
        parse_mode="Markdown",
    )

    try:
        analysis_result = await run_analysis_with_progress(status_msg)
        if analysis_result["success"]:
            response = format_results_for_telegram(analysis_result["stdout"])
            if len(response) <= 4000:
                await status_msg.edit_text(response, parse_mode="Markdown")
            else:
                parts = [response[i : i + 4000] for i in range(0, len(response), 4000)]
                await status_msg.edit_text(parts[0], parse_mode="Markdown")
                for part in parts[1:]:
                    await asyncio.sleep(0.4)
                    await message.answer(part, parse_mode="Markdown")
        else:
            error_text = analysis_result["stderr"][:1500] if analysis_result["stderr"] else "Неизвестная ошибка"
            await status_msg.edit_text(
                f"*Ошибка при выполнении анализа*\n\n```text\n{error_text}\n```",
                parse_mode="Markdown",
            )
    except Exception as exc:
        logger.error("Ошибка в /start: %s", exc)
        await status_msg.edit_text(f"Ошибка при выполнении анализа:\n\n{str(exc)[:1000]}")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = (
        "*Бот для анализа рынка автомобилей*\n\n"
        "/start - запустить анализ тестового датасета\n"
        "/help - показать справку\n"
        "/status - проверить состояние Spark и HDFS\n\n"
        "Бот использует Hadoop HDFS для хранения данных и Spark для распределенного расчета метрик."
    )
    await message.answer(help_text, parse_mode="Markdown")


@dp.message(Command("status"))
async def cmd_status(message: Message):
    try:
        lines = ["*Статус системы*"]

        spark_result = subprocess.run(
            ["docker", "ps", "--filter", "name=spark-master", "--format", "{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        lines.append(
            f"Spark: {spark_result.stdout.strip()}" if spark_result.returncode == 0 and spark_result.stdout.strip() else "Spark: не запущен"
        )

        hdfs_result = subprocess.run(
            ["docker", "ps", "--filter", "name=namenode", "--format", "{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        lines.append(
            f"HDFS: {hdfs_result.stdout.strip()}" if hdfs_result.returncode == 0 and hdfs_result.stdout.strip() else "HDFS: не запущен"
        )

        await message.answer("\n".join(lines), parse_mode="Markdown")
    except Exception as exc:
        await message.answer(f"Ошибка при проверке статуса: {exc}")


@dp.message()
async def echo_handler(message: Message):
    await message.answer("Неизвестная команда. Используй /start, /help или /status.")


async def main():
    logger.info("Запуск Telegram бота...")
    try:
        check_result = subprocess.run(
            ["docker", "ps", "--filter", "name=spark-master", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if "spark-master" not in check_result.stdout:
            logger.warning("Контейнер spark-master не найден. Убедись, что compose уже поднят.")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем.")
