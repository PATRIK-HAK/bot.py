import sqlite3
import re
import asyncio
import os
from datetime import datetime
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes, CommandHandler
import threading
import time

# ========== КОНФИГ ==========
BOT_TOKEN = os.environ.get("BOT_TOKEN")

if not BOT_TOKEN:
    print("❌ ОШИБКА: BOT_TOKEN не найден!")
    exit(1)

# ГРУППЫ ДЛЯ СБОРА ЗАЯВОК
SOURCE_GROUPS = [
    -5230519955,
    -5050689393,
    -4723482033,
    -4228409129,
    -100228732128,
    -1002619425684
]

TARGET_GROUP = -1003901607049
BATCH_SIZE = 40
TOTAL_DISPATCHERS = 7
DISPATCHER_NAMES = [f"Диспетчер {i}" for i in range(1, TOTAL_DISPATCHERS + 1)]

db_lock = threading.Lock()

# ========== РАБОТА С БД ==========
def init_db():
    with db_lock:
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dispatcher_cities (
                dispatcher TEXT,
                city TEXT,
                orders TEXT,
                PRIMARY KEY (dispatcher, city)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_number TEXT,
                city TEXT,
                employee_name TEXT,
                assigned_to TEXT,
                timestamp TEXT,
                is_exported INTEGER DEFAULT 0,
                source_group TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS export_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_date TEXT,
                orders_count INTEGER
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_exported ON orders_history(is_exported)')
        
        conn.commit()
        conn.close()

def execute_with_retry(func, *args, **kwargs):
    max_retries = 10
    for attempt in range(max_retries):
        try:
            with db_lock:
                return func(*args, **kwargs)
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise
    return None

def get_dispatcher_by_city(city):
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("SELECT dispatcher FROM dispatcher_cities WHERE city = ?", (city,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    return execute_with_retry(_query)

def get_city_count_for_all_dispatchers():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("SELECT dispatcher, COUNT(DISTINCT city) FROM dispatcher_cities GROUP BY dispatcher")
        counts = dict(cursor.fetchall())
        conn.close()
        for d in DISPATCHER_NAMES:
            if d not in counts:
                counts[d] = 0
        return counts
    return execute_with_retry(_query)

def find_best_dispatcher():
    counts = get_city_count_for_all_dispatchers()
    best = min(counts.items(), key=lambda x: x[1])
    return best[0]

def assign_dispatcher(city):
    existing = get_dispatcher_by_city(city)
    if existing:
        return existing
    return find_best_dispatcher()

def add_order_to_dispatcher(dispatcher, city, order_number):
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT orders FROM dispatcher_cities WHERE dispatcher = ? AND city = ?",
            (dispatcher, city)
        )
        row = cursor.fetchone()
        
        if row:
            existing_orders = row[0].split(', ') if row[0] else []
            if order_number not in existing_orders:
                existing_orders.append(order_number)
            new_orders = ', '.join(existing_orders)
            cursor.execute(
                "UPDATE dispatcher_cities SET orders = ? WHERE dispatcher = ? AND city = ?",
                (new_orders, dispatcher, city)
            )
        else:
            cursor.execute(
                "INSERT INTO dispatcher_cities (dispatcher, city, orders) VALUES (?, ?, ?)",
                (dispatcher, city, order_number)
            )
        
        conn.commit()
        conn.close()
        return True
    return execute_with_retry(_query)

def save_to_history(order_number, city, employee_name, assigned_to, source_group):
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders_history (order_number, city, employee_name, assigned_to, timestamp, is_exported, source_group) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (order_number, city, employee_name, assigned_to, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0, str(source_group))
        )
        conn.commit()
        conn.close()
        return True
    return execute_with_retry(_query)

def get_pending_orders_count():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders_history WHERE is_exported = 0")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    return execute_with_retry(_query)

def get_exported_orders_count():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM orders_history WHERE is_exported = 1")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    return execute_with_retry(_query)

def get_unexported_orders():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("SELECT order_number, city, assigned_to FROM orders_history WHERE is_exported = 0")
        rows = cursor.fetchall()
        conn.close()
        
        result = {}
        for order_num, city, dispatcher in rows:
            if dispatcher not in result:
                result[dispatcher] = {}
            if city not in result[dispatcher]:
                result[dispatcher][city] = []
            result[dispatcher][city].append(order_num)
        return result
    return execute_with_retry(_query)

def mark_orders_as_exported():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("UPDATE orders_history SET is_exported = 1 WHERE is_exported = 0")
        conn.commit()
        conn.close()
        return True
    return execute_with_retry(_query)

def clear_dispatcher_cities():
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dispatcher_cities")
        conn.commit()
        conn.close()
        return True
    return execute_with_retry(_query)

def log_export(orders_count):
    def _query():
        conn = sqlite3.connect('applications.db', timeout=20)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO export_log (export_date, orders_count) VALUES (?, ?)",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), orders_count)
        )
        conn.commit()
        conn.close()
        return True
    return execute_with_retry(_query)

# ========== ФОРМИРОВАНИЕ ОТЧЁТА ==========
def format_report_for_dispatchers(orders_by_dispatcher):
    if not orders_by_dispatcher:
        return None
    
    report = f"📋 *НОВЫЕ ЗАЯВКИ* (пакет из {BATCH_SIZE} заявок)\n"
    report += f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
    report += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    total_all = 0
    for dispatcher, cities in orders_by_dispatcher.items():
        report += f"👤 *{dispatcher}*\n"
        total_orders = 0
        for city, orders in cities.items():
            orders_str = ', '.join(orders)
            report += f" 🏙 {city}: {orders_str}\n"
            total_orders += len(orders)
        report += f" 📊 *Итого: {total_orders} заявок*\n\n"
        total_all += total_orders
    
    report += f"━━━━━━━━━━━━━━━━━━━━━\n"
    report += f"📦 Всего заявок в пакете: {total_all}\n"
    report += f"✅ Следующая выгрузка через {BATCH_SIZE} заявок"
    
    return report

# ========== ОБРАБОТЧИКИ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Бот для сбора заявок\n\n"
        "📝 Формат: @имя_бота 12345 Москва\n\n"
        "📊 Команды:\n"
        "/status - сколько заявок в очереди\n"
        "/export_manual - ручная выгрузка\n"
        "/stats - статистика\n"
        "/last_report - последний отчёт"
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pending = get_pending_orders_count()
        exported = get_exported_orders_count()
        
        await update.message.reply_text(
            f"📊 *Статус очереди*\n\n"
            f"📝 В очереди: {pending} / {BATCH_SIZE} заявок\n"
            f"✅ Выгружено всего: {exported}\n"
            f"📦 До выгрузки: {BATCH_SIZE - pending if pending < BATCH_SIZE else 0}\n\n"
            f"📥 Заявки принимаются из {len(SOURCE_GROUPS)} групп\n"
            f"📤 Выгрузка в группу: {TARGET_GROUP}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def export_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Принудительная выгрузка...")
    await export_to_dispatchers_group(context.bot)
    await update.message.reply_text("✅ Выгрузка выполнена")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with db_lock:
            conn = sqlite3.connect('applications.db', timeout=20)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM orders_history")
            total = cursor.fetchone()[0]
            
            cursor.execute("SELECT assigned_to, COUNT(*) FROM orders_history GROUP BY assigned_to")
            by_dispatcher = cursor.fetchall()
            
            cursor.execute("SELECT COUNT(DISTINCT city) FROM orders_history")
            unique_cities = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM orders_history WHERE is_exported = 0")
            pending = cursor.fetchone()[0]
            
            cursor.execute("SELECT source_group, COUNT(*) FROM orders_history GROUP BY source_group")
            by_source = cursor.fetchall()
            
            conn.close()
        
        text = f"📊 *ОБЩАЯ СТАТИСТИКА*\n\n"
        text += f"📝 Всего заявок: {total}\n"
        text += f"🏙 Уникальных городов: {unique_cities}\n"
        text += f"⏳ В очереди: {pending}\n\n"
        text += "*По диспетчерам:*\n"
        
        for dispatcher, count in by_dispatcher:
            text += f" • {dispatcher}: {count}\n"
        
        if by_source:
            text += f"\n*По группам-источникам:*\n"
            for source, count in by_source:
                text += f" • {source}: {count}\n"
        
        await update.message.reply_text(text)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def last_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with db_lock:
            conn = sqlite3.connect('applications.db', timeout=20)
            cursor = conn.cursor()
            cursor.execute("SELECT export_date, orders_count FROM export_log ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            conn.close()
        
        if row:
            await update.message.reply_text(f"📋 Последняя выгрузка: {row[0]}\n📦 Заявок: {row[1]}")
        else:
            await update.message.reply_text("📭 Выгрузок ещё не было")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    
    if message.chat_id not in SOURCE_GROUPS:
        return
    
    text = message.text
    if not text:
        return
    
    bot_info = await context.bot.get_me()
    bot_username = bot_info.username
    
    if f"@{bot_username}" not in text:
        return
    
    pattern = r'@\w+\s+(?:№?\s*)?(\d+)\s+[г\.]?\s*([А-Яа-яёЁ\s\-]+)'
    match = re.search(pattern, text)
    
    if not match:
        await message.reply_text(f"❌ Формат: @{bot_username} 12345 Москва")
        return
    
    order_number = match.group(1)
    raw_city = match.group(2).strip()
    raw_city = re.sub(r'^г\.', '', raw_city)
    city = raw_city[0].upper() + raw_city[1:].lower()
    
    dispatcher = assign_dispatcher(city)
    
    saved = False
    for attempt in range(5):
        try:
            add_order_to_dispatcher(dispatcher, city, order_number)
            save_to_history(order_number, city, message.from_user.full_name, dispatcher, message.chat_id)
            saved = True
            print(f"✅ Заявка #{order_number} сохранена | Город: {city} | Диспетчер: {dispatcher}")
            break
        except Exception as e:
            print(f"⚠️ Попытка {attempt+1}/5 сохранить заявку #{order_number}: {e}")
            await asyncio.sleep(0.5)
    
    if not saved:
        print(f"❌ НЕ УДАЛОСЬ СОХРАНИТЬ ЗАЯВКУ #{order_number}!")
        await message.reply_text("❌ Ошибка сервера, попробуйте еще раз")
        return
    
    try:
        await message.set_reaction(reaction=["👌"])
    except Exception as e:
        print(f"⚠️ Реакция не работает: {e}")
        await message.reply_text("👌")
    
    pending_count = get_pending_orders_count()
    print(f"📊 В очереди: {pending_count}/{BATCH_SIZE}")
    
    if pending_count >= BATCH_SIZE:
        print(f"🎯 Выгружаю {pending_count} заявок...")
        await export_to_dispatchers_group(context.bot)

# ========== ВЫГРУЗКА ==========
async def export_to_dispatchers_group(bot):
    try:
        pending_count = get_pending_orders_count()
        
        if pending_count == 0:
            return
        
        unexported = get_unexported_orders()
        
        if not unexported:
            return
        
        report = format_report_for_dispatchers(unexported)
        
        if report:
            await bot.send_message(
                chat_id=TARGET_GROUP,
                text=report,
                parse_mode="Markdown"
            )
            print(f"✅ Выгружено {pending_count} заявок")
            
            mark_orders_as_exported()
            log_export(pending_count)
            clear_dispatcher_cities()
            
    except Exception as e:
        print(f"❌ Ошибка выгрузки: {e}")

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========
async def run_once():
    """Запускаем бота на 25 минут"""
    print("=" * 50)
    print("🚀 ЗАПУСК БОТА")
    print("=" * 50)
    
    init_db()
    print("✅ База данных готова")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("export_manual", export_now))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("last_report", last_report))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    print("✅ Бот запущен на 25 МИНУТ")
    print("📝 Жду заявки...")
    
    # Запускаем бота
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    # Работаем 25 минут (1500 секунд)
    await asyncio.sleep(1500)
    
    # Выключаемся
    print("⏰ Время вышло, завершаем работу")
    await application.updater.stop()
    await application.shutdown()
    
    # Финальная выгрузка
    pending = get_pending_orders_count()
    if pending > 0:
        print(f"📤 Финальная выгрузка {pending} заявок...")
        await export_to_dispatchers_group(application.bot)
    
    print("✅ Бот остановлен")

def main():
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не найден!")
        print("Добавь секрет BOT_TOKEN в GitHub репозиторий")
        return
    
    asyncio.run(run_once())

if __name__ == "__main__":
    main()
