import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from contextlib import asynccontextmanager
import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ParseMode
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.markdown import hbold, hcode
from aiogram.types.message import ContentType
from aiogram.dispatcher.middlewares import BaseMiddleware
import re
from collections import Counter

# --- НАСТРОЙКИ (ЗАМЕНИ НА СВОИ) ---
BOT_TOKEN = "8717294075:AAFFq4OhGMK6YoDiIHcXo7o8kZyJmTSL7qc"
ADMIN_IDS = [667474295, 8190085943, 2116440928, 960544314]  # ID администраторов
REQUIRED_CHANNEL = "@I_S_B_DNR"  # Канал для подписки
GROUP_ID = -1003881232125  # ID вашей группы
CACHE_TTL = 300  # Время жизни кэша подписки (5 минут)
APPEALS_PER_PAGE = 5  # Обращений на странице
MAX_MESSAGE_LENGTH = 4000

# Настройки фильтра спама (МЯГКАЯ ВЕРСИЯ)
SPAM_THRESHOLD_WARNING = 5  # Предупреждение при 5+ сообщениях
SPAM_THRESHOLD_STRICT = 7   # Строгое предупреждение при 7+ сообщениях
SPAM_THRESHOLD_BLOCK = 10   # Временное ограничение при 10+ сообщениях
SPAM_KEYWORDS = ['спам', 'реклама', 'казино', 'бонус', 'кредит', 'сайт', 'перейди']  # Ключевые слова для спама
# -----------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Используем MemoryStorage для FSM (быстрее)
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

# ----- КЭШ ПОДПИСОК -----
subscription_cache: Dict[int, tuple[bool, float]] = {}

def check_cache(user_id: int) -> Optional[bool]:
    if user_id in subscription_cache:
        status, timestamp = subscription_cache[user_id]
        if time.time() - timestamp < CACHE_TTL:
            return status
    return None

def update_cache(user_id: int, status: bool):
    subscription_cache[user_id] = (status, time.time())

# ----- ФИЛЬТР СПАМА (МЯГКАЯ ВЕРСИЯ) -----
class SpamFilter:
    def __init__(self):
        self.user_messages: Dict[int, List[float]] = {}  # user_id: [timestamps]
        self.user_warnings: Dict[int, int] = {}  # user_id: warnings count
        self.blocked_users: Dict[int, float] = {}  # user_id: unblock_time (только для админской блокировки)
        
    def check_spam(self, user_id: int, text: str = "") -> tuple[bool, str, int]:
        """
        Проверка на спам.
        Возвращает (True/False, причина, количество сообщений)
        True - это спам, False - нормальное сообщение
        """
        # Проверяем не заблокирован ли пользователь администратором
        if user_id in self.blocked_users:
            if time.time() < self.blocked_users[user_id]:
                return True, "ADMIN_BLOCK", 0
            else:
                del self.blocked_users[user_id]
        
        # Проверяем частоту сообщений
        now = time.time()
        if user_id not in self.user_messages:
            self.user_messages[user_id] = []
        
        # Очищаем старые сообщения (старше 1 минуты)
        self.user_messages[user_id] = [t for t in self.user_messages[user_id] if now - t < 60]
        message_count = len(self.user_messages[user_id])
        
        # Проверяем лимиты
        if message_count >= SPAM_THRESHOLD_BLOCK:
            return True, "TOO_MANY_BLOCK", message_count
        elif message_count >= SPAM_THRESHOLD_STRICT:
            return True, "TOO_MANY_STRICT", message_count
        elif message_count >= SPAM_THRESHOLD_WARNING:
            return True, "TOO_MANY_WARNING", message_count
        
        # Проверяем ключевые слова
        if text:
            text_lower = text.lower()
            for keyword in SPAM_KEYWORDS:
                if keyword in text_lower:
                    return True, "KEYWORD", message_count
        
        return False, "", message_count
    
    def add_message(self, user_id: int):
        """Добавляем сообщение в историю"""
        if user_id not in self.user_messages:
            self.user_messages[user_id] = []
        self.user_messages[user_id].append(time.time())
    
    def get_warning_count(self, user_id: int) -> int:
        """Получить количество предупреждений"""
        return self.user_warnings.get(user_id, 0)
    
    def add_warning(self, user_id: int) -> int:
        """Добавить предупреждение, вернуть общее количество"""
        self.user_warnings[user_id] = self.user_warnings.get(user_id, 0) + 1
        return self.user_warnings[user_id]
    
    def get_message_count(self, user_id: int) -> int:
        """Получить количество сообщений за минуту"""
        if user_id not in self.user_messages:
            return 0
        now = time.time()
        self.user_messages[user_id] = [t for t in self.user_messages[user_id] if now - t < 60]
        return len(self.user_messages[user_id])
    
    def admin_block(self, user_id: int, duration: int = 3600):
        """Блокировка пользователя администратором"""
        self.blocked_users[user_id] = time.time() + duration
    
    def admin_unblock(self, user_id: int):
        """Разблокировка пользователя администратором"""
        if user_id in self.blocked_users:
            del self.blocked_users[user_id]
        if user_id in self.user_messages:
            self.user_messages[user_id] = []
        if user_id in self.user_warnings:
            del self.user_warnings[user_id]

spam_filter = SpamFilter()

# ----- ПУЛ ПОДКЛЮЧЕНИЙ К БД -----
class DatabasePool:
    def __init__(self, db_path: str, max_connections: int = 10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections = []
        self._in_use = set()
        self._lock = asyncio.Lock()
        
    @asynccontextmanager
    async def get_connection(self):
        async with self._lock:
            if len(self._connections) < self.max_connections:
                conn = await aiosqlite.connect(self.db_path)
                conn.row_factory = aiosqlite.Row
                self._connections.append(conn)
            conn = self._connections.pop(0)
            self._in_use.add(conn)
        
        try:
            yield conn
        finally:
            async with self._lock:
                self._in_use.remove(conn)
                self._connections.append(conn)
    
    async def close_all(self):
        async with self._lock:
            for conn in self._connections + list(self._in_use):
                try:
                    await conn.close()
                except:
                    pass
            self._connections.clear()
            self._in_use.clear()

db_pool = DatabasePool('bot_database.db')

# ----- АСИНХРОННАЯ БАЗА ДАННЫХ -----
async def init_db():
    try:
        async with db_pool.get_connection() as db:
            # Пользователи (расширенные поля)
            await db.execute('''CREATE TABLE IF NOT EXISTS users
                     (user_id INTEGER PRIMARY KEY, 
                      username TEXT, 
                      first_name TEXT, 
                      last_name TEXT, 
                      reg_date TEXT, 
                      last_activity TEXT,
                      appeals_count INTEGER DEFAULT 0,
                      language TEXT DEFAULT 'ru',
                      is_blocked INTEGER DEFAULT 0,
                      block_reason TEXT,
                      block_date TEXT,
                      block_expires TEXT,
                      notes TEXT,
                      warnings INTEGER DEFAULT 0,
                      trust_score INTEGER DEFAULT 100)''')
            
            # Обращения
            await db.execute('''CREATE TABLE IF NOT EXISTS appeals
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                      user_id INTEGER,
                      category TEXT,
                      message_text TEXT,
                      file_id TEXT,
                      file_type TEXT,
                      date TEXT,
                      status TEXT DEFAULT 'новое',
                      priority TEXT DEFAULT 'средний',
                      assigned_to INTEGER,
                      resolution TEXT,
                      resolution_date TEXT,
                      group_message_id INTEGER,
                      last_notification TEXT,
                      notification_sent INTEGER DEFAULT 0,
                      FOREIGN KEY (user_id) REFERENCES users (user_id))''')
            
            # История статусов (для уведомлений)
            await db.execute('''CREATE TABLE IF NOT EXISTS status_history
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      appeal_id INTEGER,
                      old_status TEXT,
                      new_status TEXT,
                      changed_by INTEGER,
                      change_date TEXT,
                      notification_sent INTEGER DEFAULT 0,
                      FOREIGN KEY (appeal_id) REFERENCES appeals (id))''')
            
            # Ответы админов
            await db.execute('''CREATE TABLE IF NOT EXISTS admin_replies
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      appeal_id INTEGER,
                      admin_id INTEGER,
                      reply_text TEXT,
                      date TEXT,
                      read_by_user INTEGER DEFAULT 0,
                      FOREIGN KEY (appeal_id) REFERENCES appeals (id))''')
            
            # Статистика
            await db.execute('''CREATE TABLE IF NOT EXISTS stats
                     (date TEXT PRIMARY KEY,
                      new_users INTEGER DEFAULT 0,
                      new_appeals INTEGER DEFAULT 0,
                      resolved_appeals INTEGER DEFAULT 0,
                      avg_response_time REAL DEFAULT 0)''')
            
            # Логи действий
            await db.execute('''CREATE TABLE IF NOT EXISTS logs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      user_id INTEGER,
                      action TEXT,
                      details TEXT,
                      date TEXT)''')
            
            # Жалобы на спам
            await db.execute('''CREATE TABLE IF NOT EXISTS spam_reports
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      reported_user INTEGER,
                      reported_by INTEGER,
                      reason TEXT,
                      date TEXT,
                      action_taken TEXT)''')
            
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка инициализации БД: {e}")

# ----- ФУНКЦИИ ДЛЯ РАБОТЫ С БД -----
async def log_action(user_id: int, action: str, details: str = ""):
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                "INSERT INTO logs (user_id, action, details, date) VALUES (?, ?, ?, ?)",
                (user_id, action, details, datetime.now().isoformat())
            )
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка логирования: {e}")

async def add_user(user_id, username, first_name, last_name):
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                """INSERT OR IGNORE INTO users 
                   (user_id, username, first_name, last_name, reg_date, last_activity) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, username, first_name, last_name, 
                 datetime.now().isoformat(), datetime.now().isoformat())
            )
            await db.commit()
            
            # Обновляем статистику
            today = datetime.now().date().isoformat()
            await db.execute(
                "INSERT INTO stats (date, new_users) VALUES (?, 1) "
                "ON CONFLICT(date) DO UPDATE SET new_users = new_users + 1",
                (today,)
            )
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка добавления пользователя: {e}")

async def update_last_activity(user_id: int):
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                "UPDATE users SET last_activity = ? WHERE user_id = ?",
                (datetime.now().isoformat(), user_id)
            )
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка обновления активности: {e}")

async def save_appeal(user_id, category, message_text, file_id, file_type) -> int:
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                """INSERT INTO appeals 
                   (user_id, category, message_text, file_id, file_type, date) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, category, message_text, file_id, file_type, datetime.now().isoformat())
            )
            await db.commit()
            appeal_id = cursor.lastrowid
            
            # Обновляем статистику
            today = datetime.now().date().isoformat()
            await db.execute(
                "INSERT INTO stats (date, new_appeals) VALUES (?, 1) "
                "ON CONFLICT(date) DO UPDATE SET new_appeals = new_appeals + 1",
                (today,)
            )
            await db.commit()
            
            return appeal_id
    except Exception as e:
        logging.error(f"Ошибка сохранения обращения: {e}")
        return 0

async def update_appeal_status(appeal_id, status, admin_id=None):
    try:
        async with db_pool.get_connection() as db:
            # Получаем старый статус
            cursor = await db.execute(
                "SELECT status, user_id FROM appeals WHERE id=?",
                (appeal_id,)
            )
            appeal = await cursor.fetchone()
            old_status = appeal['status']
            user_id = appeal['user_id']
            
            # Обновляем статус
            await db.execute(
                "UPDATE appeals SET status=? WHERE id=?",
                (status, appeal_id)
            )
            
            if admin_id:
                await db.execute(
                    "UPDATE appeals SET assigned_to=? WHERE id=?",
                    (admin_id, appeal_id)
                )
            
            # Сохраняем в историю
            await db.execute(
                """INSERT INTO status_history 
                   (appeal_id, old_status, new_status, changed_by, change_date) 
                   VALUES (?, ?, ?, ?, ?)""",
                (appeal_id, old_status, status, admin_id, datetime.now().isoformat())
            )
            
            await db.commit()
            
            # Отправляем уведомление пользователю
            await send_status_notification(user_id, appeal_id, old_status, status)
            
    except Exception as e:
        logging.error(f"Ошибка обновления статуса: {e}")

async def send_status_notification(user_id: int, appeal_id: int, old_status: str, new_status: str):
    """Отправка уведомления об изменении статуса"""
    status_emojis = {
        'новое': '🆕',
        'в работе': '⏳',
        'рассмотрено': '✅',
        'отклонено': '❌'
    }
    
    status_texts = {
        'новое': 'поступило в обработку',
        'в работе': 'взято в работу',
        'рассмотрено': 'рассмотрено',
        'отклонено': 'отклонено'
    }
    
    emoji = status_emojis.get(new_status, '📌')
    text = status_texts.get(new_status, 'изменен')
    
    notification = (
        f"{emoji} {hbold('Статус обращения изменен')}\n\n"
        f"📋 Обращение #{appeal_id}\n"
        f"📊 Новый статус: {hbold(new_status.upper())}\n"
        f"🔄 Предыдущий статус: {old_status}\n\n"
        f"Следить за обращением можно в разделе {hbold('📋 Мои обращения')}"
    )
    
    try:
        await bot.send_message(user_id, notification, parse_mode=ParseMode.HTML)
        logging.info(f"Уведомление о статусе отправлено пользователю {user_id}")
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")

async def update_group_message_id(appeal_id: int, message_id: int):
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                "UPDATE appeals SET group_message_id = ? WHERE id = ?",
                (message_id, appeal_id)
            )
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка обновления message_id: {e}")

async def increment_appeals(user_id):
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                "UPDATE users SET appeals_count = appeals_count + 1 WHERE user_id = ?", 
                (user_id,)
            )
            await db.commit()
    except Exception as e:
        logging.error(f"Ошибка инкремента обращений: {e}")

async def get_user_appeals(user_id, page=1, limit=APPEALS_PER_PAGE):
    offset = (page - 1) * limit
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                """SELECT id, category, date, status, priority 
                   FROM appeals 
                   WHERE user_id=? 
                   ORDER BY date DESC 
                   LIMIT ? OFFSET ?""", 
                (user_id, limit, offset)
            )
            appeals = await cursor.fetchall()
            
            cursor = await db.execute(
                "SELECT COUNT(*) FROM appeals WHERE user_id=?",
                (user_id,)
            )
            total = (await cursor.fetchone())[0]
            
            return appeals, total
    except Exception as e:
        logging.error(f"Ошибка получения обращений: {e}")
        return [], 0

async def block_user(user_id: int, admin_id: int, reason: str = "", duration: int = None):
    """Блокировка пользователя"""
    try:
        async with db_pool.get_connection() as db:
            expires = None
            if duration:
                expires = (datetime.now() + timedelta(seconds=duration)).isoformat()
            
            await db.execute(
                """UPDATE users SET 
                   is_blocked=1, 
                   block_reason=?,
                   block_date=?,
                   block_expires=?
                   WHERE user_id=?""",
                (reason, datetime.now().isoformat(), expires, user_id)
            )
            await db.commit()
            
            # Блокируем в спам-фильтре
            if duration:
                spam_filter.admin_block(user_id, duration)
            else:
                spam_filter.admin_block(user_id, 365*24*3600)  # Навсегда
            
            await log_action(admin_id, "block_user", f"Заблокирован пользователь {user_id}, причина: {reason}")
            
            # Уведомляем пользователя
            try:
                block_text = f"❌ {hbold('Вы заблокированы')}\n\n"
                block_text += f"Причина: {reason}\n"
                if duration:
                    block_text += f"Срок: {duration // 60} минут"
                await bot.send_message(user_id, block_text, parse_mode=ParseMode.HTML)
            except:
                pass
                
            return True
    except Exception as e:
        logging.error(f"Ошибка блокировки пользователя: {e}")
        return False

async def unblock_user(user_id: int, admin_id: int):
    """Разблокировка пользователя"""
    try:
        async with db_pool.get_connection() as db:
            await db.execute(
                "UPDATE users SET is_blocked=0, block_reason=NULL, block_expires=NULL WHERE user_id=?",
                (user_id,)
            )
            await db.commit()
            
            # Разблокируем в спам-фильтре
            spam_filter.admin_unblock(user_id)
            
            await log_action(admin_id, "unblock_user", f"Разблокирован пользователь {user_id}")
            
            # Уведомляем пользователя
            try:
                await bot.send_message(
                    user_id, 
                    f"✅ {hbold('Вы разблокированы')}\n\nВы снова можете пользоваться ботом.",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
                
            return True
    except Exception as e:
        logging.error(f"Ошибка разблокировки пользователя: {e}")
        return False

async def get_user_info(user_id: int) -> Optional[dict]:
    """Получение полной информации о пользователе"""
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                """SELECT * FROM users WHERE user_id=?""",
                (user_id,)
            )
            user = await cursor.fetchone()
            
            if not user:
                return None
            
            # Получаем обращения пользователя
            cursor = await db.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN status='новое' THEN 1 ELSE 0 END) as new,
                          SUM(CASE WHEN status='в работе' THEN 1 ELSE 0 END) as in_progress,
                          SUM(CASE WHEN status='рассмотрено' THEN 1 ELSE 0 END) as resolved
                   FROM appeals WHERE user_id=?""",
                (user_id,)
            )
            stats = await cursor.fetchone()
            
            # Последние действия
            cursor = await db.execute(
                """SELECT action, details, date FROM logs 
                   WHERE user_id=? ORDER BY date DESC LIMIT 5""",
                (user_id,)
            )
            last_actions = await cursor.fetchall()
            
            return {
                'user': dict(user),
                'appeals': dict(stats),
                'last_actions': [dict(a) for a in last_actions]
            }
    except Exception as e:
        logging.error(f"Ошибка получения информации о пользователе: {e}")
        return None

async def add_user_note(user_id: int, admin_id: int, note: str):
    """Добавление заметки о пользователе"""
    try:
        async with db_pool.get_connection() as db:
            # Получаем текущие заметки
            cursor = await db.execute(
                "SELECT notes FROM users WHERE user_id=?",
                (user_id,)
            )
            result = await cursor.fetchone()
            current_notes = result['notes'] if result and result['notes'] else ""
            
            # Добавляем новую заметку с датой
            new_note = f"[{datetime.now().strftime('%d.%m.%Y %H:%M')}] {note}\n"
            updated_notes = current_notes + new_note if current_notes else new_note
            
            await db.execute(
                "UPDATE users SET notes=? WHERE user_id=?",
                (updated_notes, user_id)
            )
            await db.commit()
            
            await log_action(admin_id, "add_note", f"Добавлена заметка для {user_id}")
            return True
    except Exception as e:
        logging.error(f"Ошибка добавления заметки: {e}")
        return False

async def get_admin_stats():
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_blocked=0")
            total_users = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM appeals")
            total_appeals = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM appeals WHERE status='новое'")
            new_appeals = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM appeals WHERE status='в работе'")
            in_progress = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM appeals WHERE status='рассмотрено'")
            resolved = (await cursor.fetchone())[0]
            
            cursor = await db.execute(
                "SELECT category, COUNT(*) FROM appeals GROUP BY category"
            )
            rows = await cursor.fetchall()
            categories = {row['category']: row[1] for row in rows}
            
            day_ago = (datetime.now() - timedelta(days=1)).isoformat()
            cursor = await db.execute(
                "SELECT COUNT(*) FROM users WHERE last_activity > ?",
                (day_ago,)
            )
            active_users = (await cursor.fetchone())[0]
            
            return {
                'total_users': total_users,
                'total_appeals': total_appeals,
                'new_appeals': new_appeals,
                'in_progress': in_progress,
                'resolved': resolved,
                'active_users': active_users,
                'categories': categories
            }
    except Exception as e:
        logging.error(f"Ошибка получения статистики: {e}")
        return {
            'total_users': 0, 'total_appeals': 0, 'new_appeals': 0,
            'in_progress': 0, 'resolved': 0, 'active_users': 0, 'categories': {}
        }

async def get_detailed_stats(period: str = "day"):
    """Расширенная статистика"""
    try:
        async with db_pool.get_connection() as db:
            now = datetime.now()
            
            if period == "day":
                start_date = now - timedelta(days=1)
                group_format = "%H:00"
            elif period == "week":
                start_date = now - timedelta(days=7)
                group_format = "%d.%m"
            elif period == "month":
                start_date = now - timedelta(days=30)
                group_format = "%d.%m"
            else:
                start_date = now - timedelta(days=1)
                group_format = "%H:00"
            
            start_iso = start_date.isoformat()
            
            # Общая статистика
            cursor = await db.execute(
                """SELECT 
                    COUNT(*) as total_appeals,
                    AVG(CASE WHEN status='рассмотрено' THEN 
                        (julianday(resolution_date) - julianday(date)) * 24 * 60 
                        ELSE NULL END) as avg_response_minutes,
                    COUNT(DISTINCT user_id) as unique_users
                   FROM appeals 
                   WHERE date > ?""",
                (start_iso,)
            )
            general = await cursor.fetchone()
            
            # Статистика по статусам
            cursor = await db.execute(
                """SELECT status, COUNT(*) as count 
                   FROM appeals WHERE date > ? 
                   GROUP BY status""",
                (start_iso,)
            )
            status_stats = {row['status']: row[1] for row in await cursor.fetchall()}
            
            # Статистика по категориям
            cursor = await db.execute(
                """SELECT category, COUNT(*) as count 
                   FROM appeals WHERE date > ? 
                   GROUP BY category""",
                (start_iso,)
            )
            category_stats = {row['category']: row[1] for row in await cursor.fetchall()}
            
            # Динамика по дням/часам
            cursor = await db.execute(
                f"""SELECT strftime('{group_format}', date) as period,
                          COUNT(*) as count
                   FROM appeals 
                   WHERE date > ? 
                   GROUP BY period
                   ORDER BY period""",
                (start_iso,)
            )
            timeline = await cursor.fetchall()
            
            # Активность админов
            cursor = await db.execute(
                """SELECT u.username, COUNT(*) as actions
                   FROM logs l
                   JOIN users u ON l.user_id = u.user_id
                   WHERE l.action LIKE 'admin_%' AND l.date > ?
                   GROUP BY l.user_id
                   ORDER BY actions DESC
                   LIMIT 5""",
                (start_iso,)
            )
            admin_activity = await cursor.fetchall()
            
            return {
                'period': period,
                'general': dict(general) if general else {},
                'status_stats': status_stats,
                'category_stats': category_stats,
                'timeline': [dict(t) for t in timeline],
                'admin_activity': [dict(a) for a in admin_activity]
            }
    except Exception as e:
        logging.error(f"Ошибка получения расширенной статистики: {e}")
        return None

# ----- МАШИНА СОСТОЯНИЙ -----
class FeedbackForm(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()
    waiting_for_admin_reply = State()
    waiting_for_broadcast = State()
    waiting_for_user_search = State()
    waiting_for_block_reason = State()
    waiting_for_user_note = State()

# ----- КЛАВИАТУРЫ -----
_main_keyboard = None
_confirm_keyboard = None
_admin_keyboard = None
_user_management_keyboard = None

def get_main_keyboard(user_id: int = None):
    """Основная клавиатура с динамической кнопкой админ-панели"""
    global _main_keyboard
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        KeyboardButton("🚨 Экстремизм"),
        KeyboardButton("💣 Теракт"),
        KeyboardButton("⚖️ Админ. кодекс"),
        KeyboardButton("🔨 УК РФ"),
        KeyboardButton("🕵️ Другое"),
        KeyboardButton("📋 Мои обращения"),
        KeyboardButton("❓ Помощь"),
        KeyboardButton("📢 Канал")
    ]
    
    # Если пользователь администратор - добавляем кнопку админ-панели
    if user_id and user_id in ADMIN_IDS:
        buttons.append(KeyboardButton("🔐 Административная панель"))
    
    keyboard.add(*buttons)
    return keyboard

def get_confirm_keyboard():
    global _confirm_keyboard
    if _confirm_keyboard is None:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        buttons = [
            KeyboardButton("✅ Отправить"),
            KeyboardButton("✏️ Редактировать"),
            KeyboardButton("❌ Отмена")
        ]
        keyboard.add(*buttons)
        _confirm_keyboard = keyboard
    return _confirm_keyboard

def get_done_cancel_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        KeyboardButton("✅ Готово"),
        KeyboardButton("❌ Отмена")
    ]
    keyboard.add(*buttons)
    return keyboard

def get_admin_keyboard():
    global _admin_keyboard
    if _admin_keyboard is None:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        buttons = [
            KeyboardButton("📊 Статистика"),
            KeyboardButton("📊 Расширенная статистика"),
            KeyboardButton("👥 Управление пользователями"),
            KeyboardButton("📋 Новые обращения"),
            KeyboardButton("📢 Рассылка"),
            KeyboardButton("🛡️ Антиспам"),
            KeyboardButton("🔍 Поиск"),
            KeyboardButton("◀️ В главное меню")
        ]
        keyboard.add(*buttons)
        _admin_keyboard = keyboard
    return _admin_keyboard

def get_user_management_keyboard():
    global _user_management_keyboard
    if _user_management_keyboard is None:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        buttons = [
            KeyboardButton("🔍 Найти пользователя"),
            KeyboardButton("📋 Заблокированные"),
            KeyboardButton("⚠️ Выдать предупреждение"),
            KeyboardButton("📝 Добавить заметку"),
            KeyboardButton("✅ Разблокировать"),
            KeyboardButton("◀️ Назад")
        ]
        keyboard.add(*buttons)
        _user_management_keyboard = keyboard
    return _user_management_keyboard

def get_appeal_action_keyboard(appeal_id: int, user_id: int):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ В работу", callback_data=f"take_{appeal_id}"),
        InlineKeyboardButton("📝 Ответить", callback_data=f"reply_{user_id}_{appeal_id}"),
        InlineKeyboardButton("❌ Закрыть", callback_data=f"close_{appeal_id}"),
        InlineKeyboardButton("⚠️ Высокий приоритет", callback_data=f"priority_high_{appeal_id}")
    )
    return keyboard

def get_appeals_navigation_keyboard(current_page: int, total_pages: int, user_id: int):
    keyboard = InlineKeyboardMarkup(row_width=3)
    buttons = []
    
    if current_page > 1:
        buttons.append(InlineKeyboardButton("⬅️", callback_data=f"appeals_page_{current_page-1}_{user_id}"))
    
    buttons.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="noop"))
    
    if current_page < total_pages:
        buttons.append(InlineKeyboardButton("➡️", callback_data=f"appeals_page_{current_page+1}_{user_id}"))
    
    keyboard.add(*buttons)
    return keyboard

def get_user_action_keyboard(user_id: int):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📊 Статистика", callback_data=f"user_stats_{user_id}"),
        InlineKeyboardButton("⚠️ Заблокировать", callback_data=f"user_block_{user_id}"),
        InlineKeyboardButton("📝 Заметка", callback_data=f"user_note_{user_id}"),
        InlineKeyboardButton("📋 Обращения", callback_data=f"user_appeals_{user_id}")
    )
    return keyboard

# ----- ПРОВЕРКА ПОДПИСКИ -----
async def check_subscription(user_id: int) -> bool:
    cached = check_cache(user_id)
    if cached is not None:
        return cached
    
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
        status = member.status not in ['left', 'kicked']
        update_cache(user_id, status)
        return status
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

# ----- МИДЛВАРЬ ДЛЯ ЛОГИРОВАНИЯ АКТИВНОСТИ И МЯГКОЙ ФИЛЬТРАЦИИ СПАМА -----
class ActivityMiddleware(BaseMiddleware):
    async def on_process_message(self, message: types.Message, data: dict):
        user_id = message.from_user.id
        
        # ⭐️ Администраторы не проверяются на спам
        if user_id in ADMIN_IDS:
            asyncio.create_task(update_last_activity(user_id))
            return data
        
        # Добавляем сообщение в историю
        spam_filter.add_message(user_id)
        
        # Проверяем на спам
        is_spam, reason, msg_count = spam_filter.check_spam(user_id, message.text)
        
        if is_spam:
            if reason == "ADMIN_BLOCK":
                # Заблокирован администратором
                await message.answer(
                    f"❌ {hbold('Вы заблокированы администратором')}\n\n"
                    f"Обратитесь к администрации для разблокировки.",
                    parse_mode=ParseMode.HTML
                )
                return {"handled": True}
            
            elif reason == "TOO_MANY_BLOCK":
                # Очень много сообщений - временное ограничение
                warnings = spam_filter.get_warning_count(user_id)
                new_warnings = spam_filter.add_warning(user_id)
                
                await message.answer(
                    f"⚠️ {hbold('Внимание!')}\n\n"
                    f"Вы отправляете слишком много сообщений ({msg_count} в минуту).\n"
                    f"Предупреждение #{new_warnings}\n\n"
                    f"Пожалуйста, подождите немного перед отправкой новых сообщений.",
                    parse_mode=ParseMode.HTML
                )
                
                # Логируем
                await log_action(user_id, "spam_warning", f"Причина: {reason}, сообщений: {msg_count}")
                
            elif reason == "TOO_MANY_STRICT":
                # Много сообщений - строгое предупреждение
                await message.answer(
                    f"⚠️ {hbold('Пожалуйста, не спамьте')}\n\n"
                    f"Вы отправили {msg_count} сообщений за минуту.\n"
                    f"Постарайтесь отправлять сообщения реже.",
                    parse_mode=ParseMode.HTML
                )
                
                await log_action(user_id, "spam_warning", f"Причина: {reason}, сообщений: {msg_count}")
                
            elif reason == "TOO_MANY_WARNING":
                # Легкое предупреждение
                if msg_count > SPAM_THRESHOLD_WARNING:
                    await message.answer(
                        f"ℹ️ {hbold('Не спамьте, пожалуйста')}\n\n"
                        f"Вы отправили {msg_count} сообщений за минуту.",
                        parse_mode=ParseMode.HTML
                    )
                
            elif reason == "KEYWORD":
                # Спам-слова - предупреждение
                await message.answer(
                    f"⚠️ {hbold('Обнаружены подозрительные слова')}\n\n"
                    f"Ваше сообщение содержит слова, характерные для спама.\n"
                    f"Пожалуйста, избегайте таких формулировок.",
                    parse_mode=ParseMode.HTML
                )
                
                await log_action(user_id, "spam_warning", f"Причина: ключевое слово")
            
            # Не блокируем, просто предупреждаем
            return data
        
        # Обновляем активность
        asyncio.create_task(update_last_activity(user_id))
        
        return data
    
    async def on_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        """Обработка callback запросов"""
        user_id = callback_query.from_user.id
        
        # Администраторы не проверяются
        if user_id in ADMIN_IDS:
            asyncio.create_task(update_last_activity(user_id))
            return data
        
        # Обновляем активность
        asyncio.create_task(update_last_activity(user_id))
        
        return data

# Регистрируем middleware
dp.middleware.setup(ActivityMiddleware())

# ----- КОМАНДЫ ДЛЯ УПРАВЛЕНИЯ АНТИСПАМОМ (ДЛЯ АДМИНОВ) -----
@dp.message_handler(commands=['spam'])
async def spam_commands(message: types.Message):
    """Справка по командам антиспама"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    text = (
        f"{hbold('🛡️ Команды антиспама')}\n\n"
        f"/spam_stats - статистика спама\n"
        f"/spam_reset [user_id] - сбросить предупреждения пользователя\n"
        f"/spam_unblock [user_id] - разблокировать пользователя\n"
        f"/spam_block [user_id] [минуты] - заблокировать пользователя\n"
    )
    
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['spam_stats'])
async def spam_stats_command(message: types.Message):
    """Статистика антиспама"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    # Собираем статистику
    active_users = len(spam_filter.user_messages)
    warned_users = len(spam_filter.user_warnings)
    blocked_users = len(spam_filter.blocked_users)
    
    # Топ спамеров
    top_spammers = []
    for user_id, messages in spam_filter.user_messages.items():
        msg_count = len([t for t in messages if time.time() - t < 60])
        if msg_count > 3:
            top_spammers.append((user_id, msg_count, spam_filter.user_warnings.get(user_id, 0)))
    
    top_spammers.sort(key=lambda x: x[1], reverse=True)
    
    text = f"{hbold('📊 Статистика антиспама')}\n\n"
    text += f"• Активных пользователей: {active_users}\n"
    text += f"• С предупреждениями: {warned_users}\n"
    text += f"• Заблокировано админами: {blocked_users}\n\n"
    text += f"• Порог предупреждения: {SPAM_THRESHOLD_WARNING} сообщ/мин\n"
    text += f"• Строгое предупреждение: {SPAM_THRESHOLD_STRICT} сообщ/мин\n"
    text += f"• Временное ограничение: {SPAM_THRESHOLD_BLOCK} сообщ/мин\n\n"
    
    if top_spammers:
        text += f"{hbold('Активные пользователи:')}\n"
        for user_id, count, warnings in top_spammers[:10]:
            text += f"• ID {user_id}: {count} сообщ/мин, предупреждений: {warnings}\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(commands=['spam_reset'])
async def spam_reset_command(message: types.Message):
    """Сброс предупреждений для пользователя"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    args = message.get_args()
    if not args or not args.isdigit():
        await message.answer("❌ Используйте: /spam_reset [user_id]")
        return
    
    user_id = int(args)
    
    if user_id in spam_filter.user_messages:
        spam_filter.user_messages[user_id] = []
    if user_id in spam_filter.user_warnings:
        del spam_filter.user_warnings[user_id]
    
    await message.answer(f"✅ Предупреждения для пользователя {user_id} сброшены")
    await log_action(message.from_user.id, "spam_reset", f"Сброс для {user_id}")

@dp.message_handler(commands=['spam_unblock'])
async def spam_unblock_command(message: types.Message):
    """Разблокировка пользователя"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    args = message.get_args()
    if not args or not args.isdigit():
        await message.answer("❌ Используйте: /spam_unblock [user_id]")
        return
    
    user_id = int(args)
    
    spam_filter.admin_unblock(user_id)
    await unblock_user(user_id, message.from_user.id)
    
    await message.answer(f"✅ Пользователь {user_id} разблокирован")

@dp.message_handler(commands=['spam_block'])
async def spam_block_command(message: types.Message):
    """Блокировка пользователя"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    args = message.get_args().split()
    if len(args) < 1:
        await message.answer("❌ Используйте: /spam_block [user_id] [минуты]")
        return
    
    user_id = int(args[0])
    minutes = int(args[1]) if len(args) > 1 else 60
    
    await block_user(user_id, message.from_user.id, "Заблокирован за спам", minutes * 60)
    await message.answer(f"✅ Пользователь {user_id} заблокирован на {minutes} минут")

# ----- ОБРАБОТЧИКИ КОМАНД -----
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    # Проверяем не заблокирован ли пользователь в БД
    async with db_pool.get_connection() as db:
        cursor = await db.execute(
            "SELECT is_blocked FROM users WHERE user_id=?",
            (user_id,)
        )
        user = await cursor.fetchone()
        if user and user['is_blocked']:
            await message.answer("❌ Вы заблокированы в боте.")
            return
    
    asyncio.create_task(add_user(
        user_id, 
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    ))
    
    await log_action(user_id, "start", "Запуск бота")
    
    if await check_subscription(user_id):
        welcome_text = (
            f"👋 {hbold('Добро пожаловать в бот Информационной Службы Безопасности')}\n\n"
            f"📌 Здесь вы можете анонимно и конфиденциально сообщить о:\n"
            f"• Экстремистских проявлениях\n"
            f"• Подготовке терактов\n"
            f"• Административных правонарушениях\n"
            f"• Преступлениях по УК РФ\n\n"
            f"🛡 Вся информация строго конфиденциальна.\n"
            f"⏰ Работаем круглосуточно.\n\n"
            f"Выберите нужный раздел на клавиатуре ниже:"
        )
        await message.answer(
            welcome_text,
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_keyboard(user_id)
        )
    else:
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("📢 Подписаться на канал", url=f"https://t.me/{REQUIRED_CHANNEL[1:]}")
        )
        await message.answer(
            f"❌ {hbold('Для использования бота необходимо подписаться на канал')}\n"
            f"{REQUIRED_CHANNEL}\n\n"
            "После подписки нажмите /start снова.",
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard
        )

@dp.message_handler(commands=['id'])
async def cmd_id(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    username = message.from_user.username or "отсутствует"
    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    
    if message.chat.type == 'private':
        chat_type = "Личный чат"
    elif message.chat.type == 'group':
        chat_type = "Группа"
    elif message.chat.type == 'supergroup':
        chat_type = "Супергруппа"
    else:
        chat_type = message.chat.type
    
    text = (
        f"📋 {hbold('Информация о пользователе')}\n\n"
        f"🆔 {hbold('Ваш ID:')} {hcode(str(user_id))}\n"
        f"👤 {hbold('Username:')} @{username}\n"
        f"📝 {hbold('Имя:')} {first_name} {last_name}\n\n"
        f"💬 {hbold('Информация о чате:')}\n"
        f"🆔 {hbold('ID чата:')} {hcode(str(chat_id))}\n"
        f"📌 {hbold('Тип чата:')} {chat_type}"
    )
    
    await message.answer(text, parse_mode=ParseMode.HTML)
    await log_action(user_id, "id_command", f"Запросил ID в чате {chat_id}")

@dp.message_handler(commands=['admin'])
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой команде")
        return
    
    text = f"{hbold('🔐 Административная панель')}\n\nВыберите раздел:"
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_admin_keyboard())

# ----- ОБРАБОТЧИК КНОПКИ АДМИН-ПАНЕЛИ -----
@dp.message_handler(lambda message: message.text == "🔐 Административная панель")
async def admin_panel_button(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к этой панели")
        return
    
    await admin_panel(message)

# ----- ОБРАБОТЧИК ВОЗВРАТА В ГЛАВНОЕ МЕНЮ -----
@dp.message_handler(lambda message: message.text == "◀️ В главное меню")
async def back_to_main_menu(message: types.Message):
    user_id = message.from_user.id
    await message.answer(
        "👋 Возврат в главное меню",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard(user_id)
    )

@dp.message_handler(lambda message: message.text == "📢 Канал")
async def cmd_channel(message: types.Message):
    keyboard = InlineKeyboardMarkup().add(
        InlineKeyboardButton("📢 Перейти на канал", url=f"https://t.me/{REQUIRED_CHANNEL[1:]}")
    )
    await message.answer(
        f"Наш официальный канал: {REQUIRED_CHANNEL}\n"
        "Подпишитесь, чтобы быть в курсе важных новостей!",
        reply_markup=keyboard
    )

@dp.message_handler(lambda message: message.text == "❓ Помощь")
async def cmd_help(message: types.Message):
    help_text = (
        f"{hbold('📌 Как пользоваться ботом:')}\n\n"
        f"1️⃣ Выберите категорию обращения на клавиатуре\n"
        f"2️⃣ Опишите ситуацию максимально подробно\n"
        f"3️⃣ Приложите фото/видео (если есть)\n"
        f"4️⃣ Нажмите {hbold('✅ Готово')}\n"
        f"5️⃣ Подтвердите отправку\n\n"
        f"{hbold('📎 Что можно отправлять:')}\n"
        f"• Текст\n"
        f"• Фотографии\n"
        f"• Видео\n"
        f"• Документы\n\n"
        f"{hbold('🛡 Конфиденциальность гарантируется')}\n"
        f"{hbold('ℹ️ Дополнительно:')}\n"
        f"• /start - перезапустить бота\n"
        f"• /id - узнать свой ID\n"
        f"• /cancel - отменить текущее действие"
    )
    await message.answer(help_text, parse_mode=ParseMode.HTML)

@dp.message_handler(lambda message: message.text == "📋 Мои обращения")
async def cmd_my_appeals(message: types.Message):
    await show_appeals_page(message, 1)

async def show_appeals_page(message: types.Message, page: int):
    appeals, total = await get_user_appeals(message.from_user.id, page)
    total_pages = (total + APPEALS_PER_PAGE - 1) // APPEALS_PER_PAGE
    
    if not appeals:
        await message.answer("У вас пока нет обращений.")
        return
    
    text = f"{hbold('📋 Ваши обращения:')}\n\n"
    
    for appeal in appeals:
        status_emoji = {
            'новое': '🆕',
            'в работе': '⏳',
            'рассмотрено': '✅',
            'отклонено': '❌'
        }.get(appeal['status'], '📌')
        
        priority_emoji = {
            'высокий': '🔴',
            'средний': '🟡',
            'низкий': '🟢'
        }.get(appeal['priority'], '⚪')
        
        date = datetime.fromisoformat(appeal['date']).strftime('%d.%m.%Y %H:%M')
        appeal_id = appeal['id']
        text += (
            f"{status_emoji} {priority_emoji} {hbold(f'#{appeal_id}')}\n"
            f"📂 {appeal['category']}\n"
            f"📅 {date}\n"
            f"📊 Статус: {appeal['status']}\n"
            f"─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n"
        )
    
    keyboard = get_appeals_navigation_keyboard(page, total_pages, message.from_user.id)
    
    await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('appeals_page_'))
async def process_appeals_page(callback_query: types.CallbackQuery):
    parts = callback_query.data.split('_')
    page = int(parts[2])
    user_id = int(parts[3])
    
    if callback_query.from_user.id != user_id:
        await bot.answer_callback_query(callback_query.id, "Это не ваши обращения", show_alert=True)
        return
    
    appeals, total = await get_user_appeals(user_id, page)
    total_pages = (total + APPEALS_PER_PAGE - 1) // APPEALS_PER_PAGE
    
    text = f"{hbold('📋 Ваши обращения:')}\n\n"
    
    for appeal in appeals:
        status_emoji = {
            'новое': '🆕',
            'в работе': '⏳',
            'рассмотрено': '✅',
            'отклонено': '❌'
        }.get(appeal['status'], '📌')
        
        priority_emoji = {
            'высокий': '🔴',
            'средний': '🟡',
            'низкий': '🟢'
        }.get(appeal['priority'], '⚪')
        
        date = datetime.fromisoformat(appeal['date']).strftime('%d.%m.%Y %H:%M')
        appeal_id = appeal['id']
        text += (
            f"{status_emoji} {priority_emoji} {hbold(f'#{appeal_id}')}\n"
            f"📂 {appeal['category']}\n"
            f"📅 {date}\n"
            f"📊 Статус: {appeal['status']}\n"
            f"─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n"
        )
    
    keyboard = get_appeals_navigation_keyboard(page, total_pages, user_id)
    
    await bot.edit_message_text(
        text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )
    
    await bot.answer_callback_query(callback_query.id)

# ----- ОБРАБОТКА ВЫБОРА КАТЕГОРИЙ -----
@dp.message_handler(lambda message: message.text in [
    "🚨 Экстремизм", "💣 Теракт", "⚖️ Админ. кодекс", 
    "🔨 УК РФ", "🕵️ Другое"
])
async def process_category_choice(message: types.Message, state: FSMContext):
    if not await check_subscription(message.from_user.id):
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("📢 Подписаться на канал", url=f"https://t.me/{REQUIRED_CHANNEL[1:]}")
        )
        await message.answer(
            f"❌ Подписка на канал {REQUIRED_CHANNEL} обязательна!",
            reply_markup=keyboard
        )
        return
    
    category_map = {
        "🚨 Экстремизм": "🚨 Экстремизм",
        "💣 Теракт": "💣 Теракт",
        "⚖️ Админ. кодекс": "⚖️ Административные правонарушения",
        "🔨 УК РФ": "🔨 Преступления УК РФ",
        "🕵️ Другое": "🕵️ Другое / Конфиденциально"
    }
    
    category = category_map.get(message.text)
    await state.update_data(category=category, messages=[])
    
    instruction_text = (
        f"{hbold('Вы выбрали:')} {category}\n\n"
        f"{hbold('📝 Опишите ситуацию максимально подробно:')}\n"
        f"• Что произошло?\n"
        f"• Где и когда?\n"
        f"• Кто участники?\n"
        f"• Есть ли свидетели?\n"
        f"• Любая другая важная информация\n\n"
        f"{hbold('📎 Можно прикрепить:')}\n"
        f"• Фотографии\n"
        f"• Видео\n"
        f"• Документы\n\n"
        f"Когда закончите, нажмите {hbold('✅ Готово')}"
    )
    
    await message.answer(
        instruction_text,
        parse_mode=ParseMode.HTML,
        reply_markup=get_done_cancel_keyboard()
    )
    await FeedbackForm.waiting_for_message.set()

# ----- СБОР СООБЩЕНИЯ -----
@dp.message_handler(content_types=ContentType.ANY, state=FeedbackForm.waiting_for_message)
async def collect_message(message: types.Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer(
            "❌ Действие отменено.",
            reply_markup=get_main_keyboard(message.from_user.id)
        )
        return
    
    if message.text == "✅ Готово":
        await confirm_send(message, state)
        return
    
    data = await state.get_data()
    messages = data.get('messages', [])
    
    if len(messages) >= 10:
        await message.answer("❌ Достигнут лимит материалов (максимум 10)")
        return
    
    content = None
    if message.text:
        if len(message.text) > MAX_MESSAGE_LENGTH:
            await message.answer(f"❌ Текст слишком длинный (макс. {MAX_MESSAGE_LENGTH} символов)")
            return
        content = {'type': 'text', 'content': message.text}
    elif message.photo:
        content = {'type': 'photo', 'content': message.photo[-1].file_id, 'caption': message.caption}
    elif message.video:
        content = {'type': 'video', 'content': message.video.file_id, 'caption': message.caption}
    elif message.document:
        content = {'type': 'document', 'content': message.document.file_id, 'caption': message.caption}
    elif message.voice:
        content = {'type': 'voice', 'content': message.voice.file_id, 'caption': None}
    elif message.location:
        content = {'type': 'location', 'content': f"{message.location.latitude},{message.location.longitude}"}
    else:
        await message.answer("❌ Неподдерживаемый тип файла")
        return
    
    messages.append(content)
    await state.update_data(messages=messages)
    
    await message.answer(
        f"✅ Добавлено ({len(messages)}/10). Можете добавить ещё или нажать {hbold('✅ Готово')}",
        parse_mode=ParseMode.HTML
    )

async def confirm_send(message: types.Message, state: FSMContext):
    data = await state.get_data()
    messages = data.get('messages', [])
    category = data.get('category', 'Без категории')
    
    if not messages:
        await message.answer("❌ Вы ничего не отправили. Опишите ситуацию.")
        return
    
    preview = f"{hbold('📋 Предпросмотр:')}\n\n"
    preview += f"{hbold('Категория:')} {category}\n"
    preview += f"{hbold('Материалов:')} {len(messages)}\n\n"
    preview += f"{hbold('Подтверждаете отправку?')}"
    
    await message.answer(preview, parse_mode=ParseMode.HTML, reply_markup=get_confirm_keyboard())
    await FeedbackForm.waiting_for_confirm.set()

# ----- ОТПРАВКА В ГРУППУ (только текст, без кнопок) -----
@dp.message_handler(lambda message: message.text == "✅ Отправить", state=FeedbackForm.waiting_for_confirm)
async def send_to_group(message: types.Message, state: FSMContext):
    data = await state.get_data()
    messages = data.get('messages', [])
    category = data.get('category', 'Без категории')
    
    user = message.from_user
    
    appeal_id = await save_appeal(
        user.id, 
        category, 
        json.dumps(messages, ensure_ascii=False)[:1000], 
        None, 
        None
    )
    
    if appeal_id == 0:
        await message.answer("❌ Ошибка при сохранении обращения. Попробуйте позже.")
        await state.finish()
        return
    
    await increment_appeals(user.id)
    await log_action(user.id, "create_appeal", f"Appeal #{appeal_id}")
    
    # Отправляем только текст в группу (без кнопок)
    await send_to_group_only_text(user, category, messages, appeal_id)
    
    await message.answer(
        f"✅ {hbold('Ваше обращение принято!')}\n\n"
        f"Номер обращения: #{appeal_id}\n"
        f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Сотрудники ИСБ рассмотрят его в ближайшее время.\n"
        f"Статус обращения можно отследить в разделе {hbold('📋 Мои обращения')}",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_keyboard(user.id)
    )
    await state.finish()

async def send_to_group_only_text(user: types.User, category: str, messages: list, appeal_id: int):
    """Отправка только текстовой информации в группу (без кнопок)"""
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                "SELECT appeals_count FROM users WHERE user_id=?",
                (user.id,)
            )
            result = await cursor.fetchone()
            appeals_count = result[0] if result else 0
        
        header = (
            f"🚨 НОВОЕ ОБРАЩЕНИЕ #{appeal_id}\n\n"
            f"📂 Категория: {category}\n"
            f"👤 От: @{user.username or 'нет username'}\n"
            f"🆔 ID: {user.id}\n"
            f"📊 Обращений пользователя: {appeals_count}\n"
            f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"📎 Материалов: {len(messages)}\n"
            f"{'─' * 20}\n"
        )
        
        group_message = await bot.send_message(GROUP_ID, header)
        await update_group_message_id(appeal_id, group_message.message_id)
        
        for i, item in enumerate(messages, 1):
            caption = f"[{i}/{len(messages)}] {item.get('caption', '')}"[:200]
            
            if item['type'] == 'text':
                await bot.send_message(GROUP_ID, f"📝 {item['content']}")
            elif item['type'] == 'photo':
                await bot.send_photo(GROUP_ID, item['content'], caption=caption)
            elif item['type'] == 'video':
                await bot.send_video(GROUP_ID, item['content'], caption=caption)
            elif item['type'] == 'document':
                await bot.send_document(GROUP_ID, item['content'], caption=caption)
            elif item['type'] == 'voice':
                await bot.send_voice(GROUP_ID, item['content'])
            elif item['type'] == 'location':
                lat, lon = item['content'].split(',')
                await bot.send_location(GROUP_ID, float(lat), float(lon))
        
    except Exception as e:
        logging.error(f"Ошибка отправки в группу: {e}")
        await notify_admins_direct_with_buttons(user, category, messages, appeal_id)

async def notify_admins_direct_with_buttons(user: types.User, category: str, messages: list, appeal_id: int):
    """Отправка админам в личку с кнопками (резервный вариант)"""
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                "SELECT appeals_count FROM users WHERE user_id=?",
                (user.id,)
            )
            result = await cursor.fetchone()
            appeals_count = result[0] if result else 0
        
        header = (
            f"🚨 {hbold('НОВОЕ ОБРАЩЕНИЕ')} #{appeal_id}\n\n"
            f"📂 {hbold('Категория:')} {category}\n"
            f"👤 {hbold('От:')} @{user.username or 'нет username'}\n"
            f"🆔 ID: {hcode(str(user.id))}\n"
            f"📊 {hbold('Обращений:')} {appeals_count}\n"
            f"📅 {hbold('Дата:')} {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"📎 {hbold('Материалов:')} {len(messages)}\n"
            f"{'─' * 20}\n"
        )
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, header, parse_mode=ParseMode.HTML)
                
                for i, item in enumerate(messages, 1):
                    caption = f"[{i}/{len(messages)}] {item.get('caption', '')}"[:200]
                    
                    if item['type'] == 'text':
                        await bot.send_message(admin_id, f"📝 {item['content']}")
                    elif item['type'] == 'photo':
                        await bot.send_photo(admin_id, item['content'], caption=caption)
                    elif item['type'] == 'video':
                        await bot.send_video(admin_id, item['content'], caption=caption)
                    elif item['type'] == 'document':
                        await bot.send_document(admin_id, item['content'], caption=caption)
                    elif item['type'] == 'voice':
                        await bot.send_voice(admin_id, item['content'])
                    elif item['type'] == 'location':
                        lat, lon = item['content'].split(',')
                        await bot.send_location(admin_id, float(lat), float(lon))
                
                await bot.send_message(
                    admin_id,
                    "Выберите действие:",
                    reply_markup=get_appeal_action_keyboard(appeal_id, user.id)
                )
            except Exception as e:
                logging.error(f"Ошибка отправки админу {admin_id}: {e}")
    except Exception as e:
        logging.error(f"Ошибка в notify_admins_direct: {e}")

@dp.message_handler(lambda message: message.text == "✏️ Редактировать", state=FeedbackForm.waiting_for_confirm)
async def edit_appeal(message: types.Message, state: FSMContext):
    await state.update_data(messages=[])
    await message.answer(
        "Отправьте материалы заново.",
        reply_markup=get_done_cancel_keyboard()
    )
    await FeedbackForm.waiting_for_message.set()

# ----- ОБРАБОТКА ДЕЙСТВИЙ АДМИНА -----
@dp.callback_query_handler(lambda c: c.data.startswith('take_'))
async def take_appeal(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    appeal_id = int(callback_query.data.split('_')[1])
    await update_appeal_status(appeal_id, 'в работе', callback_query.from_user.id)
    
    await bot.answer_callback_query(callback_query.id, "Обращение взято в работу")
    await bot.send_message(
        callback_query.from_user.id,
        f"✅ Вы взяли в работу обращение #{appeal_id}"
    )

@dp.callback_query_handler(lambda c: c.data.startswith('close_'))
async def close_appeal(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    appeal_id = int(callback_query.data.split('_')[1])
    await update_appeal_status(appeal_id, 'рассмотрено', callback_query.from_user.id)
    
    await bot.answer_callback_query(callback_query.id, "Обращение закрыто")
    await bot.send_message(
        callback_query.from_user.id,
        f"✅ Обращение #{appeal_id} закрыто"
    )

@dp.callback_query_handler(lambda c: c.data.startswith('priority_high_'))
async def set_high_priority(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    appeal_id = int(callback_query.data.split('_')[2])
    
    async with db_pool.get_connection() as db:
        await db.execute(
            "UPDATE appeals SET priority='высокий' WHERE id=?",
            (appeal_id,)
        )
        await db.commit()
    
    await bot.answer_callback_query(callback_query.id, "Установлен высокий приоритет")
    await bot.send_message(
        callback_query.from_user.id,
        f"⚠️ Обращению #{appeal_id} установлен высокий приоритет"
    )

@dp.callback_query_handler(lambda c: c.data.startswith('reply_'))
async def reply_to_user(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    parts = callback_query.data.split('_')
    user_id = int(parts[1])
    appeal_id = int(parts[2])
    
    await state.update_data(reply_user_id=user_id, reply_appeal_id=appeal_id)
    
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(
        callback_query.from_user.id,
        "✏️ Введите ответ пользователю (или /cancel для отмены):",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_admin_reply.set()

@dp.message_handler(state=FeedbackForm.waiting_for_admin_reply)
async def process_admin_reply(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    if message.text == "❌ Отмена":
        await state.finish()
        await message.answer("❌ Отменено", reply_markup=get_admin_keyboard())
        return
    
    data = await state.get_data()
    user_id = data['reply_user_id']
    appeal_id = data['reply_appeal_id']
    
    await save_admin_reply(appeal_id, message.from_user.id, message.text)
    
    try:
        await bot.send_message(
            user_id,
            f"📩 {hbold('Ответ по вашему обращению')} #{appeal_id}:\n\n{message.text}",
            parse_mode=ParseMode.HTML
        )
        await message.answer("✅ Ответ отправлен пользователю", reply_markup=get_admin_keyboard())
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить ответ: {e}", reply_markup=get_admin_keyboard())
    
    await state.finish()

# ----- АДМИН ПАНЕЛЬ (расширенная) -----
@dp.message_handler(lambda message: message.text == "👥 Управление пользователями")
async def user_management(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    text = f"{hbold('👥 Управление пользователями')}\n\nВыберите действие:"
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_user_management_keyboard())

@dp.message_handler(lambda message: message.text == "◀️ Назад")
async def back_to_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await admin_panel(message)

@dp.message_handler(lambda message: message.text == "🔍 Найти пользователя")
async def find_user(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🔍 Введите ID пользователя или username для поиска:",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_user_search.set()

@dp.message_handler(state=FeedbackForm.waiting_for_user_search)
async def process_user_search(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    if message.text == "❌ Отмена":
        await state.finish()
        await user_management(message)
        return
    
    search_term = message.text.strip()
    user_id = None
    
    # Пытаемся найти по ID
    if search_term.isdigit():
        user_id = int(search_term)
    # Или по username
    elif search_term.startswith('@'):
        username = search_term[1:]
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                "SELECT user_id FROM users WHERE username=?",
                (username,)
            )
            result = await cursor.fetchone()
            if result:
                user_id = result['user_id']
    
    if not user_id:
        await message.answer("❌ Пользователь не найден. Попробуйте другой ID или username.")
        await state.finish()
        await user_management(message)
        return
    
    # Получаем информацию о пользователе
    user_info = await get_user_info(user_id)
    
    if not user_info:
        await message.answer("❌ Пользователь не найден в базе данных.")
        await state.finish()
        await user_management(message)
        return
    
    # Формируем отчет
    user = user_info['user']
    appeals = user_info['appeals']
    
    text = (
        f"{hbold('👤 Информация о пользователе')}\n\n"
        f"🆔 ID: {hcode(str(user['user_id']))}\n"
        f"👤 Username: @{user['username'] or 'нет'}\n"
        f"📝 Имя: {user['first_name'] or ''} {user['last_name'] or ''}\n"
        f"📅 Регистрация: {user['reg_date'][:10] if user['reg_date'] else 'неизвестно'}\n"
        f"⏰ Последняя активность: {user['last_activity'][:16] if user['last_activity'] else 'нет'}\n\n"
        f"{hbold('📊 Статистика обращений:')}\n"
        f"📋 Всего: {appeals['total']}\n"
        f"🆕 Новые: {appeals['new']}\n"
        f"⏳ В работе: {appeals['in_progress']}\n"
        f"✅ Рассмотрено: {appeals['resolved']}\n"
    )
    
    if user['is_blocked']:
        text += f"\n❌ {hbold('СТАТУС: ЗАБЛОКИРОВАН')}\n"
        text += f"Причина: {user['block_reason'] or 'не указана'}\n"
        if user['block_expires']:
            text += f"Истекает: {user['block_expires'][:16]}"
    
    if user['notes']:
        text += f"\n\n{hbold('📝 Заметки:')}\n{user['notes']}"
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_user_action_keyboard(user_id))
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('user_block_'))
async def block_user_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    user_id = int(callback_query.data.split('_')[2])
    
    await bot.answer_callback_query(callback_query.id)
    await state.update_data(block_user_id=user_id)
    
    await bot.send_message(
        callback_query.from_user.id,
        "⚠️ Введите причину блокировки (или /cancel для отмены):\n"
        "Можно указать время в минутах, например: спам 60",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_block_reason.set()

@dp.message_handler(state=FeedbackForm.waiting_for_block_reason)
async def process_block_reason(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    if message.text == "❌ Отмена":
        await state.finish()
        await user_management(message)
        return
    
    data = await state.get_data()
    user_id = data['block_user_id']
    text = message.text
    
    # Парсим причину и время
    duration = 60  # 1 час по умолчанию
    reason = text
    
    # Проверяем, есть ли указание времени
    parts = text.split()
    if len(parts) > 1 and parts[-1].isdigit():
        duration = int(parts[-1])
        reason = ' '.join(parts[:-1])
    
    if await block_user(user_id, message.from_user.id, reason, duration * 60):
        await message.answer(f"✅ Пользователь {user_id} заблокирован на {duration} минут.\nПричина: {reason}")
    else:
        await message.answer("❌ Ошибка при блокировке пользователя.")
    
    await state.finish()
    await user_management(message)

@dp.callback_query_handler(lambda c: c.data.startswith('user_note_'))
async def add_note_callback(callback_query: types.CallbackQuery, state: FSMContext):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    user_id = int(callback_query.data.split('_')[2])
    
    await bot.answer_callback_query(callback_query.id)
    await state.update_data(note_user_id=user_id)
    
    await bot.send_message(
        callback_query.from_user.id,
        "📝 Введите текст заметки (или /cancel для отмены):",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_user_note.set()

@dp.message_handler(state=FeedbackForm.waiting_for_user_note)
async def process_user_note(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    if message.text == "❌ Отмена":
        await state.finish()
        await user_management(message)
        return
    
    data = await state.get_data()
    user_id = data['note_user_id']
    note = message.text
    
    if await add_user_note(user_id, message.from_user.id, note):
        await message.answer(f"✅ Заметка добавлена для пользователя {user_id}")
    else:
        await message.answer("❌ Ошибка при добавлении заметки.")
    
    await state.finish()
    await user_management(message)

@dp.message_handler(lambda message: message.text == "📋 Заблокированные")
async def list_blocked_users(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    async with db_pool.get_connection() as db:
        cursor = await db.execute(
            """SELECT user_id, username, first_name, block_reason, block_expires 
               FROM users WHERE is_blocked=1 ORDER BY block_date DESC LIMIT 20"""
        )
        users = await cursor.fetchall()
    
    if not users:
        await message.answer("✅ Нет заблокированных пользователей.")
        return
    
    text = f"{hbold('📋 Заблокированные пользователи:')}\n\n"
    
    for user in users:
        text += f"🆔 {hcode(str(user['user_id']))}\n"
        text += f"👤 @{user['username'] or 'нет'} {user['first_name'] or ''}\n"
        text += f"⚠️ Причина: {user['block_reason'] or 'не указана'}\n"
        if user['block_expires']:
            expires = datetime.fromisoformat(user['block_expires']).strftime('%d.%m.%Y %H:%M')
            text += f"⏰ Истекает: {expires}\n"
        text += "─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(lambda message: message.text == "✅ Разблокировать")
async def unblock_user_prompt(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🔍 Введите ID пользователя для разблокировки:",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_user_search.set()
    await state.update_data(action="unblock")

@dp.message_handler(lambda message: message.text == "📊 Статистика")
async def admin_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats = await get_admin_stats()
    
    text = (
        f"{hbold('📊 Статистика бота')}\n\n"
        f"👥 {hbold('Пользователи:')}\n"
        f"• Всего: {stats['total_users']}\n"
        f"• Активных (24ч): {stats['active_users']}\n\n"
        f"📩 {hbold('Обращения:')}\n"
        f"• Всего: {stats['total_appeals']}\n"
        f"• Новые: {stats['new_appeals']}\n"
        f"• В работе: {stats['in_progress']}\n"
        f"• Рассмотрено: {stats['resolved']}\n\n"
        f"📂 {hbold('По категориям:')}\n"
    )
    
    for cat, count in stats['categories'].items():
        text += f"• {cat}: {count}\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(lambda message: message.text == "📊 Расширенная статистика")
async def admin_detailed_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    stats_day = await get_detailed_stats("day")
    stats_week = await get_detailed_stats("week")
    
    if not stats_day or not stats_week:
        await message.answer("❌ Ошибка получения статистики")
        return
    
    text = f"{hbold('📊 Расширенная статистика')}\n\n"
    
    text += f"{hbold('За последние 24 часа:')}\n"
    text += f"• Обращений: {stats_day['general'].get('total_appeals', 0)}\n"
    text += f"• Пользователей: {stats_day['general'].get('unique_users', 0)}\n"
    avg_response = stats_day['general'].get('avg_response_minutes', 0)
    if avg_response:
        text += f"• Среднее время ответа: {avg_response:.1f} мин.\n"
    
    text += f"\n{hbold('За последние 7 дней:')}\n"
    text += f"• Обращений: {stats_week['general'].get('total_appeals', 0)}\n"
    text += f"• Пользователей: {stats_week['general'].get('unique_users', 0)}\n\n"
    
    text += f"{hbold('Статусы (7 дней):')}\n"
    for status, count in stats_week['status_stats'].items():
        text += f"• {status}: {count}\n"
    
    text += f"\n{hbold('Категории (7 дней):')}\n"
    for cat, count in stats_week['category_stats'].items():
        text += f"• {cat}: {count}\n"
    
    if stats_week['admin_activity']:
        text += f"\n{hbold('Активность админов:')}\n"
        for admin in stats_week['admin_activity']:
            text += f"• @{admin['username']}: {admin['actions']} действий\n"
    
    # Кнопки для выбора периода
    keyboard = InlineKeyboardMarkup(row_width=3)
    keyboard.add(
        InlineKeyboardButton("📅 День", callback_data="stats_day"),
        InlineKeyboardButton("📅 Неделя", callback_data="stats_week"),
        InlineKeyboardButton("📅 Месяц", callback_data="stats_month")
    )
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith('stats_'))
async def stats_period(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "У вас нет прав", show_alert=True)
        return
    
    period = callback_query.data.split('_')[1]
    period_map = {
        'day': 'день',
        'week': 'неделю',
        'month': 'месяц'
    }
    
    stats = await get_detailed_stats(period)
    
    if not stats:
        await bot.answer_callback_query(callback_query.id, "Ошибка получения статистики")
        return
    
    text = f"{hbold(f'📊 Статистика за {period_map[period]}')}\n\n"
    text += f"• Обращений: {stats['general'].get('total_appeals', 0)}\n"
    text += f"• Пользователей: {stats['general'].get('unique_users', 0)}\n"
    
    if stats['general'].get('avg_response_minutes'):
        text += f"• Среднее время ответа: {stats['general']['avg_response_minutes']:.1f} мин.\n"
    
    text += f"\n{hbold('По статусам:')}\n"
    for status, count in stats['status_stats'].items():
        text += f"• {status}: {count}\n"
    
    await bot.edit_message_text(
        text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        parse_mode=ParseMode.HTML
    )
    await bot.answer_callback_query(callback_query.id)

@dp.message_handler(lambda message: message.text == "📋 Новые обращения")
async def admin_new_appeals(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                """SELECT a.*, u.username, u.first_name 
                   FROM appeals a 
                   JOIN users u ON a.user_id = u.user_id 
                   WHERE a.status='новое' 
                   ORDER BY a.date DESC 
                   LIMIT 10"""
            )
            appeals = await cursor.fetchall()
        
        if not appeals:
            await message.answer("✅ Новых обращений нет")
            return
        
        for appeal in appeals:
            appeal_id = appeal['id']
            text = (
                f"🆕 {hbold(f'Обращение #{appeal_id}')}\n"
                f"📂 Категория: {appeal['category']}\n"
                f"👤 От: @{appeal['username'] or 'нет'} ({appeal['first_name']})\n"
                f"📅 {datetime.fromisoformat(appeal['date']).strftime('%d.%m.%Y %H:%M')}\n"
            )
            
            await message.answer(text, parse_mode=ParseMode.HTML)
            await message.answer(
                "Действия:",
                reply_markup=get_appeal_action_keyboard(appeal['id'], appeal['user_id'])
            )
    except Exception as e:
        logging.error(f"Ошибка получения новых обращений: {e}")
        await message.answer("❌ Ошибка при загрузке обращений")

@dp.message_handler(lambda message: message.text == "📢 Рассылка")
async def broadcast_start(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "✏️ Введите сообщение для рассылки всем пользователям:",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )
    await FeedbackForm.waiting_for_broadcast.set()

@dp.message_handler(state=FeedbackForm.waiting_for_broadcast)
async def broadcast_process(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await state.finish()
        return
    
    if message.text == "❌ Отмена":
        await state.finish()
        await admin_panel(message)
        return
    
    try:
        async with db_pool.get_connection() as db:
            cursor = await db.execute(
                "SELECT user_id FROM users WHERE is_blocked=0"
            )
            users = await cursor.fetchall()
        
        sent = 0
        failed = 0
        status_msg = await message.answer(f"📢 Начинаю рассылку {len(users)} пользователям...")
        
        for i, user in enumerate(users):
            try:
                await bot.send_message(user[0], message.text, parse_mode=ParseMode.HTML)
                sent += 1
                if i % 10 == 0:
                    await status_msg.edit_text(f"📢 Прогресс: {i}/{len(users)} (отправлено: {sent})")
                await asyncio.sleep(0.05)
            except Exception as e:
                failed += 1
                logging.error(f"Ошибка рассылки пользователю {user[0]}: {e}")
        
        await status_msg.edit_text(f"✅ Рассылка завершена\n📨 Отправлено: {sent}\n❌ Ошибок: {failed}")
    except Exception as e:
        logging.error(f"Ошибка рассылки: {e}")
        await message.answer("❌ Ошибка при рассылке")
    
    await state.finish()
    await admin_panel(message)

@dp.message_handler(lambda message: message.text == "🛡️ Антиспам")
async def antispam_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    text = f"{hbold('🛡️ Управление антиспамом')}\n\n"
    text += f"• Активных пользователей: {len(spam_filter.user_messages)}\n"
    text += f"• С предупреждениями: {len(spam_filter.user_warnings)}\n"
    text += f"• Заблокировано админами: {len(spam_filter.blocked_users)}\n\n"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("📊 Статистика", callback_data="spam_stats"),
        InlineKeyboardButton("⚙️ Настройки", callback_data="spam_settings"),
        InlineKeyboardButton("📋 Логи", callback_data="spam_logs"),
        InlineKeyboardButton("🔄 Сбросить всё", callback_data="spam_reset_all")
    )
    
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data == "spam_stats")
async def spam_stats_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "Нет прав", show_alert=True)
        return
    
    # Собираем статистику
    active_users = len(spam_filter.user_messages)
    warned_users = len(spam_filter.user_warnings)
    blocked_users = len(spam_filter.blocked_users)
    
    # Топ спамеров
    top_spammers = []
    for user_id, messages in spam_filter.user_messages.items():
        msg_count = len([t for t in messages if time.time() - t < 60])
        if msg_count > 3:
            top_spammers.append((user_id, msg_count, spam_filter.user_warnings.get(user_id, 0)))
    
    top_spammers.sort(key=lambda x: x[1], reverse=True)
    
    text = f"{hbold('📊 Детальная статистика антиспама')}\n\n"
    text += f"• Активных пользователей: {active_users}\n"
    text += f"• С предупреждениями: {warned_users}\n"
    text += f"• Заблокировано админами: {blocked_users}\n\n"
    
    if top_spammers:
        text += f"{hbold('Активные пользователи:')}\n"
        for user_id, count, warnings in top_spammers[:10]:
            text += f"• ID {user_id}: {count} сообщ/мин, предупреждений: {warnings}\n"
    
    await bot.edit_message_text(
        text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        parse_mode=ParseMode.HTML
    )
    await bot.answer_callback_query(callback_query.id)

@dp.callback_query_handler(lambda c: c.data == "spam_reset_all")
async def spam_reset_all_callback(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ADMIN_IDS:
        await bot.answer_callback_query(callback_query.id, "Нет прав", show_alert=True)
        return
    
    # Сбрасываем всё
    spam_filter.user_messages.clear()
    spam_filter.user_warnings.clear()
    
    await bot.edit_message_text(
        "✅ Все предупреждения и история сообщений сброшены",
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )
    await bot.answer_callback_query(callback_query.id, "Сброс выполнен")

@dp.message_handler(lambda message: message.text == "🔍 Поиск")
async def admin_search(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    await message.answer(
        "🔍 Введите ID пользователя или номер обращения для поиска:",
        reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("❌ Отмена"))
    )

# ----- ОБЩИЕ ОБРАБОТЧИКИ -----
@dp.message_handler(commands=['cancel'], state='*')
async def cancel_any(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.finish()
    
    if message.from_user.id in ADMIN_IDS:
        await admin_panel(message)
    else:
        await message.answer("❌ Действие отменено.", reply_markup=get_main_keyboard(message.from_user.id))

@dp.message_handler(lambda message: message.text not in [
    "🚨 Экстремизм", "💣 Теракт", "⚖️ Админ. кодекс", 
    "🔨 УК РФ", "🕵️ Другое", "📋 Мои обращения", 
    "❓ Помощь", "📢 Канал", "📊 Статистика",
    "📊 Расширенная статистика", "👥 Управление пользователями",
    "📋 Новые обращения", "👥 Пользователи", "📢 Рассылка",
    "🔍 Поиск", "◀️ Назад", "🔍 Найти пользователя",
    "📋 Заблокированные", "⚠️ Выдать предупреждение",
    "📝 Добавить заметку", "✅ Разблокировать", "🛡️ Антиспам",
    "🔐 Административная панель", "◀️ В главное меню"
])
async def unknown_command(message: types.Message):
    await message.answer(
        "❌ Неизвестная команда. Используйте кнопки на клавиатуре.",
        reply_markup=get_main_keyboard(message.from_user.id)
    )

# ----- ЗАПУСК -----
async def on_shutdown(dp):
    await db_pool.close_all()
    logging.info("Бот остановлен")

async def on_startup(dp):
    await init_db()
    print("✅ База данных инициализирована")
    print("✅ Мягкий фильтр спама активирован")
    print("✅ Система уведомлений готова")
    print(f"✅ Администраторы: {', '.join(str(id) for id in ADMIN_IDS)}")

if __name__ == '__main__':
    print("🚀 Бот запущен с новыми функциями...")
    print("📢 Обращения отправляются в группу с ID:", GROUP_ID)
    print("👥 Администраторы работают через личные сообщения")
    print("🔐 Кнопка админ-панели появится только у администраторов")
    print("🛡️ Активирован МЯГКИЙ фильтр спама (только предупреждения)")
    print("🔔 Включены уведомления о статусе")
    print("📊 Доступна расширенная статистика")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    executor.start_polling(
        dp, 
        skip_updates=True, 
        timeout=20, 
        relax=0.1, 
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        loop=loop
    )