import asyncpg
from datetime import datetime, date
from typing import Optional, List, Dict
from functools import lru_cache
import json
import os
import time

DATABASE_URL = os.getenv("DATABASE_URL")

# Connection pool
pool: Optional[asyncpg.Pool] = None

# Language cache: {user_id: (language, timestamp)}
_language_cache: Dict[int, tuple] = {}
_LANGUAGE_CACHE_TTL = 300  # 5 minutes


def _get_cached_language(user_id: int) -> Optional[str]:
    """Get language from cache if not expired."""
    if user_id in _language_cache:
        lang, ts = _language_cache[user_id]
        if time.time() - ts < _LANGUAGE_CACHE_TTL:
            return lang
        del _language_cache[user_id]
    return None


def _set_cached_language(user_id: int, language: str):
    """Set language in cache."""
    _language_cache[user_id] = (language, time.time())


async def init_db():
    """Initialize database with all tables."""
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)

    async with pool.acquire() as conn:
        # Users table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                language TEXT DEFAULT 'kk',
                notification_times TEXT DEFAULT '["08:00", "14:00", "21:00"]',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Categories/Folders table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                icon TEXT DEFAULT '📁',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Marathons table (create before habits due to foreign key)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS marathons (
                id SERIAL PRIMARY KEY,
                creator_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                invite_code TEXT UNIQUE NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Habits table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS habits (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
                name TEXT NOT NULL,
                habit_type TEXT NOT NULL CHECK(habit_type IN ('boolean', 'numeric')),
                daily_goal REAL DEFAULT 1,
                unit TEXT DEFAULT '',
                streak INTEGER DEFAULT 0,
                max_streak INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                marathon_id INTEGER REFERENCES marathons(id) ON DELETE SET NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Daily logs table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS habit_logs (
                id SERIAL PRIMARY KEY,
                habit_id INTEGER NOT NULL REFERENCES habits(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                log_date DATE NOT NULL,
                value REAL DEFAULT 0,
                completed INTEGER DEFAULT 0,
                comment TEXT,
                logged_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(habit_id, log_date)
            )
        """)

        # Pending notifications (for 10-min rule)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_notifications (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                habit_id INTEGER NOT NULL REFERENCES habits(id) ON DELETE CASCADE,
                message_id BIGINT,
                chat_id BIGINT,
                sent_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                responded INTEGER DEFAULT 0
            )
        """)

        # Add new columns if they don't exist (migration)
        try:
            await conn.execute("ALTER TABLE habit_logs ADD COLUMN IF NOT EXISTS comment TEXT")
        except:
            pass
        try:
            await conn.execute("ALTER TABLE pending_notifications ADD COLUMN IF NOT EXISTS message_id BIGINT")
            await conn.execute("ALTER TABLE pending_notifications ADD COLUMN IF NOT EXISTS chat_id BIGINT")
        except:
            pass

        # Marathon participants
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS marathon_participants (
                id SERIAL PRIMARY KEY,
                marathon_id INTEGER NOT NULL REFERENCES marathons(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                total_points REAL DEFAULT 0,
                joined_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(marathon_id, user_id)
            )
        """)

        # Marathon habit templates
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS marathon_habits (
                id SERIAL PRIMARY KEY,
                marathon_id INTEGER NOT NULL REFERENCES marathons(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                habit_type TEXT NOT NULL CHECK(habit_type IN ('boolean', 'numeric')),
                daily_goal REAL DEFAULT 1,
                unit TEXT DEFAULT '',
                points_per_goal REAL DEFAULT 1
            )
        """)


# ============ USER FUNCTIONS ============

async def get_or_create_user(user_id: int, username: str = None, first_name: str = None) -> dict:
    """Get existing user or create new one."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

        if row:
            return dict(row)

        await conn.execute(
            "INSERT INTO users (user_id, username, first_name) VALUES ($1, $2, $3)",
            user_id, username, first_name
        )

        # Create default categories
        default_categories = [
            ("🏃 Здоровье", "🏃"),
            ("🕌 Духовное", "🕌"),
            ("📚 Образование", "📚")
        ]
        for name, icon in default_categories:
            await conn.execute(
                "INSERT INTO categories (user_id, name, icon) VALUES ($1, $2, $3)",
                user_id, name, icon
            )

        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return dict(row)


async def update_notification_times(user_id: int, times: List[str]):
    """Update user's notification times."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET notification_times = $1 WHERE user_id = $2",
            json.dumps(times), user_id
        )


async def get_user_notification_times(user_id: int) -> List[str]:
    """Get user's notification times."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT notification_times FROM users WHERE user_id = $1", user_id
        )
        if row:
            return json.loads(row['notification_times'])
        return ["08:00", "14:00", "21:00"]


async def get_all_users() -> List[dict]:
    """Get all users."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users")
        return [dict(row) for row in rows]


async def get_user_language(user_id: int) -> str:
    """Get user's language preference (with caching)."""
    # Check cache first
    cached = _get_cached_language(user_id)
    if cached:
        return cached

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT language FROM users WHERE user_id = $1", user_id
        )
        lang = row['language'] if row and row['language'] else "kk"
        _set_cached_language(user_id, lang)
        return lang


async def set_user_language(user_id: int, language: str):
    """Set user's language preference."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET language = $1 WHERE user_id = $2",
            language, user_id
        )
    # Update cache
    _set_cached_language(user_id, language)


# ============ CATEGORY FUNCTIONS ============

async def get_user_categories(user_id: int) -> List[dict]:
    """Get all categories for a user."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM categories WHERE user_id = $1 ORDER BY name",
            user_id
        )
        return [dict(row) for row in rows]


async def create_category(user_id: int, name: str, icon: str = "📁") -> int:
    """Create a new category."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO categories (user_id, name, icon) VALUES ($1, $2, $3) RETURNING id",
            user_id, name, icon
        )
        return row['id']


async def delete_category(category_id: int):
    """Delete a category."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM categories WHERE id = $1", category_id)


async def get_category(category_id: int) -> Optional[dict]:
    """Get a single category by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM categories WHERE id = $1", category_id)
        return dict(row) if row else None


# ============ HABIT FUNCTIONS ============

async def create_habit(
    user_id: int,
    name: str,
    habit_type: str,
    daily_goal: float = 1,
    unit: str = "",
    category_id: int = None,
    marathon_id: int = None
) -> int:
    """Create a new habit."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO habits (user_id, name, habit_type, daily_goal, unit, category_id, marathon_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
            user_id, name, habit_type, daily_goal, unit, category_id, marathon_id
        )
        return row['id']


async def get_user_habits(user_id: int, active_only: bool = True) -> List[dict]:
    """Get all habits for a user."""
    async with pool.acquire() as conn:
        if active_only:
            rows = await conn.fetch(
                "SELECT * FROM habits WHERE user_id = $1 AND is_active = 1 ORDER BY category_id, name",
                user_id
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM habits WHERE user_id = $1 ORDER BY category_id, name",
                user_id
            )
        return [dict(row) for row in rows]


async def get_habits_by_category(user_id: int, category_id: Optional[int], active_only: bool = True) -> List[dict]:
    """Get habits for a user filtered by category."""
    async with pool.acquire() as conn:
        if category_id is None:
            if active_only:
                rows = await conn.fetch(
                    "SELECT * FROM habits WHERE user_id = $1 AND category_id IS NULL AND is_active = 1 ORDER BY name",
                    user_id
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM habits WHERE user_id = $1 AND category_id IS NULL ORDER BY name",
                    user_id
                )
        else:
            if active_only:
                rows = await conn.fetch(
                    "SELECT * FROM habits WHERE user_id = $1 AND category_id = $2 AND is_active = 1 ORDER BY name",
                    user_id, category_id
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM habits WHERE user_id = $1 AND category_id = $2 ORDER BY name",
                    user_id, category_id
                )
        return [dict(row) for row in rows]


async def get_habit(habit_id: int) -> Optional[dict]:
    """Get a single habit."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM habits WHERE id = $1", habit_id)
        return dict(row) if row else None


async def update_habit(habit_id: int, **kwargs):
    """Update habit fields."""
    async with pool.acquire() as conn:
        for key, value in kwargs.items():
            await conn.execute(
                f"UPDATE habits SET {key} = $1 WHERE id = $2",
                value, habit_id
            )


async def delete_habit(habit_id: int):
    """Delete a habit and its logs."""
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM habit_logs WHERE habit_id = $1", habit_id)
        await conn.execute("DELETE FROM habits WHERE id = $1", habit_id)


# ============ HABIT LOG FUNCTIONS ============

async def log_habit(habit_id: int, user_id: int, value: float, log_date: date = None) -> dict:
    """Log or update a habit entry for a specific date."""
    if log_date is None:
        log_date = date.today()

    async with pool.acquire() as conn:
        # Get habit info
        habit = await conn.fetchrow("SELECT * FROM habits WHERE id = $1", habit_id)
        habit = dict(habit)

        # Check if log exists for today
        existing = await conn.fetchrow(
            "SELECT * FROM habit_logs WHERE habit_id = $1 AND log_date = $2",
            habit_id, log_date
        )

        if existing:
            # Update existing log (add to value)
            new_value = existing['value'] + value
            completed = 1 if new_value >= habit['daily_goal'] else 0
            await conn.execute(
                "UPDATE habit_logs SET value = $1, completed = $2, logged_at = $3 WHERE id = $4",
                new_value, completed, datetime.now(), existing['id']
            )
        else:
            # Create new log
            new_value = value
            completed = 1 if new_value >= habit['daily_goal'] else 0
            await conn.execute(
                "INSERT INTO habit_logs (habit_id, user_id, log_date, value, completed) VALUES ($1, $2, $3, $4, $5)",
                habit_id, user_id, log_date, new_value, completed
            )

        return {
            "habit": habit,
            "new_value": new_value,
            "daily_goal": habit['daily_goal'],
            "completed": completed
        }


async def get_daily_log(habit_id: int, log_date: date = None) -> Optional[dict]:
    """Get log for a specific habit and date."""
    if log_date is None:
        log_date = date.today()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM habit_logs WHERE habit_id = $1 AND log_date = $2",
            habit_id, log_date
        )
        return dict(row) if row else None


async def get_daily_logs_batch(habit_ids: List[int], log_date: date = None) -> Dict[int, dict]:
    """Get logs for multiple habits at once. Returns dict mapping habit_id -> log."""
    if not habit_ids:
        return {}

    if log_date is None:
        log_date = date.today()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM habit_logs WHERE habit_id = ANY($1) AND log_date = $2",
            habit_ids, log_date
        )
        return {row['habit_id']: dict(row) for row in rows}


async def update_log_comment(habit_id: int, comment: str, log_date: date = None):
    """Update comment on a habit log."""
    if log_date is None:
        log_date = date.today()

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE habit_logs SET comment = $1 WHERE habit_id = $2 AND log_date = $3",
            comment, habit_id, log_date
        )


async def get_last_comment(habit_id: int) -> Optional[str]:
    """Get the last comment for a habit (for backwards compatibility)."""
    comments = await get_last_comments(habit_id, limit=1)
    return comments[0]['comment'] if comments else None


async def get_last_comments(habit_id: int, limit: int = 3) -> List[dict]:
    """Get the last N comments for a habit with dates."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT comment, log_date FROM habit_logs
               WHERE habit_id = $1 AND comment IS NOT NULL AND comment != ''
               ORDER BY log_date DESC LIMIT $2""",
            habit_id, limit
        )
        return [dict(row) for row in rows]


async def get_user_daily_logs(user_id: int, log_date: date = None) -> List[dict]:
    """Get all logs for a user on a specific date."""
    if log_date is None:
        log_date = date.today()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT hl.*, h.name, h.habit_type, h.daily_goal, h.unit
               FROM habit_logs hl
               JOIN habits h ON hl.habit_id = h.id
               WHERE hl.user_id = $1 AND hl.log_date = $2""",
            user_id, log_date
        )
        return [dict(row) for row in rows]


async def get_habit_logs_range(habit_id: int, start_date: date, end_date: date) -> List[dict]:
    """Get habit logs for a date range."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM habit_logs
               WHERE habit_id = $1 AND log_date BETWEEN $2 AND $3
               ORDER BY log_date""",
            habit_id, start_date, end_date
        )
        return [dict(row) for row in rows]


# ============ STREAK FUNCTIONS ============

async def update_streak(habit_id: int, completed: bool):
    """Update streak for a habit after end-of-day check."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT streak, max_streak FROM habits WHERE id = $1", habit_id)
        current_streak, max_streak = row['streak'], row['max_streak']

        if completed:
            new_streak = current_streak + 1
            new_max = max(max_streak, new_streak)
        else:
            new_streak = 0
            new_max = max_streak

        await conn.execute(
            "UPDATE habits SET streak = $1, max_streak = $2 WHERE id = $3",
            new_streak, new_max, habit_id
        )

        return new_streak, completed


# ============ PENDING NOTIFICATION FUNCTIONS ============

async def create_pending_notification(user_id: int, habit_id: int, message_id: int = None, chat_id: int = None):
    """Create a pending notification. Uses PostgreSQL NOW() for consistent timezone handling."""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO pending_notifications (user_id, habit_id, sent_at, expires_at, message_id, chat_id)
               VALUES ($1, $2, NOW(), NOW() + INTERVAL '10 minutes', $3, $4)""",
            user_id, habit_id, message_id, chat_id
        )


async def mark_notification_responded(user_id: int, habit_id: int):
    """Mark pending notification as responded."""
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE pending_notifications SET responded = 1
               WHERE user_id = $1 AND habit_id = $2 AND responded = 0""",
            user_id, habit_id
        )


async def get_expired_notifications() -> List[dict]:
    """Get all expired pending notifications that weren't responded."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM pending_notifications
               WHERE responded = 0 AND expires_at < NOW()"""
        )
        return [dict(row) for row in rows]


async def delete_expired_notifications():
    """Delete processed expired notifications."""
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM pending_notifications WHERE responded = 1 OR expires_at < NOW()"
        )


async def get_notification_for_deletion(user_id: int) -> Optional[dict]:
    """Get pending notification for user to delete the message."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT message_id, chat_id FROM pending_notifications
               WHERE user_id = $1 AND responded = 0
               ORDER BY sent_at DESC LIMIT 1""",
            user_id
        )
        return dict(row) if row else None


# ============ MARATHON FUNCTIONS ============

async def create_marathon(
    creator_id: int,
    name: str,
    start_date: date,
    end_date: date,
    invite_code: str
) -> int:
    """Create a new marathon."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO marathons (creator_id, name, start_date, end_date, invite_code)
               VALUES ($1, $2, $3, $4, $5) RETURNING id""",
            creator_id, name, start_date, end_date, invite_code
        )
        marathon_id = row['id']

        # Add creator as participant
        await conn.execute(
            "INSERT INTO marathon_participants (marathon_id, user_id) VALUES ($1, $2)",
            marathon_id, creator_id
        )

        return marathon_id


async def add_marathon_habit(
    marathon_id: int,
    name: str,
    habit_type: str,
    daily_goal: float = 1,
    unit: str = "",
    points_per_goal: float = 1
):
    """Add a habit template to marathon."""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO marathon_habits (marathon_id, name, habit_type, daily_goal, unit, points_per_goal)
               VALUES ($1, $2, $3, $4, $5, $6)""",
            marathon_id, name, habit_type, daily_goal, unit, points_per_goal
        )


async def get_marathon_by_code(invite_code: str) -> Optional[dict]:
    """Get marathon by invite code."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM marathons WHERE invite_code = $1", invite_code
        )
        return dict(row) if row else None


async def join_marathon(user_id: int, marathon_id: int):
    """Join a marathon and copy its habits."""
    async with pool.acquire() as conn:
        # Check if already joined
        existing = await conn.fetchrow(
            "SELECT * FROM marathon_participants WHERE marathon_id = $1 AND user_id = $2",
            marathon_id, user_id
        )
        if existing:
            return False

        # Add participant
        await conn.execute(
            "INSERT INTO marathon_participants (marathon_id, user_id) VALUES ($1, $2)",
            marathon_id, user_id
        )

        # Copy marathon habits to user
        habits = await conn.fetch(
            "SELECT * FROM marathon_habits WHERE marathon_id = $1", marathon_id
        )

        for habit in habits:
            await conn.execute(
                """INSERT INTO habits (user_id, name, habit_type, daily_goal, unit, marathon_id)
                   VALUES ($1, $2, $3, $4, $5, $6)""",
                user_id, habit['name'], habit['habit_type'], habit['daily_goal'],
                habit['unit'], marathon_id
            )

        return True


async def get_marathon_leaderboard(marathon_id: int) -> List[dict]:
    """Get marathon leaderboard."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT mp.*, u.first_name, u.username
               FROM marathon_participants mp
               JOIN users u ON mp.user_id = u.user_id
               WHERE mp.marathon_id = $1
               ORDER BY mp.total_points DESC""",
            marathon_id
        )
        return [dict(row) for row in rows]


async def update_marathon_points(user_id: int, marathon_id: int, points: float):
    """Add points to user in marathon."""
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE marathon_participants
               SET total_points = total_points + $1
               WHERE user_id = $2 AND marathon_id = $3""",
            points, user_id, marathon_id
        )


async def get_user_marathons(user_id: int) -> List[dict]:
    """Get all marathons user is participating in."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT m.* FROM marathons m
               JOIN marathon_participants mp ON m.id = mp.marathon_id
               WHERE mp.user_id = $1 AND m.is_active = 1
               ORDER BY m.start_date""",
            user_id
        )
        return [dict(row) for row in rows]


async def get_active_marathons_today() -> List[dict]:
    """Get marathons that are active today."""
    today = date.today()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT * FROM marathons
               WHERE is_active = 1 AND start_date <= $1 AND end_date >= $1""",
            today
        )
        return [dict(row) for row in rows]


async def leave_marathon(user_id: int, marathon_id: int, keep_habits: bool = False):
    """Leave a marathon and optionally keep or delete habits."""
    async with pool.acquire() as conn:
        # Remove from participants
        await conn.execute(
            "DELETE FROM marathon_participants WHERE user_id = $1 AND marathon_id = $2",
            user_id, marathon_id
        )

        if keep_habits:
            # Unlink habits from marathon (keep them as personal)
            await conn.execute(
                "UPDATE habits SET marathon_id = NULL WHERE user_id = $1 AND marathon_id = $2",
                user_id, marathon_id
            )
        else:
            # Delete marathon habits and their logs
            habits = await conn.fetch(
                "SELECT id FROM habits WHERE user_id = $1 AND marathon_id = $2",
                user_id, marathon_id
            )

            for habit in habits:
                await conn.execute("DELETE FROM habit_logs WHERE habit_id = $1", habit['id'])

            await conn.execute(
                "DELETE FROM habits WHERE user_id = $1 AND marathon_id = $2",
                user_id, marathon_id
            )


async def get_marathon_by_id(marathon_id: int) -> Optional[dict]:
    """Get marathon by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM marathons WHERE id = $1", marathon_id
        )
        return dict(row) if row else None


async def get_marathon_participant_info(marathon_id: int, user_id: int) -> Optional[dict]:
    """Get detailed info about marathon participant."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT u.first_name, u.username, mp.total_points, mp.joined_at
               FROM users u
               JOIN marathon_participants mp ON u.user_id = mp.user_id
               WHERE mp.marathon_id = $1 AND mp.user_id = $2""",
            marathon_id, user_id
        )
        return dict(row) if row else None


async def get_user_marathon_habits(user_id: int, marathon_id: int) -> List[dict]:
    """Get user's habits for a specific marathon."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT name, daily_goal, unit, streak
               FROM habits
               WHERE user_id = $1 AND marathon_id = $2""",
            user_id, marathon_id
        )
        return [dict(row) for row in rows]


# ============ ANALYTICS FUNCTIONS ============

async def get_habit_stats(habit_id: int, start_date: date, end_date: date) -> dict:
    """Get detailed statistics for a habit over a period."""
    async with pool.acquire() as conn:
        # Get habit info
        habit = await conn.fetchrow("SELECT * FROM habits WHERE id = $1", habit_id)
        habit = dict(habit)

        # Get logs
        rows = await conn.fetch(
            """SELECT * FROM habit_logs
               WHERE habit_id = $1 AND log_date BETWEEN $2 AND $3
               ORDER BY log_date""",
            habit_id, start_date, end_date
        )
        logs = [dict(row) for row in rows]

        # Calculate days in period
        total_days = (end_date - start_date).days + 1

        if habit['habit_type'] == 'boolean':
            completed = sum(1 for log in logs if log['completed'])
            return {
                "habit": habit,
                "total_days": total_days,
                "completed_days": completed,
                "missed_days": total_days - completed,
                "efficiency": round(completed / total_days * 100, 1) if total_days > 0 else 0,
                "logs": logs
            }
        else:
            total_value = sum(log['value'] for log in logs)
            best_day = max(logs, key=lambda x: x['value']) if logs else None

            return {
                "habit": habit,
                "total_days": total_days,
                "total_value": total_value,
                "average": round(total_value / total_days, 2) if total_days > 0 else 0,
                "best_day": best_day,
                "completed_days": sum(1 for log in logs if log['completed']),
                "logs": logs
            }


async def get_weekly_report(user_id: int, week_start: date) -> dict:
    """Generate weekly report."""
    from datetime import timedelta
    week_end = week_start + timedelta(days=6)

    habits = await get_user_habits(user_id)
    report = {
        "period": f"{week_start} - {week_end}",
        "habits": []
    }

    for habit in habits:
        stats = await get_habit_stats(habit['id'], week_start, week_end)
        report["habits"].append(stats)

    return report


# ============ Admin Functions ============

async def get_all_user_ids() -> List[int]:
    """Get all user IDs for broadcasting."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [row['user_id'] for row in rows]


async def get_admin_stats() -> dict:
    """Get statistics for admin panel."""
    async with pool.acquire() as conn:
        # Total users
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")

        # Users today
        users_today = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE DATE(created_at) = CURRENT_DATE"
        )

        # Total habits
        total_habits = await conn.fetchval("SELECT COUNT(*) FROM habits WHERE is_active = 1")

        # Total marathons
        total_marathons = await conn.fetchval("SELECT COUNT(*) FROM marathons")

        # Active marathons
        active_marathons = await conn.fetchval(
            "SELECT COUNT(*) FROM marathons WHERE end_date >= CURRENT_DATE"
        )

        # Total logs today
        logs_today = await conn.fetchval(
            "SELECT COUNT(*) FROM habit_logs WHERE DATE(logged_at) = CURRENT_DATE"
        )

        return {
            "total_users": total_users,
            "users_today": users_today,
            "total_habits": total_habits,
            "total_marathons": total_marathons,
            "active_marathons": active_marathons,
            "logs_today": logs_today
        }
