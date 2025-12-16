import aiosqlite
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import json

DB_PATH = "habit_tracker.db"


async def init_db():
    """Initialize database with all tables."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Users table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                language TEXT DEFAULT 'kk',
                notification_times TEXT DEFAULT '["08:00", "14:00", "21:00"]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add language column if not exists (for existing databases)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN language TEXT DEFAULT 'kk'")
            await db.commit()
        except:
            pass  # Column already exists

        # Categories/Folders table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                icon TEXT DEFAULT '📁',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)

        # Habits table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS habits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category_id INTEGER,
                name TEXT NOT NULL,
                habit_type TEXT NOT NULL CHECK(habit_type IN ('boolean', 'numeric')),
                daily_goal REAL DEFAULT 1,
                unit TEXT DEFAULT '',
                streak INTEGER DEFAULT 0,
                max_streak INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                marathon_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
                FOREIGN KEY (marathon_id) REFERENCES marathons(id) ON DELETE SET NULL
            )
        """)

        # Daily logs table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS habit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                habit_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                log_date DATE NOT NULL,
                value REAL DEFAULT 0,
                completed INTEGER DEFAULT 0,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (habit_id) REFERENCES habits(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(habit_id, log_date)
            )
        """)

        # Pending notifications (for 10-min rule)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pending_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                habit_id INTEGER NOT NULL,
                sent_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                responded INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (habit_id) REFERENCES habits(id) ON DELETE CASCADE
            )
        """)

        # Marathons table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS marathons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creator_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                invite_code TEXT UNIQUE NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creator_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)

        # Marathon participants
        await db.execute("""
            CREATE TABLE IF NOT EXISTS marathon_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marathon_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                total_points REAL DEFAULT 0,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (marathon_id) REFERENCES marathons(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                UNIQUE(marathon_id, user_id)
            )
        """)

        # Marathon habit templates
        await db.execute("""
            CREATE TABLE IF NOT EXISTS marathon_habits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marathon_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                habit_type TEXT NOT NULL CHECK(habit_type IN ('boolean', 'numeric')),
                daily_goal REAL DEFAULT 1,
                unit TEXT DEFAULT '',
                points_per_goal REAL DEFAULT 1,
                FOREIGN KEY (marathon_id) REFERENCES marathons(id) ON DELETE CASCADE
            )
        """)

        await db.commit()


# ============ USER FUNCTIONS ============

async def get_or_create_user(user_id: int, username: str = None, first_name: str = None) -> dict:
    """Get existing user or create new one."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()

        if row:
            return dict(row)

        await db.execute(
            "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user_id, username, first_name)
        )
        await db.commit()

        # Create default categories
        default_categories = [
            ("🏃 Здоровье", "🏃"),
            ("🕌 Духовное", "🕌"),
            ("📚 Образование", "📚")
        ]
        for name, icon in default_categories:
            await db.execute(
                "INSERT INTO categories (user_id, name, icon) VALUES (?, ?, ?)",
                (user_id, name, icon)
            )
        await db.commit()

        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return dict(await cursor.fetchone())


async def update_notification_times(user_id: int, times: List[str]):
    """Update user's notification times."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET notification_times = ? WHERE user_id = ?",
            (json.dumps(times), user_id)
        )
        await db.commit()


async def get_user_notification_times(user_id: int) -> List[str]:
    """Get user's notification times."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT notification_times FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        if row:
            return json.loads(row[0])
        return ["08:00", "14:00", "21:00"]


async def get_all_users() -> List[dict]:
    """Get all users."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users")
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_user_language(user_id: int) -> str:
    """Get user's language preference."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT language FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        if row and row[0]:
            return row[0]
        return "kk"  # Default to Kazakh


async def set_user_language(user_id: int, language: str):
    """Set user's language preference."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET language = ? WHERE user_id = ?",
            (language, user_id)
        )
        await db.commit()


# ============ CATEGORY FUNCTIONS ============

async def get_user_categories(user_id: int) -> List[dict]:
    """Get all categories for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM categories WHERE user_id = ? ORDER BY name",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def create_category(user_id: int, name: str, icon: str = "📁") -> int:
    """Create a new category."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO categories (user_id, name, icon) VALUES (?, ?, ?)",
            (user_id, name, icon)
        )
        await db.commit()
        return cursor.lastrowid


async def delete_category(category_id: int):
    """Delete a category."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM categories WHERE id = ?", (category_id,))
        await db.commit()


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
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO habits (user_id, name, habit_type, daily_goal, unit, category_id, marathon_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, name, habit_type, daily_goal, unit, category_id, marathon_id)
        )
        await db.commit()
        return cursor.lastrowid


async def get_user_habits(user_id: int, active_only: bool = True) -> List[dict]:
    """Get all habits for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        query = "SELECT * FROM habits WHERE user_id = ?"
        if active_only:
            query += " AND is_active = 1"
        query += " ORDER BY category_id, name"
        cursor = await db.execute(query, (user_id,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_habits_by_category(user_id: int, category_id: Optional[int], active_only: bool = True) -> List[dict]:
    """Get habits for a user filtered by category."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if category_id is None:
            query = "SELECT * FROM habits WHERE user_id = ? AND category_id IS NULL"
        else:
            query = "SELECT * FROM habits WHERE user_id = ? AND category_id = ?"
        if active_only:
            query += " AND is_active = 1"
        query += " ORDER BY name"

        if category_id is None:
            cursor = await db.execute(query, (user_id,))
        else:
            cursor = await db.execute(query, (user_id, category_id))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_category(category_id: int) -> Optional[dict]:
    """Get a single category by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM categories WHERE id = ?", (category_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_habit(habit_id: int) -> Optional[dict]:
    """Get a single habit."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM habits WHERE id = ?", (habit_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def update_habit(habit_id: int, **kwargs):
    """Update habit fields."""
    async with aiosqlite.connect(DB_PATH) as db:
        fields = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [habit_id]
        await db.execute(f"UPDATE habits SET {fields} WHERE id = ?", values)
        await db.commit()


async def delete_habit(habit_id: int):
    """Delete a habit and its logs."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM habit_logs WHERE habit_id = ?", (habit_id,))
        await db.execute("DELETE FROM habits WHERE id = ?", (habit_id,))
        await db.commit()


# ============ HABIT LOG FUNCTIONS ============

async def log_habit(habit_id: int, user_id: int, value: float, log_date: date = None) -> dict:
    """Log or update a habit entry for a specific date."""
    if log_date is None:
        log_date = date.today()

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Get habit info
        cursor = await db.execute("SELECT * FROM habits WHERE id = ?", (habit_id,))
        habit = dict(await cursor.fetchone())

        # Check if log exists for today
        cursor = await db.execute(
            "SELECT * FROM habit_logs WHERE habit_id = ? AND log_date = ?",
            (habit_id, log_date.isoformat())
        )
        existing = await cursor.fetchone()

        if existing:
            # Update existing log (add to value)
            new_value = existing['value'] + value
            completed = 1 if new_value >= habit['daily_goal'] else 0
            await db.execute(
                "UPDATE habit_logs SET value = ?, completed = ?, logged_at = ? WHERE id = ?",
                (new_value, completed, datetime.now().isoformat(), existing['id'])
            )
        else:
            # Create new log
            new_value = value
            completed = 1 if new_value >= habit['daily_goal'] else 0
            await db.execute(
                "INSERT INTO habit_logs (habit_id, user_id, log_date, value, completed) VALUES (?, ?, ?, ?, ?)",
                (habit_id, user_id, log_date.isoformat(), new_value, completed)
            )

        await db.commit()

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

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM habit_logs WHERE habit_id = ? AND log_date = ?",
            (habit_id, log_date.isoformat())
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_user_daily_logs(user_id: int, log_date: date = None) -> List[dict]:
    """Get all logs for a user on a specific date."""
    if log_date is None:
        log_date = date.today()

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT hl.*, h.name, h.habit_type, h.daily_goal, h.unit
               FROM habit_logs hl
               JOIN habits h ON hl.habit_id = h.id
               WHERE hl.user_id = ? AND hl.log_date = ?""",
            (user_id, log_date.isoformat())
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_habit_logs_range(habit_id: int, start_date: date, end_date: date) -> List[dict]:
    """Get habit logs for a date range."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM habit_logs
               WHERE habit_id = ? AND log_date BETWEEN ? AND ?
               ORDER BY log_date""",
            (habit_id, start_date.isoformat(), end_date.isoformat())
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


# ============ STREAK FUNCTIONS ============

async def update_streak(habit_id: int, completed: bool):
    """Update streak for a habit after end-of-day check."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT streak, max_streak FROM habits WHERE id = ?", (habit_id,))
        row = await cursor.fetchone()
        current_streak, max_streak = row

        if completed:
            new_streak = current_streak + 1
            new_max = max(max_streak, new_streak)
        else:
            new_streak = 0
            new_max = max_streak

        await db.execute(
            "UPDATE habits SET streak = ?, max_streak = ? WHERE id = ?",
            (new_streak, new_max, habit_id)
        )
        await db.commit()

        return new_streak, completed


# ============ PENDING NOTIFICATION FUNCTIONS ============

async def create_pending_notification(user_id: int, habit_id: int, expires_at: datetime):
    """Create a pending notification."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO pending_notifications (user_id, habit_id, sent_at, expires_at)
               VALUES (?, ?, ?, ?)""",
            (user_id, habit_id, datetime.now().isoformat(), expires_at.isoformat())
        )
        await db.commit()


async def mark_notification_responded(user_id: int, habit_id: int):
    """Mark pending notification as responded."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE pending_notifications SET responded = 1
               WHERE user_id = ? AND habit_id = ? AND responded = 0""",
            (user_id, habit_id)
        )
        await db.commit()


async def get_expired_notifications() -> List[dict]:
    """Get all expired pending notifications that weren't responded."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM pending_notifications
               WHERE responded = 0 AND expires_at < ?""",
            (datetime.now().isoformat(),)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def delete_expired_notifications():
    """Delete processed expired notifications."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM pending_notifications WHERE responded = 1 OR expires_at < ?",
            (datetime.now().isoformat(),)
        )
        await db.commit()


# ============ MARATHON FUNCTIONS ============

async def create_marathon(
    creator_id: int,
    name: str,
    start_date: date,
    end_date: date,
    invite_code: str
) -> int:
    """Create a new marathon."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO marathons (creator_id, name, start_date, end_date, invite_code)
               VALUES (?, ?, ?, ?, ?)""",
            (creator_id, name, start_date.isoformat(), end_date.isoformat(), invite_code)
        )
        await db.commit()

        # Add creator as participant
        await db.execute(
            "INSERT INTO marathon_participants (marathon_id, user_id) VALUES (?, ?)",
            (cursor.lastrowid, creator_id)
        )
        await db.commit()

        return cursor.lastrowid


async def add_marathon_habit(
    marathon_id: int,
    name: str,
    habit_type: str,
    daily_goal: float = 1,
    unit: str = "",
    points_per_goal: float = 1
):
    """Add a habit template to marathon."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO marathon_habits (marathon_id, name, habit_type, daily_goal, unit, points_per_goal)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (marathon_id, name, habit_type, daily_goal, unit, points_per_goal)
        )
        await db.commit()


async def get_marathon_by_code(invite_code: str) -> Optional[dict]:
    """Get marathon by invite code."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM marathons WHERE invite_code = ?", (invite_code,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def join_marathon(user_id: int, marathon_id: int):
    """Join a marathon and copy its habits."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Check if already joined
        cursor = await db.execute(
            "SELECT * FROM marathon_participants WHERE marathon_id = ? AND user_id = ?",
            (marathon_id, user_id)
        )
        if await cursor.fetchone():
            return False

        # Add participant
        await db.execute(
            "INSERT INTO marathon_participants (marathon_id, user_id) VALUES (?, ?)",
            (marathon_id, user_id)
        )

        # Copy marathon habits to user
        cursor = await db.execute(
            "SELECT * FROM marathon_habits WHERE marathon_id = ?", (marathon_id,)
        )
        habits = await cursor.fetchall()

        for habit in habits:
            await db.execute(
                """INSERT INTO habits (user_id, name, habit_type, daily_goal, unit, marathon_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, habit['name'], habit['habit_type'], habit['daily_goal'],
                 habit['unit'], marathon_id)
            )

        await db.commit()
        return True


async def get_marathon_leaderboard(marathon_id: int) -> List[dict]:
    """Get marathon leaderboard."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT mp.*, u.first_name, u.username
               FROM marathon_participants mp
               JOIN users u ON mp.user_id = u.user_id
               WHERE mp.marathon_id = ?
               ORDER BY mp.total_points DESC""",
            (marathon_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def update_marathon_points(user_id: int, marathon_id: int, points: float):
    """Add points to user in marathon."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE marathon_participants
               SET total_points = total_points + ?
               WHERE user_id = ? AND marathon_id = ?""",
            (points, user_id, marathon_id)
        )
        await db.commit()


async def get_user_marathons(user_id: int) -> List[dict]:
    """Get all marathons user is participating in."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT m.* FROM marathons m
               JOIN marathon_participants mp ON m.id = mp.marathon_id
               WHERE mp.user_id = ? AND m.is_active = 1
               ORDER BY m.start_date""",
            (user_id,)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def get_active_marathons_today() -> List[dict]:
    """Get marathons that are active today."""
    today = date.today().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM marathons
               WHERE is_active = 1 AND start_date <= ? AND end_date >= ?""",
            (today, today)
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]


async def leave_marathon(user_id: int, marathon_id: int, keep_habits: bool = False):
    """Leave a marathon and optionally keep or delete habits."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Remove from participants
        await db.execute(
            "DELETE FROM marathon_participants WHERE user_id = ? AND marathon_id = ?",
            (user_id, marathon_id)
        )

        if keep_habits:
            # Unlink habits from marathon (keep them as personal)
            await db.execute(
                "UPDATE habits SET marathon_id = NULL WHERE user_id = ? AND marathon_id = ?",
                (user_id, marathon_id)
            )
        else:
            # Delete marathon habits and their logs
            cursor = await db.execute(
                "SELECT id FROM habits WHERE user_id = ? AND marathon_id = ?",
                (user_id, marathon_id)
            )
            habit_ids = [row[0] for row in await cursor.fetchall()]

            for habit_id in habit_ids:
                await db.execute("DELETE FROM habit_logs WHERE habit_id = ?", (habit_id,))

            await db.execute(
                "DELETE FROM habits WHERE user_id = ? AND marathon_id = ?",
                (user_id, marathon_id)
            )

        await db.commit()


async def get_marathon_by_id(marathon_id: int) -> Optional[dict]:
    """Get marathon by ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM marathons WHERE id = ?", (marathon_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


# ============ ANALYTICS FUNCTIONS ============

async def get_habit_stats(habit_id: int, start_date: date, end_date: date) -> dict:
    """Get detailed statistics for a habit over a period."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Get habit info
        cursor = await db.execute("SELECT * FROM habits WHERE id = ?", (habit_id,))
        habit = dict(await cursor.fetchone())

        # Get logs
        cursor = await db.execute(
            """SELECT * FROM habit_logs
               WHERE habit_id = ? AND log_date BETWEEN ? AND ?
               ORDER BY log_date""",
            (habit_id, start_date.isoformat(), end_date.isoformat())
        )
        logs = [dict(row) for row in await cursor.fetchall()]

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
            values = [log['value'] for log in logs]
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
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        rows = await cursor.fetchall()
        return [row[0] for row in rows]


async def get_admin_stats() -> dict:
    """Get statistics for admin panel."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Total users
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]

        # Users today
        cursor = await db.execute(
            "SELECT COUNT(*) FROM users WHERE DATE(created_at) = DATE('now')"
        )
        users_today = (await cursor.fetchone())[0]

        # Total habits
        cursor = await db.execute("SELECT COUNT(*) FROM habits WHERE is_active = 1")
        total_habits = (await cursor.fetchone())[0]

        # Total marathons
        cursor = await db.execute("SELECT COUNT(*) FROM marathons")
        total_marathons = (await cursor.fetchone())[0]

        # Active marathons
        cursor = await db.execute(
            "SELECT COUNT(*) FROM marathons WHERE end_date >= DATE('now')"
        )
        active_marathons = (await cursor.fetchone())[0]

        # Total logs today
        cursor = await db.execute(
            "SELECT COUNT(*) FROM habit_logs WHERE DATE(logged_at) = DATE('now')"
        )
        logs_today = (await cursor.fetchone())[0]

        return {
            "total_users": total_users,
            "users_today": users_today,
            "total_habits": total_habits,
            "total_marathons": total_marathons,
            "active_marathons": active_marathons,
            "logs_today": logs_today
        }
