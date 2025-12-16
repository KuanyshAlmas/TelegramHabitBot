import asyncio
from datetime import datetime, date, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import database as db
from keyboards import notification_response_keyboard
from texts import get_text

# Kazakhstan timezone (UTC+5 for most of Kazakhstan / UTC+6 for some regions)
# Using Almaty timezone (UTC+5)
KZ_TZ = pytz.timezone('Asia/Almaty')

scheduler = AsyncIOScheduler(timezone=KZ_TZ)
bot_instance = None


def set_bot(bot):
    """Set bot instance for sending messages."""
    global bot_instance
    bot_instance = bot


async def send_habit_notification(user_id: int, habit: dict):
    """Send notification for a single habit."""
    if not bot_instance:
        return

    try:
        lang = await db.get_user_language(user_id)
        habit_type = habit['habit_type']
        unit = habit.get('unit', '')
        goal = habit['daily_goal']

        if lang == "kk":
            if habit_type == 'boolean':
                text = f"🔔 **Есеп беру уақыты!**\n\n"
                text += f"Әдет: **{habit['name']}**\n"
                text += f"Бүгін орындалды ма?\n\n"
                text += f"⏱ Жауап беруге 10 минут бар."
            else:
                text = f"🔔 **Есеп беру уақыты!**\n\n"
                text += f"Әдет: **{habit['name']}**\n"
                text += f"Күнделікті мақсат: {goal} {unit}\n\n"
                text += f"⏱ Жауап беруге 10 минут бар."
        else:
            if habit_type == 'boolean':
                text = f"🔔 **Пора сдать отчет!**\n\n"
                text += f"Привычка: **{habit['name']}**\n"
                text += f"Выполнено сегодня?\n\n"
                text += f"⏱ У тебя есть 10 минут на ответ."
            else:
                text = f"🔔 **Пора сдать отчет!**\n\n"
                text += f"Привычка: **{habit['name']}**\n"
                text += f"Цель на день: {goal} {unit}\n\n"
                text += f"⏱ У тебя есть 10 минут на ответ."

        keyboard = notification_response_keyboard(habit['id'], habit_type, lang)

        await bot_instance.send_message(
            user_id,
            text,
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

        # Create pending notification with 10-min expiry
        expires_at = datetime.now(KZ_TZ) + timedelta(minutes=10)
        await db.create_pending_notification(user_id, habit['id'], expires_at)

    except Exception as e:
        print(f"Error sending notification to {user_id}: {e}")


async def check_notifications_for_time(check_time: str):
    """Check all users who need notifications at this time."""
    users = await db.get_all_users()

    for user in users:
        user_times = await db.get_user_notification_times(user['user_id'])

        if check_time in user_times:
            habits = await db.get_user_habits(user['user_id'])

            for habit in habits:
                # Check if already completed today
                log = await db.get_daily_log(habit['id'])
                if log and log['completed']:
                    continue

                await send_habit_notification(user['user_id'], habit)
                await asyncio.sleep(0.1)  # Small delay between messages


async def check_expired_notifications():
    """Check for expired notifications and log zeros."""
    expired = await db.get_expired_notifications()

    for notif in expired:
        try:
            # Log zero for this habit
            await db.log_habit(notif['habit_id'], notif['user_id'], 0)

            # Notify user
            habit = await db.get_habit(notif['habit_id'])
            if bot_instance and habit:
                await bot_instance.send_message(
                    notif['user_id'],
                    f"⏳ Время вышло для **{habit['name']}**.\n"
                    f"Засчитан 0. Ты можешь внести результат вручную позже.",
                    parse_mode="Markdown"
                )
        except Exception as e:
            print(f"Error processing expired notification: {e}")

    await db.delete_expired_notifications()


async def process_end_of_day():
    """Process end of day at 23:59 - calculate streaks and send reports."""
    if not bot_instance:
        return

    users = await db.get_all_users()
    today = date.today()

    for user in users:
        try:
            habits = await db.get_user_habits(user['user_id'])
            if not habits:
                continue

            report_lines = ["🌙 **Итоги дня (23:59):**\n"]
            streak_updates = []

            for habit in habits:
                log = await db.get_daily_log(habit['id'], today)
                value = log['value'] if log else 0
                goal = habit['daily_goal']
                completed = value >= goal

                # Update streak
                new_streak, _ = await db.update_streak(habit['id'], completed)

                if habit['habit_type'] == 'boolean':
                    status = "✅ Выполнено" if completed else "❌ Не выполнено"
                    report_lines.append(f"• {habit['name']}: {status}")
                else:
                    unit = habit.get('unit', '')
                    status = "✅" if completed else "❌"
                    report_lines.append(f"• {habit['name']}: {value}/{goal} {unit} {status}")

                # Streak message
                if completed and new_streak > 1:
                    streak_updates.append(f"🔥 {habit['name']}: {new_streak} дней подряд!")
                elif not completed and habit['streak'] > 0:
                    streak_updates.append(f"💔 {habit['name']}: огонек погас...")

                # Update marathon points if applicable
                if habit.get('marathon_id') and completed:
                    await db.update_marathon_points(
                        user['user_id'],
                        habit['marathon_id'],
                        1  # 1 point per completed goal
                    )

            report_text = "\n".join(report_lines)

            if streak_updates:
                report_text += "\n\n" + "\n".join(streak_updates)

            await bot_instance.send_message(
                user['user_id'],
                report_text,
                parse_mode="Markdown"
            )

        except Exception as e:
            print(f"Error processing end of day for user {user['user_id']}: {e}")


async def send_weekly_report():
    """Send weekly report every Sunday."""
    if not bot_instance:
        return

    users = await db.get_all_users()
    today = date.today()
    week_start = today - timedelta(days=6)

    for user in users:
        try:
            report = await db.get_weekly_report(user['user_id'], week_start)

            if not report['habits']:
                continue

            text = f"📊 **Отчет за неделю**\n"
            text += f"({week_start.strftime('%d.%m')} - {today.strftime('%d.%m')})\n\n"

            for habit_stats in report['habits']:
                habit = habit_stats['habit']
                if habit['habit_type'] == 'boolean':
                    text += f"• {habit['name']}: {habit_stats['completed_days']}/7 дней "
                    text += f"({habit_stats['efficiency']}%)\n"
                else:
                    text += f"• {habit['name']}: {habit_stats['total_value']} {habit.get('unit', '')}\n"
                    text += f"  (в среднем {habit_stats['average']} в день)\n"

            await bot_instance.send_message(user['user_id'], text, parse_mode="Markdown")

        except Exception as e:
            print(f"Error sending weekly report to {user['user_id']}: {e}")


async def send_monthly_report():
    """Send monthly report on 1st of each month."""
    if not bot_instance:
        return

    users = await db.get_all_users()
    today = date.today()

    # Get previous month range
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)
    first_of_prev_month = last_of_prev_month.replace(day=1)

    months_ru = ["", "января", "февраля", "марта", "апреля", "мая", "июня",
                 "июля", "августа", "сентября", "октября", "ноября", "декабря"]

    for user in users:
        try:
            habits = await db.get_user_habits(user['user_id'])

            if not habits:
                continue

            text = f"📅 **Отчет за {months_ru[last_of_prev_month.month]}**\n\n"

            for habit in habits:
                stats = await db.get_habit_stats(
                    habit['id'],
                    first_of_prev_month,
                    last_of_prev_month
                )

                if habit['habit_type'] == 'boolean':
                    text += f"**{habit['name']}**\n"
                    text += f"• Выполнено: {stats['completed_days']} из {stats['total_days']} дней\n"
                    text += f"• Эффективность: {stats['efficiency']}%\n\n"
                else:
                    text += f"**{habit['name']}**\n"
                    text += f"• Всего: {stats['total_value']} {habit.get('unit', '')}\n"
                    text += f"• В среднем: {stats['average']} в день\n"
                    if stats['best_day']:
                        best_date = stats['best_day']['log_date']
                        text += f"• Лучший день: {stats['best_day']['value']} ({best_date})\n"
                    text += "\n"

            await bot_instance.send_message(user['user_id'], text, parse_mode="Markdown")

        except Exception as e:
            print(f"Error sending monthly report to {user['user_id']}: {e}")


def setup_scheduler():
    """Setup all scheduled jobs."""
    # Check notifications every hour from 6:00 to 22:00
    notification_hours = [6, 7, 8, 9, 10, 12, 13, 14, 15, 18, 19, 20, 21, 22]

    for hour in notification_hours:
        scheduler.add_job(
            check_notifications_for_time,
            CronTrigger(hour=hour, minute=0, timezone=KZ_TZ),
            args=[f"{hour:02d}:00"],
            id=f"notif_{hour}",
            replace_existing=True
        )

    # Check expired notifications every minute
    scheduler.add_job(
        check_expired_notifications,
        CronTrigger(minute='*', timezone=KZ_TZ),
        id="check_expired",
        replace_existing=True
    )

    # End of day processing at 23:59
    scheduler.add_job(
        process_end_of_day,
        CronTrigger(hour=23, minute=59, timezone=KZ_TZ),
        id="end_of_day",
        replace_existing=True
    )

    # Weekly report every Sunday at 10:00
    scheduler.add_job(
        send_weekly_report,
        CronTrigger(day_of_week='sun', hour=10, minute=0, timezone=KZ_TZ),
        id="weekly_report",
        replace_existing=True
    )

    # Monthly report on 1st of each month at 10:00
    scheduler.add_job(
        send_monthly_report,
        CronTrigger(day=1, hour=10, minute=0, timezone=KZ_TZ),
        id="monthly_report",
        replace_existing=True
    )

    return scheduler
