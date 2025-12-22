import asyncio
from datetime import datetime, date, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import database as db
from keyboards import log_habits_keyboard
from texts import get_text, TEXTS

# Kazakhstan timezone (UTC+5 for most of Kazakhstan / UTC+6 for some regions)
# Using Almaty timezone (UTC+5)
KZ_TZ = pytz.timezone('Asia/Almaty')

scheduler = AsyncIOScheduler(timezone=KZ_TZ)
bot_instance = None


def set_bot(bot):
    """Set bot instance for sending messages."""
    global bot_instance
    bot_instance = bot


async def send_consolidated_notification(user_id: int, uncompleted_habits: list):
    """Send ONE notification with all uncompleted habits."""
    if not bot_instance or not uncompleted_habits:
        return

    try:
        lang = await db.get_user_language(user_id)

        text = get_text("report_time", lang) + "\n\n"
        text += get_text("uncompleted_habits", lang) + "\n"

        for habit in uncompleted_habits:
            if habit['habit_type'] == 'boolean':
                text += f"• {habit['name']}\n"
            else:
                unit = habit.get('unit', '')
                goal_text = get_text("goal_label", lang, goal=habit['daily_goal'], unit=unit)
                text += f"• {habit['name']} ({goal_text})\n"

        text += "\n" + get_text("response_time", lang)

        # Use log_habits_keyboard to show all habits
        keyboard = log_habits_keyboard(uncompleted_habits, lang)

        msg = await bot_instance.send_message(
            user_id,
            text,
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

        # Create pending notification (store message_id for deletion)
        # Use first habit's id as reference, but store message for deletion
        await db.create_pending_notification(user_id, uncompleted_habits[0]['id'], msg.message_id, user_id)

    except Exception as e:
        print(f"Error sending notification to {user_id}: {e}")


async def check_notifications_for_time(check_time: str):
    """Check all users who need notifications at this time."""
    users = await db.get_all_users()

    for user in users:
        user_times = await db.get_user_notification_times(user['user_id'])

        if check_time in user_times:
            habits = await db.get_user_habits(user['user_id'])

            if not habits:
                continue

            # Get all logs at once (batch query)
            habit_ids = [h['id'] for h in habits]
            logs = await db.get_daily_logs_batch(habit_ids)

            # Collect uncompleted habits
            uncompleted = [h for h in habits if not logs.get(h['id'], {}).get('completed')]

            # Send ONE notification with all uncompleted habits
            if uncompleted:
                await send_consolidated_notification(user['user_id'], uncompleted)
                await asyncio.sleep(0.1)


async def check_expired_notifications():
    """Check for expired notifications and delete messages."""
    expired = await db.get_expired_notifications()

    for notif in expired:
        try:
            # Delete the notification message to keep chat clean
            if bot_instance and notif.get('message_id') and notif.get('chat_id'):
                try:
                    await bot_instance.delete_message(notif['chat_id'], notif['message_id'])
                except Exception:
                    pass  # Message may already be deleted

            # Log zero for ALL uncompleted habits of this user
            habits = await db.get_user_habits(notif['user_id'])

            if habits:
                # Get all logs at once (batch query)
                habit_ids = [h['id'] for h in habits]
                logs = await db.get_daily_logs_batch(habit_ids)

                for habit in habits:
                    log = logs.get(habit['id'])
                    if not log or not log['completed']:
                        await db.log_habit(habit['id'], notif['user_id'], 0)

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
            lang = await db.get_user_language(user['user_id'])
            habits = await db.get_user_habits(user['user_id'])
            if not habits:
                continue

            # Get all logs at once (batch query)
            habit_ids = [h['id'] for h in habits]
            logs = await db.get_daily_logs_batch(habit_ids, today)

            report_lines = [get_text("end_of_day_title", lang) + "\n"]
            streak_updates = []

            for habit in habits:
                log = logs.get(habit['id'])
                value = log['value'] if log else 0
                goal = habit['daily_goal']
                completed = value >= goal

                # Update streak
                new_streak, _ = await db.update_streak(habit['id'], completed)

                if habit['habit_type'] == 'boolean':
                    status = get_text("done_label", lang) if completed else get_text("not_done_label", lang)
                    report_lines.append(f"• {habit['name']}: {status}")
                else:
                    unit = habit.get('unit', '')
                    status = "✅" if completed else "❌"
                    report_lines.append(f"• {habit['name']}: {value}/{goal} {unit} {status}")

                # Streak message
                if completed and new_streak > 1:
                    streak_updates.append(get_text("streak_fire", lang, name=habit['name'], streak=new_streak))
                elif not completed and habit['streak'] > 0:
                    streak_updates.append(get_text("streak_lost", lang, name=habit['name']))

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
            lang = await db.get_user_language(user['user_id'])
            report = await db.get_weekly_report(user['user_id'], week_start)

            if not report['habits']:
                continue

            text = get_text("weekly_report_title", lang) + "\n"
            text += f"({week_start.strftime('%d.%m')} - {today.strftime('%d.%m')})\n\n"

            for habit_stats in report['habits']:
                habit = habit_stats['habit']
                if habit['habit_type'] == 'boolean':
                    text += f"• {habit['name']}: " + get_text("days_out_of", lang, completed=habit_stats['completed_days'])
                    text += f" ({habit_stats['efficiency']}%)\n"
                else:
                    text += f"• {habit['name']}: {habit_stats['total_value']} {habit.get('unit', '')}\n"
                    text += f"  " + get_text("average_per_day", lang, avg=habit_stats['average']) + "\n"

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

    for user in users:
        try:
            lang = await db.get_user_language(user['user_id'])
            habits = await db.get_user_habits(user['user_id'])

            if not habits:
                continue

            months_genitive = get_text("months_genitive", lang)
            month_name = months_genitive[last_of_prev_month.month - 1]
            text = get_text("monthly_report_title", lang, month=month_name) + "\n\n"

            for habit in habits:
                stats = await db.get_habit_stats(
                    habit['id'],
                    first_of_prev_month,
                    last_of_prev_month
                )

                if habit['habit_type'] == 'boolean':
                    text += f"**{habit['name']}**\n"
                    text += get_text("completed_out_of", lang, completed=stats['completed_days'], total=stats['total_days']) + "\n"
                    text += get_text("efficiency_percent", lang, percent=stats['efficiency']) + "\n\n"
                else:
                    text += f"**{habit['name']}**\n"
                    text += get_text("total_amount", lang, total=stats['total_value'], unit=habit.get('unit', '')) + "\n"
                    text += get_text("average_per_day_stat", lang, avg=stats['average']) + "\n"
                    if stats['best_day']:
                        best_date = stats['best_day']['log_date']
                        text += get_text("best_day_stat", lang, value=stats['best_day']['value'], date=best_date) + "\n"
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
