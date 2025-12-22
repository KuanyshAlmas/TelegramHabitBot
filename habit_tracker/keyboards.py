from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from typing import List, Optional
from datetime import date, timedelta
from texts import get_text


def main_menu_keyboard(lang: str = "kk") -> ReplyKeyboardMarkup:
    """Main menu keyboard."""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text=get_text("btn_my_habits", lang)),
        KeyboardButton(text=get_text("btn_add_result", lang))
    )
    builder.row(
        KeyboardButton(text=get_text("btn_my_stats", lang)),
        KeyboardButton(text=get_text("btn_marathons", lang))
    )
    builder.row(
        KeyboardButton(text=get_text("btn_categories", lang)),
        KeyboardButton(text=get_text("btn_settings", lang))
    )
    return builder.as_markup(resize_keyboard=True)


def habits_keyboard(habits: List[dict], lang: str = "kk", action: str = "view") -> InlineKeyboardMarkup:
    """Keyboard with list of habits."""
    builder = InlineKeyboardBuilder()

    for habit in habits:
        icon = "✅" if habit.get('completed_today') else "⬜"
        streak_icon = f"🔥{habit['streak']}" if habit['streak'] > 0 else ""
        text = f"{icon} {habit['name']} {streak_icon}"
        builder.row(InlineKeyboardButton(
            text=text,
            callback_data=f"habit_{action}_{habit['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_create_habit", lang),
        callback_data="habit_create"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def habits_categories_keyboard(categories: List[dict], habits: List[dict], lang: str = "kk") -> InlineKeyboardMarkup:
    """Keyboard with categories for habit filtering. Shows habit count per category."""
    builder = InlineKeyboardBuilder()

    for cat in categories:
        # Count habits in this category
        count = sum(1 for h in habits if h.get('category_id') == cat['id'])
        text = f"{cat['icon']} {cat['name']} ({count})"
        builder.row(InlineKeyboardButton(
            text=text,
            callback_data=f"habits_cat_{cat['id']}"
        ))

    # Habits without category
    no_cat_count = sum(1 for h in habits if h.get('category_id') is None)
    if no_cat_count > 0:
        no_cat_text = get_text("no_category", lang) if lang else "Без категории"
        builder.row(InlineKeyboardButton(
            text=f"📌 {no_cat_text} ({no_cat_count})",
            callback_data="habits_cat_none"
        ))

    # Show all habits button
    all_habits_text = get_text("all_habits", lang)
    builder.row(InlineKeyboardButton(
        text=f"{all_habits_text} ({len(habits)})",
        callback_data="habits_cat_all"
    ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_create_habit", lang),
        callback_data="habit_create"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def habits_in_category_keyboard(habits: List[dict], category_id: Optional[int], lang: str = "kk") -> InlineKeyboardMarkup:
    """Keyboard with habits filtered by category."""
    builder = InlineKeyboardBuilder()

    for habit in habits:
        icon = "✅" if habit.get('completed_today') else "⬜"
        streak_icon = f"🔥{habit['streak']}" if habit['streak'] > 0 else ""
        text = f"{icon} {habit['name']} {streak_icon}"
        builder.row(InlineKeyboardButton(
            text=text,
            callback_data=f"habit_view_{habit['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_create_habit", lang),
        callback_data="habit_create"
    ))

    # Back to categories
    builder.row(InlineKeyboardButton(
        text=get_text("back_categories", lang),
        callback_data="back_to_habits"
    ))

    return builder.as_markup()


def habit_detail_keyboard(habit: dict, lang: str = "kk", is_marathon: bool = False) -> InlineKeyboardMarkup:
    """Detail view of a habit."""
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(
        text=get_text("btn_add_result", lang),
        callback_data=f"habit_log_{habit['id']}"
    ))
    builder.row(InlineKeyboardButton(
        text="📊 " + get_text("btn_my_stats", lang).replace("📊 ", ""),
        callback_data=f"habit_stats_{habit['id']}"
    ))

    builder.row(InlineKeyboardButton(
        text=get_text("back_categories", lang),
        callback_data="back_to_habits"
    ))

    return builder.as_markup()


def habit_type_keyboard(lang: str = "kk") -> InlineKeyboardMarkup:
    """Choose habit type."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=get_text("type_boolean", lang),
        callback_data="type_boolean"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("type_numeric", lang),
        callback_data="type_numeric"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_cancel", lang),
        callback_data="back_to_habits"
    ))
    return builder.as_markup()


def categories_keyboard(categories: List[dict], lang: str = "kk", action: str = "select") -> InlineKeyboardMarkup:
    """Keyboard with categories."""
    builder = InlineKeyboardBuilder()

    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"{cat['icon']} {cat['name']}",
            callback_data=f"cat_{action}_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("no_category", lang),
        callback_data=f"cat_{action}_none"
    ))

    if action == "select":
        builder.row(InlineKeyboardButton(
            text=get_text("new_category", lang),
            callback_data="cat_create"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def boolean_input_keyboard(habit_id: int, lang: str = "kk") -> InlineKeyboardMarkup:
    """Yes/No input for boolean habits."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=get_text("btn_yes", lang), callback_data=f"log_bool_{habit_id}_1"),
        InlineKeyboardButton(text=get_text("btn_no", lang), callback_data=f"log_bool_{habit_id}_0")
    )
    builder.row(InlineKeyboardButton(
        text=get_text("btn_cancel", lang),
        callback_data="back_to_habits"
    ))
    return builder.as_markup()


def comment_keyboard(habit_id: int, lang: str = "kk") -> InlineKeyboardMarkup:
    """Keyboard for optional comment after logging."""
    builder = InlineKeyboardBuilder()
    skip_text = "⏭ Өткізіп жіберу" if lang == "kk" else "⏭ Пропустить"
    builder.row(InlineKeyboardButton(
        text=skip_text,
        callback_data=f"skip_comment_{habit_id}"
    ))
    return builder.as_markup()


def numeric_quick_input_keyboard(habit_id: int, lang: str = "kk", unit: str = "") -> InlineKeyboardMarkup:
    """Quick numeric input buttons."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="+0.5", callback_data=f"log_num_{habit_id}_0.5"),
        InlineKeyboardButton(text="+1", callback_data=f"log_num_{habit_id}_1"),
        InlineKeyboardButton(text="+2", callback_data=f"log_num_{habit_id}_2"),
    )
    builder.row(
        InlineKeyboardButton(text="+5", callback_data=f"log_num_{habit_id}_5"),
        InlineKeyboardButton(text="+10", callback_data=f"log_num_{habit_id}_10"),
        InlineKeyboardButton(text=get_text("other_value", lang), callback_data=f"log_custom_{habit_id}")
    )
    builder.row(InlineKeyboardButton(
        text=get_text("btn_cancel", lang),
        callback_data="back_to_habits"
    ))
    return builder.as_markup()


def confirm_keyboard(action: str, item_id: int, lang: str = "kk") -> InlineKeyboardMarkup:
    """Confirmation keyboard."""
    yes_delete = "✅ Иә, жою" if lang == "kk" else "✅ Да, удалить"
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=yes_delete, callback_data=f"confirm_{action}_{item_id}"),
        InlineKeyboardButton(text=get_text("btn_cancel", lang), callback_data="back_to_habits")
    )
    return builder.as_markup()


def notification_response_keyboard(habit_id: int, habit_type: str, lang: str = "kk") -> InlineKeyboardMarkup:
    """Keyboard for notification response."""
    builder = InlineKeyboardBuilder()

    if habit_type == 'boolean':
        completed_text = get_text("completed", lang)
        builder.row(
            InlineKeyboardButton(text=completed_text, callback_data=f"notif_resp_{habit_id}_1"),
            InlineKeyboardButton(text=get_text("btn_no", lang), callback_data=f"notif_resp_{habit_id}_0")
        )
    else:
        builder.row(
            InlineKeyboardButton(text="+1", callback_data=f"notif_resp_{habit_id}_1"),
            InlineKeyboardButton(text="+2", callback_data=f"notif_resp_{habit_id}_2"),
            InlineKeyboardButton(text="+5", callback_data=f"notif_resp_{habit_id}_5"),
        )
        enter_text = "✏️ Санды енгіз" if lang == "kk" else "✏️ Ввести число"
        builder.row(InlineKeyboardButton(
            text=enter_text,
            callback_data=f"notif_custom_{habit_id}"
        ))

    return builder.as_markup()


def notification_times_keyboard(current_times: List[str], lang: str = "kk") -> InlineKeyboardMarkup:
    """Notification times setup."""
    builder = InlineKeyboardBuilder()

    available_times = [
        "06:00", "07:00", "08:00", "09:00", "10:00",
        "12:00", "13:00", "14:00", "15:00",
        "18:00", "19:00", "20:00", "21:00", "22:00"
    ]

    for time in available_times:
        icon = "✅" if time in current_times else "⬜"
        builder.button(
            text=f"{icon} {time}",
            callback_data=f"notif_time_{time}"
        )

    builder.adjust(3)  # 3 buttons per row

    builder.row(InlineKeyboardButton(
        text=get_text("btn_save", lang),
        callback_data="notif_save"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_settings"
    ))

    return builder.as_markup()


def settings_keyboard(lang: str = "kk") -> InlineKeyboardMarkup:
    """Settings menu."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=get_text("btn_language", lang),
        callback_data="settings_language"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("settings_notifications", lang),
        callback_data="settings_notifications"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("settings_categories", lang),
        callback_data="settings_categories"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("settings_habits", lang),
        callback_data="settings_habits"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back_menu", lang),
        callback_data="back_to_menu"
    ))
    return builder.as_markup()


def language_keyboard(current_lang: str = "kk") -> InlineKeyboardMarkup:
    """Language selection keyboard."""
    builder = InlineKeyboardBuilder()

    kk_check = "✅ " if current_lang == "kk" else ""
    ru_check = "✅ " if current_lang == "ru" else ""

    builder.row(InlineKeyboardButton(
        text=f"{kk_check}🇰🇿 Қазақша",
        callback_data="set_lang_kk"
    ))
    builder.row(InlineKeyboardButton(
        text=f"{ru_check}🇷🇺 Русский",
        callback_data="set_lang_ru"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", current_lang),
        callback_data="back_to_settings"
    ))
    return builder.as_markup()


def stats_period_keyboard(lang: str = "kk", habit_id: int = None) -> InlineKeyboardMarkup:
    """Period selection for statistics."""
    builder = InlineKeyboardBuilder()

    prefix = f"stats_{habit_id}_" if habit_id else "stats_all_"

    builder.row(InlineKeyboardButton(
        text=get_text("period_7d", lang),
        callback_data=f"{prefix}7d"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("period_month", lang),
        callback_data=f"{prefix}month"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("period_year", lang),
        callback_data=f"{prefix}year"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("period_custom", lang),
        callback_data=f"{prefix}custom"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def stats_habits_keyboard(habits: List[dict], lang: str = "kk") -> InlineKeyboardMarkup:
    """Select habit for statistics."""
    builder = InlineKeyboardBuilder()

    for habit in habits:
        builder.row(InlineKeyboardButton(
            text=habit['name'],
            callback_data=f"stats_habit_{habit['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("all_habits", lang),
        callback_data="stats_habit_all"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def calendar_keyboard(year: int, month: int, prefix: str = "cal", lang: str = "kk") -> InlineKeyboardMarkup:
    """Calendar for date selection."""
    import calendar

    builder = InlineKeyboardBuilder()

    # Month/Year header
    months = get_text("months", lang)
    builder.row(InlineKeyboardButton(
        text=f"{months[month-1]} {year}",
        callback_data="ignore"
    ))

    # Day headers
    days = get_text("weekdays", lang)
    builder.row(*[InlineKeyboardButton(text=d, callback_data="ignore") for d in days])

    # Calendar days
    cal = calendar.monthcalendar(year, month)
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="ignore"))
            else:
                row.append(InlineKeyboardButton(
                    text=str(day),
                    callback_data=f"{prefix}_{year}_{month}_{day}"
                ))
        builder.row(*row)

    # Navigation
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1

    builder.row(
        InlineKeyboardButton(text="«", callback_data=f"{prefix}_nav_{prev_year}_{prev_month}"),
        InlineKeyboardButton(text=get_text("today", lang), callback_data=f"{prefix}_today"),
        InlineKeyboardButton(text="»", callback_data=f"{prefix}_nav_{next_year}_{next_month}")
    )

    builder.row(InlineKeyboardButton(text=get_text("btn_cancel", lang), callback_data="back_to_stats"))

    return builder.as_markup()


# Marathon keyboards
def marathons_menu_keyboard(lang: str = "kk") -> InlineKeyboardMarkup:
    """Marathon main menu."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=get_text("my_marathons", lang),
        callback_data="marathon_list"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("create_marathon", lang),
        callback_data="marathon_create"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_back_menu", lang),
        callback_data="back_to_menu"
    ))
    return builder.as_markup()


def marathon_detail_keyboard(marathon: dict, is_creator: bool, lang: str = "kk") -> InlineKeyboardMarkup:
    """Marathon detail view."""
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(
        text=get_text("btn_leaderboard", lang),
        callback_data=f"marathon_leaderboard_{marathon['id']}"
    ))
    builder.row(InlineKeyboardButton(
        text=get_text("btn_invite", lang),
        callback_data=f"marathon_invite_{marathon['id']}"
    ))

    if is_creator:
        builder.row(InlineKeyboardButton(
            text=get_text("btn_manage", lang),
            callback_data=f"marathon_manage_{marathon['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_leave", lang),
        callback_data=f"marathon_leave_{marathon['id']}"
    ))

    back_text = "« " + get_text("btn_marathons", lang).replace("🏆 ", "")
    builder.row(InlineKeyboardButton(
        text=back_text,
        callback_data="marathon_list"
    ))

    return builder.as_markup()


def log_habits_keyboard(habits: List[dict], lang: str = "kk") -> InlineKeyboardMarkup:
    """Quick log habits keyboard."""
    builder = InlineKeyboardBuilder()

    for habit in habits:
        status = "✅" if habit.get('completed_today') else "⬜"
        if habit['habit_type'] == 'numeric' and habit.get('today_value', 0) > 0:
            status = f"📝 {habit['today_value']}/{habit['daily_goal']}"
        builder.row(InlineKeyboardButton(
            text=f"{status} {habit['name']}",
            callback_data=f"quick_log_{habit['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text=get_text("btn_back_menu", lang),
        callback_data="back_to_menu"
    ))

    return builder.as_markup()


def marathon_add_habit_keyboard(lang: str = "kk", habit_count: int = 0) -> InlineKeyboardMarkup:
    """Keyboard for adding habits to marathon with Done button."""
    builder = InlineKeyboardBuilder()

    if habit_count > 0:
        builder.row(InlineKeyboardButton(
            text=get_text("btn_done", lang),
            callback_data="marathon_habits_done"
        ))

    return builder.as_markup()


def cancel_keyboard(lang: str = "kk") -> InlineKeyboardMarkup:
    """Cancel button keyboard."""
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=get_text("btn_cancel", lang),
        callback_data="cancel_input"
    ))
    return builder.as_markup()
