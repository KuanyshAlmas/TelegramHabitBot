from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import date, datetime, timedelta
from typing import Union
import json
import secrets

import database as db
import analytics
from keyboards import (
    main_menu_keyboard, habits_keyboard, habit_detail_keyboard,
    habit_type_keyboard, categories_keyboard, boolean_input_keyboard,
    numeric_quick_input_keyboard, confirm_keyboard, notification_times_keyboard,
    settings_keyboard, stats_period_keyboard, stats_habits_keyboard,
    calendar_keyboard, marathons_menu_keyboard, marathon_detail_keyboard,
    log_habits_keyboard, marathon_add_habit_keyboard, cancel_keyboard,
    language_keyboard, habits_categories_keyboard, habits_in_category_keyboard
)
from texts import get_text, get_menu_buttons

router = Router()

# Admin username (without @)
ADMIN_USERNAME = "KuanyshAlmas"


def is_menu_button(text: str) -> bool:
    """Check if text is a menu button in any language."""
    return text in get_menu_buttons("kk") or text in get_menu_buttons("ru")


# ============ FSM States ============
class HabitStates(StatesGroup):
    waiting_name = State()
    waiting_type = State()
    waiting_goal = State()
    waiting_unit = State()
    waiting_category = State()
    waiting_custom_value = State()


class CategoryStates(StatesGroup):
    waiting_name = State()


class MarathonStates(StatesGroup):
    waiting_name = State()
    waiting_start_date = State()
    waiting_end_date = State()
    adding_habits = State()
    waiting_habit_name = State()
    waiting_habit_type = State()
    waiting_habit_goal = State()


class AdminStates(StatesGroup):
    waiting_broadcast_message = State()


class StatsStates(StatesGroup):
    selecting_habit = State()
    selecting_period = State()
    selecting_start_date = State()
    selecting_end_date = State()


class NotificationStates(StatesGroup):
    editing_times = State()


# ============ Start & Menu ============
@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start command with optional invite code."""
    user = await db.get_or_create_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.first_name
    )

    lang = await db.get_user_language(message.from_user.id)

    # Check for invite code in deep link (e.g., /start marathon_ABC123)
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("marathon_"):
        invite_code = args[1].replace("marathon_", "")
        marathon = await db.get_marathon_by_code(invite_code)

        if marathon:
            success = await db.join_marathon(message.from_user.id, marathon['id'])

            if success:
                start = date.fromisoformat(marathon['start_date'])
                end = date.fromisoformat(marathon['end_date'])
                await message.answer(
                    get_text("joined_marathon", lang,
                             name=marathon['name'],
                             start=start.strftime('%d.%m.%Y'),
                             end=end.strftime('%d.%m.%Y')),
                    reply_markup=main_menu_keyboard(lang),
                    parse_mode="Markdown"
                )
            else:
                await message.answer(
                    get_text("already_in_marathon", lang),
                    reply_markup=main_menu_keyboard(lang)
                )
            return
        else:
            await message.answer(
                get_text("marathon_not_found", lang),
                reply_markup=main_menu_keyboard(lang)
            )
            return

    await message.answer(
        get_text("welcome", lang, name=message.from_user.first_name),
        reply_markup=main_menu_keyboard(lang),
        parse_mode="Markdown"
    )


@router.message(F.text.in_({"« Главное меню", "« Басты мәзір"}))
@router.callback_query(F.data == "back_to_menu")
async def back_to_menu(event: Union[Message, CallbackQuery]):
    """Return to main menu."""
    user_id = event.from_user.id
    lang = await db.get_user_language(user_id)

    if isinstance(event, CallbackQuery):
        await event.message.edit_text(
            get_text("main_menu", lang),
            reply_markup=None
        )
        await event.message.answer(get_text("choose_action", lang), reply_markup=main_menu_keyboard(lang))
        await event.answer()
    else:
        await event.answer(get_text("main_menu", lang), reply_markup=main_menu_keyboard(lang))


@router.callback_query(F.data == "cancel_input")
async def cancel_input(callback: CallbackQuery, state: FSMContext):
    """Cancel current input and return to menu."""
    await state.clear()
    lang = await db.get_user_language(callback.from_user.id)
    await callback.message.edit_text(get_text("action_cancelled", lang))
    await callback.message.answer(get_text("main_menu", lang), reply_markup=main_menu_keyboard(lang))
    await callback.answer()


# ============ Habits ============
@router.message(F.text.in_({"📝 Мои привычки", "📝 Менің әдеттерім"}))
async def show_habits(message: Message):
    """Show categories first, then habits when category is selected."""
    lang = await db.get_user_language(message.from_user.id)
    habits = await db.get_user_habits(message.from_user.id)
    categories = await db.get_user_categories(message.from_user.id)

    # Add completion status for today
    for habit in habits:
        log = await db.get_daily_log(habit['id'])
        habit['completed_today'] = log['completed'] if log else False

    if not habits:
        text = get_text("no_habits", lang)
        await message.answer(
            text,
            reply_markup=habits_keyboard(habits, lang),
            parse_mode="Markdown"
        )
    else:
        # Show categories with habit counts
        text = get_text("your_habits", lang)
        text += "\n\n"
        select_cat_text = "Выбери категорию:" if lang == "ru" else "Санатты таңда:"
        text += select_cat_text

        await message.answer(
            text,
            reply_markup=habits_categories_keyboard(categories, habits, lang),
            parse_mode="Markdown"
        )


@router.callback_query(F.data == "back_to_habits")
async def back_to_habits(callback: CallbackQuery, state: FSMContext):
    """Return to categories list (habits filter)."""
    await state.clear()
    lang = await db.get_user_language(callback.from_user.id)

    habits = await db.get_user_habits(callback.from_user.id)
    categories = await db.get_user_categories(callback.from_user.id)

    for habit in habits:
        log = await db.get_daily_log(habit['id'])
        habit['completed_today'] = log['completed'] if log else False

    text = get_text("your_habits_short", lang)
    text += "\n\n"
    select_cat_text = "Выбери категорию:" if lang == "ru" else "Санатты таңда:"
    text += select_cat_text

    await callback.message.edit_text(
        text,
        reply_markup=habits_categories_keyboard(categories, habits, lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("habits_cat_"))
async def show_habits_in_category(callback: CallbackQuery):
    """Show habits filtered by category."""
    lang = await db.get_user_language(callback.from_user.id)
    cat_id_str = callback.data.replace("habits_cat_", "")

    if cat_id_str == "all":
        # Show all habits
        habits = await db.get_user_habits(callback.from_user.id)
        title = "📋 Все привычки:" if lang == "ru" else "📋 Барлық әдеттер:"
        category_id = None
    elif cat_id_str == "none":
        # Habits without category
        habits = await db.get_habits_by_category(callback.from_user.id, None)
        title = "📌 Без категории:" if lang == "ru" else "📌 Санатсыз:"
        category_id = None
    else:
        # Specific category
        category_id = int(cat_id_str)
        habits = await db.get_habits_by_category(callback.from_user.id, category_id)
        category = await db.get_category(category_id)
        if category:
            title = f"{category['icon']} {category['name']}:"
        else:
            title = "Привычки:"

    # Add completion status
    for habit in habits:
        log = await db.get_daily_log(habit['id'])
        habit['completed_today'] = log['completed'] if log else False

    if not habits:
        empty_text = "В этой категории пока нет привычек." if lang == "ru" else "Бұл санатта әдеттер жоқ."
        text = f"{title}\n\n_{empty_text}_"
    else:
        text = title

    await callback.message.edit_text(
        text,
        reply_markup=habits_in_category_keyboard(habits, category_id, lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "habit_create")
async def start_create_habit(callback: CallbackQuery, state: FSMContext):
    """Start habit creation flow."""
    await callback.message.edit_text(
        "Давай создадим новую привычку! 🎯\n\n"
        "Введи название привычки:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(HabitStates.waiting_name)
    await callback.answer()


@router.message(HabitStates.waiting_name)
async def habit_name_received(message: Message, state: FSMContext):
    """Receive habit name."""
    if message.text and is_menu_button(message.text):
        await message.answer("Введи название привычки:")
        return

    await state.update_data(name=message.text)
    await message.answer(
        f"Отлично! Привычка: **{message.text}**\n\n"
        "Выбери тип привычки:",
        reply_markup=habit_type_keyboard(),
        parse_mode="Markdown"
    )
    await state.set_state(HabitStates.waiting_type)


@router.callback_query(F.data.startswith("type_"), HabitStates.waiting_type)
async def habit_type_selected(callback: CallbackQuery, state: FSMContext):
    """Receive habit type."""
    habit_type = callback.data.split("_")[1]
    await state.update_data(habit_type=habit_type)

    if habit_type == "boolean":
        # Skip goal and unit for boolean
        await state.update_data(daily_goal=1, unit="")

        categories = await db.get_user_categories(callback.from_user.id)
        await callback.message.edit_text(
            "Выбери категорию для привычки:",
            reply_markup=categories_keyboard(categories, "select")
        )
        await state.set_state(HabitStates.waiting_category)
    else:
        await callback.message.edit_text(
            "Введи дневную цель (число):\n"
            "Например: 3 (литра воды), 50 (страниц)",
            reply_markup=None
        )
        await state.set_state(HabitStates.waiting_goal)

    await callback.answer()


@router.message(HabitStates.waiting_goal)
async def habit_goal_received(message: Message, state: FSMContext):
    """Receive daily goal."""
    try:
        goal = float(message.text.replace(",", "."))
        await state.update_data(daily_goal=goal)

        await message.answer(
            f"Цель: **{goal}**\n\n"
            "Введи единицу измерения:\n"
            "Например: литров, страниц, минут, км",
            parse_mode="Markdown"
        )
        await state.set_state(HabitStates.waiting_unit)
    except ValueError:
        await message.answer("Пожалуйста, введи число. Например: 3 или 2.5")


@router.message(HabitStates.waiting_unit)
async def habit_unit_received(message: Message, state: FSMContext):
    """Receive unit."""
    await state.update_data(unit=message.text)

    categories = await db.get_user_categories(message.from_user.id)
    await message.answer(
        "Выбери категорию для привычки:",
        reply_markup=categories_keyboard(categories, "select")
    )
    await state.set_state(HabitStates.waiting_category)


@router.callback_query(F.data.startswith("cat_select_"), HabitStates.waiting_category)
async def habit_category_selected(callback: CallbackQuery, state: FSMContext):
    """Receive category and create habit."""
    cat_id = callback.data.split("_")[2]
    category_id = None if cat_id == "none" else int(cat_id)

    data = await state.get_data()

    habit_id = await db.create_habit(
        user_id=callback.from_user.id,
        name=data['name'],
        habit_type=data['habit_type'],
        daily_goal=data['daily_goal'],
        unit=data.get('unit', ''),
        category_id=category_id
    )

    await state.clear()

    type_text = "Галочка" if data['habit_type'] == 'boolean' else f"Цель: {data['daily_goal']} {data.get('unit', '')}"

    await callback.message.edit_text(
        f"✅ Привычка создана!\n\n"
        f"**{data['name']}**\n"
        f"Тип: {type_text}\n\n"
        f"Теперь я буду напоминать тебе о ней!",
        parse_mode="Markdown"
    )

    lang = await db.get_user_language(callback.from_user.id)
    habits = await db.get_user_habits(callback.from_user.id)
    await callback.message.answer(
        get_text("your_habits_short", lang),
        reply_markup=habits_keyboard(habits, lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("habit_view_"))
async def view_habit(callback: CallbackQuery):
    """View habit details."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    if not habit:
        await callback.answer("Привычка не найдена", show_alert=True)
        return

    log = await db.get_daily_log(habit_id)
    today_value = log['value'] if log else 0

    if habit['habit_type'] == 'boolean':
        status = "✅ Выполнено" if log and log['completed'] else "⬜ Не выполнено"
        text = f"**{habit['name']}**\n\n"
        text += f"Тип: Галочка (да/нет)\n"
        text += f"Сегодня: {status}\n"
    else:
        text = f"**{habit['name']}**\n\n"
        text += f"Тип: Цифровая\n"
        text += f"Цель: {habit['daily_goal']} {habit['unit']}\n"
        text += f"Сегодня: {today_value}/{habit['daily_goal']} {habit['unit']}\n"

    text += f"\n🔥 Страйк: {habit['streak']} дней"
    text += f"\n🏆 Рекорд: {habit['max_streak']} дней"

    is_marathon = habit.get('marathon_id') is not None
    await callback.message.edit_text(
        text,
        reply_markup=habit_detail_keyboard(habit, is_marathon),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("habit_delete_"))
async def delete_habit_confirm(callback: CallbackQuery):
    """Confirm habit deletion."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    if habit.get('marathon_id'):
        await callback.answer(
            "Нельзя удалить привычку марафона. Выйди из марафона целиком.",
            show_alert=True
        )
        return

    await callback.message.edit_text(
        f"Удалить привычку **{habit['name']}**?\n\n"
        "⚠️ Вся история и страйки будут удалены!",
        reply_markup=confirm_keyboard("delete_habit", habit_id),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_delete_habit_"))
async def delete_habit_confirmed(callback: CallbackQuery):
    """Delete habit."""
    habit_id = int(callback.data.split("_")[3])
    await db.delete_habit(habit_id)

    await callback.message.edit_text("✅ Привычка удалена")

    lang = await db.get_user_language(callback.from_user.id)
    habits = await db.get_user_habits(callback.from_user.id)
    await callback.message.answer(
        get_text("your_habits_short", lang),
        reply_markup=habits_keyboard(habits, lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("habit_edit_"))
async def edit_habit_menu(callback: CallbackQuery):
    """Show habit edit options."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    if not habit:
        await callback.answer("Привычка не найдена", show_alert=True)
        return

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(
        text="✏️ Изменить название",
        callback_data=f"edit_name_{habit_id}"
    ))

    if habit['habit_type'] == 'numeric':
        builder.row(InlineKeyboardButton(
            text="🎯 Изменить цель",
            callback_data=f"edit_goal_{habit_id}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад",
        callback_data=f"habit_view_{habit_id}"
    ))

    await callback.message.edit_text(
        f"✏️ Редактирование: **{habit['name']}**\n\n"
        f"Что изменить?",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


class EditHabitStates(StatesGroup):
    waiting_new_name = State()
    waiting_new_goal = State()


@router.callback_query(F.data.startswith("edit_name_"))
async def edit_habit_name_start(callback: CallbackQuery, state: FSMContext):
    """Start editing habit name."""
    habit_id = int(callback.data.split("_")[2])
    await state.update_data(editing_habit_id=habit_id)
    await state.set_state(EditHabitStates.waiting_new_name)

    await callback.message.edit_text("Введи новое название привычки:")
    await callback.answer()


@router.message(EditHabitStates.waiting_new_name)
async def edit_habit_name_done(message: Message, state: FSMContext):
    """Save new habit name."""
    data = await state.get_data()
    habit_id = data['editing_habit_id']

    await db.update_habit(habit_id, name=message.text)
    await state.clear()

    await message.answer(f"✅ Название изменено на: **{message.text}**", parse_mode="Markdown")

    habit = await db.get_habit(habit_id)
    habits = await db.get_user_habits(message.from_user.id)
    await message.answer(
        "📝 Твои привычки:",
        reply_markup=habits_keyboard(habits, lang)
    )


@router.callback_query(F.data.startswith("edit_goal_"))
async def edit_habit_goal_start(callback: CallbackQuery, state: FSMContext):
    """Start editing habit goal."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    await state.update_data(editing_habit_id=habit_id)
    await state.set_state(EditHabitStates.waiting_new_goal)

    await callback.message.edit_text(
        f"Текущая цель: {habit['daily_goal']} {habit.get('unit', '')}\n\n"
        f"Введи новую дневную цель (число):"
    )
    await callback.answer()


@router.message(EditHabitStates.waiting_new_goal)
async def edit_habit_goal_done(message: Message, state: FSMContext):
    """Save new habit goal."""
    try:
        new_goal = float(message.text.replace(",", "."))
        data = await state.get_data()
        habit_id = data['editing_habit_id']

        await db.update_habit(habit_id, daily_goal=new_goal)
        await state.clear()

        await message.answer(f"✅ Цель изменена на: **{new_goal}**", parse_mode="Markdown")

        habits = await db.get_user_habits(message.from_user.id)
        await message.answer(
            "📝 Твои привычки:",
            reply_markup=habits_keyboard(habits, lang)
        )
    except ValueError:
        await message.answer("Введи число. Например: 5 или 2.5")


# ============ Quick Log ============
@router.message(F.text.in_({"➕ Внести результат", "➕ Нәтиже енгізу"}))
async def quick_log_menu(message: Message):
    """Show habits for quick logging."""
    lang = await db.get_user_language(message.from_user.id)
    habits = await db.get_user_habits(message.from_user.id)

    for habit in habits:
        log = await db.get_daily_log(habit['id'])
        habit['completed_today'] = log['completed'] if log else False
        habit['today_value'] = log['value'] if log else 0

    if not habits:
        await message.answer(get_text("no_habits", lang))
        return

    await message.answer(
        get_text("select_habit_to_log", lang),
        reply_markup=log_habits_keyboard(habits, lang)
    )


@router.callback_query(F.data.startswith("quick_log_"))
@router.callback_query(F.data.startswith("habit_log_"))
async def start_log_habit(callback: CallbackQuery, state: FSMContext):
    """Start logging a habit."""
    habit_id = int(callback.data.split("_")[-1])
    habit = await db.get_habit(habit_id)

    if not habit:
        await callback.answer("Привычка не найдена", show_alert=True)
        return

    await state.update_data(logging_habit_id=habit_id)

    log = await db.get_daily_log(habit_id)
    today_value = log['value'] if log else 0

    if habit['habit_type'] == 'boolean':
        await callback.message.edit_text(
            f"**{habit['name']}**\n\nВыполнено сегодня?",
            reply_markup=boolean_input_keyboard(habit_id),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            f"**{habit['name']}**\n\n"
            f"Сейчас: {today_value}/{habit['daily_goal']} {habit['unit']}\n\n"
            f"Сколько добавить?",
            reply_markup=numeric_quick_input_keyboard(habit_id, habit['unit']),
            parse_mode="Markdown"
        )

    await callback.answer()


@router.callback_query(F.data.startswith("log_bool_"))
async def log_boolean(callback: CallbackQuery, state: FSMContext):
    """Log boolean habit."""
    parts = callback.data.split("_")
    habit_id = int(parts[2])
    value = int(parts[3])

    result = await db.log_habit(habit_id, callback.from_user.id, value)
    await db.mark_notification_responded(callback.from_user.id, habit_id)

    status = "✅ Выполнено" if value else "❌ Не выполнено"
    await callback.message.edit_text(
        f"Принято! {status}\n\n"
        f"**{result['habit']['name']}**",
        parse_mode="Markdown"
    )
    await state.clear()
    await callback.answer("Записано!")


@router.callback_query(F.data.startswith("log_num_"))
async def log_numeric(callback: CallbackQuery, state: FSMContext):
    """Log numeric habit with quick button."""
    parts = callback.data.split("_")
    habit_id = int(parts[2])
    value = float(parts[3])

    result = await db.log_habit(habit_id, callback.from_user.id, value)
    await db.mark_notification_responded(callback.from_user.id, habit_id)

    habit = result['habit']
    new_value = result['new_value']
    goal = result['daily_goal']
    unit = habit.get('unit', '')

    status = "✅" if new_value >= goal else ""
    await callback.message.edit_text(
        f"Принято +{value} {unit}!\n\n"
        f"**{habit['name']}**: {new_value}/{goal} {unit} {status}",
        parse_mode="Markdown"
    )
    await state.clear()
    await callback.answer(f"+{value} {unit}")


@router.callback_query(F.data.startswith("log_custom_"))
async def log_custom_start(callback: CallbackQuery, state: FSMContext):
    """Start custom value input."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    await state.update_data(logging_habit_id=habit_id)
    await state.set_state(HabitStates.waiting_custom_value)

    await callback.message.edit_text(
        f"**{habit['name']}**\n\n"
        f"Введи число ({habit.get('unit', '')}):",
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(HabitStates.waiting_custom_value)
async def log_custom_value(message: Message, state: FSMContext):
    """Log custom numeric value."""
    try:
        value = float(message.text.replace(",", "."))
        data = await state.get_data()
        habit_id = data['logging_habit_id']

        result = await db.log_habit(habit_id, message.from_user.id, value)
        await db.mark_notification_responded(message.from_user.id, habit_id)

        habit = result['habit']
        new_value = result['new_value']
        goal = result['daily_goal']
        unit = habit.get('unit', '')

        status = "✅" if new_value >= goal else ""
        await message.answer(
            f"Принято +{value} {unit}!\n\n"
            f"**{habit['name']}**: {new_value}/{goal} {unit} {status}",
            parse_mode="Markdown"
        )
        await state.clear()

    except ValueError:
        await message.answer("Пожалуйста, введи число. Например: 2.5")


# ============ Notification Response ============
@router.callback_query(F.data.startswith("notif_resp_"))
async def notification_response(callback: CallbackQuery):
    """Handle notification response."""
    parts = callback.data.split("_")
    habit_id = int(parts[2])
    value = float(parts[3])

    result = await db.log_habit(habit_id, callback.from_user.id, value)
    await db.mark_notification_responded(callback.from_user.id, habit_id)

    habit = result['habit']
    new_value = result['new_value']
    goal = result['daily_goal']

    if habit['habit_type'] == 'boolean':
        status = "✅ Выполнено" if value else "❌ Не выполнено"
        await callback.message.edit_text(f"Принято! {status}")
    else:
        unit = habit.get('unit', '')
        status = "✅" if new_value >= goal else ""
        await callback.message.edit_text(
            f"Принято +{value} {unit}!\n"
            f"Итого за сегодня: {new_value}/{goal} {unit} {status}"
        )

    await callback.answer("Записано вовремя! ⏱")


@router.callback_query(F.data.startswith("notif_custom_"))
async def notification_custom_input(callback: CallbackQuery, state: FSMContext):
    """Start custom input from notification."""
    habit_id = int(callback.data.split("_")[2])
    await state.update_data(logging_habit_id=habit_id)
    await state.set_state(HabitStates.waiting_custom_value)

    habit = await db.get_habit(habit_id)
    await callback.message.edit_text(
        f"**{habit['name']}**\n\nВведи число:",
        parse_mode="Markdown"
    )
    await callback.answer()


# ============ Categories ============
@router.message(F.text.in_({"📁 Категории", "📁 Санаттар"}))
async def show_categories(message: Message):
    """Show categories with their habits."""
    lang = await db.get_user_language(message.from_user.id)
    categories = await db.get_user_categories(message.from_user.id)
    habits = await db.get_user_habits(message.from_user.id)

    text = get_text("your_categories", lang)

    for cat in categories:
        text += f"**{cat['icon']} {cat['name']}**\n"
        # Find habits in this category
        cat_habits = [h for h in habits if h.get('category_id') == cat['id']]
        if cat_habits:
            for h in cat_habits:
                streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
                text += f"  • {h['name']}{streak}\n"
        else:
            text += f"  _(пусто)_\n"
        text += "\n"

    # Habits without category
    no_cat_habits = [h for h in habits if h.get('category_id') is None]
    if no_cat_habits:
        text += "**📌 Без категории**\n"
        for h in no_cat_habits:
            streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
            text += f"  • {h['name']}{streak}\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    # Buttons to view each category
    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"{cat['icon']} {cat['name']}",
            callback_data=f"cat_view_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Главное меню",
        callback_data="back_to_menu"
    ))

    await message.answer(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )


@router.callback_query(F.data.startswith("cat_view_"))
async def view_category(callback: CallbackQuery):
    """View habits in a category."""
    cat_id = int(callback.data.split("_")[2])
    categories = await db.get_user_categories(callback.from_user.id)
    habits = await db.get_user_habits(callback.from_user.id)

    cat = next((c for c in categories if c['id'] == cat_id), None)
    if not cat:
        await callback.answer("Категория не найдена", show_alert=True)
        return

    cat_habits = [h for h in habits if h.get('category_id') == cat_id]

    text = f"**{cat['icon']} {cat['name']}**\n\n"

    if cat_habits:
        for h in cat_habits:
            streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
            if h['habit_type'] == 'numeric':
                text += f"• {h['name']} (цель: {h['daily_goal']} {h.get('unit', '')}){streak}\n"
            else:
                text += f"• {h['name']}{streak}\n"
    else:
        text += "_(В этой категории пока нет привычек)_"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(
        text="« Назад к категориям",
        callback_data="back_to_categories"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: CallbackQuery):
    """Return to categories list."""
    categories = await db.get_user_categories(callback.from_user.id)
    habits = await db.get_user_habits(callback.from_user.id)

    text = "📁 **Твои категории:**\n\n"

    for cat in categories:
        text += f"**{cat['icon']} {cat['name']}**\n"
        cat_habits = [h for h in habits if h.get('category_id') == cat['id']]
        if cat_habits:
            for h in cat_habits:
                streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
                text += f"  • {h['name']}{streak}\n"
        else:
            text += f"  _(пусто)_\n"
        text += "\n"

    no_cat_habits = [h for h in habits if h.get('category_id') is None]
    if no_cat_habits:
        text += "**📌 Без категории**\n"
        for h in no_cat_habits:
            streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
            text += f"  • {h['name']}{streak}\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"{cat['icon']} {cat['name']}",
            callback_data=f"cat_view_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Главное меню",
        callback_data="back_to_menu"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "cat_create")
async def create_category_start(callback: CallbackQuery, state: FSMContext):
    """Start category creation."""
    await callback.message.edit_text(
        "Введи название новой категории:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(CategoryStates.waiting_name)
    await callback.answer()


@router.message(CategoryStates.waiting_name)
async def create_category_name(message: Message, state: FSMContext):
    """Create category with name."""
    if message.text and is_menu_button(message.text):
        await message.answer("Введи название категории:")
        return

    await db.create_category(message.from_user.id, message.text)
    await state.clear()

    await message.answer(f"✅ Категория **{message.text}** создана!", parse_mode="Markdown")

    categories = await db.get_user_categories(message.from_user.id)

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"🗑 {cat['name']}",
            callback_data=f"cat_delete_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="➕ Новая категория",
        callback_data="cat_create"
    ))
    builder.row(InlineKeyboardButton(
        text="« Главное меню",
        callback_data="back_to_menu"
    ))

    await message.answer(
        "📁 Твои категории:",
        reply_markup=builder.as_markup()
    )


# ============ Settings ============
@router.message(F.text.in_({"⚙️ Настройки", "⚙️ Баптаулар"}))
async def show_settings(message: Message):
    """Show settings menu."""
    lang = await db.get_user_language(message.from_user.id)
    await message.answer(
        get_text("settings", lang),
        reply_markup=settings_keyboard(lang),
        parse_mode="Markdown"
    )


@router.callback_query(F.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery):
    """Return to settings."""
    lang = await db.get_user_language(callback.from_user.id)
    await callback.message.edit_text(
        get_text("settings", lang),
        reply_markup=settings_keyboard(lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "settings_language")
async def settings_language(callback: CallbackQuery):
    """Show language settings."""
    lang = await db.get_user_language(callback.from_user.id)
    await callback.message.edit_text(
        get_text("select_language", lang),
        reply_markup=language_keyboard(lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("set_lang_"))
async def set_language(callback: CallbackQuery):
    """Set user language."""
    new_lang = callback.data.split("_")[2]
    await db.set_user_language(callback.from_user.id, new_lang)

    await callback.message.edit_text(
        get_text("language_changed", new_lang),
        reply_markup=None
    )
    await callback.message.answer(
        get_text("settings", new_lang),
        reply_markup=settings_keyboard(new_lang),
        parse_mode="Markdown"
    )
    await callback.message.answer(
        get_text("choose_action", new_lang),
        reply_markup=main_menu_keyboard(new_lang)
    )
    await callback.answer()


@router.callback_query(F.data == "settings_categories")
async def settings_categories(callback: CallbackQuery):
    """Manage categories from settings."""
    categories = await db.get_user_categories(callback.from_user.id)

    text = "📁 **Управление категориями:**\n\n"
    for cat in categories:
        text += f"• {cat['icon']} {cat['name']}\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"🗑 {cat['name']}",
            callback_data=f"cat_delete_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="➕ Новая категория",
        callback_data="cat_create"
    ))
    builder.row(InlineKeyboardButton(
        text="« Назад",
        callback_data="back_to_settings"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cat_delete_"))
async def delete_category_confirm(callback: CallbackQuery):
    """Delete a category."""
    cat_id = int(callback.data.split("_")[2])
    await db.delete_category(cat_id)
    await callback.answer("Категория удалена!")

    # Refresh categories list
    categories = await db.get_user_categories(callback.from_user.id)

    text = "📁 **Управление категориями:**\n\n"
    for cat in categories:
        text += f"• {cat['icon']} {cat['name']}\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for cat in categories:
        builder.row(InlineKeyboardButton(
            text=f"🗑 {cat['name']}",
            callback_data=f"cat_delete_{cat['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="➕ Новая категория",
        callback_data="cat_create"
    ))
    builder.row(InlineKeyboardButton(
        text="« Назад",
        callback_data="back_to_settings"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )


@router.callback_query(F.data == "settings_habits")
async def settings_habits(callback: CallbackQuery):
    """Manage habits from settings."""
    habits = await db.get_user_habits(callback.from_user.id)

    if not habits:
        await callback.message.edit_text(
            "📝 У тебя пока нет привычек.",
            reply_markup=settings_keyboard()
        )
        await callback.answer()
        return

    text = "📝 **Управление привычками:**\n\n"
    text += "Выбери привычку для редактирования или удаления:"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for habit in habits:
        is_marathon = habit.get('marathon_id') is not None
        icon = "🏆" if is_marathon else "📌"
        builder.row(InlineKeyboardButton(
            text=f"{icon} {habit['name']}",
            callback_data=f"manage_habit_{habit['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад",
        callback_data="back_to_settings"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("manage_habit_"))
async def manage_habit_menu(callback: CallbackQuery):
    """Show habit management options."""
    habit_id = int(callback.data.split("_")[2])
    habit = await db.get_habit(habit_id)

    if not habit:
        await callback.answer("Привычка не найдена", show_alert=True)
        return

    is_marathon = habit.get('marathon_id') is not None

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(
        text="✏️ Изменить название",
        callback_data=f"edit_name_{habit_id}"
    ))

    if habit['habit_type'] == 'numeric':
        builder.row(InlineKeyboardButton(
            text="🎯 Изменить цель",
            callback_data=f"edit_goal_{habit_id}"
        ))

    if is_marathon:
        builder.row(InlineKeyboardButton(
            text="⚠️ Нельзя удалить (марафон)",
            callback_data="ignore"
        ))
    else:
        builder.row(InlineKeyboardButton(
            text="🗑 Удалить привычку",
            callback_data=f"habit_delete_{habit_id}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад к списку",
        callback_data="settings_habits"
    ))

    streak_text = f"🔥 Страйк: {habit['streak']} дней" if habit['streak'] > 0 else ""
    goal_text = f"Цель: {habit['daily_goal']} {habit.get('unit', '')}" if habit['habit_type'] == 'numeric' else "Тип: Галочка"

    await callback.message.edit_text(
        f"⚙️ **{habit['name']}**\n\n"
        f"{goal_text}\n"
        f"{streak_text}",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "settings_notifications")
async def edit_notifications(callback: CallbackQuery, state: FSMContext):
    """Edit notification times."""
    current_times = await db.get_user_notification_times(callback.from_user.id)
    await state.update_data(selected_times=current_times)

    await callback.message.edit_text(
        "🔔 **Время уведомлений**\n\n"
        "Выбери время, когда бот будет напоминать о привычках.\n"
        "Все время указано по Казахстану.\n\n"
        "✅ — выбрано\n⬜ — не выбрано",
        reply_markup=notification_times_keyboard(current_times),
        parse_mode="Markdown"
    )
    await state.set_state(NotificationStates.editing_times)
    await callback.answer()


@router.callback_query(F.data.startswith("notif_time_"), NotificationStates.editing_times)
async def toggle_notification_time(callback: CallbackQuery, state: FSMContext):
    """Toggle notification time."""
    time = callback.data.split("_")[2]
    data = await state.get_data()
    times = data.get('selected_times', [])

    if time in times:
        times.remove(time)
    else:
        times.append(time)
        times.sort()

    await state.update_data(selected_times=times)

    await callback.message.edit_reply_markup(
        reply_markup=notification_times_keyboard(times)
    )
    await callback.answer()


@router.callback_query(F.data == "notif_save", NotificationStates.editing_times)
async def save_notification_times(callback: CallbackQuery, state: FSMContext):
    """Save notification times."""
    data = await state.get_data()
    times = data.get('selected_times', [])

    if not times:
        await callback.answer("Выбери хотя бы одно время!", show_alert=True)
        return

    await db.update_notification_times(callback.from_user.id, times)
    await state.clear()

    times_text = ", ".join(times)
    await callback.message.edit_text(
        f"✅ Время уведомлений сохранено!\n\n"
        f"Ты будешь получать напоминания в: {times_text}",
        reply_markup=settings_keyboard()
    )
    await callback.answer("Сохранено!")


# ============ Statistics ============
@router.message(F.text.in_({"📊 Моя статистика", "📊 Менің статистикам"}))
async def show_stats_menu(message: Message, state: FSMContext):
    """Show statistics menu."""
    lang = await db.get_user_language(message.from_user.id)
    habits = await db.get_user_habits(message.from_user.id)

    if not habits:
        await message.answer(get_text("no_habits_for_stats", lang))
        return

    await message.answer(
        get_text("statistics", lang),
        reply_markup=stats_habits_keyboard(habits, lang),
        parse_mode="Markdown"
    )
    await state.set_state(StatsStates.selecting_habit)


@router.callback_query(F.data == "back_to_stats")
async def back_to_stats(callback: CallbackQuery, state: FSMContext):
    """Return to stats menu."""
    habits = await db.get_user_habits(callback.from_user.id)
    await callback.message.edit_text(
        "📊 **Статистика**\n\n"
        "По какой привычке нужен отчет?",
        reply_markup=stats_habits_keyboard(habits, lang),
        parse_mode="Markdown"
    )
    await state.set_state(StatsStates.selecting_habit)
    await callback.answer()


@router.callback_query(F.data.startswith("stats_habit_"), StatsStates.selecting_habit)
async def select_stats_habit(callback: CallbackQuery, state: FSMContext):
    """Select habit for statistics."""
    habit_id = callback.data.split("_")[2]

    if habit_id == "all":
        await state.update_data(stats_habit_id=None)
    else:
        await state.update_data(stats_habit_id=int(habit_id))

    await callback.message.edit_text(
        "📅 За какой период?\n\n"
        "Выбери период или укажи даты вручную:",
        reply_markup=stats_period_keyboard(habit_id if habit_id != "all" else None)
    )
    await state.set_state(StatsStates.selecting_period)
    await callback.answer()


@router.callback_query(F.data.startswith("stats_"), StatsStates.selecting_period)
async def select_stats_period(callback: CallbackQuery, state: FSMContext):
    """Select period for statistics."""
    parts = callback.data.split("_")
    period = parts[-1]

    data = await state.get_data()
    habit_id = data.get('stats_habit_id')

    today = date.today()

    if period == "7d":
        start_date = today - timedelta(days=6)
        end_date = today
    elif period == "month":
        start_date = today.replace(day=1)
        end_date = today
    elif period == "year":
        start_date = today.replace(month=1, day=1)
        end_date = today
    elif period == "custom":
        await callback.message.edit_text(
            "📅 Выбери дату начала:",
            reply_markup=calendar_keyboard(today.year, today.month, "cal_start")
        )
        await state.set_state(StatsStates.selecting_start_date)
        await callback.answer()
        return
    else:
        await callback.answer()
        return

    await generate_and_send_stats(callback, state, habit_id, start_date, end_date)


@router.callback_query(F.data.startswith("cal_start_"), StatsStates.selecting_start_date)
async def select_start_date(callback: CallbackQuery, state: FSMContext):
    """Select start date from calendar."""
    parts = callback.data.split("_")

    if parts[2] == "nav":
        # Navigation
        year, month = int(parts[3]), int(parts[4])
        await callback.message.edit_reply_markup(
            reply_markup=calendar_keyboard(year, month, "cal_start")
        )
    elif parts[2] == "today":
        today = date.today()
        await state.update_data(stats_start=today.isoformat())
        await callback.message.edit_text(
            f"Дата начала: {today.strftime('%d.%m.%Y')}\n\n"
            "📅 Теперь выбери дату окончания:",
            reply_markup=calendar_keyboard(today.year, today.month, "cal_end")
        )
        await state.set_state(StatsStates.selecting_end_date)
    else:
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        selected = date(year, month, day)
        await state.update_data(stats_start=selected.isoformat())
        await callback.message.edit_text(
            f"Дата начала: {selected.strftime('%d.%m.%Y')}\n\n"
            "📅 Теперь выбери дату окончания:",
            reply_markup=calendar_keyboard(year, month, "cal_end")
        )
        await state.set_state(StatsStates.selecting_end_date)

    await callback.answer()


@router.callback_query(F.data.startswith("cal_end_"), StatsStates.selecting_end_date)
async def select_end_date(callback: CallbackQuery, state: FSMContext):
    """Select end date from calendar."""
    parts = callback.data.split("_")

    if parts[2] == "nav":
        year, month = int(parts[3]), int(parts[4])
        await callback.message.edit_reply_markup(
            reply_markup=calendar_keyboard(year, month, "cal_end")
        )
        await callback.answer()
        return
    elif parts[2] == "today":
        end_date = date.today()
    else:
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        end_date = date(year, month, day)

    data = await state.get_data()
    start_date = date.fromisoformat(data['stats_start'])
    habit_id = data.get('stats_habit_id')

    if end_date < start_date:
        await callback.answer("Дата окончания должна быть позже даты начала!", show_alert=True)
        return

    await generate_and_send_stats(callback, state, habit_id, start_date, end_date)


async def generate_and_send_stats(callback: CallbackQuery, state: FSMContext,
                                   habit_id: int, start_date: date, end_date: date):
    """Generate and send statistics."""
    await callback.message.edit_text("⏳ Генерирую отчет...")

    if habit_id:
        # Single habit stats
        stats = await db.get_habit_stats(habit_id, start_date, end_date)
        habit = stats['habit']

        days = (end_date - start_date).days + 1
        text = f"📊 **Статистика: {habit['name']}**\n"
        text += f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')} ({days} дней)\n\n"

        if habit['habit_type'] == 'boolean':
            text += f"✅ Выполнено: {stats['completed_days']} раз\n"
            text += f"❌ Пропущено: {stats['missed_days']} раз\n"
            text += f"📈 Эффективность: {stats['efficiency']}%"
        else:
            text += f"📊 Всего: {stats['total_value']} {habit.get('unit', '')}\n"
            text += f"📈 В среднем: {stats['average']} в день\n"
            if stats['best_day']:
                best_date = stats['best_day']['log_date']
                text += f"🏆 Лучший результат: {stats['best_day']['value']} ({best_date})"

        await callback.message.edit_text(text, parse_mode="Markdown")

        # Generate chart
        if stats.get('logs'):
            chart = analytics.generate_habit_report_chart(stats)
            if chart:
                await callback.message.answer_photo(
                    BufferedInputFile(chart.read(), filename="stats.png")
                )
    else:
        # All habits summary
        habits = await db.get_user_habits(callback.from_user.id)
        text = f"📊 **Общая статистика**\n"
        text += f"Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"

        for habit in habits:
            stats = await db.get_habit_stats(habit['id'], start_date, end_date)
            if habit['habit_type'] == 'boolean':
                text += f"• **{habit['name']}**: {stats['efficiency']}% ({stats['completed_days']} дней)\n"
            else:
                text += f"• **{habit['name']}**: {stats['total_value']} {habit.get('unit', '')} (ср. {stats['average']})\n"

        await callback.message.edit_text(text, parse_mode="Markdown")

        # Generate streak chart
        if habits:
            chart = analytics.create_streak_chart(habits)
            await callback.message.answer_photo(
                BufferedInputFile(chart.read(), filename="streaks.png")
            )

    await state.clear()
    await callback.answer()


@router.callback_query(F.data.startswith("habit_stats_"))
async def quick_habit_stats(callback: CallbackQuery, state: FSMContext):
    """Quick stats for habit (last 7 days)."""
    habit_id = int(callback.data.split("_")[2])
    today = date.today()
    start_date = today - timedelta(days=6)

    await state.update_data(stats_habit_id=habit_id)
    await generate_and_send_stats(callback, state, habit_id, start_date, today)


# ============ Marathons ============
@router.message(F.text.in_({"🏆 Марафоны", "🏆 Марафондар"}))
async def show_marathons(message: Message):
    """Show marathons menu."""
    lang = await db.get_user_language(message.from_user.id)
    await message.answer(
        get_text("marathons", lang),
        reply_markup=marathons_menu_keyboard(lang),
        parse_mode="Markdown"
    )


@router.callback_query(F.data == "marathon_list")
async def list_marathons(callback: CallbackQuery):
    """List user's marathons."""
    lang = await db.get_user_language(callback.from_user.id)
    marathons = await db.get_user_marathons(callback.from_user.id)

    if not marathons:
        await callback.message.edit_text(
            "У тебя пока нет активных марафонов.\n\n"
            "Создай свой или присоединись к существующему!",
            reply_markup=marathons_menu_keyboard(lang)
        )
        await callback.answer()
        return

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for m in marathons:
        status = "🟢" if date.fromisoformat(m['start_date']) <= date.today() else "⏳"
        builder.row(InlineKeyboardButton(
            text=f"{status} {m['name']}",
            callback_data=f"marathon_view_{m['id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад",
        callback_data="back_to_marathons"
    ))

    await callback.message.edit_text(
        "🏆 **Твои марафоны:**",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


from aiogram.types import InlineKeyboardButton


@router.callback_query(F.data == "back_to_marathons")
async def back_to_marathons(callback: CallbackQuery):
    """Return to marathons menu."""
    lang = await db.get_user_language(callback.from_user.id)
    await callback.message.edit_text(
        get_text("marathons_short", lang),
        reply_markup=marathons_menu_keyboard(lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "marathon_create")
async def create_marathon_start(callback: CallbackQuery, state: FSMContext):
    """Start marathon creation."""
    await callback.message.edit_text(
        "Создание марафона 🏃\n\n"
        "Введи название марафона:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(MarathonStates.waiting_name)
    await callback.answer()


@router.message(MarathonStates.waiting_name)
async def marathon_name_received(message: Message, state: FSMContext):
    """Receive marathon name."""
    if message.text and is_menu_button(message.text):
        await message.answer("Введи название марафона:")
        return

    await state.update_data(marathon_name=message.text)
    today = date.today()

    await message.answer(
        f"Марафон: **{message.text}**\n\n"
        "📅 Выбери дату старта:",
        reply_markup=calendar_keyboard(today.year, today.month, "mstart"),
        parse_mode="Markdown"
    )
    await state.set_state(MarathonStates.waiting_start_date)


@router.callback_query(F.data.startswith("mstart_"), MarathonStates.waiting_start_date)
async def marathon_start_date(callback: CallbackQuery, state: FSMContext):
    """Select marathon start date."""
    parts = callback.data.split("_")

    if parts[1] == "nav":
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_reply_markup(
            reply_markup=calendar_keyboard(year, month, "mstart")
        )
        await callback.answer()
        return

    if parts[1] == "today":
        selected = date.today()
    else:
        year, month, day = int(parts[1]), int(parts[2]), int(parts[3])
        selected = date(year, month, day)

    if selected < date.today():
        await callback.answer("Дата старта не может быть в прошлом!", show_alert=True)
        return

    await state.update_data(marathon_start=selected.isoformat())

    await callback.message.edit_text(
        f"Дата старта: {selected.strftime('%d.%m.%Y')}\n\n"
        "📅 Теперь выбери дату финиша:",
        reply_markup=calendar_keyboard(selected.year, selected.month, "mend")
    )
    await state.set_state(MarathonStates.waiting_end_date)
    await callback.answer()


@router.callback_query(F.data.startswith("mend_"), MarathonStates.waiting_end_date)
async def marathon_end_date(callback: CallbackQuery, state: FSMContext):
    """Select marathon end date."""
    parts = callback.data.split("_")

    if parts[1] == "nav":
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_reply_markup(
            reply_markup=calendar_keyboard(year, month, "mend")
        )
        await callback.answer()
        return

    if parts[1] == "today":
        selected = date.today()
    else:
        year, month, day = int(parts[1]), int(parts[2]), int(parts[3])
        selected = date(year, month, day)

    data = await state.get_data()
    start = date.fromisoformat(data['marathon_start'])

    if selected <= start:
        await callback.answer("Дата финиша должна быть позже даты старта!", show_alert=True)
        return

    await state.update_data(marathon_end=selected.isoformat())
    await state.update_data(marathon_habits=[])

    await callback.message.edit_text(
        f"📅 Марафон: {start.strftime('%d.%m.%Y')} - {selected.strftime('%d.%m.%Y')}\n\n"
        "Теперь добавь привычки для марафона.\n"
        "Введи название первой привычки:",
        reply_markup=cancel_keyboard()
    )
    await state.set_state(MarathonStates.waiting_habit_name)
    await callback.answer()


@router.message(MarathonStates.waiting_habit_name)
async def marathon_habit_name(message: Message, state: FSMContext):
    """Add habit to marathon."""
    if message.text.lower() == "готово":
        await finalize_marathon(message, state)
        return

    if message.text and is_menu_button(message.text):
        await message.answer("Введи название привычки:")
        return

    await state.update_data(current_habit_name=message.text)
    await message.answer(
        f"Привычка: **{message.text}**\n\n"
        "Выбери тип:",
        reply_markup=habit_type_keyboard(),
        parse_mode="Markdown"
    )
    await state.set_state(MarathonStates.waiting_habit_type)


@router.callback_query(F.data == "marathon_habits_done", MarathonStates.waiting_habit_name)
async def marathon_habits_done_callback(callback: CallbackQuery, state: FSMContext):
    """Handle Done button for marathon habits."""
    await finalize_marathon_from_callback(callback, state)
    await callback.answer()


@router.callback_query(F.data.startswith("type_"), MarathonStates.waiting_habit_type)
async def marathon_habit_type(callback: CallbackQuery, state: FSMContext):
    """Set marathon habit type."""
    habit_type = callback.data.split("_")[1]
    await state.update_data(current_habit_type=habit_type)

    if habit_type == "boolean":
        data = await state.get_data()
        habits = data.get('marathon_habits', [])
        habits.append({
            'name': data['current_habit_name'],
            'habit_type': 'boolean',
            'daily_goal': 1,
            'unit': ''
        })
        await state.update_data(marathon_habits=habits)

        await callback.message.edit_text(
            f"✅ Привычка добавлена!\n\n"
            f"Привычек в марафоне: {len(habits)}\n\n"
            f"Введи название следующей привычки или нажми «Готово»:",
            parse_mode="Markdown",
            reply_markup=marathon_add_habit_keyboard(len(habits))
        )
        await state.set_state(MarathonStates.waiting_habit_name)
    else:
        await callback.message.edit_text(
            "Введи дневную цель (число):"
        )
        await state.set_state(MarathonStates.waiting_habit_goal)

    await callback.answer()


@router.message(MarathonStates.waiting_habit_goal)
async def marathon_habit_goal(message: Message, state: FSMContext):
    """Set marathon habit goal."""
    try:
        goal = float(message.text.replace(",", "."))
        data = await state.get_data()

        habits = data.get('marathon_habits', [])
        habits.append({
            'name': data['current_habit_name'],
            'habit_type': data['current_habit_type'],
            'daily_goal': goal,
            'unit': ''  # Could add unit input here
        })
        await state.update_data(marathon_habits=habits)

        await message.answer(
            f"✅ Привычка добавлена!\n\n"
            f"Привычек в марафоне: {len(habits)}\n\n"
            f"Введи название следующей привычки или нажми «Готово»:",
            parse_mode="Markdown",
            reply_markup=marathon_add_habit_keyboard(len(habits))
        )
        await state.set_state(MarathonStates.waiting_habit_name)

    except ValueError:
        await message.answer("Введи число. Например: 3 или 2.5")


async def finalize_marathon(message: Message, state: FSMContext):
    """Finalize marathon creation."""
    data = await state.get_data()
    habits = data.get('marathon_habits', [])

    if not habits:
        await message.answer("Добавь хотя бы одну привычку!")
        return

    invite_code = secrets.token_urlsafe(8)

    marathon_id = await db.create_marathon(
        creator_id=message.from_user.id,
        name=data['marathon_name'],
        start_date=date.fromisoformat(data['marathon_start']),
        end_date=date.fromisoformat(data['marathon_end']),
        invite_code=invite_code
    )

    # Add habits to marathon
    for habit in habits:
        await db.add_marathon_habit(
            marathon_id=marathon_id,
            name=habit['name'],
            habit_type=habit['habit_type'],
            daily_goal=habit['daily_goal'],
            unit=habit.get('unit', '')
        )

    # Copy habits to creator
    for habit in habits:
        await db.create_habit(
            user_id=message.from_user.id,
            name=habit['name'],
            habit_type=habit['habit_type'],
            daily_goal=habit['daily_goal'],
            unit=habit.get('unit', ''),
            marathon_id=marathon_id
        )

    await state.clear()

    start = date.fromisoformat(data['marathon_start'])
    end = date.fromisoformat(data['marathon_end'])

    # Get bot username for deep link
    bot_info = await message.bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=marathon_{invite_code}"

    await message.answer(
        f"🎉 Марафон создан!\n\n"
        f"**{data['marathon_name']}**\n"
        f"📅 {start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}\n"
        f"📝 Привычек: {len(habits)}\n\n"
        f"🔗 {invite_link}",
        parse_mode=None
    )


async def finalize_marathon_from_callback(callback: CallbackQuery, state: FSMContext):
    """Finalize marathon creation from callback."""
    data = await state.get_data()
    habits = data.get('marathon_habits', [])

    if not habits:
        await callback.message.edit_text("Добавь хотя бы одну привычку!")
        return

    invite_code = secrets.token_urlsafe(8)

    marathon_id = await db.create_marathon(
        creator_id=callback.from_user.id,
        name=data['marathon_name'],
        start_date=date.fromisoformat(data['marathon_start']),
        end_date=date.fromisoformat(data['marathon_end']),
        invite_code=invite_code
    )

    # Add habits to marathon
    for habit in habits:
        await db.add_marathon_habit(
            marathon_id=marathon_id,
            name=habit['name'],
            habit_type=habit['habit_type'],
            daily_goal=habit['daily_goal'],
            unit=habit.get('unit', '')
        )

    # Copy habits to creator
    for habit in habits:
        await db.create_habit(
            user_id=callback.from_user.id,
            name=habit['name'],
            habit_type=habit['habit_type'],
            daily_goal=habit['daily_goal'],
            unit=habit.get('unit', ''),
            marathon_id=marathon_id
        )

    await state.clear()

    start = date.fromisoformat(data['marathon_start'])
    end = date.fromisoformat(data['marathon_end'])

    # Get bot username for deep link
    bot_info = await callback.bot.get_me()
    invite_link = f"https://t.me/{bot_info.username}?start=marathon_{invite_code}"

    await callback.message.edit_text(
        f"🎉 Марафон создан!\n\n"
        f"{data['marathon_name']}\n"
        f"📅 {start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}\n"
        f"📝 Привычек: {len(habits)}\n\n"
        f"🔗 {invite_link}",
        parse_mode=None
    )


@router.callback_query(F.data.startswith("marathon_view_"))
async def view_marathon(callback: CallbackQuery):
    """View marathon details."""
    marathon_id = int(callback.data.split("_")[2])
    marathon = await db.get_marathon_by_code(
        (await db.get_user_marathons(callback.from_user.id))[0]['invite_code']
    )

    # Get marathon directly
    async with aiosqlite.connect(db.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            "SELECT * FROM marathons WHERE id = ?", (marathon_id,)
        )
        marathon = dict(await cursor.fetchone())

    start = date.fromisoformat(marathon['start_date'])
    end = date.fromisoformat(marathon['end_date'])
    today = date.today()

    if today < start:
        status = f"⏳ Старт через {(start - today).days} дней"
    elif today > end:
        status = "🏁 Завершен"
    else:
        days_left = (end - today).days
        status = f"🟢 Идет ({days_left} дней осталось)"

    is_creator = marathon['creator_id'] == callback.from_user.id

    await callback.message.edit_text(
        f"🏆 **{marathon['name']}**\n\n"
        f"📅 {start.strftime('%d.%m.%Y')} - {end.strftime('%d.%m.%Y')}\n"
        f"Статус: {status}\n\n"
        f"🔗 Код: `{marathon['invite_code']}`",
        reply_markup=marathon_detail_keyboard(marathon, is_creator),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("marathon_leaderboard_"))
async def show_leaderboard(callback: CallbackQuery):
    """Show marathon leaderboard."""
    marathon_id = int(callback.data.split("_")[2])
    participants = await db.get_marathon_leaderboard(marathon_id)

    text = "🏆 **Таблица лидеров**\n\n"
    medals = ["🥇", "🥈", "🥉"]

    for i, p in enumerate(participants[:10]):
        name = p.get('first_name') or p.get('username') or f"User"
        medal = medals[i] if i < 3 else f"{i+1}."
        text += f"{medal} {name}: {p['total_points']} баллов\n"

    await callback.message.edit_text(text, parse_mode="Markdown")

    # Send chart
    if participants:
        chart = analytics.generate_leaderboard_chart(participants)
        await callback.message.answer_photo(
            BufferedInputFile(chart.read(), filename="leaderboard.png")
        )

    await callback.answer()


@router.callback_query(F.data.startswith("marathon_invite_"))
async def invite_to_marathon(callback: CallbackQuery):
    """Show invite link."""
    marathon_id = int(callback.data.split("_")[2])

    async with aiosqlite.connect(db.DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT invite_code, name FROM marathons WHERE id = ?", (marathon_id,)
        )
        row = await cursor.fetchone()

    if row:
        bot_info = await callback.bot.get_me()
        invite_link = f"https://t.me/{bot_info.username}?start=marathon_{row[0]}"

        await callback.message.answer(invite_link, parse_mode=None)

    await callback.answer()


@router.callback_query(F.data.startswith("marathon_manage_"))
async def manage_marathon(callback: CallbackQuery):
    """Manage marathon - show participants with options."""
    marathon_id = int(callback.data.split("_")[2])
    marathon = await db.get_marathon_by_id(marathon_id)
    participants = await db.get_marathon_leaderboard(marathon_id)

    if not marathon:
        await callback.answer("Марафон не найден", show_alert=True)
        return

    text = f"⚙️ **Управление: {marathon['name']}**\n\n"
    text += f"👥 Участников: {len(participants)}\n\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for p in participants:
        name = p.get('first_name') or p.get('username') or f"User {p['user_id']}"
        points = p['total_points']
        builder.row(InlineKeyboardButton(
            text=f"👤 {name} ({points} б.)",
            callback_data=f"manage_user_{marathon_id}_{p['user_id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад к марафону",
        callback_data=f"marathon_view_{marathon_id}"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("manage_user_"))
async def manage_marathon_user(callback: CallbackQuery):
    """Manage specific user in marathon."""
    parts = callback.data.split("_")
    marathon_id = int(parts[2])
    user_id = int(parts[3])

    marathon = await db.get_marathon_by_id(marathon_id)

    # Get user info
    async with aiosqlite.connect(db.DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute(
            """SELECT u.first_name, u.username, mp.total_points, mp.joined_at
               FROM users u
               JOIN marathon_participants mp ON u.user_id = mp.user_id
               WHERE mp.marathon_id = ? AND mp.user_id = ?""",
            (marathon_id, user_id)
        )
        user = await cursor.fetchone()

        # Get user's habits for this marathon
        cursor = await conn.execute(
            """SELECT h.name, h.daily_goal, h.unit, h.streak
               FROM habits h
               WHERE h.user_id = ? AND h.marathon_id = ?""",
            (user_id, marathon_id)
        )
        habits = await cursor.fetchall()

    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    name = user['first_name'] or user['username'] or f"User {user_id}"

    text = f"👤 **{name}**\n\n"
    text += f"📊 Баллов: {user['total_points']}\n"
    text += f"📅 Присоединился: {user['joined_at'][:10]}\n\n"

    if habits:
        text += "**Привычки:**\n"
        for h in habits:
            streak = f" 🔥{h['streak']}" if h['streak'] > 0 else ""
            text += f"• {h['name']}{streak}\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    # Only show remove if not the creator
    if user_id != marathon['creator_id']:
        builder.row(InlineKeyboardButton(
            text="🚫 Удалить из марафона",
            callback_data=f"kick_user_{marathon_id}_{user_id}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад к участникам",
        callback_data=f"marathon_manage_{marathon_id}"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("kick_user_"))
async def kick_user_from_marathon(callback: CallbackQuery):
    """Remove user from marathon."""
    parts = callback.data.split("_")
    marathon_id = int(parts[2])
    user_id = int(parts[3])

    # Remove user from marathon
    await db.leave_marathon(user_id, marathon_id, keep_habits=False)

    await callback.answer("Пользователь удалён из марафона", show_alert=True)

    # Refresh participant list
    marathon = await db.get_marathon_by_id(marathon_id)
    participants = await db.get_marathon_leaderboard(marathon_id)

    text = f"⚙️ **Управление: {marathon['name']}**\n\n"
    text += f"👥 Участников: {len(participants)}\n\n"

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()

    for p in participants:
        name = p.get('first_name') or p.get('username') or f"User {p['user_id']}"
        points = p['total_points']
        builder.row(InlineKeyboardButton(
            text=f"👤 {name} ({points} б.)",
            callback_data=f"manage_user_{marathon_id}_{p['user_id']}"
        ))

    builder.row(InlineKeyboardButton(
        text="« Назад к марафону",
        callback_data=f"marathon_view_{marathon_id}"
    ))

    await callback.message.edit_text(
        text,
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )


@router.callback_query(F.data.startswith("marathon_leave_confirm_"))
async def leave_marathon_execute(callback: CallbackQuery):
    """Execute leaving marathon."""
    lang = await db.get_user_language(callback.from_user.id)
    parts = callback.data.split("_")
    marathon_id = int(parts[3])
    action = parts[4]  # 'delete' or 'keep'

    keep_habits = (action == "keep")
    await db.leave_marathon(callback.from_user.id, marathon_id, keep_habits)

    if keep_habits:
        msg = get_text("left_marathon_kept", lang)
    else:
        msg = get_text("left_marathon_deleted", lang)

    await callback.message.edit_text(msg)
    await callback.message.answer(
        get_text("marathons_short", lang),
        reply_markup=marathons_menu_keyboard(lang),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("marathon_leave_"))
async def leave_marathon_confirm(callback: CallbackQuery):
    """Confirm leaving marathon."""
    marathon_id = int(callback.data.split("_")[2])
    marathon = await db.get_marathon_by_id(marathon_id)

    if not marathon:
        await callback.answer("Марафон не найден", show_alert=True)
        return

    from aiogram.utils.keyboard import InlineKeyboardBuilder
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🗑 Удалить привычки",
            callback_data=f"marathon_leave_confirm_{marathon_id}_delete"
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📝 Оставить привычки",
            callback_data=f"marathon_leave_confirm_{marathon_id}_keep"
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="« Отмена",
            callback_data=f"marathon_view_{marathon_id}"
        )
    )

    await callback.message.edit_text(
        f"🚪 Выйти из марафона **{marathon['name']}**?\n\n"
        f"Что сделать с привычками марафона?",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer()


# ============ Admin Panel ============

def is_admin(username: str) -> bool:
    """Check if user is admin."""
    return username == ADMIN_USERNAME


@router.message(Command("admin"))
async def admin_panel(message: Message):
    """Show admin panel."""
    if not message.from_user.username or not is_admin(message.from_user.username):
        await message.answer("⛔ Доступ запрещён")
        return

    stats = await db.get_admin_stats()

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="📨 Рассылка всем",
        callback_data="admin_broadcast"
    ))
    builder.row(InlineKeyboardButton(
        text="🔄 Обновить",
        callback_data="admin_refresh"
    ))

    await message.answer(
        f"👑 **Админ-панель**\n\n"
        f"👥 Пользователей: **{stats['total_users']}**\n"
        f"📅 Новых сегодня: **{stats['users_today']}**\n\n"
        f"📝 Активных привычек: **{stats['total_habits']}**\n"
        f"✅ Записей сегодня: **{stats['logs_today']}**\n\n"
        f"🏆 Всего марафонов: **{stats['total_marathons']}**\n"
        f"🏃 Активных марафонов: **{stats['active_marathons']}**",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )


@router.callback_query(F.data == "admin_refresh")
async def admin_refresh(callback: CallbackQuery):
    """Refresh admin stats."""
    if not callback.from_user.username or not is_admin(callback.from_user.username):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    stats = await db.get_admin_stats()

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="📨 Рассылка всем",
        callback_data="admin_broadcast"
    ))
    builder.row(InlineKeyboardButton(
        text="🔄 Обновить",
        callback_data="admin_refresh"
    ))

    await callback.message.edit_text(
        f"👑 **Админ-панель**\n\n"
        f"👥 Пользователей: **{stats['total_users']}**\n"
        f"📅 Новых сегодня: **{stats['users_today']}**\n\n"
        f"📝 Активных привычек: **{stats['total_habits']}**\n"
        f"✅ Записей сегодня: **{stats['logs_today']}**\n\n"
        f"🏆 Всего марафонов: **{stats['total_marathons']}**\n"
        f"🏃 Активных марафонов: **{stats['active_marathons']}**",
        reply_markup=builder.as_markup(),
        parse_mode="Markdown"
    )
    await callback.answer("Обновлено!")


@router.message(Command("test_notif"))
async def test_notification(message: Message):
    """Test notification - send yourself a test notification for your habits."""
    if not message.from_user.username or not is_admin(message.from_user.username):
        await message.answer("⛔ Тек админ үшін")
        return

    from scheduler import send_habit_notification

    habits = await db.get_user_habits(message.from_user.id)

    if not habits:
        await message.answer("У тебя нет привычек для тестирования")
        return

    await message.answer(f"🧪 Отправляю тестовые уведомления для {len(habits)} привычек...")

    for habit in habits:
        # Check if already completed today
        log = await db.get_daily_log(habit['id'])
        if log and log['completed']:
            await message.answer(f"✅ {habit['name']} - уже выполнено сегодня, пропускаю")
            continue

        await send_habit_notification(message.from_user.id, habit)

    await message.answer("✅ Тестовые уведомления отправлены!")


@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """Start broadcast message flow."""
    if not callback.from_user.username or not is_admin(callback.from_user.username):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.message.edit_text(
        "📨 **Рассылка**\n\n"
        "Отправь сообщение, которое получат все пользователи бота:",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )
    await state.set_state(AdminStates.waiting_broadcast_message)
    await callback.answer()


@router.message(AdminStates.waiting_broadcast_message)
async def admin_broadcast_send(message: Message, state: FSMContext):
    """Send broadcast message to all users."""
    if not message.from_user.username or not is_admin(message.from_user.username):
        await state.clear()
        return

    if message.text and is_menu_button(message.text):
        await message.answer("Введи текст сообщения для рассылки:")
        return

    await state.clear()

    user_ids = await db.get_all_user_ids()
    success = 0
    failed = 0

    status_msg = await message.answer(f"📨 Начинаю рассылку... 0/{len(user_ids)}")

    for i, user_id in enumerate(user_ids):
        try:
            await message.bot.send_message(
                user_id,
                f"📢 **Сообщение от администратора:**\n\n{message.text}",
                parse_mode="Markdown"
            )
            success += 1
        except Exception:
            failed += 1

        # Update status every 10 users
        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"📨 Рассылка... {i + 1}/{len(user_ids)}")
            except Exception:
                pass

    await status_msg.edit_text(
        f"✅ **Рассылка завершена!**\n\n"
        f"📨 Отправлено: **{success}**\n"
        f"❌ Ошибок: **{failed}**",
        parse_mode="Markdown"
    )


import aiosqlite
