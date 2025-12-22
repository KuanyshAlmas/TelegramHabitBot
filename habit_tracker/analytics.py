import io
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from functools import partial
from typing import List, Dict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure

# Set Russian locale for dates
import locale
try:
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
except:
    pass

# Use non-interactive backend
plt.switch_backend('Agg')

# Thread pool for chart generation (matplotlib is not thread-safe, use 1 worker)
_chart_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="chart_")

# Style settings
plt.style.use('seaborn-v0_8-whitegrid')
COLORS = {
    'primary': '#4CAF50',
    'secondary': '#2196F3',
    'warning': '#FFC107',
    'danger': '#F44336',
    'success': '#8BC34A',
    'background': '#FAFAFA'
}


def create_line_chart(
    dates: List[date],
    values: List[float],
    title: str,
    ylabel: str,
    goal_line: float = None
) -> io.BytesIO:
    """Create a line chart for habit progress."""
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(COLORS['background'])
    ax.set_facecolor(COLORS['background'])

    # Plot data
    ax.plot(dates, values, color=COLORS['primary'], linewidth=2, marker='o', markersize=6)
    ax.fill_between(dates, values, alpha=0.3, color=COLORS['primary'])

    # Goal line
    if goal_line:
        ax.axhline(y=goal_line, color=COLORS['danger'], linestyle='--',
                   linewidth=2, label=f'Цель: {goal_line}')
        ax.legend(loc='upper right')

    # Formatting
    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel(ylabel, fontsize=11)

    # Date formatting
    if len(dates) <= 14:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
        ax.xaxis.set_major_locator(mdates.DayLocator())
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator())

    plt.xticks(rotation=45, ha='right')
    ax.tick_params(axis='both', which='major', labelsize=10)

    # Grid
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)

    plt.tight_layout()

    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


def create_bar_chart(
    dates: List[date],
    values: List[float],
    title: str,
    ylabel: str,
    goal_line: float = None
) -> io.BytesIO:
    """Create a bar chart for habit progress."""
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(COLORS['background'])
    ax.set_facecolor(COLORS['background'])

    # Color bars based on goal completion
    if goal_line:
        colors = [COLORS['success'] if v >= goal_line else COLORS['warning'] for v in values]
    else:
        colors = [COLORS['primary']] * len(values)

    bars = ax.bar(dates, values, color=colors, width=0.8, edgecolor='white')

    # Goal line
    if goal_line:
        ax.axhline(y=goal_line, color=COLORS['danger'], linestyle='--',
                   linewidth=2, label=f'Цель: {goal_line}')
        ax.legend(loc='upper right')

    # Formatting
    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
    ax.set_ylabel(ylabel, fontsize=11)

    if len(dates) <= 14:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

    plt.xticks(rotation=45, ha='right')

    ax.grid(True, axis='y', linestyle='--', alpha=0.7)
    ax.set_axisbelow(True)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


def create_completion_chart(
    dates: List[date],
    completed: List[bool],
    title: str
) -> io.BytesIO:
    """Create a completion chart for boolean habits."""
    fig, ax = plt.subplots(figsize=(10, 3))
    fig.patch.set_facecolor(COLORS['background'])
    ax.set_facecolor(COLORS['background'])

    # Create colored squares
    colors = [COLORS['success'] if c else COLORS['danger'] for c in completed]
    values = [1] * len(dates)

    ax.bar(dates, values, color=colors, width=0.8, edgecolor='white')

    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)
    ax.set_ylim(0, 1.5)
    ax.set_yticks([])

    if len(dates) <= 14:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    else:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator())

    plt.xticks(rotation=45, ha='right')

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS['success'], label='Выполнено'),
        Patch(facecolor=COLORS['danger'], label='Пропущено')
    ]
    ax.legend(handles=legend_elements, loc='upper right')

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


def create_streak_chart(habits: List[Dict]) -> io.BytesIO:
    """Create a horizontal bar chart showing streaks."""
    fig, ax = plt.subplots(figsize=(10, max(4, len(habits) * 0.6)))
    fig.patch.set_facecolor(COLORS['background'])
    ax.set_facecolor(COLORS['background'])

    names = [h['name'][:20] for h in habits]  # Truncate long names
    streaks = [h['streak'] for h in habits]
    max_streaks = [h['max_streak'] for h in habits]

    y_pos = range(len(names))

    # Max streak bars (background)
    ax.barh(y_pos, max_streaks, color=COLORS['secondary'], alpha=0.3,
            label='Рекорд', height=0.4)

    # Current streak bars
    ax.barh(y_pos, streaks, color=COLORS['primary'], label='Текущий', height=0.4)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names)
    ax.set_xlabel('Дни', fontsize=11)
    ax.set_title('🔥 Страйки', fontsize=14, fontweight='bold', pad=15)

    ax.legend(loc='lower right')
    ax.grid(True, axis='x', linestyle='--', alpha=0.7)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


def create_weekly_summary_chart(habits_data: List[Dict]) -> io.BytesIO:
    """Create weekly summary pie chart."""
    fig, axes = plt.subplots(1, min(len(habits_data), 3), figsize=(12, 4))
    fig.patch.set_facecolor(COLORS['background'])

    if len(habits_data) == 1:
        axes = [axes]

    for i, data in enumerate(habits_data[:3]):
        ax = axes[i]
        ax.set_facecolor(COLORS['background'])

        completed = data.get('completed_days', 0)
        total = data.get('total_days', 7)
        missed = total - completed

        if completed + missed > 0:
            sizes = [completed, missed]
            colors = [COLORS['success'], COLORS['danger']]
            labels = [f'Выполнено\n{completed}', f'Пропущено\n{missed}']

            ax.pie(sizes, labels=labels, colors=colors, autopct='%1.0f%%',
                   startangle=90, textprops={'fontsize': 9})

        habit_name = data['habit']['name']
        if len(habit_name) > 15:
            habit_name = habit_name[:15] + '...'
        ax.set_title(habit_name, fontsize=11, fontweight='bold')

    plt.suptitle('Результаты за неделю', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


def generate_habit_report_chart(stats: Dict) -> io.BytesIO:
    """Generate appropriate chart based on habit type and data."""
    habit = stats['habit']
    logs = stats.get('logs', [])

    if not logs:
        return None

    dates = [date.fromisoformat(log['log_date']) for log in logs]
    values = [log['value'] for log in logs]

    if habit['habit_type'] == 'boolean':
        completed = [log['completed'] for log in logs]
        return create_completion_chart(
            dates,
            [bool(c) for c in completed],
            f"📊 {habit['name']}"
        )
    else:
        return create_bar_chart(
            dates,
            values,
            f"📊 {habit['name']}",
            habit.get('unit', 'Значение'),
            goal_line=habit['daily_goal']
        )


def generate_leaderboard_chart(participants: List[Dict]) -> io.BytesIO:
    """Generate leaderboard chart for marathon."""
    fig, ax = plt.subplots(figsize=(10, max(4, len(participants) * 0.5)))
    fig.patch.set_facecolor(COLORS['background'])
    ax.set_facecolor(COLORS['background'])

    names = []
    points = []
    for p in participants[:10]:  # Top 10
        name = p.get('first_name') or p.get('username') or f"User {p['user_id']}"
        names.append(name[:15])
        points.append(p['total_points'])

    y_pos = range(len(names))
    colors = [COLORS['warning'] if i == 0 else
              COLORS['secondary'] if i == 1 else
              COLORS['primary'] for i in range(len(names))]

    bars = ax.barh(y_pos, points, color=colors, height=0.6)

    # Add medals for top 3
    medals = ['🥇', '🥈', '🥉']
    for i, bar in enumerate(bars[:3]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                medals[i], va='center', fontsize=14)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names)
    ax.set_xlabel('Баллы', fontsize=11)
    ax.set_title('🏆 Таблица лидеров', fontsize=14, fontweight='bold', pad=15)

    ax.invert_yaxis()
    ax.grid(True, axis='x', linestyle='--', alpha=0.7)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=COLORS['background'])
    buf.seek(0)
    plt.close(fig)

    return buf


# ============ ASYNC WRAPPERS ============
# These wrap sync matplotlib functions to run in thread pool without blocking event loop

async def create_line_chart_async(
    dates: List[date],
    values: List[float],
    title: str,
    ylabel: str,
    goal_line: float = None
) -> io.BytesIO:
    """Async wrapper for create_line_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(create_line_chart, dates, values, title, ylabel, goal_line)
    )


async def create_bar_chart_async(
    dates: List[date],
    values: List[float],
    title: str,
    ylabel: str,
    goal_line: float = None
) -> io.BytesIO:
    """Async wrapper for create_bar_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(create_bar_chart, dates, values, title, ylabel, goal_line)
    )


async def create_completion_chart_async(
    dates: List[date],
    completed: List[bool],
    title: str
) -> io.BytesIO:
    """Async wrapper for create_completion_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(create_completion_chart, dates, completed, title)
    )


async def create_streak_chart_async(habits: List[Dict]) -> io.BytesIO:
    """Async wrapper for create_streak_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(create_streak_chart, habits)
    )


async def create_weekly_summary_chart_async(habits_data: List[Dict]) -> io.BytesIO:
    """Async wrapper for create_weekly_summary_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(create_weekly_summary_chart, habits_data)
    )


async def generate_habit_report_chart_async(stats: Dict) -> io.BytesIO:
    """Async wrapper for generate_habit_report_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(generate_habit_report_chart, stats)
    )


async def generate_leaderboard_chart_async(participants: List[Dict]) -> io.BytesIO:
    """Async wrapper for generate_leaderboard_chart."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _chart_executor,
        partial(generate_leaderboard_chart, participants)
    )
