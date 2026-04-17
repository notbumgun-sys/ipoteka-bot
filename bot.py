import asyncio
import json
import logging
import aiohttp
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import (
    BOT_TOKEN, WEBHOOK_URL, MANAGER_LINK, MANAGER_USERNAME,
    MANAGER_PHONE, SITE_URL, NOTIFY_CHAT_ID, HERO_RENDER
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Shared HTTP session ===
_http_session = None

async def get_session():
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
    return _http_session

# === Constants ===
MAX_LOAN_AMOUNT = 12_000_000
TOP_RESULTS = 5

FINISHING_MAP = {0: "Без отделки", 1: "Предчистовая", 2: "Чистовая", 3: "Предчистовая"}

DISTRICTS = {
    "north": {
        "label": "🔵 Север",
        "hint": "Химки, Подрезково, Ленинградское ш.",
        "complexes": ["1-й Химкинский", "1-й Шереметьевский", "1-й Ленинградский"],
    },
    "south": {
        "label": "🟢 Юг и Новая Москва",
        "hint": "Саларьево, Ясенево, Битца и др.",
        "complexes": ["1-й Саларьевский", "1-й Ясеневский", "Южная Битца", "1-й Южный", "1-й Донской"],
    },
    "east": {
        "label": "🟡 Восток Москвы",
        "hint": "Измайлово, Люберцы",
        "complexes": ["1-й Измайловский", "1-й Лермонтовский"],
    },
}

# Per-complex location info: (МКАД distance label, station line)
PROJECT_TRAVEL = {
    "1-й Химкинский":     ("7 км от МКАД",    "🚆 Химки (МЦД D3) · 🚗 10 мин"),
    "1-й Шереметьевский": ("9 км от МКАД",    "🚆 Подрезково (МЦД D3) · 🚶 6 мин"),
    "1-й Ленинградский":  ("6 км от МКАД",    "🚆 Молжаниново (МЦД D3) · 🚶 15 мин"),
    "1-й Измайловский":   ("внутри МКАД",     "🚇 Щёлковская · 🚶 10 мин"),
    "1-й Лермонтовский":  ("5 км от МКАД",    "🚆 Люберцы (МЦД D3) · 🚶 15 мин"),
    "1-й Саларьевский":   ("2 км от МКАД",    "🚇 Саларьево · 🚶 5 мин"),
    "1-й Ясеневский":     ("1 км от МКАД",    "🚇 Корниловская · 🚶 9 мин"),
    "Южная Битца":        ("9 км от МКАД",    "🚆 Битца (МЦД D2) · 🚗 6 мин"),
    "1-й Южный":          ("1 км от МКАД",    "🚆 Булатниково (МЦД D5) · 🚶 10 мин"),
    "1-й Донской":        ("5 км от МКАД",    "🚆 Калинина (МЦД D5) · 🚶 15 мин"),
}

# === Load lots ===
LOTS_PATH = Path(__file__).parent / "lots.json"
lots_data = []

def load_lots():
    global lots_data
    try:
        with open(LOTS_PATH, "r", encoding="utf-8-sig") as f:
            lots_data = json.load(f)
        logger.info(f"Loaded {len(lots_data)} lots")
    except Exception as e:
        logger.error(f"Failed to load lots: {e}")

# === Helpers ===
def rooms_label(rooms):
    if rooms == 0:
        return "Студия"
    return f"{rooms}-комн."

def format_price(price):
    return f"{price:,.0f}".replace(",", " ")

def adjusted_payment(lot):
    p = lot.get("mortgagePayment", 0) or 0
    if p:
        return p
    # Fallback: estimate from price (6% family mortgage, 30 years, 0% down)
    price = lot.get("price", 0) or 0
    if not price:
        return 0
    r = 0.06 / 12
    n = 360
    return round(price * r * (1 + r) ** n / ((1 + r) ** n - 1))

def finishing_label(lot):
    return FINISHING_MAP.get(lot.get("finishing"), "")

def plan_url(lot):
    plan = lot.get("plan", "")
    if not plan:
        return None
    if "/production/" in plan and "/rsz/" not in plan:
        url = plan.replace("/production/", "/rsz/fit/600/600/ce/0/plain/production/")
    else:
        url = plan
    if url.endswith(".svg"):
        url += "@png"
    return url

def developer_url(lot):
    return lot.get("developerUrl") or lot.get("developer_url") or ""

def lot_payload(lot):
    if not lot:
        return {}
    return {
        "lot_id": lot.get("id", ""),
        "lot_info": f"{rooms_label(lot['rooms'])}, {lot['area']} м², кв. №{lot['number']}, корп. {lot['corpus']}",
        "lot_number": lot.get("number", ""),
        "lot_rooms": lot.get("rooms", ""),
        "lot_area": lot.get("area", ""),
        "lot_price": lot.get("price", ""),
        "lot_corpus": lot.get("corpus", ""),
        "developer_url": developer_url(lot),
    }

def extract_start_param(message: Message):
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) > 1:
        return parts[1].strip()
    return ""

def find_lots(rooms=None, max_payment=None, district=None):
    allowed = None
    if district and district in DISTRICTS:
        allowed = set(DISTRICTS[district]["complexes"])
    results = []
    for lot in lots_data:
        if allowed and lot.get("complex", "") not in allowed:
            continue
        if rooms is not None and lot.get("rooms") != rooms:
            continue
        if lot.get("rooms") == 3:
            continue
        if lot.get("price", 0) > MAX_LOAN_AMOUNT:
            continue
        if max_payment and adjusted_payment(lot) > max_payment:
            continue
        results.append(lot)
    results.sort(key=lambda x: x.get("price", 0))
    return results

def pick_diverse_lots(results, n=5):
    """Round-robin by complex, then by rooms type within complex.
    Pass 1: one per unique complex (all swipes from different ЖК).
    Pass 2+: repeat complex only with a different rooms type.
    Never: same (complex, rooms) pair twice.
    Fallback: fill any remaining slots by price spread."""
    if len(results) <= n:
        return results
    from collections import defaultdict
    # Group: complex -> rooms -> [lots sorted by price asc]
    by_complex = defaultdict(lambda: defaultdict(list))
    for lot in sorted(results, key=lambda x: x.get("price", 0)):
        by_complex[lot.get("complex", "")][lot.get("rooms", -1)].append(lot)
    # Order complexes by their cheapest lot
    complex_order = sorted(
        by_complex.keys(),
        key=lambda c: min(v[0]["price"] for v in by_complex[c].values())
    )
    selected = []
    selected_ids = set()
    used_rooms_per_complex = defaultdict(set)
    while len(selected) < n:
        added = 0
        for c in complex_order:
            if len(selected) >= n:
                break
            available = [r for r in sorted(by_complex[c].keys())
                         if r not in used_rooms_per_complex[c]]
            if not available:
                continue
            rooms_type = available[0]
            candidates = [l for l in by_complex[c][rooms_type] if l["id"] not in selected_ids]
            if candidates:
                lot = candidates[0]
                selected.append(lot)
                selected_ids.add(lot["id"])
                used_rooms_per_complex[c].add(rooms_type)
                added += 1
        if added == 0:
            break
    # Fallback: fill remaining by price spread ignoring diversity
    if len(selected) < n:
        extra = [l for l in results if l["id"] not in selected_ids]
        needed = n - len(selected)
        if extra:
            step = (len(extra) - 1) / max(needed - 1, 1)
            extras = [extra[min(round(i * step), len(extra) - 1)] for i in range(needed)]
            selected.extend(extras)
    return selected[:n]

def district_counts(rooms=None, max_payment=None):
    counts = {"any": len(find_lots(rooms=rooms, max_payment=max_payment))}
    for key in DISTRICTS:
        counts[key] = len(find_lots(rooms=rooms, max_payment=max_payment, district=key))
    return counts

def budget_counts(rooms=None):
    """Считает кол-во вариантов в каждом диапазоне бюджета для данной комнатности."""
    up25 = len(find_lots(rooms=rooms, max_payment=25000))
    up40 = len(find_lots(rooms=rooms, max_payment=40000))
    up60 = len(find_lots(rooms=rooms, max_payment=60000))
    total = len(find_lots(rooms=rooms))
    return {
        "25000": up25,
        "40000": up40 - up25,
        "60000": up60 - up40,
        "any":   total,
    }

# === Event tracking (fire-and-forget) ===
def track_event(user_id, username, event_type, extra=None, lot=None, phone="", name="", journey_id=""):
    lot_meta = lot_payload(lot)
    current_journey = journey_id or f"tg:{user_id}"
    payload = {
        "source": "bot",
        "channel": "telegram_bot",
        "event_type": event_type,
        "session_id": current_journey,
        "journey_id": current_journey,
        "profile_key": f"tg:{user_id}",
        "telegram_user_id": user_id,
        "telegram_username": username or "",
        "name": name or "",
        "phone": phone or "",
        "page_url": "https://t.me/ipoteka0pv_bot",
        "timestamp": datetime.now().isoformat(),
        "extra": extra or {},
        **lot_meta,
    }
    async def _send():
        try:
            session = await get_session()
            await session.post(WEBHOOK_URL, json=payload)
        except Exception as e:
            logger.error(f"Track event error: {e}")
    asyncio.create_task(_send())

# === Notify manager ===
async def notify_lead(bot: Bot, user_id, username, name, phone, lot=None):
    lot_meta = lot_payload(lot)
    lot_info = lot_meta.get("lot_info", "")
    text = (
        f"🔥 <b>Новая заявка из бота!</b>\n\n"
        f"👤 {name or 'Не указано'}\n"
        f"📱 {phone}\n"
        f"🔗 @{username}\n"
        f"🏠 {lot_info}\n"
        f"📊 User ID: {user_id}"
    )
    try:
        await bot.send_message(NOTIFY_CHAT_ID, text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Notify error: {e}")

    payload = {
        "source": "bot",
        "channel": "telegram_bot",
        "session_id": f"tg:{user_id}",
        "journey_id": f"tg:{user_id}",
        "profile_key": f"tg:{user_id}",
        "name": name or "",
        "phone": phone,
        "telegram_user_id": user_id,
        "telegram_username": username or "",
        **lot_meta,
        "project": "",
        "utm_source": "telegram_bot",
        "utm_medium": "bot",
        "page_url": "https://t.me/ipoteka0pv_bot",
        "referrer": f"@{username}" if username else str(user_id),
    }
    try:
        session = await get_session()
        await session.post(WEBHOOK_URL, json=payload)
    except Exception as e:
        logger.error(f"Sheet save error: {e}")

# === Double-click protection ===
_processed_callbacks = {}

def is_duplicate_click(callback_id):
    now = datetime.now().timestamp()
    to_delete = [k for k, v in _processed_callbacks.items() if now - v > 5]
    for k in to_delete:
        del _processed_callbacks[k]
    if callback_id in _processed_callbacks:
        return True
    _processed_callbacks[callback_id] = now
    return False

# === Photo file_id cache (lot_id → Telegram file_id) ===
_photo_cache: dict[str, str] = {}

# === Per-user navigation lock (prevents double-sends while photo loads) ===
_browse_locks: dict[int, bool] = {}

# === States ===
class Quiz(StatesGroup):
    waiting_rooms = State()
    waiting_budget = State()
    waiting_district = State()
    browsing = State()
    waiting_name = State()
    waiting_phone = State()

# === Router ===
router = Router()

# --- SCREEN 1: Hook ---
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user = message.from_user
    start_param = extract_start_param(message)
    await state.clear()
    await state.update_data(start_param=start_param)
    track_event(user.id, user.username, "start", {
        "start_param": start_param,
    })

    min_payment = 0
    payments = [adjusted_payment(l) for l in lots_data if adjusted_payment(l) > 0 and l.get("rooms") != 3]
    if payments:
        min_payment = min(payments)

    await message.answer_photo(
        photo=HERO_RENDER,
        caption=(
            "🏠 <b>Каждый месяц ты платишь за чужую квартиру.\n"
            "А мог бы — за свою.</b>\n\n"
            f"Квартира рядом с Москвой — от <b>{format_price(min_payment)} ₽/мес</b>\n"
            "Без первоначального взноса. Ключи от 3 месяцев.\n"
            "От 5 мин пешком до МЦД или метро.\n\n"
            "Мы работаем напрямую с застройщиком. Подберём квартиру, поможем с ипотекой, сопроводим до ключей.\n\n"
            "Давай проверим что тебе доступно 👇"
        ),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подобрать квартиру →", callback_data="how_it_works")]
        ])
    )

# --- SCREEN 2: How it works + rooms (step 1/3) ---
@router.callback_query(F.data == "how_it_works")
async def how_it_works(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    await state.clear()
    user = callback.from_user
    track_event(user.id, user.username, "step_how_it_works")

    await callback.message.answer(
        "💡 <b>Как купить квартиру без первоначального взноса:</b>\n\n"
        "1️⃣ Оставьте заявку — мы подберём подходящую квартиру и окажем содействие в одобрении ипотеки\n"
        "2️⃣ После одобрения бронируем квартиру и сопровождаем вас на всех этапах сделки\n"
        "3️⃣ Если понадобится небольшая сумма на оформление — можно взять потребкредит после одобрения\n"
        "4️⃣ Получаете ключи 🏠\n\n"
        "Это официальная программа от застройщика.\n\n"
        "ℹ️ <i>Семейная ипотека от 6% (дети до 7 лет). "
        "На каждом этапе вам поможет менеджер — бесплатно.</i>\n\n"
        "Подберём под вас. Сколько комнат рассматриваете? 👇\n"
        "<i>Шаг 1 из 4</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Студия", callback_data="rooms_0"),
             InlineKeyboardButton(text="1-комн", callback_data="rooms_1")],
            [InlineKeyboardButton(text="2-комн", callback_data="rooms_2"),
             InlineKeyboardButton(text="Любая", callback_data="rooms_any")],
        ])
    )
    await state.set_state(Quiz.waiting_rooms)

# --- SCREEN 3: Budget (step 2/3) ---
@router.callback_query(F.data.startswith("rooms_"))
async def quiz_rooms(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    user = callback.from_user
    choice = callback.data.replace("rooms_", "")
    rooms = None if choice == "any" else int(choice)

    await state.update_data(rooms=rooms, rooms_label=choice)
    track_event(user.id, user.username, "step_quiz_budget", {"rooms": choice})

    counts = budget_counts(rooms=rooms)

    def btn(label, key):
        n = counts[key]
        hint = f" · {n} вар." if n > 0 else " · нет"
        return InlineKeyboardButton(text=f"{label}{hint}", callback_data=f"budget_{key}")

    await callback.message.answer(
        "💰 <b>Комфортный платёж в месяц?</b>\n\n"
        "<i>Подберём лучшие варианты и рассчитаем точный платёж</i>\n\n"
        "<i>Шаг 2 из 4</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [btn("до 25 000 ₽", "25000"),
             btn("26 000–40 000 ₽", "40000")],
            [btn("41 000–60 000 ₽", "60000"),
             btn("Не важно", "any")],
            [InlineKeyboardButton(text="← Назад", callback_data="how_it_works")],
        ])
    )
    await state.set_state(Quiz.waiting_budget)

# --- Вернуться к выбору бюджета (из фолбэк-сообщения) ---
@router.callback_query(F.data == "change_budget")
async def change_budget(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    data = await state.get_data()
    rooms = data.get("rooms")
    rooms_label = data.get("rooms_label", "")

    counts = budget_counts(rooms=rooms)

    def btn(label, key):
        n = counts[key]
        hint = f" · {n} вар." if n > 0 else " · нет"
        return InlineKeyboardButton(text=f"{label}{hint}", callback_data=f"budget_{key}")

    await callback.message.answer(
        "💰 <b>Выберите другой бюджет:</b>\n\n"
        "<i>Шаг 2 из 4</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [btn("до 25 000 ₽", "25000"),
             btn("26 000–40 000 ₽", "40000")],
            [btn("41 000–60 000 ₽", "60000"),
             btn("Не важно", "any")],
            [InlineKeyboardButton(text="← Другие параметры", callback_data="how_it_works")],
        ])
    )
    await state.set_state(Quiz.waiting_budget)

# --- Shared: find results and show cards ---
async def _do_show_results(message, state: FSMContext, user, rooms, rooms_label_val,
                           max_payment, budget_choice, district):
    d = district if district != "any" else None
    results = find_lots(rooms=rooms, max_payment=max_payment, district=d)
    exact_count = len(results)

    track_event(user.id, user.username, "step_quiz_results", {
        "rooms": rooms_label_val, "budget": budget_choice,
        "district": district, "count": exact_count,
    })

    fallback_used = False
    if not results:
        fallback_used = True
        results = find_lots(rooms=rooms, district=d)
        if not results:
            results = find_lots()

    total_count = len(results)
    top = pick_diverse_lots(results, n=TOP_RESULTS)
    lot_ids = [lot["id"] for lot in top]

    await state.update_data(lot_ids=lot_ids, browse_index=0, total_count=total_count,
                            district=district)
    await state.set_state(Quiz.browsing)

    if fallback_used and budget_choice != "any":
        nonzero = [adjusted_payment(lot) for lot in results if adjusted_payment(lot) > 0]
        min_payment = min(nonzero) if nonzero else 0
        budget_labels = {"25000": "до 25 000 ₽", "40000": "до 40 000 ₽", "60000": "до 60 000 ₽"}
        budget_str = budget_labels.get(budget_choice, f"до {budget_choice} ₽")
        await message.answer(
            f"💡 С платежом <b>{budget_str}</b> пока нет точных совпадений.\n\n"
            f"Показываем ближайшие варианты — платёж от <b>{format_price(min_payment)} ₽/мес</b>.\n"
            f"Это всё равно выгоднее аренды 🏠",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="← Другой бюджет", callback_data="change_budget")],
            ])
        )
    else:
        await message.answer(
            f"🏠 <b>Нашли {total_count} квартир</b>\n"
            f"Показываем {len(top)} вариантов — листайте и выбирайте:",
            parse_mode="HTML"
        )

    await show_apartment(message, state, 0)

# --- SCREEN 4: District selection (step 3/4) or auto-skip ---
@router.callback_query(F.data.startswith("budget_"))
async def quiz_budget(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    user = callback.from_user
    choice = callback.data.replace("budget_", "")
    max_payment = None if choice == "any" else int(choice)

    data = await state.get_data()
    rooms = data.get("rooms")

    await state.update_data(budget_choice=choice, max_payment=max_payment)
    track_event(user.id, user.username, "step_quiz_budget_done", {
        "rooms": data.get("rooms_label"), "budget": choice,
    })

    dcounts = district_counts(rooms=rooms, max_payment=max_payment)
    active = [k for k in DISTRICTS if dcounts[k] > 0]

    # Auto-skip district step if only 0-1 districts have results
    if len(active) <= 1:
        district = active[0] if active else "any"
        await _do_show_results(callback.message, state, user, rooms,
                               data.get("rooms_label", ""), max_payment, choice, district)
        return

    # Show district selection
    # Build description lines for active districts
    desc_lines = []
    active_btns = []
    for key, info in DISTRICTS.items():
        n = dcounts[key]
        if n > 0:
            desc_lines.append(f"{info['label']} — {info['hint']}")
            active_btns.append(InlineKeyboardButton(
                text=f"{info['label']} · {n}",
                callback_data=f"district_{key}"
            ))

    # Pair buttons into rows of 2
    buttons = []
    for i in range(0, len(active_btns), 2):
        buttons.append(active_btns[i:i + 2])

    total = dcounts["any"]
    buttons.append([InlineKeyboardButton(
        text=f"🗺 Все районы · {total} вар.",
        callback_data="district_any"
    )])
    buttons.append([InlineKeyboardButton(text="← Назад", callback_data="change_budget")])

    desc_text = "\n".join(desc_lines)
    await callback.message.answer(
        f"📍 <b>Какой район рассматриваете?</b>\n\n"
        f"{desc_text}\n\n"
        f"<i>Шаг 3 из 4</i>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await state.set_state(Quiz.waiting_district)

# --- SCREEN 5: Results + cards (step 4/4) ---
@router.callback_query(F.data.startswith("district_"))
async def quiz_district(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    user = callback.from_user
    district = callback.data.replace("district_", "")

    data = await state.get_data()
    await _do_show_results(callback.message, state, user,
                           data.get("rooms"), data.get("rooms_label", ""),
                           data.get("max_payment"), data.get("budget_choice", "any"),
                           district)

# --- Apartment card ---
async def show_apartment(message, state: FSMContext, index: int):
    data = await state.get_data()
    lot_ids = data.get("lot_ids", [])
    total_shown = len(lot_ids)
    total_count = data.get("total_count", total_shown)

    if not lot_ids or index < 0 or index >= total_shown:
        return

    lot_id = lot_ids[index]
    lot = next((l for l in lots_data if l["id"] == lot_id), None)
    if not lot:
        return

    await state.update_data(browse_index=index)
    track_event(
        message.chat.id,
        getattr(message.chat, "username", None),
        "lot_shown",
        {
            "index": index,
            "shown_total": total_shown,
            "results_total": total_count,
        },
        lot=lot,
    )

    payment = adjusted_payment(lot)
    payment_str = f"от {format_price(payment)} ₽/мес" if payment else "по запросу"
    finish = finishing_label(lot)

    complex_name = lot.get("complex", "")
    travel = PROJECT_TRAVEL.get(complex_name)
    if travel:
        mkad, station = travel
        location_line = f"📍 {mkad}\n{station}"
    else:
        location_line = "📍 Москва и Подмосковье"

    caption = (
        f"<b>{rooms_label(lot['rooms'])}, {lot['area']} м²</b>\n"
        f"Корпус {lot['corpus']} · {lot['floor']}/{lot['totalFloors']} эт.\n"
        f"Отделка: {finish}\n"
        f"Сдача: {lot.get('deadlineLabel', '—')}\n\n"
        f"💰 Платёж: <b>{payment_str}</b>\n"
        f"Семейная ипотека от 6%\n\n"
        f"{location_line}\n\n"
        f"<i>{index + 1} из {total_shown} (всего {total_count})</i>"
    )

    nav_buttons = []
    if index > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"browse_{index - 1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{index + 1}/{total_shown}", callback_data="noop"))
    if index < total_shown - 1:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"browse_{index + 1}"))

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        nav_buttons,
        [InlineKeyboardButton(text="📞 Позвонить", callback_data="show_phone"),
         InlineKeyboardButton(text="✍️ Написать", url=MANAGER_LINK)],
        [InlineKeyboardButton(text="📱 Оставить номер — перезвоним", callback_data="leave_phone")],
        [InlineKeyboardButton(text="← Другие параметры", callback_data="how_it_works")],
    ])

    img = plan_url(lot)
    cached = _photo_cache.get(lot_id)
    photo = cached or img
    if photo:
        try:
            sent = await message.answer_photo(photo=photo, caption=caption, parse_mode="HTML", reply_markup=keyboard)
            if not cached and sent.photo:
                _photo_cache[lot_id] = sent.photo[-1].file_id
        except Exception as e:
            logger.error(f"Failed to send plan photo: {e}")
            await message.answer(caption, parse_mode="HTML", reply_markup=keyboard)
    else:
        await message.answer(caption, parse_mode="HTML", reply_markup=keyboard)

    # After last card — send CTA
    if index == total_shown - 1:
        await send_final_cta(message, state)

# --- FINAL CTA after last card ---
async def send_final_cta(message, state: FSMContext):
    data = await state.get_data()
    total_count = data.get("total_count", 0)
    shown = len(data.get("lot_ids", []))

    await message.answer(
        f"👆 <b>Показали {shown} из {total_count} квартир по вашим параметрам.</b>\n\n"
        f"Менеджер подберёт остальные под вас, рассчитает точный платёж и поможет с одобрением.\n\n"
        f"Консультация бесплатная, без обязательств.\n\n"
        f"⬇️ <b>Выберите удобный способ связи:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Позвонить", callback_data="show_phone"),
             InlineKeyboardButton(text="✍️ Написать", url=MANAGER_LINK)],
            [InlineKeyboardButton(text="📱 Оставить номер — перезвоним", callback_data="leave_phone")],
            [InlineKeyboardButton(text="🌐 Все квартиры на сайте", url=SITE_URL)],
            [InlineKeyboardButton(text="🔄 Подобрать заново", callback_data="how_it_works")],
        ])
    )

@router.callback_query(F.data.startswith("browse_"))
async def browse(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    user_id = callback.from_user.id
    if _browse_locks.get(user_id):
        await callback.answer("⏳ Загружаю…")
        return
    _browse_locks[user_id] = True
    await callback.answer()
    try:
        index = int(callback.data.replace("browse_", ""))
        user = callback.from_user
        track_event(user.id, user.username, "browse_click", {"index": index})
        await show_apartment(callback.message, state, index)
    finally:
        _browse_locks[user_id] = False

@router.callback_query(F.data == "show_phone")
async def show_phone(callback: CallbackQuery):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    track_event(callback.from_user.id, callback.from_user.username, "phone_revealed")
    await callback.message.answer(
        f"📞 Позвоните менеджеру:\n\n<b>{MANAGER_PHONE}</b>\n\nРаботаем ежедневно 9:00 — 21:00",
        parse_mode="HTML"
    )

@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()

# --- Phone collection ---
@router.callback_query(F.data == "leave_phone")
async def ask_phone(callback: CallbackQuery, state: FSMContext):
    if is_duplicate_click(callback.id):
        await callback.answer()
        return
    await callback.answer()
    user = callback.from_user
    track_event(user.id, user.username, "step_name_request")

    await callback.message.answer(
        "👤 Как к вам обращаться? Напишите ваше имя.",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Quiz.waiting_name)

@router.message(Quiz.waiting_name)
async def got_name(message: Message, state: FSMContext):
    name = message.text.strip() if message.text else message.from_user.first_name or ""
    await state.update_data(lead_name=name)
    track_event(message.from_user.id, message.from_user.username, "step_phone_request", {"name": name})

    await message.answer(
        f"Приятно познакомиться, {name}! 📱\n\n"
        "Отправьте номер телефона кнопкой ниже или напишите вручную.\n"
        "Перезвоним в течение часа.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Отправить номер", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True
        )
    )
    await state.set_state(Quiz.waiting_phone)

@router.message(Quiz.waiting_phone, F.contact)
async def got_contact(message: Message, state: FSMContext, bot: Bot):
    user = message.from_user
    phone = message.contact.phone_number

    data = await state.get_data()
    name = data.get("lead_name") or f"{message.contact.first_name or ''} {message.contact.last_name or ''}".strip()
    lot_ids = data.get("lot_ids", [])
    browse_index = data.get("browse_index", 0)
    lot = None
    if lot_ids and browse_index < len(lot_ids):
        lot = next((l for l in lots_data if l["id"] == lot_ids[browse_index]), None)

    track_event(user.id, user.username, "lead_phone", {"name": name}, lot=lot, phone=phone, name=name)
    await notify_lead(bot, user.id, user.username, name, phone, lot)

    await message.answer(
        "✅ <b>Спасибо! Ваша заявка принята.</b>\n\n"
        "Мы свяжемся с вами в течение 1 часа.\n"
        "А пока можете посмотреть все квартиры на сайте 👇",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await message.answer(
        "Что дальше?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Смотреть квартиры на сайте", url=SITE_URL)],
            [InlineKeyboardButton(text="🔄 Подобрать другую квартиру", callback_data="how_it_works")],
        ])
    )
    await state.clear()

@router.message(Quiz.waiting_phone, F.text)
async def got_phone_text(message: Message, state: FSMContext, bot: Bot):
    user = message.from_user
    phone = message.text.strip()

    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) < 10:
        await message.answer("Пожалуйста, введите корректный номер телефона (например, +7 999 123-45-67)")
        return

    data = await state.get_data()
    lot_ids = data.get("lot_ids", [])
    browse_index = data.get("browse_index", 0)
    lot = None
    if lot_ids and browse_index < len(lot_ids):
        lot = next((l for l in lots_data if l["id"] == lot_ids[browse_index]), None)

    data = await state.get_data()
    name = data.get("lead_name") or user.first_name or ""

    track_event(user.id, user.username, "lead_phone_manual", {"name": name}, lot=lot, phone=phone, name=name)
    await notify_lead(bot, user.id, user.username, name, phone, lot)

    await message.answer(
        "✅ <b>Спасибо! Ваша заявка принята.</b>\n\n"
        "Мы свяжемся с вами в течение 1 часа.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await message.answer(
        "Что дальше?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 Смотреть квартиры на сайте", url=SITE_URL)],
            [InlineKeyboardButton(text="🔄 Подобрать другую квартиру", callback_data="how_it_works")],
        ])
    )
    await state.clear()

@router.message(F.text == "/about")
async def cmd_about(message: Message):
    await message.answer(
        "ℹ️ <b>О сервисе</b>\n\n"
        "Мы — сервис подбора квартир без первоначального взноса в новостройках рядом с Москвой.\n\n"
        "Что делаем:\n"
        "• Подбираем квартиру под ваши параметры\n"
        "• Помогаем с одобрением семейной ипотеки от 6%\n"
        "• Сопровождаем на всех этапах сделки до получения ключей\n\n"
        "Работаем напрямую с застройщиками. Консультация бесплатная.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Подобрать квартиру", callback_data="how_it_works")],
        ])
    )

@router.message(F.text == "/contact")
async def cmd_contact(message: Message):
    await message.answer(
        "📞 <b>Связаться с нами</b>\n\n"
        f"Менеджер: @{MANAGER_USERNAME}\n"
        "Напишите — ответим в течение часа.\n\n"
        "Или оставьте номер прямо здесь — перезвоним.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Написать менеджеру", url=MANAGER_LINK)],
            [InlineKeyboardButton(text="📞 Оставить номер", callback_data="leave_phone")],
        ])
    )

@router.message()
async def fallback(message: Message, state: FSMContext):
    # Only respond in private chats, ignore groups
    if message.chat.type != "private":
        return
    await message.answer(
        "Нажмите /start чтобы начать подбор квартиры.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏠 Начать подбор", callback_data="how_it_works")]
        ])
    )

# === Main ===
async def main():
    load_lots()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    logger.info("Bot starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
