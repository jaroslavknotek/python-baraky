from urllib.parse import urlparse
from baraky.storages import EstatesHitQueue
import logging

from baraky.models import EstateReaction
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from baraky.settings import TelegramBotSettings

logger = logging.getLogger("baraky.notifications.telegram")


class TelegramNotificationsBot:
    def __init__(
        self,
        queue: EstatesHitQueue,
        reactions_storage,
        settings: TelegramBotSettings | None = None,
    ) -> None:
        if settings is None:
            settings = TelegramBotSettings()

        self.settings = settings
        self.reactions_storage = reactions_storage

        application = Application.builder().token(settings.token).build()

        application.add_handler(CommandHandler("send_links", self.send_update))
        application.add_handler(CommandHandler("auto", self.start_auto_messaging))
        application.add_handler(CommandHandler("stop", self.stop_notify))
        application.add_handler(CallbackQueryHandler(self.button))
        self.application = application
        self.queue = queue

    def start(self):
        self.application.run_polling()

    async def send_message(self, chat_id, context):
        try:
            estate_res = self.queue.peek()

            if not estate_res:
                return
            estate_id, estate = estate_res

            link = estate.link
            logger.debug(f"Trying {link}")
            buttons = parse_reaction_keys(
                parse_estate_id_from_uri(link), self.settings.reactions
            )
            commute_min = estate.pid_commute_time_min
            path = estate.station_nearby
            transfers = estate.transfers_count
            base_message_text = (
                f"{link}\n{commute_min=:.0f}.\n*Path*:{path}\n{transfers=}"
            )
            await context.bot.send_message(
                chat_id=chat_id, text=base_message_text, reply_markup=buttons
            )
            self.queue.delete(estate_id)
            logger.debug(f"Sent {link}")

        except Exception:
            logger.exception("Job send links failed")

    async def send_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        chat_id = update.message.chat_id
        await self.send_message(chat_id, context)

    async def start_auto_messaging(self, update, context):
        chat_id = update.message.chat_id

        queued_items = self.queue.total()
        message = f"Starting automatic messages! \nQueued items:{queued_items}\ninterval:{self.settings.interval_sec} sec"

        await context.bot.send_message(chat_id=chat_id, text=message)

        async def send_links(context):
            await self.send_message(chat_id, context)

        context.job_queue.run_repeating(
            send_links,
            self.settings.interval_sec,
            chat_id=chat_id,
            name=str(chat_id),
        )

    async def stop_notify(self, update, context):
        chat_id = update.message.chat_id
        await context.bot.send_message(
            chat_id=chat_id, text="Stopping automatic messages!"
        )
        jobs = context.job_queue.get_jobs_by_name(str(chat_id))
        if len(jobs):
            jobs[0].schedule_removal()

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        def parse_reactions_by_user(link_reactions):
            link_reactions = dict(sorted(link_reactions.items()))
            return "\n".join(
                f"{user}: {self.settings.reactions[reaction]}"
                for user, reaction in link_reactions.items()
            )

        query = update.callback_query
        await query.answer()
        link, msg_text = query.message.text.split("\n")[:2]

        reaction, link_id = query.data.split("_")
        user = query.from_user.username

        estate_id = parse_estate_id_from_uri(link)
        estate_reaction = EstateReaction(
            estate_id=estate_id,
            username=user,
            reaction=reaction,
        )
        self.reactions_storage.write(estate_reaction)
        link_reactions = self.reactions_storage.read_by_estate(estate_id)
        reactions_dict = {r.username: r.reaction for r in link_reactions}
        reactions = parse_reactions_by_user(reactions_dict)

        base_message_text = f"{link}\n{msg_text}"
        await query.edit_message_text(
            text=f"{base_message_text}\n{reactions}",
            reply_markup=parse_reaction_keys(link_id, self.settings.reactions),
        )


def parse_reaction_keys(link, reactions):
    buttons = [
        InlineKeyboardButton(emoji, callback_data=f"{reaction}_{link}")
        for reaction, emoji in reactions.items()
    ]

    keyboard = [buttons]
    return InlineKeyboardMarkup(keyboard)


def parse_last_path_part(maybe_uri_text):
    uri_text = str(maybe_uri_text)
    return urlparse(uri_text).path.split("/")[-1]


def parse_estate_id_from_uri(maybe_uri_text) -> str:
    estate_id_text = parse_last_path_part(maybe_uri_text)
    return estate_id_text
