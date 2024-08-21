import asyncio
import logging

from baraky.queues import RabbitQueueConsumer
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from baraky.settings import TelegramBotSettings

logger = logging.getLogger("baraky.notifications.telegram")


class TelegramNotificationsBot:
    
    def __init__(
        self, 
        settings:TelegramBotSettings|None=None,
        queue_consumer:RabbitQueueConsumer|None=None,
            ) -> None:
        if settings is None:
            settings = TelegramBotSettings()

        self.settings = settings
        if queue_consumer is None:
            queue_consumer = RabbitQueueConsumer(settings.queue_name)

        application = Application.builder().token(settings.bot_token).build()

        application.add_handler(CommandHandler("send_links", create_send_links(params)))
        application.add_handler(CommandHandler("auto", create_start_auto_messaging(params)))
        application.add_handler(CommandHandler("stop", stop_notify))
        application.add_handler(CallbackQueryHandler(create_button(params)))
        self.application = application

    def start(self):
        self.application.run_polling()

    
    async def send_links(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE
    ) -> None:

        chat_id = update.message.chat_id
        try:
            advert = params.queue.pop()

            if not advert:
                return

            link = advert["link"]
            logger.debug(f"Trying {link}")
            buttons = parse_reaction_keys(
                parse_estate_id_from_uri(link), params.reactions
            )
            commute_min = advert["commute_min"]
            station = advert["closest_station_name"]
            km = advert["closest_station_km"]
            base_message_text = f"{link}\n{commute_min=:.0f}. From:{station}({km=:.0f})"

            await context.bot.send_message(
                chat_id=chat_id, text=base_message_text, reply_markup=buttons
            )
            logger.debug(f"Sent {link}")
            await asyncio.sleep(1)

        except Exception as e:
            logger.exception("Job send links failed")
        return send_links

    


class Param:
    def __init__(
        self,
        bot_token,
        reactions_db,
        queue,
        reactions_map=None,
        interval=None,
    ) -> None:
        self.bot_token = bot_token
        self.interval = interval or 60  # seconds
        self.reactions_db = reactions_db
        self.queue = queue
        self.reactions = reactions_map

def parse_reaction_keys(link, reactions):
    buttons = [
        InlineKeyboardButton(emoji, callback_data=f"{reaction}_{link}")
        for reaction, emoji in reactions.items()
    ]

    keyboard = [buttons]
    return InlineKeyboardMarkup(keyboard)


def create_start_auto_messaging(params):
    async def start_auto_messaging(update, context):
        chat_id = update.message.chat_id

        queued_items = params.queue.total()
        message = f"Starting automatic messages! \nQueued items:{queued_items}"

        await context.bot.send_message(chat_id=chat_id, text=message)
        context.job_queue.run_repeating(
            create_send_links_cr(params, chat_id),
            params.interval,
            chat_id=chat_id,
            name=str(chat_id),
        )

    return start_auto_messaging


async def stop_notify(update, context):
    chat_id = update.message.chat_id
    await context.bot.send_message(chat_id=chat_id, text="Stopping automatic messages!")
    jobs = context.job_queue.get_jobs_by_name(str(chat_id))
    if len(jobs):
        jobs[0].schedule_removal()




def create_button(params):
    async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        def parse_reactions_by_user(link_reactions):
            link_reactions = dict(sorted(link_reactions.items()))
            return "\n".join(
                f"{user}: {params.reactions[reaction]}"
                for user, reaction in link_reactions.items()
            )

        query = update.callback_query
        await query.answer()
        link, msg_text = query.message.text.split("\n")[:2]

        reaction, link_id = query.data.split("_")
        user = query.from_user.username

        estate_id = parse_estate_id_from_uri(link)
        params.reactions_db.write(estate_id, user, reaction)
        link_reactions = params.reactions_db.read_by_estate(estate_id)
        reactions_dict = {r.username: r.reaction for r in link_reactions}
        reactions = parse_reactions_by_user(reactions_dict)

        base_message_text = f"{link}\n{msg_text}"
        await query.edit_message_text(
            text=f"{base_message_text}\n{reactions}",
            reply_markup=parse_reaction_keys(link_id, params.reactions),
        )

    return button




def create_bot(params) -> None:
 
