# -*- coding: utf-8 -*-
"""
Exceptions when sending messages through a Whatsapp Bot.
This module assumes that the bot inherits from the template bot at
https://github.com/prefeitura-rio/whatsapp-bot
"""


class WhatsAppSendMessageError(Exception):
    """
    Exception raised when sending a message through a Whatsapp Bot fails.
    """


class WhatsAppBotNotFoundError(Exception):
    """
    Exception raised when a Whatsapp Bot is not found.
    """
