import datetime
import discord
import os
import pytz

global message_found
utc = pytz.UTC
token = os.getenv('DISCORD_TOKEN')


class DiscordUtils:
    def __init__(self):
        intents = discord.Intents.default()
        self.client = discord.Client(intents=intents)

    def send_message(self, message, attachment: list = None, channel_id='1203738391443939399'):
        @self.client.event
        async def on_ready():
            channel = self.client.get_channel(int(channel_id))
            if attachment is not None:
                file_list = list()
                for file in attachment:
                    file_list.append(discord.File(file))
                await channel.send(files=file_list, content=message)
            else:
                await channel.send(content=message)
            await self.client.close()

        self.client.run(token)

    def check_for_message(self,
                          message_content: str,
                          message_age_limit: int = 45,
                          channel_id='1203738391443939399'):

        global message_found
        message_found = None

        @self.client.event
        async def on_ready():
            utc_dt = datetime.datetime.utcnow()
            utc_dt = utc.localize(utc_dt)
            channel = self.client.get_channel(int(channel_id))
            async for message in channel.history(limit=5, oldest_first=False):
                try:
                    message_dt = message.created_at
                    if message_dt > (utc_dt - datetime.timedelta(seconds=message_age_limit)):
                        if message_content.upper() in message.content.upper():
                            global message_found
                            message_found = message.content
                            await self.client.close()
                            return
                except UnicodeEncodeError:
                    print('Encode Error Passing')

            await self.client.close()
            return

        self.client.run(token)
        self.client.clear()
        self.client.close()
        return message_found
