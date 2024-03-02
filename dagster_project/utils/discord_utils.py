import discord
import os

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
