import discord
import os

token = os.getenv('DISCORD_TOKEN')


class DiscordUtils:
    def __init__(self):
        intents = discord.Intents.default()
        self.client = discord.Client(intents=intents)

    def send_message(self, message, channel_id='1203738391443939399'):
        @self.client.event
        async def on_ready():
            channel = self.client.get_channel(int(channel_id))
            await channel.send(message)
            await self.client.close()

        self.client.run(token)
