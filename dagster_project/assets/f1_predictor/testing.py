from dagster import asset, Output, MetadataValue
from dagster_project.utils.discord_utils import DiscordUtils
@asset()
def send_discord(context):
    dis = DiscordUtils()
    dis.send_message(message='This is a Dagster test message!')
    return
