from dagster import asset, Output, MetadataValue, op, OpExecutionContext
from dagster_project.utils.discord_utils import DiscordUtils


@asset(config_schema={'event_type': str, 'event_name': str, 'year': int})
def get_session_data(context):
    event_type = context.op_config['event_type']
    event_name = context.op_config['event_name']
    year = context.op_config['year']
    return Output(value=(event_type, event_name, year),
                  metadata={'Event Type': event_type,
                            'Event Name': event_name,
                            'Event Year': year})


#@asset()
#def send_discord(context):
#    dis = DiscordUtils()
#    dis.send_message(message='This is a Dagster test message!')
#    return
