from dagster import asset, Output, MetadataValue

@asset()
def testing(context):
    context.log.info("testing")
    return Output(value='test')