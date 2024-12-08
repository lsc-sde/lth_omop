from sqlmesh import macro

@macro()
def convert_event_ts_to_datetime(evaluator, event_ts):

    out = """
    dateadd(
		SECOND,
		86400*cast(substring(event_ts, charindex('.', event_ts), LEN(event_ts)) as float),
		dateadd(DAY, cast(substring(event_ts,0, charindex('.', event_ts)) as int), '1900-01-01')
		)
    """

    return out

