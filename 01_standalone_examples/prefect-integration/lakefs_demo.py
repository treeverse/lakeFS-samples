import datetime

def generate_flow_run_name():
    date = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    #flow_name = runtime.flow_run.flow_name

    return f"@ {date}"
