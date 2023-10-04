from locust import FastHttpUser, task, constant_pacing, events
from locust.runners import MasterRunner, WorkerRunner
import time
import json
import requests


class HelloWorldUser(FastHttpUser):
    # https://docs.locust.io/en/stable/writing-a-locustfile.html#wait-time-attribute
    wait_time = constant_pacing(1)

    @task
    def hello_world(self):
        self.client.get("/")


# https://github.com/locustio/locust/blob/master/examples/custom_messages.py
SEND_EVENTS_MSG = "send_events"
event_data = []
event_count_threshold = 5

csv_headers = [
    "endpoint",
    "status_code",
    "request_start_ms",
    "response_duration_ms",
    "error_type",
    "error_message",
]
props_by_header = {
    "endpoint": "endpoint",
    "status_code": "status_code",
    "request_start_ms": "request_start_ms",
    "response_duration_ms": "response_duration_ms",
    "error_type": "error.type",
    "error_message": "error.message",
}

curr_environment = None


def write_events_to_file(event_data):
    with open("events.csv", "a") as f:
        for event in event_data:
            event_row = ",".join(
                [str(event.get(props_by_header[header], "")) for header in csv_headers]
            )
            f.write(event_row + "\n")


def process_events_any_runner(event_data):
    if isinstance(curr_environment.runner, WorkerRunner):
        curr_environment.runner.send_message(SEND_EVENTS_MSG, event_data)
    else:
        write_events_to_file(event_data)


@events.request.add_listener
def on_request(
    name,
    response_time,
    response,
    exception,
    start_time,
    **kwargs,
):
    event_data.append(
        {
            "endpoint": name,
            "status_code": response.status_code,
            "request_start_ms": start_time,
            "response_duration_ms": response_time,
            "error": {"type": type(exception).__name__, "message": str(exception)},
        }
    )

    if len(event_data) >= event_count_threshold:
        process_events_any_runner(event_data)
        event_data.clear()


def process_raw(environment, msg, **kwargs):
    # Message class contains fields 'type', 'data', and 'node_id'
    # https://github.com/locustio/locust/blob/master/locust/rpc/protocol.py
    print(f"Received {len(msg.data)} raw events from worker {msg.node_id}")
    write_events_to_file(msg.data)


@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    global curr_environment
    curr_environment = environment
    if isinstance(environment.runner, MasterRunner):
        with open("events.csv", "w") as f:
            f.write(",".join(csv_headers) + "\n")
        environment.runner.register_message(SEND_EVENTS_MSG, process_raw)


@events.test_stop.add_listener
def on_locust_stop(environment, **_kwargs):
    process_events_any_runner(event_data)
    event_data.clear()
