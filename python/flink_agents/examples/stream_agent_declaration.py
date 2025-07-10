################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
from typing import Any

from pyflink.datastream import StreamExecutionEnvironment

from flink_agents.api.decorators import action
from flink_agents.api.event import Event, InputEvent, OutputEvent
from flink_agents.api.runner_context import RunnerContext
from flink_agents.api.workflow import Workflow


print("load custom workflow")

class MyEvent(Event):  # noqa D101
    value: Any


def list_to_str(lst):
    return ','.join(str(x) if x is not None else '' for x in lst)

def dict_to_str(d):
    return "{" + ", ".join(
        f"{k!r}: {v!r}" for k, v in d.items() if v is not None
    ) + "}"


class MyWorkflow(Workflow):
    """Workflow used for explaining integrating agents with DataStream.

    Because pemja will find action in this class when execute workflow, we can't
    define this class directly in stream_agent_example.py for module name will be set
    to __main__.
    """

    @action(InputEvent)
    @staticmethod
    def first_action(event: Event, ctx: RunnerContext):  # noqa D102
        print("============first action function ====")
        input = event.input
        content = input.get_review() + " first action11111111."
        ctx.get_short_term_memory().set("a.b",1)
        # print("state access: set m true," + ctx.get_short_term_memory().set("m",True))
        print("state access: get a, fieldNames, " + list_to_str(ctx.get_short_term_memory().get("a").get_field_names()))
        print("state access: get a fields," + dict_to_str(ctx.get_short_term_memory().get("a").get_fields()))
        print("state access: get a get b," + str(ctx.get_short_term_memory().get("a").get("b")))
        ctx.send_event(MyEvent(value=content))

    @action(MyEvent)
    @staticmethod
    def second_action(event: Event, ctx: RunnerContext):  # noqa D102
        input = event.value
        content = input + " second action."
        print("state access: get a fields, " + dict_to_str(ctx.get_short_term_memory().get("a").get_fields()))
        print("state access: get a get b," + str(ctx.get_short_term_memory().get("a").get("b")))
        ctx.send_event(OutputEvent(output=content))
