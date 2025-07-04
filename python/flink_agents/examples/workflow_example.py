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

from flink_agents.api.decorators import action
from flink_agents.api.event import Event, InputEvent, OutputEvent
from flink_agents.api.execution_enviroment import AgentsExecutionEnvironment
from flink_agents.api.runner_context import RunnerContext
from flink_agents.api.workflow import Workflow


class MyEvent(Event): #noqa D101
    value: Any

#TODO: Replace this workflow with more practical example.
class MyWorkflow(Workflow):
    """An example of Workflow to show the basic usage.

    Currently, this workflow doesn't really make sense, and it's mainly for developing
    validation.
    """
    @action(InputEvent)
    @staticmethod
    def first_action(event: Event, ctx: RunnerContext): #noqa D102
        input = event.input
        memory = ctx.get_short_term_memory()

        counter = memory.get("counter") or 0
        memory.set("counter", counter + 1)
        count = memory.get("counter")

        if not memory.is_exist("metadata"):
            meta_obj = memory.new_object("metadata")
            meta_obj.set("source","web")
            meta_obj.set("version",1)
        else:
            meta_obj = memory.get("metadata")
            version = meta_obj.get("version") or 0
            meta_obj.set("version", version + 1)

        fields = meta_obj.get_fields()
        fields_str = ", ".join(f"{k}={v}" for k, v in fields.items())

        content = f"{input} -> first_action"
        key_with_count = f"(seen {count} times)"

        ctx.send_event(MyEvent(value=content))
        ctx.send_event(OutputEvent(output={key_with_count: content}))

    @action(MyEvent)
    @staticmethod
    def second_action(event: Event, ctx: RunnerContext): #noqa D102
        memory = ctx.get_short_term_memory()

        counter = memory.get("counter") or 0
        memory.set("counter", counter + 1)
        count = memory.get("counter")

        base_message = event.value.split('->')[0].strip()
        content = f"{base_message} -> second_action"

        key_with_count = f"(seen {count} times)"
        ctx.send_event(OutputEvent(output={key_with_count: content}))

        fields = memory.get_fields()
        memory_contents = ", ".join(f"{k}: {v}" for k, v in fields.items())
        ctx.send_event(OutputEvent(output={f"memory contents": memory_contents}))



if __name__ == "__main__":
    env = AgentsExecutionEnvironment.get_execution_environment()

    input_list = []
    workflow = MyWorkflow()

    output_list = env.from_list(input_list).apply(workflow).to_list()


    input_list.append({'key': 'bob', 'value': 'The message from bob'})
    input_list.append({'key': 'bob', 'value': 'Second message from bob'})
    input_list.append({'k': 'john', 'v': 'The message from john'})
    input_list.append({'key': 'john', 'value': 'Second message from john'})
    input_list.append({'value': 'Message from unknown'})   # will automatically generate a new unique key
    input_list.append({'value': 'Another from unknown'})

    env.execute()

    for output in output_list:
        print(output)
