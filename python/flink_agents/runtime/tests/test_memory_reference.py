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

from flink_agents.api.memory_reference import MemoryRef
from flink_agents.runtime.local_memory_object import LocalMemoryObject


class MockRunnerContext:  # noqa D101
    def __init__(self, memory: LocalMemoryObject) -> None:
        """Mock RunnerContext for testing resolve() method."""
        self._memory = memory

    def get_short_term_memory(self) -> LocalMemoryObject:  # noqa D102
        return self._memory


def create_memory() -> LocalMemoryObject:
    """Return a MemoryObject for every test case."""
    # Assuming the store now holds a tuple (value, type) for data,
    # and _ObjMarker for nested objects.
    return LocalMemoryObject({})


class User:  # noqa: D101
    def __init__(self, name: str, age: int) -> None:
        """Store for later comparison."""
        self.name = name
        self.age = age

    def __eq__(self, other: object) -> bool:
        return (
                isinstance(other, User)
                and other.name == self.name
                and other.age == self.age
        )


def test_set_returns_valid_memory_ref() -> None:
    """Verify that `set()` returns a correctly formed MemoryRef."""
    mem = create_memory()
    user = User("Alice", 30)

    # set() should now return a MemoryRef
    ref = mem.set("user_profile", user)

    assert isinstance(ref, MemoryRef)
    assert ref.path == "user_profile"
    # Check for fully qualified type name
    expected_type_name = f"{User.__module__}.{User.__qualname__}"
    assert ref.type_name == expected_type_name


def test_get_with_string_path_after_ref_change() -> None:
    """Test that `get(path)` still works as before."""
    mem = create_memory()
    user = User("Bob", 25)
    mem.set("user.bob", user)

    retrieved_user = mem.get("user.bob")
    assert retrieved_user == user


def test_get_with_memory_ref() -> None:
    """Test that `get()` can correctly resolve a MemoryRef."""
    mem = create_memory()
    data = {"id": "data123", "values": [1, 2, 3]}

    # 1. Store data and get the reference
    ref = mem.set("shared.data", data)

    # 2. Use the reference to get the data back
    retrieved_data = mem.get(ref)

    assert retrieved_data == data


def test_nested_get_with_memory_ref() -> None:
    """Test using get with a ref pointing to a nested location."""
    mem = create_memory()
    transaction_id = "txn-abc-123"

    ref = mem.set("system.logs.latest_transaction", transaction_id)

    # Use a parent object to get the reference
    logs_obj = mem.get("system.logs")
    retrieved_id = logs_obj.get(ref)

    assert retrieved_id == transaction_id


def test_memory_ref_create_and_properties() -> None:
    """Test the static `create` method and properties of MemoryRef."""
    path = "a.b.c"
    type_name = "builtins.str"
    ref = MemoryRef.create(path, type_name)

    assert isinstance(ref, MemoryRef)
    assert ref.path == path
    assert ref.type_name == type_name


def test_memory_ref_resolve_method() -> None:
    """Test the `resolve()` method of a MemoryRef instance."""
    mem = create_memory()
    ctx = MockRunnerContext(mem)

    chat_history = ["Hello", "How are you?", "I'm a test case."]
    ref = mem.set("conversation.history", chat_history)

    # Resolve the reference using the context
    resolved_history = ref.resolve(ctx)

    assert resolved_history == chat_history


def test_passing_ref_simulates_action_chain() -> None:
    """Simulate passing a MemoryRef between two 'actions'."""
    mem = create_memory()

    # --- "Action A" ---
    # Creates a large object and stores it, gets a reference
    large_dataset = {"data": "x" * 1024 * 1024}  # 1MB string
    data_ref = mem.set("datasets.large_data", large_dataset)

    # Action A passes `data_ref` to Action B, not the large_dataset itself.

    # --- "Action B" ---
    # Receives the reference and resolves it to get the data
    retrieved_dataset = mem.get(data_ref)

    assert retrieved_dataset == large_dataset
    assert len(retrieved_dataset["data"]) == 1024 * 1024
