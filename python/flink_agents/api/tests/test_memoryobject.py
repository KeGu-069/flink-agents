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
from flink_agents.api.memoryobject import LocalMemoryObject


def create_memory():
    return LocalMemoryObject({"": {"__OBJ__": set()}})


def test_basic_set_get():
    mem = create_memory()
    mem.set("count", 1)
    assert mem.get("count") == 1
    assert mem.get_field_names() == ["count"]
    assert mem.get_fields() == {"count": 1}


def test_nested_set_and_get():
    mem = create_memory()
    mem.set("a.b.c", True)
    assert mem.get("a").get("b").get("c") is True
    assert "b" in mem.get("a").get_field_names()
    assert mem.get("a").get_fields()["b"].startswith("__OBJ__")
    assert mem.get("a.b.c") is True
    assert mem.get("a.b").get("c") == True


def test_new_object_and_is_exist():
    mem = create_memory()
    mem.new_object("foo.bar")
    assert mem.is_exist("foo") is True
    assert mem.is_exist("foo.bar") is True
    fields = mem.get("foo").get_fields()
    assert "bar" in fields


def test_overwrite_behavior():
    mem = create_memory()
    mem.set("profile", "active")
    try:
        mem.new_object("profile")
        assert False, "Should raise error when creating object on primitive"
    except ValueError:
        pass
    mem.new_object("profile", overwrite=True)
    mem.get("profile").set("status", "ok")
    assert mem.get("profile").get("status") == "ok"


def test_auto_parent_fill_and_children():
    mem = create_memory()
    mem.new_object("x.y.z")
    assert mem.is_exist("x") is True
    assert mem.is_exist("x.y") is True
    assert mem.is_exist("x.y.z") is True
    assert "x" in mem.get_field_names()
    fields = mem.get_fields()
    assert fields["x"].startswith("__OBJ__")


def test_disallow_overwrite_object_with_primitive():
    mem = create_memory()
    mem.new_object("obj")
    try:
        mem.set("obj", 123)
        assert False, "Should raise error when overwriting object with primitive"
    except ValueError:
        pass

