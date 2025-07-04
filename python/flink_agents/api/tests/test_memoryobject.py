
from flink_agents.api.memoryobject import LocalMemoryObject


def create_memory():
    return LocalMemoryObject({"": {"__OBJ__": set()}})


def test_basic_set_get():
    mem = create_memory()
    mem.set("count", 1)
    assert mem.get("count") == 1
    assert mem.get_field_names() == ["count"]
    assert mem.get_fields() == {"count": 1}
    print("test_basic_set_get passed.")


def test_nested_set_and_get():
    mem = create_memory()
    mem.set("a.b.c", True)
    assert mem.get("a").get("b").get("c") is True
    assert "b" in mem.get("a").get_field_names()
    assert mem.get("a").get_fields()["b"].startswith("__OBJ__")
    assert mem.get("a.b.c") is True
    assert mem.get("a.b").get("c") == True
    print("test_nested_set_and_get passed.")


def test_new_object_and_is_exist():
    mem = create_memory()
    mem.new_object("foo.bar")
    assert mem.is_exist("foo") is True
    assert mem.is_exist("foo.bar") is True
    fields = mem.get("foo").get_fields()
    assert "bar" in fields
    print("test_new_object_and_is_exist passed.")


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
    print("test_overwrite_behavior passed.")


def test_auto_parent_fill_and_children():
    mem = create_memory()
    mem.new_object("x.y.z")
    assert mem.is_exist("x") is True
    assert mem.is_exist("x.y") is True
    assert mem.is_exist("x.y.z") is True
    assert "x" in mem.get_field_names()
    fields = mem.get_fields()
    assert fields["x"].startswith("__OBJ__")
    print("test_auto_parent_fill_and_children passed.")


def test_disallow_overwrite_object_with_primitive():
    mem = create_memory()
    mem.new_object("obj")
    try:
        mem.set("obj", 123)
        assert False, "Should raise error when overwriting object with primitive"
    except ValueError:
        pass
    print("test_disallow_overwrite_object_with_primitive passed.")


if __name__ == "__main__":
    test_basic_set_get()
    test_nested_set_and_get()
    test_new_object_and_is_exist()
    test_overwrite_behavior()
    test_auto_parent_fill_and_children()
    test_disallow_overwrite_object_with_primitive()
    print("\nAll tests passed.")
