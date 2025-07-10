from typing import Any, Dict, List, Optional, Union

from flink_agents.api.memoryobject import MemoryObject


class FlinkMemoryObject(MemoryObject):
    """
    Python wrapper for Java's MemoryObjectImpl, providing access to short-term memory
    in Flink environment.

    This class allows Python functions to interact with the Flink-based short-term memory
    implemented in Java.
    """

    def __init__(self, j_memory_object: Any) -> None:
        """Initialize with a Java MemoryObject instance."""
        self._j_memory_object = j_memory_object


    def get(self, path: str) -> Any:
        """Get a nested object or value by path.

        If the field is an object, returns a new FlinkMemoryObject.
        Returns None if the field does not exist.
        """
        try:
            j_result = self._j_memory_object.get(path)
            if j_result is None:
                return None
            if not j_result.isPrefix():
                return j_result.getValue()
            return FlinkMemoryObject(j_result)
        except Exception as e:
            raise RuntimeError(f"Failed to get field '{path}' from memory") from e

    def set(self, path: str, value: Any) -> None:
        """Set a value at the given path. Creates intermediate objects if needed."""
        try:
            self._j_memory_object.set(path, value)
        except Exception as e:
            raise RuntimeError(f"Failed to set value at path '{path}'") from e

    def new_object(self, path: str, overwrite: bool = False) -> "FlinkMemoryObject":
        """Create a new object at the given path."""
        try:
            return FlinkMemoryObject(
                self._j_memory_object.newObject(path, overwrite)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create new object at path '{path}'") from e

    def is_exist(self, path: str) -> bool:
        """Check if a field exists at the given path."""
        try:
            return self._j_memory_object.isExist(path)
        except Exception as e:
            raise RuntimeError(f"Failed to check existence of path '{path}'") from e

    def get_field_names(self) -> List[str]:
        """Get names of all direct fields in the current object."""
        try:
            return list(self._j_memory_object.getFieldNames())
        except Exception as e:
            raise RuntimeError("Failed to get field names") from e

    def get_fields(self) -> Dict[str, Any]:
        """Get all direct fields and their values."""
        try:
            return dict(self._j_memory_object.getFields())
        except Exception as e:
            raise RuntimeError("Failed to get fields") from e
