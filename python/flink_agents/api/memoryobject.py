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

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from pydantic import BaseModel


class MemoryObject(BaseModel, ABC):
    """
    Representation of an object in the short-term memory.
    """

    @abstractmethod
    def get(self, path: str) -> Any:
        """
        Get the value of a (direct or indirect) field in the object.

        Parameters
        ----------
        path: str
          Relative path from the current object to the target field.

        Returns:
        -------
        Any
          The value of the field. If the field is an object, another MemoryObject will be returned. If the field doesn't exist, returns None.
        """


    @abstractmethod
    def set(self, path: str, value: Any):
        """
        Set the value of a (direct or indirect) field in the object.
        This will also create the intermediate objects if not exist.

        Parameters
        ----------
        path: str
          Relative path from the current object to the target field.
        value: Any
          New value of the field. The type of the value must be either a primary type, or MemoryObject.
        """

    @abstractmethod
    def new_object(self, path: str) -> 'MemoryObject':
        """
        Create a new object as the value of a (direct or indirect) field in the object.

        Parameters
        ----------
        path: str
          Relative path from the current object to the target field.

        Returns:
        -------
        MemoryObject
          The created object.
        """

    @abstractmethod
    def is_exist(self, path: str) -> bool:
        """
        Check whether a (direct or indirect) field exist in the object.

        Parameters
        ----------
        path: str
          Relative path from the current object to the target field.

        Returns:
        -------
        bool
          Whether the field exists.
        """

    @abstractmethod
    def get_field_names(self) -> List[str]:
        """
        Get names of all the direct fields of the object.

        Returns:
        -------
        List[str]
          Direct field names of the object in a list.
        """

    @abstractmethod
    def get_fields(self) -> Dict[str, Any]:
        """
        Get all the direct fields of the object.

        Returns:
        -------
        Dict[str, Any]
          Direct fields in a dictionary.
        """

class LocalMemoryObject(MemoryObject):
    """
    LocalMemoryObject: Flattened hierarchical key-value state.

    Each object keeps a prefix to represent its path.
    """

    def __init__(self, store: dict, prefix: str = ""):
        """
        Initialize a memory object with the shared store and prefix.
        """
        # 具体数据，MemoryObject
        self._store = store  # shared flattened map
        self._prefix = prefix  # current object prefix path

    def get(self, path: str) -> Any:
        """
        Get a value by relative path.
        If the field is an object, return a new LocalMemoryObject.
        """
        abs_path = self._full_path(path) #
        if abs_path in self._store:
            value = self._store[abs_path]
            if self._is_object_marker(value):
                return LocalMemoryObject(self._store, abs_path)
            return value
        return None

    def set(self, path: str, value: Any):
        abs_path = self._full_path(path)
        parts = abs_path.split(".")

        self._fill_parents(parts)

        parent = ".".join(parts[:-1])
        # always add child even for root
        if parent in self._store:
            self._store[parent]["__OBJ__"].add(parts[-1])
        else:
            self._store[parent] = {"__OBJ__": {parts[-1]}}

        if abs_path in self._store and self._is_object_marker(self._store[abs_path]):
            raise ValueError(f"Cannot overwrite object field '{abs_path}' with primitive.")

        if isinstance(value, LocalMemoryObject):
            raise ValueError("Do not directly set a MemoryObject. Use new_object().")

        self._store[abs_path] = value

    def new_object(self, path: str, overwrite: bool = False) -> 'LocalMemoryObject':
        abs_path = self._full_path(path)
        parts = abs_path.split(".")

        self._fill_parents(parts)

        # Now create or overwrite final marker
        if abs_path in self._store:
            if not self._is_object_marker(self._store[abs_path]):
                if not overwrite:
                    raise ValueError(f"Field '{abs_path}' exists but is not object.")
                else:
                    self._store[abs_path] = {"__OBJ__": set()}
        else:
            self._store[abs_path] = {"__OBJ__": set()}

        # Ensure parent knows about this child
        if len(parts) >= 1:
            parent = ".".join(parts[:-1]) if len(parts) > 1 else ""
            if parent not in self._store:
                self._store[parent] = {"__OBJ__": set()}
            self._store[parent]["__OBJ__"].add(parts[-1])

        return LocalMemoryObject(self._store, abs_path)

    def is_exist(self, path: str) -> bool:
        """
        Check if a path exists.
        """
        return self._full_path(path) in self._store

    def get_field_names(self) -> List[str]:
        """
        Get all direct field names of the current object.
        """
        marker = self._store.get(self._prefix)
        if self._is_object_marker(marker):
            return sorted(marker["__OBJ__"])
        return []

    def get_fields(self) -> Dict[str, Any]:
        """
        Get all direct fields and their values.
        If a field is an object, return a placeholder with its children.
        """
        result = {}
        for name in self.get_field_names():
            abs_path = self._full_path(name)
            value = self._store.get(abs_path)
            if self._is_object_marker(value):
                children = sorted(value["__OBJ__"])
                result[name] = f"__OBJ__:{children}"
            else:
                result[name] = value
        return result

    def _fill_parents(self, parts: List[str]):
        """
        Ensure all intermediate parent objects exist and register their children.
        """
        for i in range(1, len(parts)):
            partial = ".".join(parts[:i])
            parent = ".".join(parts[:i - 1]) if i > 1 else ""

            if partial not in self._store:
                self._store[partial] = {"__OBJ__": set()}

            # ALWAYS ensure parent has child
            if parent not in self._store:
                self._store[parent] = {"__OBJ__": set()}
            self._store[parent]["__OBJ__"].add(parts[i - 1])

    def _full_path(self, path: str) -> str:
        """
        Build absolute path from prefix + relative path.
        """
        return f"{self._prefix}.{path}".strip(".") if path else self._prefix

    def _is_object_marker(self, value: Any) -> bool:
        """
        Check if a value is an object marker.
        """
        return isinstance(value, dict) and "__OBJ__" in value


if __name__ == '__main__':
    memoryobject = LocalMemoryObject({})
    memoryobject.set("a.b.c", 1)
    print(memoryobject.get("a.b").model_dump_json())
    o1 = memoryobject.get("a.b")
    o1.get('c')

