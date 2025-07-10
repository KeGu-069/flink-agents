package org.apache.flink.agents.runtime.memory;

import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.api.common.state.MapState;

import java.util.*;

public class MemoryObjectImpl implements MemoryObject {

    //    enum ValueType {
    //        prefix(0),
    //        valueType(-1);
    //
    //        private Object value;
    //        private List<String> subKeys;
    //
    //        ValueType(Object value) {
    //            this.value = value;
    //        }
    //
    //        ValueType() {
    //            this.subKeys = new ArrayList<>();
    //    }
    public enum ValueType {
        PREFIX,
        VALUE
    }

    public static class ValueWrapper {
        private final ValueType type;
        private Object value; // only for VALUE
        private Set<String> subKeys; // only for PREFIX

        public ValueWrapper(ValueType type) {
            this.type = type;
            if (type == ValueType.PREFIX) {
                this.subKeys = new HashSet<>();
            }
        }

        public ValueType getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public Set<String> getSubKeys() {
            return subKeys;
        }
    }

    private final MapState<String, ValueWrapper> store;
    private final String prefix;

    public MemoryObjectImpl(MapState<String, ValueWrapper> store, String prefix) throws Exception {
        this.store = store;
        this.prefix = prefix;
        if (!store.contains("")) {
            store.put("", new ValueWrapper(ValueType.PREFIX));
        }
    }

    private String fullPath(String path) {
        return (prefix.isEmpty() ? path : prefix + "." + path);
    }

    private void fillParents(String[] parts) throws Exception {
        for (int i = 1; i < parts.length; i++) {
            String partial = String.join(".", Arrays.copyOfRange(parts, 0, i));
            String parent = i > 1 ? String.join(".", Arrays.copyOfRange(parts, 0, i - 1)) : "";

            if (!store.contains(partial)) {
                store.put(partial, new ValueWrapper(ValueType.PREFIX));
            }
            if (!store.contains(parent)) {
                store.put(parent, new ValueWrapper(ValueType.PREFIX));
            }
            ValueWrapper parentWrapper = store.get(parent);
            if (parentWrapper != null && parentWrapper.getType() == ValueType.PREFIX) {
                parentWrapper.getSubKeys().add(parts[i - 1]);
                store.put(parent, parentWrapper);
            }
        }
    }

    @Override
    public boolean isPrefix() throws Exception {
        ValueWrapper wrapper = store.get(prefix);
        return wrapper != null && wrapper.getType() == ValueType.PREFIX;
    }

    @Override
    public MemoryObject get(String path) throws Exception {
        //        store 查到a.b存在并且是前缀
        //        return new MemoryObjectImpl(store, "a.b");
        String absPath = fullPath(path);
        if (store.contains(absPath)) {
            return new MemoryObjectImpl(store, absPath);
        }
        return null;
    }

    @Override
    public Object getValue() throws Exception {
        ValueWrapper wrapper = store.get(prefix);
        if (wrapper != null && wrapper.getType() == ValueType.VALUE) {
            return wrapper.getValue();
        }
        return null;
    }

    @Override
    public void set(String path, Object value) throws Exception {
        String absPath = fullPath(path);
        String[] parts = absPath.split("\\.");
        fillParents(parts);

        String parent =
                parts.length > 1 ? String.join(".", Arrays.copyOf(parts, parts.length - 1)) : "";
        ValueWrapper parentWrapper = store.get(parent);
        parentWrapper.getSubKeys().add(parts[parts.length - 1]);

        ValueWrapper existing = store.get(absPath);
        if (existing != null && existing.getType() == ValueType.PREFIX) {
            throw new IllegalArgumentException("Cannot overwrite object with value: " + absPath);
        }

        ValueWrapper val = new ValueWrapper(ValueType.VALUE);
        val.setValue(value);
        store.put(absPath, val);
    }

    @Override
    public MemoryObject newObject(String path, boolean overwrite) throws Exception {
        String absPath = fullPath(path);
        String[] parts = absPath.split("\\.");

        fillParents(parts);

        if (store.contains(absPath)) {
            ValueWrapper existing = store.get(absPath);
            if (existing.getType() != ValueType.PREFIX) {
                if (!overwrite) {
                    throw new IllegalArgumentException(
                            "Field '" + absPath + "' exists but is not an object.");
                }
                store.put(absPath, new ValueWrapper(ValueType.PREFIX));
            }
        } else {
            store.put(absPath, new ValueWrapper(ValueType.PREFIX));
        }

        String parent =
                parts.length > 1 ? String.join(".", Arrays.copyOf(parts, parts.length - 1)) : "";
        ValueWrapper parentWrapper = store.get(parent);
        parentWrapper.getSubKeys().add(parts[parts.length - 1]);

        return new MemoryObjectImpl(store, absPath);
    }

    @Override
    public boolean isExist(String path) throws Exception {
        return store.contains(fullPath(path));
    }

    @Override
    public List<String> getFieldNames() throws Exception {
        ValueWrapper wrapper = store.get(prefix);
        if (wrapper != null && wrapper.getType() == ValueType.PREFIX) {
            return new ArrayList<>(wrapper.getSubKeys());
        }
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> getFields() throws Exception {
        Map<String, Object> result = new HashMap<>();
        for (String name : getFieldNames()) {
            String absPath = fullPath(name);
            ValueWrapper wrapper = store.get(absPath);
            if (wrapper.getType() == ValueType.PREFIX) {
                result.put(name, "__OBJ__");
            } else {
                result.put(name, wrapper.getValue());
            }
        }
        return result;
    }
}
