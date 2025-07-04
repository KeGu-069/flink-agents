package org.apache.flink.agents.api.context;

import java.util.List;
import java.util.Map;

// ObjectMemoryObject, RecursiveMemoryObject
public interface MemoryObject {
    /**
     * Get the value of a (direct or indirect) field in the object.
     *
     * @param path Relative path from the current object to the target field.
     * @return The value of the field. If the field is an object, another MemoryObject will be
     *     returned. If the field doesn't exist, returns null.
     */
    MemoryObject get(String path) throws Exception;

    /**
     * Set the value of a (direct or indirect) field in the object. This will also create the
     * intermediate objects if they don't exist.
     *
     * @param path Relative path from the current object to the target field.
     * @param value New value of the field.
     * @throws IllegalArgumentException if trying to overwrite an object with primitive type or set
     *     a MemoryObject directly
     */
    void set(String path, Object value) throws Exception;

    /**
     * Create a new object as the value of a (direct or indirect) field in the object.
     *
     * @param path Relative path from the current object to the target field.
     * @param overwrite Whether to overwrite existing field if it's not an object
     * @return The created object.
     * @throws IllegalArgumentException if field exists but is not an object and overwrite is false
     */
    MemoryObject newObject(String path, boolean overwrite) throws Exception;

    /**
     * Check whether a (direct or indirect) field exists in the object.
     *
     * @param path Relative path from the current object to the target field.
     * @return Whether the field exists.
     */
    boolean isExist(String path) throws Exception;

    /**
     * Get names of all the direct fields of the object.
     *
     * @return Direct field names of the object in a list.
     */
    List<String> getFieldNames() throws Exception;

    /**
     * Get all the direct fields of the object.
     *
     * @return Direct fields in a map.
     */
    Map<String, Object> getFields() throws Exception;

    /**
     * Get the primitive value stored at the current path.
     *
     * @return The primitive value if the current path is a value type, or null if this MemoryObject
     *     represents a nested object (prefix).
     */
    Object getValue() throws Exception;

    /**
     * Check whether the current object is a nested object (prefix).
     *
     * @return true if this MemoryObject is a prefix object (i.e., it has subfields), false if it
     *     holds a primitive value.
     */
    boolean isPrefix() throws Exception;
}
