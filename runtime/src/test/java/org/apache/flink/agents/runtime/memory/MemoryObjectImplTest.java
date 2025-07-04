package org.apache.flink.agents.runtime.memory;

import org.apache.flink.agents.api.context.MemoryObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryObjectImplTest {

    private MemoryObjectImpl memoryObjectImpl;

    @BeforeEach
    public void setUp() {
        //         Initialize a fresh store (Map) before each test.
        //        memoryObjectImpl = new MemoryObjectImpl(new HashMap<>(), "");
    }

    @Test
    public void testSetAndGet() throws Exception {
        // Test setting and getting a value
        memoryObjectImpl.set("a", "value1");
        assertEquals("value1", memoryObjectImpl.get("a").getValue());

        memoryObjectImpl.set("b", 10);
        assertEquals(10, memoryObjectImpl.get("b").getValue());
    }

    @Test
    public void testNewObject() throws Exception {
        // Test creating new objects and retrieving them
        MemoryObject newObj = memoryObjectImpl.newObject("obj1", false);
        assertNotNull(newObj);
        assertTrue(newObj.isPrefix());
    }

    @Test
    public void testSetAndGetNestedObjects() throws Exception {
        // Test setting and getting nested objects
        memoryObjectImpl.set("a.b.c", "nestedValue");
        assertEquals("nestedValue", memoryObjectImpl.get("a.b.c").getValue());
    }

    @Test
    public void testGetFieldNames() throws Exception {
        // Test retrieving field names
        memoryObjectImpl.set("a", "value1");
        memoryObjectImpl.set("b", "value2");

        // Get all field names under current prefix
        assertTrue(memoryObjectImpl.getFieldNames().contains("a"));
        assertTrue(memoryObjectImpl.getFieldNames().contains("b"));
    }

    @Test
    public void testGetFields() throws Exception {
        // Test getting fields (both primitive and nested objects)
        memoryObjectImpl.set("a", "value1");
        memoryObjectImpl.set("b", "value2");

        Map<String, Object> fields = memoryObjectImpl.getFields();
        assertEquals("value1", fields.get("a"));
        assertEquals("value2", fields.get("b"));
    }

    @Test
    public void testOverwriteObject() throws Exception {
        // Test overwriting object behavior
        MemoryObject newObj = memoryObjectImpl.newObject("obj", false);
        newObj.set("x", "value");

        assertEquals("value", newObj.get("x").getValue());

        // Now try overwriting the object
        MemoryObject overwriteObj = memoryObjectImpl.newObject("obj", true);
        overwriteObj.set("y", "newValue");

        assertEquals("newValue", overwriteObj.get("y").getValue());
    }

    @Test
    public void testIsExist() throws Exception {
        // Test checking if fields exist
        memoryObjectImpl.set("a", "value1");
        assertTrue(memoryObjectImpl.isExist("a"));
        assertFalse(memoryObjectImpl.isExist("nonexistentField"));
    }
}
