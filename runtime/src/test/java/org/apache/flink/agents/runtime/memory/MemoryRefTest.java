/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.memory;

import org.apache.flink.agents.api.context.MemoryObject;
import org.apache.flink.agents.api.context.MemoryRef;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.metrics.FlinkAgentsMetricGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for {@link MemoryRef} and its integration with {@link MemoryObject}. */
public class MemoryRefTest {

    private MemoryObjectImpl memory;

    /** Simple POJO for testing. */
    static class Task {
        String id;
        List<String> steps;

        Task(String id, List<String> steps) {
            this.id = id;
            this.steps = steps;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Task task = (Task) o;
            return Objects.equals(id, task.id) && Objects.equals(steps, task.steps);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, steps);
        }
    }

    /** Mock RunnerContext for testing resolve(). */
    static class MockRunnerContext implements RunnerContext {
        private final MemoryObject memoryObject;

        MockRunnerContext(MemoryObject memoryObject) {
            this.memoryObject = memoryObject;
        }

        @Override
        public MemoryObject getShortTermMemory() {
            return memoryObject;
        }

        @Override
        public void sendEvent(org.apache.flink.agents.api.Event event) {
            // No-op for this test
        }

        @Override
        public FlinkAgentsMetricGroup getAgentMetricGroup() {
            return null;
        }

        @Override
        public FlinkAgentsMetricGroup getActionMetricGroup() {
            return null;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        ForTestMemoryMapState<MemoryObjectImpl.MemoryItem> mapState = new ForTestMemoryMapState<>();
        memory = new MemoryObjectImpl(mapState, MemoryObjectImpl.ROOT_KEY);
    }

    @Test
    public void testSetReturnsValidMemoryRef() throws Exception {
        // Action: Set a value and get a reference back
        MemoryRef ref =
                memory.set("my_task", new Task("task-001", Arrays.asList("step1", "step2")));

        // Assert: The returned reference is correctly formed
        assertNotNull(ref);
        assertEquals("my_task", ref.getPath());
        assertEquals(Task.class.getName(), ref.getTypeName());
    }

    @Test
    public void testGetWithMemoryRef() throws Exception {
        Task originalTask = new Task("task-002", Arrays.asList("init", "process", "finalize"));

        // 1. Store the object and get its reference
        MemoryRef taskRef = memory.set("system.tasks.current", originalTask);

        // 2. Use the reference to get the MemoryObject
        MemoryObject refMemoryObject = memory.get(taskRef);
        assertNotNull(refMemoryObject);

        // 3. Get the actual value from the MemoryObject
        Object retrievedValue = refMemoryObject.getValue();

        // Assert: The retrieved value is equal to the original object
        assertInstanceOf(Task.class, retrievedValue);
        assertEquals(originalTask, retrievedValue);
    }

    @Test
    public void testMemoryRefResolveMethod() throws Exception {
        Task originalTask = new Task("task-003", Arrays.asList("A", "B", "C"));
        MockRunnerContext ctx = new MockRunnerContext(memory);

        // Store object and create a reference
        MemoryRef taskRef = memory.set("user.data.task", originalTask);

        // Resolve the reference using the context
        MemoryObject resolvedMemoryObject = taskRef.resolve(ctx);
        Object resolvedValue = resolvedMemoryObject.getValue();

        // Assert: The resolved value is correct
        assertEquals(originalTask, resolvedValue);
    }

    @Test
    public void testGetWithRefToNestedObject() throws Exception {
        // Create a nested object structure
        memory.newObject("config.database", false);
        memory.set("config.database.host", "localhost");
        memory.set("config.database.port", 5432);

        // Create a reference to the nested object 'database'
        MemoryRef configRef = MemoryRef.create("config.database", MemoryObject.class.getName());

        // Use the reference to get the MemoryObject
        MemoryObject dbConfig = memory.get(configRef);

        // Assert: We got the correct nested object
        assertTrue(dbConfig.isNestedObject());

        List<String> expectedNames = Arrays.asList("host", "port");
        List<String> acturalNames = dbConfig.getFieldNames();

        Collections.sort(expectedNames);
        Collections.sort(acturalNames);

        assertEquals(expectedNames, acturalNames);
        assertEquals("localhost", dbConfig.get("host").getValue());
    }

    @Test
    public void testSimulatePassingRefBetweenActions() throws Exception {
        // --- "Action A" ---
        // Creates a "large" object, stores it, and gets a reference
        List<Double> largeData = Arrays.asList(Math.random(), Math.PI, Math.E);
        MemoryRef dataRef = memory.set("shared.large_data", largeData);

        // Action A would now pass `dataRef` in an Event to Action B.

        // --- "Action B" ---
        // Receives the reference and resolves it to get the data
        Object retrievedData = memory.get(dataRef).getValue();

        // Assert: The data is correctly retrieved in "Action B"
        assertEquals(largeData, retrievedData);
    }

    @Test
    public void testGetWithNonExistentRef() throws Exception {
        // Create a reference to a path that does not exist
        MemoryRef nonExistentRef = MemoryRef.create("this.path.does.not.exist", "java.lang.String");

        // Assert: Getting a non-existent path via a ref returns null
        assertNull(memory.get(nonExistentRef));
    }
}
