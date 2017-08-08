package com.datastax.tinkerpop.mongodb.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MongoDBStep extends ReducingBarrierStep<Path, JSONObject> {

    public MongoDBStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(JSONObject::new);
        this.setReducingBiOperator(JSONMaker.instance());
    }

    @Override
    public JSONObject projectTraverser(final Traverser.Admin<Path> traverser) {
        final JSONObject root = new JSONObject();
        JSONObject currentNode = root;
        for (final Object object : traverser.get()) {
            if (object instanceof Map) {
                currentNode.putAll((Map) object);
            } else if (object instanceof String) {
                currentNode.put(object, currentNode = new JSONObject());
            }
        }
        return root;
    }

    public static class JSONMaker implements BinaryOperator<JSONObject> {

        private static final JSONMaker INSTANCE = new JSONMaker();

        private JSONMaker() {

        }

        @Override
        public JSONObject apply(final JSONObject a, final JSONObject b) {
            JSONObject currentA = a;
            JSONObject currentB = b;
            for (final Map.Entry entry : (Set<Map.Entry>) currentB.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    if (currentA.containsKey(entry.getKey())) {
                        final Object v = currentA.get(entry.getKey());
                        if (v instanceof JSONArray) {
                            ((JSONArray) v).add(entry.getValue());
                        } else {

                            final JSONArray array = new JSONArray();
                            array.add(v);
                            array.add(entry.getValue());
                            currentA.put(entry.getKey(), array);
                        }
                    } else {
                        currentA.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    currentA.put(entry.getKey(), entry.getValue());
                }
            }
            return a;
        }

        public static final JSONMaker instance() {
            return INSTANCE;
        }
    }


}
