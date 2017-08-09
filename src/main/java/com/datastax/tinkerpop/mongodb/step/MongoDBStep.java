package com.datastax.tinkerpop.mongodb.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;
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
            for (final Map.Entry entry : (Set<Map.Entry>) b.entrySet()) {
                if (entry.getValue() instanceof JSONObject) {
                    if (a.containsKey(entry.getKey())) {
                        final Object object = a.get(entry.getKey());
                        if (object instanceof JSONArray) {
                            final JSONObject other = findById((JSONArray) object, ((JSONObject) entry.getValue()));
                            if (null == other)
                                ((JSONArray) object).add(format((JSONObject) entry.getValue()));
                            else
                                apply(other, (JSONObject) entry.getValue());
                        } else {
                            final JSONArray array = new JSONArray();
                            array.add(object);
                            array.add(format((JSONObject) entry.getValue()));
                            a.put(entry.getKey(), array);
                        }
                    } else
                        a.put(entry.getKey(), format((JSONObject) entry.getValue()));

                } else if (entry.getValue() instanceof List)
                    a.put(entry.getKey(),
                            ((List) entry.getValue()).size() == 1 ?
                                    ((List) entry.getValue()).get(0) : entry.getValue());
                else
                    a.put(entry.getKey(), entry.getValue());
            }
            return a;
        }

        private JSONObject format(final JSONObject object) {
            return apply(new JSONObject(), object);
        }

        private JSONObject findById(final JSONArray array, final JSONObject object) {
            if (object.containsKey(T.id)) {
                final Object id = object.get(T.id);
                for (final Object temp : array) {
                    if (temp instanceof JSONObject && id.equals(((JSONObject) temp).get(T.id)))
                        return (JSONObject) temp;
                }
            }
            return null;
        }

        public static final JSONMaker instance() {
            return INSTANCE;
        }
    }


}
