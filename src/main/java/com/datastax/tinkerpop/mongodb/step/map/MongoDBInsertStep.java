package com.datastax.tinkerpop.mongodb.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.json.simple.JSONObject;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBInsertStep extends MapStep<Object, JSONObject> {

    public MongoDBInsertStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public JSONObject map(final Traverser.Admin<Object> traverser) {
        final JSONObject result = new JSONObject();
        result.put("acknowledged", true);
        result.put("insertId", traverser.get());
        return result;
    }
}
