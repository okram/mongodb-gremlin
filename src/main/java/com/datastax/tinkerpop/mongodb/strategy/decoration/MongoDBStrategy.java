package com.datastax.tinkerpop.mongodb.strategy.decoration;

import com.datastax.tinkerpop.mongodb.step.map.MongoDBInsertStep;
import com.datastax.tinkerpop.mongodb.step.map.MongoDBQueryStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MongoDBStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private static final MongoDBStrategy INSTANCE = new MongoDBStrategy();

    private MongoDBStrategy() {
        // no construction
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> admin) {
        if (!(admin.getParent() instanceof EmptyStep))
            return;

        // get the JSON CRUD document (json) and realize the CRUD operation (type)
        final GraphTraversal.Admin traversal = (GraphTraversal.Admin) admin;
        final String type = (((InjectStep) traversal.getStartStep()).getInjections()[0]).toString();
        final String json = (((InjectStep) traversal.getStartStep()).getInjections()[1]).toString();
        final JSONObject query;
        try {
            query = (JSONObject) new JSONParser().parse(json);
        } catch (final ParseException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        // don't need the inject()-step anymore. at this point we have an empty traversal that will get built up accordingly
        admin.removeStep(0);

        //////////////////////////////////////////////////////////////////////////////////////////////////////

        // PROCESS INSERT JSON OBJECT //
        if (type.equals("insert")) {
            traversal.addStep(new AddVertexStartStep(traversal, null)).as("a");
            insertMap(query, traversal);
            traversal.select("a").id().asAdmin().addStep(new MongoDBInsertStep(traversal));
        }
        // PROCESS QUERY USING JSON OBJECT //
        else if (type.equals("query")) {
            // create the query document
            traversal.addStep(new GraphStep<Vertex, Vertex>(traversal, Vertex.class, true));
            for (final Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) query.entrySet()) {
                if (entry.getValue() instanceof Map) { // { age : { gt : 30 }} -> has('age',gt(30)
                    final Map map = (Map) entry.getValue();
                    final String predicate = map.keySet().iterator().next().toString();
                    final Object value = map.values().iterator().next();
                    traversal.has(entry.getKey(), generateGremlinPredicate(predicate, value));
                } else
                    traversal.has(entry.getKey(), P.eq(entry.getValue()));
            }
            // create the result document
            final GraphTraversal resultTraversal =
                    __.<Vertex>until(out().count().is(0)).
                            repeat(outE().inV()).
                            path().
                            by(valueMap(true)).
                            by(label());
            resultTraversal.asAdmin().addStep(new MongoDBQueryStep(traversal));
            traversal.map(resultTraversal);
        } else {
            throw new IllegalStateException("Unknown MongoDB CRUD document type: " + type);
        }
    }

    // TRAVERSAL CONSTRUCTION HELPER METHODS

    private static P generateGremlinPredicate(final String mongoPredicate, final Object value) {
        switch (mongoPredicate) {
            case "$gt":
                return P.gt(value);
            case "$lt":
                return P.lt(value);
            case "$gte":
                return P.gte(value);
            case "$lte":
                return P.lte(value);
            case "$eq":
                return P.eq(value);
            case "$neq":
                return P.neq(value);
        }
        throw new IllegalArgumentException("Unknown MongoDB Predicate: " + mongoPredicate);
    }

    private static GraphTraversal insertMap(final Map map, final GraphTraversal traversal) {
        if (!(traversal.asAdmin().getStartStep() instanceof AddVertexStartStep)) {
            if (map.containsKey(T.id.getAccessor()))
                traversal.V().hasId(map.get(T.id.getAccessor()));
            else
                traversal.addV((String) map.getOrDefault(T.label.getAccessor(), Vertex.DEFAULT_LABEL));
        }
        for (final Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) map.entrySet()) {
            final String label = entry.getKey();
            if (label.equals(T.label.getAccessor()) || label.equals(T.id.getAccessor()))
                continue;
            final Object value = entry.getValue();
            if (value instanceof List && !((List) value).isEmpty())
                insertList((List) value, label, traversal);
            else if (value instanceof Map) {
                traversal.map(insertMap((Map) value, new DefaultGraphTraversal()));
                traversal.sideEffect(addE(label).from("a")).select("a");
            } else
                traversal.property(label, value);

        }
        return traversal;
    }

    private static void insertList(final List list, final String listKey, final GraphTraversal traversal) {
        if (list.get(0) instanceof List)
            throw new IllegalArgumentException("Lists can not contain nested lists");
        char state = list.get(0) instanceof Map ? 'o' : 'p';
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i) instanceof Map && 'p' == state)
                throw new IllegalArgumentException("Lists can only support all objects or all primitives");
            else if (list.get(i) instanceof List)
                throw new IllegalArgumentException("Lists can not contain nested lists");
            else if (!(list.get(i) instanceof Map) && 'o' == state)
                throw new IllegalArgumentException("Lists can only support all objects or all primitives");
        }
        for (final Object object : list) {
            if ('p' == state)
                traversal.property(VertexProperty.Cardinality.list, listKey, object);
            else {
                traversal.map(insertMap((Map) object, new DefaultGraphTraversal()));
                traversal.addE(listKey).from("a").select("a");
            }
        }
    }

    public static MongoDBStrategy instance() {
        return INSTANCE;
    }
}
