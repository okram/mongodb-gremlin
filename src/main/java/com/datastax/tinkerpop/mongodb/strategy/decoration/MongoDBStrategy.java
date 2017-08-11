package com.datastax.tinkerpop.mongodb.strategy.decoration;

import com.datastax.tinkerpop.mongodb.step.MongoDBStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
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
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
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
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.getParent() instanceof EmptyStep))
            return;

        // get the JSON CRUD document (json) and realize the CRUD operation (type)
        final GraphTraversal.Admin graphTraversal = (GraphTraversal.Admin) traversal;
        final String type = (((InjectStep) graphTraversal.getStartStep()).getInjections()[0]).toString();
        final String json = (((InjectStep) graphTraversal.getStartStep()).getInjections()[1]).toString();
        final JSONObject query;
        try {
            query = (JSONObject) new JSONParser().parse(json);
        } catch (final ParseException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        // don't need the inject()-step anymore. at this point we have an empty traversal that will get built up accordingly
        traversal.removeStep(0);

        //////////////////////////////////////////////////////////////////////////////////////////////////////

        if (type.equals("insert")) {
            graphTraversal.addStep(new AddVertexStartStep(graphTraversal, (String) query.getOrDefault(T.label.getAccessor(), Vertex.DEFAULT_LABEL)));
            graphTraversal.as("a");
            processMap(query, graphTraversal);
            System.out.println(graphTraversal);
        } else if (type.equals("query")) {
            // create the query document
            graphTraversal.addStep(new GraphStep<Vertex, Vertex>(graphTraversal, Vertex.class, true));
            for (final Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) query.entrySet()) {
                if (entry.getValue() instanceof Map) { // { age : { gt : 30 }} -> has('age',gt(30)
                    final Map map = (Map) entry.getValue();
                    final String predicate = map.keySet().iterator().next().toString();
                    final Object value = map.values().iterator().next();
                    graphTraversal.has(entry.getKey(), generateGremlinPredicate(predicate, value));
                } else
                    graphTraversal.has(entry.getKey(), P.eq(entry.getValue()));
            }
            // create the result document
            final GraphTraversal resultTraversal =
                    __.<Vertex>until(out().count().is(0)).
                            repeat(outE().inV()).
                            path().
                            by(valueMap(true)).
                            by(label());
            resultTraversal.asAdmin().addStep(new MongoDBStep(graphTraversal));
            graphTraversal.map(resultTraversal);
        } else {
            throw new IllegalStateException("Unknown MongoDB CRUD document type: " + type);
        }
    }

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

    private static GraphTraversal processMap(final Map map, final GraphTraversal graphTraversal) {
        for (final Map.Entry<String, Object> entry : (Set<Map.Entry<String, Object>>) map.entrySet()) {
            final String label = entry.getKey();
            if (label.equals(T.label.getAccessor()) || label.equals(T.id.getAccessor()))
                continue;
            final Object value = entry.getValue();
            if (value instanceof List && !((List) value).isEmpty()) {
                if ('p' == validateList((List) value)) {
                    for (final Object object : (List) value) {
                        graphTraversal.property(VertexProperty.Cardinality.list, label, object);
                    }
                } else {
                    graphTraversal.sideEffect(processMap((Map) value, addV().sideEffect(addE(label).from("a")).as("a")));
                }
            } else if (value instanceof Map) {
                graphTraversal.sideEffect(processMap((Map) value, addV().sideEffect(addE(label).from("a")).as("a")));
            } else {
                graphTraversal.property(label, value);
            }
        }
        return graphTraversal;
    }

    private static char validateList(final List list) {
        if (list.isEmpty())
            return 'x';
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
        return state;
    }

    public static MongoDBStrategy instance() {
        return INSTANCE;
    }
}
