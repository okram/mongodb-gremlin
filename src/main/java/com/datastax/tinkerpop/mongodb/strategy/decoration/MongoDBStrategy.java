package com.datastax.tinkerpop.mongodb.strategy.decoration;

import com.datastax.tinkerpop.mongodb.step.MongoDBStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Map;
import java.util.Set;

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
        if (traversal.getParent() instanceof EmptyStep) {
            try {
                final GraphTraversal.Admin graphTraversal = (GraphTraversal.Admin) traversal;
                final String json = (((InjectStep) graphTraversal.getStartStep()).getInjections()[0]).toString();
                final JSONObject query = (JSONObject) new JSONParser().parse(json);
                traversal.removeStep(0);
                // create the query document
                traversal.addStep(new GraphStep<>(traversal, Vertex.class, true));
                for (final Map.Entry<String, Object> entry : (Set<Map.Entry>) query.entrySet()) {
                    if (entry.getValue() instanceof Map) {
                        final Map map = (Map) entry.getValue();
                        final String predicate = map.keySet().iterator().next().toString();
                        final Object value = map.values().iterator().next();
                        final P p;
                        if (predicate.equals("$gt"))
                            p = P.gt(value);
                        else if (predicate.equals("$lt"))
                            p = P.lt(value);
                        else
                            p = P.eq(value);
                        graphTraversal.has(entry.getKey(), p);
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
            } catch (final ParseException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }

    public static final MongoDBStrategy instance() {
        return INSTANCE;
    }
}
