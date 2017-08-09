package com.datastax.tinkerpop.mongodb.strategy.decoration;

import com.datastax.tinkerpop.mongodb.step.MongoDBStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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
public class MongoDBStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

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
                    graphTraversal.has(entry.getKey(), P.eq(entry.getValue()));
                }
                // create the result document
                graphTraversal.
                        until(out().count().is(0)).
                        repeat(outE().inV()).
                        path().
                        by(valueMap(true)).
                        by(label());
                graphTraversal.addStep(new MongoDBStep(graphTraversal));
            } catch (final ParseException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }
        }
    }
}
