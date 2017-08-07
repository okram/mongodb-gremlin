package com.datastax.tinkerpop.mongodb;

import com.datastax.tinkerpop.mongodb.strategy.decoration.MongoDBStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBTest {

    @Test
    public void shouldWork() {
        final Graph graph = TinkerGraph.open();
        final MongoDBTraversalSource g = graph.traversal(MongoDBTraversalSource.class).withStrategies(new MongoDBStrategy());
        System.out.println(g.inject("{ name:'marko' }").next());

    }
}
