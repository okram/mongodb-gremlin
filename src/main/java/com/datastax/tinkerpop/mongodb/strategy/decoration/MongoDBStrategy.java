package com.datastax.tinkerpop.mongodb.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    @Override
    public void apply(final Traversal.Admin<?, ?> admin) {
        final String json = (String) ((InjectStep<String>)admin.getStartStep()).getInjections()[0];
        System.out.println(json);
    }
}
