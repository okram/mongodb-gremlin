package com.datastax.tinkerpop.mongodb;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.json.simple.JSONObject;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBTraversal<S, E> extends DefaultTraversal<S, E> {

    public MongoDBTraversal() {
        super();
    }

    public MongoDBTraversal(final GraphTraversalSource graphTraversalSource) {
        super(graphTraversalSource);
    }

    public MongoDBTraversal(final Graph graph) {
        super(graph);
    }

    @Override
    public MongoDBTraversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public MongoDBTraversal<S, E> iterate() {
        super.iterate();
        return this;
    }

    @Override
    public MongoDBTraversal<S, E> clone() {
        return (MongoDBTraversal<S, E>) super.clone();
    }

    /////

    public MongoDBTraversal<S, JSONObject> find(final String queryDocument) {
        this.getBytecode().addStep("find", queryDocument);
        return (MongoDBTraversal) this.asAdmin().addStep(new InjectStep<>(this, queryDocument));
    }
}
