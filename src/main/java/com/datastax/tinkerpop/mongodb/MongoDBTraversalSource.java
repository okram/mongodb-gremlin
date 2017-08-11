package com.datastax.tinkerpop.mongodb;

import com.datastax.tinkerpop.mongodb.strategy.decoration.MongoDBStrategy;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.json.simple.JSONObject;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MongoDBTraversalSource implements TraversalSource {

    protected transient RemoteConnection connection;
    protected final Graph graph;
    protected TraversalStrategies strategies;
    protected Bytecode bytecode = new Bytecode();

    public MongoDBTraversalSource(final Graph graph, final TraversalStrategies strategies) {
        this.graph = graph;
        this.strategies = strategies.clone().addStrategies(MongoDBStrategy.instance());
    }

    public MongoDBTraversalSource(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
    }

    @Override
    public Optional<Class> getAnonymousTraversalClass() {
        return Optional.empty();
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public Graph getGraph() {
        return this.graph;
    }

    @Override
    public Bytecode getBytecode() {
        return this.bytecode;
    }

    @Override
    public MongoDBTraversalSource withStrategies(TraversalStrategy... traversalStrategies) {
        this.strategies.addStrategies(traversalStrategies);
        return this;
    }

    @Override
    public MongoDBTraversalSource withRemote(final RemoteConnection remoteConnection) {
        this.connection = remoteConnection;
        return this;
    }

    public <S> Traversal<S, JSONObject> find(final String queryDocument) {
        final MongoDBTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, "query", queryDocument);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(new GraphTraversalSource(clone.getGraph(), clone.getStrategies()));
        return (GraphTraversal) traversal.addStep(new InjectStep(traversal, "query", queryDocument));
    }

    public <S> Traversal<S, JSONObject> insertOne(final String insertDocument) {
        final MongoDBTraversalSource clone = this.clone();
        clone.bytecode.addStep(GraphTraversal.Symbols.inject, "insert", insertDocument);
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(new GraphTraversalSource(clone.getGraph(), clone.getStrategies()));
        return (GraphTraversal) traversal.addStep(new InjectStep(traversal, "insert", insertDocument));
    }


    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public MongoDBTraversalSource clone() {
        try {
            final MongoDBTraversalSource clone = (MongoDBTraversalSource) super.clone();
            clone.strategies = this.strategies.clone();
            clone.bytecode = this.bytecode.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public void close() throws Exception {
        if (this.connection != null) this.connection.close();
    }

    @Override
    public String toString() {
        return StringFactory.traversalSourceString(this);
    }
}
