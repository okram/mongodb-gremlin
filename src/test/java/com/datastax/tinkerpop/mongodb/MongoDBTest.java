package com.datastax.tinkerpop.mongodb;

import com.datastax.tinkerpop.mongodb.strategy.decoration.MongoDBStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.json.simple.parser.JSONParser;
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

    @Test
    public void shouldParseJSON() throws Exception {
        final JSONParser parser = new JSONParser();
        final StringBuilder builder = new StringBuilder();
        builder.append("{\n");
        builder.append("\"_id\" : \"71223bf3-9dcc-4de1-b95a-13fdb8aba9e0\",\n");
        builder.append("\"name\" : \"Gremlin\",\n");
        builder.append("\"hobbies\" : [\"traversing\", \"reflecting\"],\n");
        builder.append("\"birthyear\" : 2009,\n");
        builder.append("\"alive\" : true,\n");
        builder.append(" \"languages\" : [\n");
        builder.append("{");
        builder.append("\"name\" : \"Gremlin-Java\",\n");
        builder.append("\"language\" : \"Java8\"\n");
        builder.append("},\n");
        builder.append("{\n");
        builder.append("\"name\" : \"Gremlin-Python\",\n");
        builder.append("\"language\" : \"Python\"\n");
        builder.append("},\n");
        builder.append("{\n");
        builder.append("\"name\" : \"Orge\",\n");
        builder.append("\n");
        builder.append("\"language\" : \"Clojure\" }\n");
        builder.append("]\n");
        builder.append("}\n");
        System.out.println(parser.parse(builder.toString()));
    }
}
