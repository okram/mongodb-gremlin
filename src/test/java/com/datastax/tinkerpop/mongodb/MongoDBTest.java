package com.datastax.tinkerpop.mongodb;

import com.datastax.tinkerpop.mongodb.strategy.decoration.MongoDBStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBTest {

    private static JSONParser parser = new JSONParser();

    @Test
    public void shouldWork() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final MongoDBTraversalSource db = graph.traversal(MongoDBTraversalSource.class);
        assertEquals(
                parser.parse("{\"id\":1,\"created\":{\"id\":3,\"name\":\"lop\",\"lang\":\"java\",\"label\":\"software\"}," +
                        "\"name\":\"marko\",\"label\":\"person\",\"age\":29,\"knows\":[{\"id\":2,\"name\":\"vadas\",\"label\":\"person\",\"age\":27}," +
                        "{\"id\":4,\"created\":[{\"id\":5,\"name\":\"ripple\",\"lang\":\"java\",\"label\":\"software\"}," +
                        "{\"id\":3,\"name\":\"lop\",\"lang\":\"java\",\"label\":\"software\"}],\"name\":\"josh\",\"label\":\"person\",\"age\":32}]}"),
                parser.parse(db.find("{ \"name\": \"marko\" }").next().toString()));
        System.out.println(db.find("{\"age\" : {\"$gt\" : 30}}").toList());

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
