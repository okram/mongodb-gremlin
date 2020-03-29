package com.datastax.tinkerpop.mongodb;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MongoDBTest {

    private static JSONParser parser = new JSONParser();

    @Test
    public void shouldSupportFindDocuments() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal(GraphTraversalSource.class);
        final MongoDBTraversalSource db = graph.traversal(MongoDBTraversalSource.class);

        // test find(name,marko)
        assertEquals(
                parser.parse("{\"~label\":\"person\",\"created\":{\"~label\":\"software\",\"~id\":3,\"name\":\"lop\",\"lang\":\"java\"}," +
                        "\"~id\":1,\"name\":\"marko\",\"age\":29,\"knows\":[{\"~label\":\"person\",\"~id\":2,\"name\":\"vadas\",\"age\":27}," +
                        "{\"~label\":\"person\",\"created\":{\"~label\":\"software\",\"~id\":5,\"name\":\"ripple\",\"lang\":\"java\"}," +
                        "\"~id\":4,\"name\":\"josh\",\"age\":32},{\"~label\":\"person\",\"created\":{\"~label\":\"software\",\"~id\":3,\"name\":\"lop\",\"lang\":\"java\"}," +
                        "\"~id\":4,\"name\":\"josh\",\"age\":32}]}"),
                parser.parse(db.find("{ \"name\": \"marko\" }").next().toString()));

        compareQueryTraversalSegment(g.V().has("age", P.gt(30)), db.find("{\"age\" : {\"$gt\" : 30}}"));
        compareQueryTraversalSegment(g.V().has("name", "vadas").has("age", 27), db.find("{\"age\" : 27, \"name\":\"vadas\"}"));


    }

    @Test
    public void shouldSupportInsertDocuments() throws Exception {
        final Graph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal(GraphTraversalSource.class);
        final MongoDBTraversalSource db = graph.traversal(MongoDBTraversalSource.class);

        System.out.println(db.insertOne("{\"name\" : \"stephen\", \"~label\":\"person\", \"hobbies\":[\"art\",\"emails\",\"lame stuff\"], " +
                "\"created\" : {\"name\":\"Gremlin DSL\"}," +
                "\"likes\" : [{\"name\":\"Bob\", \"~label\":\"android\"},{\"name\":\"marko\",\"~id\":1}]}").toList());
        System.out.println(g.E().project("a", "b", "c").by(outV().values("name")).by(T.label).by(inV().values("name")).toList());
        System.out.println("##########");
        System.out.println(db.find("{\"name\" : \"stephen\"}").next());
    }

    private static void compareQueryTraversalSegment(Traversal<?, ?> gremlinTraversal, Traversal<?, ?> mongoTraversal) {
        gremlinTraversal.iterate();
        mongoTraversal.iterate();
        for (int i = 0; i < mongoTraversal.asAdmin().getSteps().size() - 1; i++) {
            Step mongoStep = mongoTraversal.asAdmin().getSteps().get(i);
            Step gremlinStep = gremlinTraversal.asAdmin().getSteps().get(i);
            assertEquals(gremlinStep,mongoStep);
        }
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
