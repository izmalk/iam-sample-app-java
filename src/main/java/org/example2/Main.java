package org.example2;

import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBOptions;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typeql.lang.TypeQL;
import static com.vaticle.typeql.lang.TypeQL.*;
import com.vaticle.typeql.lang.query.TypeQLMatch;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typedb.client.api.answer.ConceptMap;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    static int k = 0; // Counter
    public static void main(String[] args) {
        System.out.println("IAM Sample App");

        System.out.println("Connecting to the server");
        TypeDBClient client = TypeDB.coreClient("0.0.0.0:1729"); // client is connected to the server
        System.out.println("Connecting to the `iam` database");
        try (TypeDBSession session = client.session("iam", TypeDBSession.Type.DATA)) { // session is open

            System.out.println("");
            System.out.println("Request #1: User listing");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                k = 0; // reset the counter
                readTransaction.query().match(
                        "match $u isa user, has full-name $n, has email $e;" // TypeQL query
                ).forEach(result -> {
                    String name = result.get("n").asAttribute().asString().getValue();
                    String email = result.get("e").asAttribute().asString().getValue();
                    k += 1;
                    System.out.println("User #" + k + ": " + name + ", has E-mail: " + email);
                });
                System.out.println("Users found: " + k);
            }

            System.out.println("");
            System.out.println("Request #2: Files that Kevin Morrison has access to");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                 List<String> answer = new ArrayList<>();
                // String getQuery = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " +
                //        "$o isa object, has path $fp; $pa($o, $va) isa access; get $fp;"; // Example of the same TypeQL query
                TypeQLMatch.Filtered getQuery = TypeQL.match(
                        var("u").isa("user").has("full-name", "Kevin Morrison"),
                        var("p").rel("u").rel("pa").isa("permission"),
                        var("o").isa("object").has("path", var("fp")),
                        var("pa").rel("o").rel("va").isa("access")
                ).get("fp");
                Stream<ConceptMap> answers = readTransaction.query().match(getQuery);
                //answers.forEach(answer -> System.out.println(answer.get("fp").asThing().asAttribute().getValue()));
                answers.forEach(result -> answer.add(result.get("fp").asAttribute().asString().getValue()));
                if (answer.isEmpty()) {
                    System.out.println("Response is empty.");
                } else {
                    k = 0; // reset the counter
                    for (String i : answer) {
                        k += 1;
                        System.out.println("File #" + k + ": " + i);
                    }
                    System.out.println("Files found: " + k);
                }
            }

            System.out.println("");
            System.out.println("Request #3: Files that Kevin Morrison has view access to (with inference)");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ, TypeDBOptions.core().infer(true))) { // READ transaction is open
                List<String> answer = new ArrayList<>();
                // String getQuery = "match $u isa user, has full-name 'Kevin Morrison';
                // $p($u, $pa) isa permission;
                // $o isa object, has path $fp;
                // $pa($o, $va) isa access;
                // $va isa action, has action-name 'view_file';
                // get $fp; sort $fp asc; offset 0; limit 5;"
                TypeQLMatch.Limited getQuery = TypeQL.match(
                        var("u").isa("user").has("full-name", "Kevin Morrison"),
                        var("p").rel("u").rel("pa").isa("permission"),
                        var("o").isa("object").has("path", var("fp")),
                        var("pa").rel("o").rel("va").isa("access"),
                        var("va").isa("action").has("action-name", "view_file")
                ).get("fp").sort("fp").offset(0).limit(5);
                Stream<ConceptMap> answers = readTransaction.query().match(getQuery);
                answers.forEach(result -> answer.add(result.get("fp").asAttribute().asString().getValue()));
                int k = 0; // create a counter
                if (answer.isEmpty()) {
                    System.out.println("Response is empty.");
                } else {
                    for (String i : answer) {
                        k += 1;
                        System.out.println("File #" + k + ": " + i);
                    }
                    //System.out.println("Files found (first batch): " + k);
                }

                List<String> answer2 = new ArrayList<>();
                TypeQLMatch.Limited getQuery2 = TypeQL.match(
                        var("u").isa("user").has("full-name", "Kevin Morrison"),
                        var("p").rel("u").rel("pa").isa("permission"),
                        var("o").isa("object").has("path", var("fp")),
                        var("pa").rel("o").rel("va").isa("access"),
                        var("va").isa("action").has("action-name", "view_file")
                ).get("fp").sort("fp").offset(5).limit(5);
                Stream<ConceptMap> answers2 = readTransaction.query().match(getQuery2);
                answers2.forEach(result -> answer2.add(result.get("fp").asAttribute().asString().getValue()));
                if (answer2.isEmpty()) {
                    System.out.println("Response is empty for the files #6-10.");
                } else {
                    for (String i : answer2) {
                        k += 1;
                        System.out.println("File #" + k + ": " + i);
                    }
                    System.out.println("Files found: " + k);
                }
            }

            System.out.println("");
            System.out.println("Request #4: Add a new file and a view access to it");
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) { // WRITE transaction is open
                String timestamp = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss").format(new Date(System.currentTimeMillis()));
                String filepath = "logs/" + timestamp + ".log";
                // "insert $f isa file, has path '" + filepath + "';"
                TypeQLInsert insertQuery = TypeQL.insert(var("f").isa("file").has("path", filepath));
                System.out.println("Inserting file: " + filepath);
                List<ConceptMap> insertedId = writeTransaction.query().insert(insertQuery).collect(Collectors.toList());
                //System.out.println("Inserted a file with iid: " + insertedId.get(0).get("f").asThing().getIID());

                // "match $f isa file, has path '" + filepath + "';
                // $vav isa action, has action-name 'view_file';
                // insert ($vav, $f) isa access;"
                TypeQLInsert matchInsertQuery = TypeQL.match(
                        var("f").isa("file").has("path", filepath),
                        var("vav").isa("action").has("action-name", "view_file")
                                )
                        .insert(var("pa").rel("vav").rel("f").isa("access"));
                System.out.println("Adding view access to the file");
                List<ConceptMap> matchInsertedId = writeTransaction.query().insert(matchInsertQuery).collect(Collectors.toList());
                //System.out.println("Inserted a relation with iid: " + matchInsertedId.get(0).get("pa").asThing().getIID());
                writeTransaction.commit(); // to persist changes, a write transaction must always be committed
            }
        }
        client.close(); // closing server connection
    }
}
