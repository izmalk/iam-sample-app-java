package org.example2;

import com.vaticle.typedb.driver.api.*;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typeql.lang.TypeQL;
import static com.vaticle.typeql.lang.TypeQL.*;

//import com.vaticle.typeql.lang.common.TypeQLToken;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLInsert;

import java.security.KeyStore;
import java.text.SimpleDateFormat;
//import java.util.Collections;
import java.util.Date;
//import java.util.Set;

public class Main {
    static int k = 0; // Counter
    public static void main(String[] args) {
        System.out.println("IAM Sample App - Enterprise edition");

        System.out.println("Connecting to the server");
        TypeDBDriver driver = TypeDB.enterpriseDriver("127.0.0.1:1729", new TypeDBCredential("admin", "password", false )); // the driver is connected to the server
        System.out.println("Connecting to the `iam` database");
        try (TypeDBSession session = driver.session("iam", TypeDBSession.Type.DATA)) { // session is open
            // #todo Add DB manipulation request (check, create or re-create a DB)
            // #todo Add a define request

            System.out.println(" ");
            System.out.println("Request #1: User listing");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                k = 0; // reset the counter
                readTransaction.query().get( // Executing query
                        "match $u isa user, has full-name $n, has email $e; get $u, $n, $e;" // TypeQL query
                ).forEach(result -> { // Iterating through results
                    String name = result.get("n").asAttribute().getValue().asString();
                    String email = result.get("e").asAttribute().getValue().asString();
                    k += 1;
                    System.out.println("User #" + k + ": " + name + ", has E-mail: " + email);
                });
                System.out.println("Users found: " + k);
            }

            System.out.println(" ");
            System.out.println("Request #2: Files that Kevin Morrison has access to");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                // String getQuery = "match $u isa user, has full-name 'Kevin Morrison'; $p($u, $pa) isa permission; " +
                //        "$o isa object, has path $fp; $pa($o, $va) isa access; get $fp;"; // Example of the same TypeQL query
                TypeQLGet getQuery = TypeQL.match( // Java query builder to prepare TypeQL query string
                        cVar("u").isa("user").has("full-name", "Kevin Morrison"),
                        cVar("p").rel(cVar("u")).rel(cVar("pa")).isa("permission"),
                        cVar("o").isa("object").has("path", cVar("fp")),
                        cVar("pa").rel(cVar("o")).rel(cVar("va")).isa("access")
                ).get(cVar("fp"));
                k = 0; // reset the counter
                readTransaction.query().get(getQuery).forEach(result -> { // Executing query
                    k += 1;
                    System.out.println("File #" + k + ": " + result.get("fp").asAttribute().getValue().asString());
                });
                System.out.println("Files found: " + k);
            }

            System.out.println("");
            System.out.println("Request #3: Files that Kevin Morrison has view access to (with inference)");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ, new TypeDBOptions().infer(true))) { // READ transaction is open
                // String getQuery = "match $u isa user, has full-name 'Kevin Morrison';
                // $p($u, $pa) isa permission;
                // $o isa object, has path $fp;
                // $pa($o, $va) isa access;
                // $va isa action, has name 'view_file';
                // get $fp; sort $fp asc; offset 0; limit 5;"
                TypeQLGet getQuery = TypeQL.match( // Java query builder to prepare TypeQL query string
                        cVar("u").isa("user").has("full-name", "Kevin Morrison"),
                        cVar("p").rel(cVar("u")).rel(cVar("pa")).isa("permission"),
                        cVar("o").isa("object").has("path", cVar("fp")),
                        cVar("pa").rel(cVar("o")).rel(cVar("va")).isa("access"),
                        cVar("va").isa("action").has("name", "view_file")
                ).get(cVar("fp")).sort(cVar("fp")).offset(0).limit(5);
                k = 0; // reset the counter
                readTransaction.query().get(getQuery).forEach(result -> { // Executing query
                    k += 1;
                    System.out.println("File #" + k + ": " + result.get("fp").asAttribute().getValue().asString());
                });

                getQuery = TypeQL.match( // Java query builder to prepare TypeQL query string
                        cVar("u").isa("user").has("full-name", "Kevin Morrison"),
                        cVar("p").rel(cVar("u")).rel(cVar("pa")).isa("permission"),
                        cVar("o").isa("object").has("path", cVar("fp")),
                        cVar("pa").rel(cVar("o")).rel(cVar("va")).isa("access"),
                        cVar("va").isa("action").has("name", "view_file")
                ).get(cVar("fp")).sort(cVar("fp")).offset(5).limit(5);
                readTransaction.query().get(getQuery).forEach(result -> { // Executing query
                    k += 1;
                    System.out.println("File #" + k + ": " + result.get("fp").asAttribute().getValue().asString());
                });
                System.out.println("Files found: " + k);
            }

            System.out.println("");
            System.out.println("Request #4: Add a new file and a view access to it");
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) { // WRITE transaction is open
                String filepath = "logs/" + new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS").format(new Date(System.currentTimeMillis())) + ".log";
                // "insert $f isa file, has path '" + filepath + "';"
                TypeQLInsert insertQuery = TypeQL.insert(cVar("f").isa("file").has("path", filepath)); // Java query builder to prepare TypeQL query string
                System.out.println("Inserting file: " + filepath);
                // TypeQLInsert insertQuery = TypeQL.insert(cVar("f").isa("file").has("path", "logs/2023-06-30T12:04:36.351+0100.log"));
                writeTransaction.query().insert(insertQuery); // Executing query
                // "match $f isa file, has path '" + filepath + "';
                // $vav isa action, has name 'view_file';
                // insert ($vav, $f) isa access;"
                insertQuery = TypeQL.match( // Java query builder to prepare TypeQL query string
                        cVar("f").isa("file").has("path", filepath),
                        cVar("vav").isa("action").has("name", "view_file")
                                )
                        .insert(cVar("pa").rel(cVar("vav")).rel(cVar("f")).isa("access"));
                System.out.println("Adding view access to the file");
                writeTransaction.query().insert(insertQuery); // Executing query
                writeTransaction.commit(); // to persist changes, a 'write' transaction must be committed
            }

            System.out.println(" ");
            System.out.println("Request #5: Computation");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                String computationQuery = "match $f isa file, has size-kb $sk; ?sm = $sk / 1024; get ?sm;";
                k = 0; // reset the counter
                readTransaction.query().get(computationQuery).forEach(result -> { // Executing query
                    k += 1;
                    System.out.println("File #" + k + ": " + " size in MB: " + result.get("sm").asValue().asDouble());
/*                    Set<TypeQLToken.Annotation> annotations = Collections.emptySet();
                    result.get("f").asThing().asRemote(readTransaction).getHas(

                            annotations
                    ).forEach(attr -> System.out.println("---" + attr.getValue()));*/
                });
                System.out.println("Files found: " + k);
            }
        }
        driver.close(); // closing server connection
    }
}
