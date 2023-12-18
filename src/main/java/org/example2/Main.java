package org.example2;

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBOptions;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.query.QueryManager;
import com.vaticle.typedb.driver.common.Promise;
import com.vaticle.typeql.lang.TypeQL;
import static com.vaticle.typeql.lang.TypeQL.*;
import com.vaticle.typeql.lang.query.TypeQLGet;
import com.vaticle.typeql.lang.query.TypeQLInsert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    static int k = 0; // Counter
    public static void main(String[] args) {
        final String dbName = "iam"; // Name of a database to connect to
        final String serverAddr = "127.0.0.1:1729"; // Address of a TypeDB Core server to connect to

        System.out.println("IAM Sample App");

        System.out.println("Attempting to connect to a TypeDB Core server: " + serverAddr);
        TypeDBDriver driver = TypeDB.coreDriver(serverAddr); // the driver is connected to the server
        if (driver.databases().contains(dbName)) {
            System.out.println("Found a pre-existing database! Re-creating with the default schema and data...");
            driver.databases().get(dbName).delete();
        }
        driver.databases().create(dbName);
        if (driver.databases().contains(dbName)) {
            System.out.println("Empty database created");
        }
        System.out.println("Opening a Schema session to define a schema.");
        try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.SCHEMA)) {
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
                String defineQuery = Files.readString(Paths.get("iam-schema.tql"));
                writeTransaction.query().define(defineQuery);
                writeTransaction.commit();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Opening a Data session to insert data.");
        try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.DATA)) {
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
                String insertQuery = Files.readString(Paths.get("iam-data-single-query.tql"));
                writeTransaction.query().insert(insertQuery);
                writeTransaction.commit();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Testing the new database.");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // Re-using a session to open a new transaction
                long result = readTransaction.query().getAggregate("match $u isa user; get $u; count;").resolve().get().asLong();
                if (result == 3) {
                    System.out.println("Database setup complete. Test passed.");
                } else {
                    System.out.println("Test failed with the following result:" + result + " expected result: 3.");
                    System.exit(0);
                }
            }
        }

        System.out.println("Commencing sample requests");
        try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.DATA)) { // session is open

            System.out.println("");
            System.out.println("Request #1: User listing");
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ)) { // READ transaction is open
                k = 0; // reset the counter
                readTransaction.query().get( // Executing query
                        "match $u isa user, has full-name $n, has email $e; get;" // TypeQL query
                ).forEach(result -> { // Iterating through results
                    String name = result.get("n").asAttribute().getValue().asString();
                    String email = result.get("e").asAttribute().getValue().asString();
                    k += 1;
                    System.out.println("User #" + k + ": " + name + ", has E-mail: " + email);
                });
                System.out.println("Users found: " + k);
            }

            System.out.println("");
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
            TypeDBOptions options = new TypeDBOptions().infer(true);
            try (TypeDBTransaction readTransaction = session.transaction(TypeDBTransaction.Type.READ, options)) { // READ transaction is open
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
                writeTransaction.query().insert(insertQuery); // Executing query
                // "match $f isa file, has path '" + filepath + "';
                // $vav isa action, has name 'view_file';
                // insert ($vav, $f) isa access;"
                insertQuery = TypeQL.match( // Java query builder to prepare TypeQL query string
                        cVar("f").isa("file").has("path", filepath),
                        cVar("vav").isa("action").has("name", "view_file")
                ).insert(
                    cVar("pa").rel(cVar("vav")).rel(cVar("f")).isa("access")
                );
                System.out.println("Adding view access to the file");
                writeTransaction.query().insert(insertQuery); // Executing query
                writeTransaction.commit(); // to persist changes, a 'write' transaction must be committed
            }
        }
        driver.close(); // closing server connection
    }
}