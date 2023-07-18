package org.example2;

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.TypeDBClient;
import com.vaticle.typedb.client.api.TypeDBOptions;
import com.vaticle.typedb.client.api.TypeDBSession;
import com.vaticle.typedb.client.api.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLInsert;
import com.vaticle.typeql.lang.query.TypeQLMatch;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.vaticle.typeql.lang.TypeQL.cVar;

public class define_test {

    public static void main(String[] args) {
        System.out.println("IAM Sample App");

        System.out.println("Connecting to the server");
        TypeDBClient client = TypeDB.coreClient("0.0.0.0:1729"); // client is connected to the server
        System.out.println("Connecting to the `123` database");
        try (TypeDBSession session = client.session("123", TypeDBSession.Type.SCHEMA)) { // session is open

            System.out.println(" ");
            System.out.println("Define a new type");
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) { // WRITE transaction is open
                String defineQuery = "define new2 sub entity;";
                System.out.println("Sending query");
                writeTransaction.query().define(defineQuery); // Executing query
                System.out.println("Committing changes");
                writeTransaction.commit(); // to persist changes, a 'write' transaction must be committed
            }
        }
        client.close(); // closing server connection
    }
}

