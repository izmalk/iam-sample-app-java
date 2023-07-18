package org.example2;

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.TypeDBClient;

public class methods_testing {
    static int k = 0; // Counter

    public static void main(String[] args) {
        System.out.println("IAM database list");

        System.out.println("Connecting to the server");
        TypeDBClient client = TypeDB.coreClient("0.0.0.0:1729"); // client is connected to the server
        client.databases().all().forEach(db -> {
            k += 1;
            System.out.println(k + ": " + db.name());
        });
        client.close(); // closing server connection
    }
}
