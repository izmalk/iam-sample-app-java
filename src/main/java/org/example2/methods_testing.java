package org.example2;

import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.TypeDBDriver;

public class methods_testing {
    static int k = 0; // Counter

    public static void main(String[] args) {
        System.out.println("IAM database list");

        System.out.println("Connecting to the server");
        TypeDBDriver driver = TypeDB.coreDriver("0.0.0.0:1729"); // driver is connected to the server
        driver.databases().all().forEach(db -> {
            k += 1;
            System.out.println(k + ": " + db.name());
        });
        driver.close(); // closing server connection
    }
}
