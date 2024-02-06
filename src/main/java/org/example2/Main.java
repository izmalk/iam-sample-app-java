package org.example2;

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.concept.Concept;
import com.vaticle.typedb.driver.api.concept.type.AttributeType;
import com.vaticle.typedb.driver.api.concept.value.Value;

public class Main {
    public static void main(String[] args) {
        TypeDBDriver driver = TypeDB.coreDriver("0.0.0.0:1729");
        try (TypeDBSession session = driver.session("sample_db", TypeDBSession.Type.SCHEMA)) {
            try (TypeDBTransaction writeTransaction = session.transaction(TypeDBTransaction.Type.WRITE)) {
                AttributeType tag = writeTransaction.concepts().putAttributeType("tag", Value.Type.STRING).resolve();
                writeTransaction.concepts().getRootEntityType().getSubtypes(writeTransaction, Concept.Transitivity.EXPLICIT).forEach(result -> {
                    if (! result.isAbstract()) {
                        result.setOwns(writeTransaction,tag).resolve();
                    }
                });
                writeTransaction.commit();
            }
        }
        driver.close();
    }
}