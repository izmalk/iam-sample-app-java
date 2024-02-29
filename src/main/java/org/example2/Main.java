package org.example2;

import com.vaticle.typedb.driver.api.TypeDBDriver;
import com.vaticle.typedb.driver.api.TypeDBOptions;
import com.vaticle.typedb.driver.api.TypeDBSession;
import com.vaticle.typedb.driver.api.TypeDBTransaction;
import com.vaticle.typedb.driver.api.answer.ConceptMap;
import com.vaticle.typedb.driver.TypeDB;
import com.vaticle.typedb.driver.api.answer.JSON;
import com.vaticle.typedb.driver.common.exception.TypeDBDriverException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Main {

    private static final String DB_NAME = "sample_app_db";
    private static final String SERVER_ADDR = "127.0.0.1:1729";

    public static void main(String[] args) {
        try (TypeDBDriver driver = TypeDB.coreDriver(SERVER_ADDR)) {
            if (dbSetup(driver, DB_NAME, false)) {
                System.out.println("Setup complete.");
            } else {
                System.out.println("Setup failed.");
            }

            System.out.println("Request 1 of 6: Fetch all users as JSON objects with full names and emails");
            List<JSON> users = fetchAllUsers(driver);

            String name = "Jack Keeper";
            String email = "jk@vaticle.com";
            String secondRequestMessage = String.format("Request 2 of 6: Request 2 of 6: Add a new user with the full-name \"%s\" and email \"%s\"", name, email);
            System.out.println(secondRequestMessage);
            List<ConceptMap> newUser = insertNewUser(driver, name, email);
            // getFilesByUser, updateFilePath, deleteFile

            String nameKevin = "Kevin Morrison";
            String thirdRequestMessage = String.format("Request 3 of 6: Find all files that the user \"%s\" has access to view (no inference)", nameKevin);
            System.out.println(thirdRequestMessage);
            List<ConceptMap> no_files = getFilesByUser(driver, nameKevin, false);

            String fourthRequestMessage = String.format("Request 4 of 6: Find all files that the user \"%s\" has access to view (with inference)", nameKevin);
            System.out.println(fourthRequestMessage);
            List<ConceptMap> files = getFilesByUser(driver, nameKevin, true);

            String old_path = "lzfkn.java";
            String new_path = "lzfkn2.java";
            String fifthRequestMessage = String.format("Request 5 of 6: Update the path of a file from \"%s\" to \"%s\"", old_path, new_path);
            System.out.println(fifthRequestMessage);
            List<ConceptMap> updated_files = updateFilePath(driver, old_path, new_path);

            String sixthRequestMessage = String.format("Request 6 of 6: Delete the file with path \"%s\"", new_path);
            System.out.println(sixthRequestMessage);
            boolean deleted = deleteFile(driver, new_path);

        } catch (TypeDBDriverException e) {
            e.printStackTrace();
        }
    }


    private static List<JSON> fetchAllUsers(TypeDBDriver driver) {
        try (TypeDBSession session = driver.session(DB_NAME, TypeDBSession.Type.DATA)) {
            try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.READ)) {
                String query = "match $u isa user; fetch $u: full-name, email;";
                List<JSON> answers = tx.query().fetch(query).toList();
                answers.forEach(json -> System.out.println("JSON: " + json.toString()));
                return answers;
            }
        }
    }

    public static List<ConceptMap> insertNewUser(TypeDBDriver driver, String name, String email) {
        try (TypeDBSession session = driver.session(DB_NAME, TypeDBSession.Type.DATA)) {
            try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
                String query = String.format(
                        "insert $p isa person, has full-name $fn, has email $e; $fn \"%s\"; $e \"%s\";", name, email);
                List<ConceptMap> response = tx.query().insert(query).toList();
                tx.commit();
                for (ConceptMap conceptMap : response) {
                    String fullName = conceptMap.get("fn").asAttribute().getValue().asString();
                    String emailAddress = conceptMap.get("e").asAttribute().getValue().asString();
                    System.out.println("Added new user. Name: " + fullName + ", E-mail: " + emailAddress);
                }
                return response;
            }
        }
    }

    public static List<ConceptMap> getFilesByUser(TypeDBDriver driver, String name, boolean inference) {
        List<ConceptMap> filePaths = new ArrayList<>();
        TypeDBOptions options = new TypeDBOptions().infer(inference);
        try (TypeDBSession session = driver.session(DB_NAME, TypeDBSession.Type.DATA);
             TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.READ, options)) {

            String userQuery = String.format("match $u isa user, has full-name '%s'; get;", name);
            List<ConceptMap> users = tx.query().get(userQuery).toList();

            if (users.size() > 1) {
                System.out.println("Error: Found more than one user with that name.");
                return null;
            } else if (users.size() == 1) {
                String fileQuery = String.format("""
                                                match
                                                $fn '%s';
                                                $u isa user, has full-name $fn;
                                                $p($u, $pa) isa permission;
                                                $o isa object, has path $fp;
                                                $pa($o, $va) isa access;
                                                $va isa action, has name 'view_file';
                                                get $fp;""", name);
                // Note: Sorting in the query might not be directly supported; handle sorting in Java if needed
                tx.query().get(fileQuery).forEach(filePaths::add);
                filePaths.forEach(path -> System.out.println("File: " + path.get("fp").asAttribute().getValue().toString()));
                if (filePaths.isEmpty()) {
                    System.out.println("No files found. Try enabling inference.");
                }
                return filePaths;
            } else {
                System.out.println("Warning: No users found with that name.");
                return null;
            }
        } catch (TypeDBDriverException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static List<ConceptMap> updateFilePath(TypeDBDriver driver, String oldPath, String newPath) {
        List<ConceptMap> response = new ArrayList<>();
        try (TypeDBSession session = driver.session(DB_NAME, TypeDBSession.Type.DATA);
             TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
            String query = String.format("""
                                        match
                                        $f isa file, has path $old_path;
                                        $old_path = '%s';
                                        delete
                                        $f has $old_path;
                                        insert
                                        $f has path '%s';""", oldPath, newPath);
            response = tx.query().update(query).toList();
            tx.commit();
            System.out.println("Path updated from " + oldPath + " to " + newPath);
            return response;
        } catch (TypeDBDriverException e) {
            e.printStackTrace();
        }
        return response;
    }

    public static boolean deleteFile(TypeDBDriver driver, String path) {
        try (TypeDBSession session = driver.session(DB_NAME, TypeDBSession.Type.DATA);
             TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {

            String query = String.format("match $f isa file, has path '%s'; get;", path);
            List<ConceptMap> response = tx.query().get(query).toList();

            if (response.size() == 1) {
                tx.query().delete(String.format("match $f isa file, has path '%s'; delete $f isa file;", path)).resolve();
                tx.commit();
                System.out.println("The file has been deleted.");
                return true;
            } else if (response.size() > 1) {
                System.out.println("Matched more than one file with the same path. No files were deleted.");
                return false;
            } else {
                System.out.println("No files matched in the database. No files were deleted.");
                return false;
            }
        } catch (TypeDBDriverException e) {
            e.printStackTrace();
            return false;
        }
    }

    private static boolean dbSetup(TypeDBDriver driver, String dbName, boolean reset) {
        System.out.println("Setting up the database: " + dbName);
        boolean newDatabase = createNewDatabase(driver, dbName, reset);
        if (!driver.databases().contains(dbName)) {
            System.out.println("Database creation failed. Terminating...");
            return false;
        }
        if (newDatabase) {
            try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.SCHEMA)) {
                dbSchemaSetup(session);
            }
            try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.DATA)) {
                dbDatasetSetup(session);
            }
        }
        try (TypeDBSession session = driver.session(dbName, TypeDBSession.Type.DATA)) {
            return testInitialDatabase(session);
        }
    }

    private static boolean createNewDatabase(TypeDBDriver driver, String dbName, boolean dbReset) {
        if (driver.databases().contains(dbName)) {
            if (dbReset) {
                System.out.print("Replacing an existing database...");
                driver.databases().get(dbName).delete();
                driver.databases().create(dbName);
                System.out.println("OK");
                return true;
            } else { // dbReset = false
                System.out.println("Found a pre-existing database. Do you want to replace it? (Y/N) ");
                String answer;
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                    answer = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read schema file.", e);
                }
                // String answer = System.console().readLine();
                if (answer.equalsIgnoreCase("y")) {
                    return createNewDatabase(driver, dbName, true);
                } else {
                    System.out.println("Reusing an existing database.");
                    return false;
                }
            }
        } else { // No such database found on the server
            System.out.print("Creating a new database...");
            driver.databases().create(dbName);
            System.out.println("OK");
            return true;
        }
    }

    private static void dbSchemaSetup(TypeDBSession session) {
        String schemaFile = "iam-schema.tql";
        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
            String defineQuery = new String(Files.readAllBytes(Paths.get(schemaFile)));
            System.out.print("Defining schema...");
            tx.query().define(defineQuery).resolve();
            tx.commit();
            System.out.println("OK");
        } catch (IOException e) {
            throw new RuntimeException("Failed to read schema file.", e);
        }
    }

    private static void dbDatasetSetup(TypeDBSession session) {
        String dataFile = "iam-data-single-query.tql";
        try (TypeDBTransaction tx = session.transaction(TypeDBTransaction.Type.WRITE)) {
            String insertQuery = new String(Files.readAllBytes(Paths.get(dataFile)));
            System.out.print("Loading data...");
            tx.query().insert(insertQuery).toList();
            tx.commit();
            System.out.println("OK");
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data file.", e);
        }
    }

    private static boolean testInitialDatabase(TypeDBSession session) {
        try (TypeDBTransaction transaction = session.transaction(TypeDBTransaction.Type.READ)) {
            String testQuery = "match $u isa user; get $u; count;";
            System.out.print("Testing the database...");
            long result = transaction.query().getAggregate(testQuery).resolve().get().asLong();
            if (result == 3) {
                System.out.println("Passed");
                return true;
            } else {
                System.out.println("Failed with the result: " + result + "\n Expected result: 3.");
                return false;
            }
        }
    }
}
