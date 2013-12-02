package com.dkhenry;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RqlConnection;
import com.rethinkdb.RqlCursor;
import com.rethinkdb.RqlDriverException;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

public class ConnectionTest {

    @Test
    public void testConnection() {
        boolean rvalue = false;

        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {

        }
        catch (RqlDriverException e) {
            rvalue = true;
        }

        AssertJUnit.assertFalse("Error connecting.", rvalue);
    }

    /* Test the functionality of the ten minute Introduction */
    @Test
    public void testDatabaseCreate() {
        boolean rvalue = false;

        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            RqlCursor cursor = r.run(r.db_create("superheroes"));
        } 
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }

        AssertJUnit.assertFalse("Error Creating Datrabase", rvalue);
    }

    @Test
    public void testDatabaseList() {
        boolean rvalue = false;

        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            r.run(r.db_list());
            r.close();
        } 
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }

        AssertJUnit.assertFalse("Error Listing Databases", rvalue);
    }

    @Test
    public void testDatabaseDrop() {
        boolean rvalue = false;

        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            r.run(r.db_drop("superheroes")).toString();
            r.close();
        }
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }

        AssertJUnit.assertFalse("Error Droping Databases", rvalue);
    }

    @Test
    public void testTableCreate() {
        boolean rvalue = false;

        try(RqlConnection r = RethinkDB.connect( "localhost", 28015)) {
            r.run(r.db_create("test12345"));
            r.run(r.db("test12345").table_create("dc_universe"));
            r.run(r.db_drop("test12345"));
            r.close();
        }
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }
        
        AssertJUnit.assertFalse("Error Creating Table", rvalue);
    }

    @Test
    public void testTableList() {
        boolean rvalue = false;

        try(RqlConnection r = RethinkDB.connect( "localhost", 28015)) {
            r.run(r.db_create("test12345"));
            r.run(r.db("test12345").table_list());
            r.run(r.db_drop("test12345"));
            r.close();
        }
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }
        
        AssertJUnit.assertFalse("Error Listing Tables", rvalue);
    }

    @Test
    public void testTableDrop() {
        boolean rvalue = false;

        try( RqlConnection r = RethinkDB.connect( "localhost", 28015)) {
            r.run(r.db_create("test12345"));
            r.run(r.db("test12345").table_create("dc_universe"));
            r.run(r.db("test12345").table_drop("dc_universe"));
            r.run(r.db_drop("test12345"));
            r.close();
        }
        catch (RqlDriverException e) {
            e.printStackTrace();
            rvalue = true;
        }
        
        AssertJUnit.assertFalse("Error Droping Table", rvalue);
    }
}
