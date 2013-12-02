package com.dkhenry;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.RqlConnection;
import com.rethinkdb.RqlCursor;
import com.rethinkdb.RqlDriverException;
import com.rethinkdb.impl.RqlObject;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IntegrationTest {

    @Test
    public void createAndListDb() throws RqlDriverException {
        SecureRandom random = new SecureRandom();
        String database = new BigInteger(130, random).toString(32);
        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            RqlCursor cursor = r.execute(r.db_create(database));
            RqlObject obj = cursor.next();
            assert Double.valueOf(1.0).equals(obj.getAs("created")) : "Database was not created successfully ";
            cursor = r.execute(r.db_list());
            obj = cursor.next();
            boolean found = false;
            for (Object o : obj.getList()) {
                if (database.equals(o)) {
                    found = true;
                    break;
                }
            }
            assert found == true : "Databse was not able to be listed";
            cursor = r.execute(r.db_drop(database));
            obj = cursor.next();
            assert Double.valueOf(1.0).equals(obj.getAs("dropped")) : "Database was not dropped successfully ";
        }
    }

    @Test
    public void createAndListTable() throws RqlDriverException {
        SecureRandom random = new SecureRandom();
        String database = new BigInteger(130, random).toString(32);
        String table = new BigInteger(130, random).toString(32);
        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            r.execute(r.db_create(database));
            RqlCursor cursor = r.execute(r.db(database).table_create(table));
            assert Double.valueOf(1.0).equals(cursor.next().getAs("created")) : "Table was not created successfully ";
            cursor = r.execute(r.db(database).table_list());
            boolean found = false;
            for (Object o : cursor.next().getList()) {
                if (table.equals(o)) {
                    found = true;
                    break;
                }
            }
            assert found == true : "Table was not able to be listed";
            cursor = r.execute(r.db(database).table_drop(table));
            assert Double.valueOf(1.0).equals(cursor.next().getAs("dropped")) : "Table was not dropped successfully ";
            r.execute(r.db_drop(database));
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes", "serial"})
    @Test
    public void insertAndRetrieveData() throws RqlDriverException {
        SecureRandom random = new SecureRandom();
        String database = new BigInteger(130, random).toString(32);
        String table = new BigInteger(130, random).toString(32);
        try (RqlConnection r = RethinkDB.connect("localhost", 28015)) {
            r.execute(r.db_create(database));
            r.execute(r.db(database).table_create(table));
            
            RqlCursor cursor = r.execute(r.db(database).table(table).insert(Arrays.asList(
                    new HashMap() {
                        {
                            put("name", "Worf");
                            put("show", "Star Trek TNG");
                        }
                    },
                    new HashMap() {
                        {
                            put("name", "Data");
                            put("show", "Star Trek TNG");
                        }
                    },
                    new HashMap() {
                        {
                            put("name", "William Adama");
                            put("show", "Battlestar Galactica");
                        }
                    },
                    new HashMap() {
                        {
                            put("name", "Homer Simpson");
                            put("show", "The Simpsons");
                        }
                    }
            )));
            assert Double.valueOf(4.0).equals(cursor.next().getAs("inserted")) : "Error inserting Data into Database";
            cursor = r.execute(r.db(database).table(table).filter(new HashMap() {
                {
                    put("show", "Star Trek TNG");
                }
            }));
            // We Expect Two results
            int count = 0;
            for (RqlObject o : cursor) {
                Map<String, Object> m = o.getMap();
                assert m.containsKey("name") : "Data that came back was malformed (missing \"name\")";
                assert m.containsKey("show") : "Data that came back was malformed (missing \"show\")";
                assert "Star Trek TNG".equals(m.get("show")) : "Data that came back was just plain wrong (\"show\" was not \"Star Trek TNG\");"
                        + count++;
            }
            r.execute(r.db(database).table_drop(table));
            r.execute(r.db_drop(database));
        }
    }
}
