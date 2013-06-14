/**
 * Copyright 2013 Peergreen S.A.S.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.peergreen.db.h2;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.BundleContext;
import org.osgi.service.jdbc.DataSourceFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.peergreen.db.h2.H2Driver;

/**
 * Test the Peergreen/H2 driver.
 * @author Florent Benoit
 */
public class TestDriver {

    @Mock
    private DataSourceFactory dataSourceFactory;

    @Mock
    private BundleContext bundleContext;

    private final int portNumber = 1503;

    private String jdbcURL;


    private Connection singleConnection;


    @BeforeClass
    public void setupDriver() throws SQLException, URISyntaxException {
        MockitoAnnotations.initMocks(this);

        this.jdbcURL = "jdbc:pg+h2:tcp://localhost:"  + portNumber + "/mydatabase";

        // setup bundle context
        URL url = TestDriver.class.getClassLoader().getResource(TestDriver.class.getName().replace(".", "/").concat(".class"));
        File f = new File(url.toURI().getPath());
        doReturn(f.getParentFile()).when(bundleContext).getDataFile("H2");

        // setup factory
        doReturn(org.h2.Driver.load()).when(dataSourceFactory).createDriver(null);

        // setup driver
        H2Driver h2Driver = new H2Driver(bundleContext);
        h2Driver.bindDatasourceFactory(dataSourceFactory);
        h2Driver.validate();
    }

    @Test(expectedExceptions=ConnectException.class)
    public void testCheckNoListen() throws UnknownHostException, IOException {
        checkNotRunning();
    }


    @Test(dependsOnMethods="testCheckNoListen")
    public void testGetConnection() throws SQLException {
        singleConnection = DriverManager.getConnection(jdbcURL);
        assertNotNull(singleConnection);
        Statement statement = singleConnection.createStatement();

        // first, drop table
        statement.execute("drop table myTestDatabase IF EXISTS");

        statement.execute("create table myTestDatabase(id int primary key, name varchar(100))");
        statement.execute("insert into myTestDatabase values(1, 'test1')");
        statement.execute("insert into myTestDatabase values(2, 'test2')");
        ResultSet rs;
        rs = statement.executeQuery("select * from myTestDatabase");

        assertTrue(rs.next());
        assertEquals(rs.getString("name"),"test1");
        assertTrue(rs.next());
        assertEquals(rs.getString("name"),"test2");
        assertFalse(rs.next());

        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
        statement.close();
        //not yet closing the connection to check if database is still running in another test

    }

    @Test(dependsOnMethods="testGetConnection")
    public void testCheckDatabaseIsRunning() throws UnknownHostException, IOException  {
        checkRunning();
    }

    @Test(dependsOnMethods="testCheckDatabaseIsRunning")
    public void testCloseConnection() throws SQLException {
        singleConnection.close();
    }

    @Test(dependsOnMethods="testCloseConnection", expectedExceptions=ConnectException.class)
    public void testCheckDatabaseIsNotRunningAfetConnectionClose() throws UnknownHostException, IOException  {
        checkNotRunning();
    }

    @Test(dependsOnMethods="testCheckDatabaseIsNotRunningAfetConnectionClose")
    public void testMultipleGetConnection() throws SQLException, UnknownHostException, IOException  {
        try {
            checkNotRunning();
            fail("Should not be running");
        } catch (IOException e) {
            // expected
        }
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            Connection connection  = DriverManager.getConnection(jdbcURL);
            assertNotNull(connection);
            connections.add(connection);
            Statement statement = connection.createStatement();
            if (i == 0) {
                statement.execute("DROP TABLE multipleConnections IF EXISTS");
                statement.execute("CREATE TABLE multipleConnections(id int primary key, name varchar(100))");
            }

            statement.execute("insert into multipleConnections values(" + i + ", 'test" + i + "')");
        }
        checkRunning();

        // now, close the connections
        for (int i = 0; i < connections.size() - 1; i++) {
            connections.get(i).close();
            checkRunning();
        }

        // now, close the last connection
        connections.get(connections.size() - 1).close();

        // it should'n be running again
        try {
            checkNotRunning();
            fail("Should not be running");
        } catch (IOException e) {
            // expected
        }

    }

    @Test(dependsOnMethods="testMultipleGetConnection")
    public void testConnectionWithUserPassword() throws SQLException, UnknownHostException, IOException  {
        try {
            checkNotRunning();
            fail("Should not be running");
        } catch (IOException e) {
            // expected
        }
        Connection connection  = DriverManager.getConnection(jdbcURL, "florent", "florentpassword");
        assertNotNull(connection);
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE tableWithUser IF EXISTS");
        statement.execute("CREATE TABLE tableWithUser(id int primary key, name varchar(100))");
        statement.execute("insert into tableWithUser values(1, 'test1')");

        statement.close();
        connection.close();
        try {
            checkNotRunning();
            fail("Should not be running");
        } catch (IOException e) {
            // expected
        }
    }



    protected void checkRunning() throws UnknownHostException, IOException {
        // check that the database is running
        Socket socket = new Socket("localhost", portNumber);
        socket.close();
    }

    @SuppressWarnings("resource")
    protected void checkNotRunning() throws UnknownHostException, IOException {
        // check that the database is not running (not listening)
        // expecting connection refused
        new Socket("localhost", portNumber);

    }

}
