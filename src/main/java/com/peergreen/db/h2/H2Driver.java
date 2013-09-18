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

import java.io.File;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.PostRegistration;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import com.peergreen.db.h2.internal.H2ConnectionInvocationHandler;
import com.peergreen.db.h2.internal.H2Server;
import com.peergreen.db.h2.internal.H2ServerException;

/**
 * Peergreen H2 driver that allows to create on-the fly the required database
 * @author Florent Benoit
 */
@Component(factoryMethod = "instance")
@Instantiate
@Provides // Keep this to have @PostRegistration working to get the actual BundleContext
public class H2Driver implements Driver {

    private static H2Driver INSTANCE = new H2Driver();

    static {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            // Ignored
        }
    }

    /**
     * iPOJO factory method to ensure that no other instances of that class is created.
     */
    public static H2Driver instance() {
        return INSTANCE;
    }

    private static final String PGH2_START_URL = "jdbc:pg+h2:";

    private static final String PGH2_START_TCP_URL_LOCALHOST = PGH2_START_URL.concat("tcp://localhost:");

    /**
     * Wrapped driver.
     */
    private Driver wrappedDriver;

    private final Map<Integer, H2Server> servers = new HashMap<>();

    // map : <port numner> <---> number of connections to this port number base
    private final Map<Integer, Integer> connectionsByPortNumber = new HashMap<>();

    private File rootDir;

    private final Lock lock = new ReentrantReadWriteLock().writeLock();

    public H2Driver() {
        wrappedDriver = new org.h2.Driver();
    }

    @PostRegistration
    public void registered(ServiceReference reference) {
        BundleContext bundleContext = reference.getBundle().getBundleContext();
        File tmpFile = bundleContext.getDataFile("H2").getParentFile();
        rootDir = new File(tmpFile, "H2_DATABASES");
    }

    /**
     * Register the driver in the driver manager
     */
    @Validate
    public void validate() throws SQLException {
        // It doesn't matter if we register this Driver instance multiple times
        DriverManager.registerDriver(this);
    }

    /**
     * Unregister the driver in the driver manager
     */
    @Invalidate
    public void invalidate() throws SQLException {
        // stop all database still running
        Collection<H2Server> servers = this.servers.values();
        for (H2Server server : servers) {
            server.stop();
        }
        DriverManager.deregisterDriver(this);
    }



    @Override
    public Connection connect(String url, Properties info) throws SQLException {

        if (info == null) {
            info = new Properties();
        }
        if (!acceptsURL(url)) {
            return null;
        }

        String databaseName = null;
        int portNumber = -1;
        if (url.startsWith(PGH2_START_TCP_URL_LOCALHOST)) {
            String rightPart = url.substring(PGH2_START_TCP_URL_LOCALHOST.length());
            String portValue = rightPart.substring(0, rightPart.indexOf("/"));
            portNumber = Integer.parseInt(portValue);
            int indexDatabase = url.indexOf(portValue.concat("/"));
            int end = url.indexOf(";");
            if (end == -1) {
                databaseName = url.substring(indexDatabase + portValue.concat("/").length());
            } else {
                databaseName  = url.substring(indexDatabase + portValue.concat("/").length(), end);
            }
        }

        // needs to map pg+h2 url to a h2 url
        String h2Url = "jdbc:h2:".concat(url.substring(PGH2_START_URL.length()));

        //needs to start a database if access is remote
        String name = url.substring(Constants.START_URL.length());
        if (name.startsWith("h2:tcp://localhost:")) {
            lock.lock();
            try {
                // Here is the example of rewritten URL
                // jdbc:h2:tcp://localhost:1234/mydatabase

                // needs to start the database if it is not yet started
                if (!servers.containsKey(portNumber)) {

                    // get database


                    // start server
                    H2Server h2server = new H2Server(rootDir, portNumber);

                    // do we have a user ?
                    String user = info.getProperty("user");
                    if (user != null) {
                        String password = info.getProperty("password");
                        h2server.addUser(user, password, databaseName);

                    }

                    servers.put(portNumber, h2server);
                    try {
                        h2server.start();
                    } catch (H2ServerException e) {
                        throw new SQLException("Unable to start the associated H2 server", e);
                    }
                }
                // else it is already started so lets the connection to be done

                // increment counter
                Integer count = connectionsByPortNumber.get(portNumber);
                if (count == null) {
                    // first connection, set it to 1
                    connectionsByPortNumber.put(portNumber, 1);
                } else {
                    connectionsByPortNumber.put(portNumber, count + 1);
                }

            } finally {
                lock.unlock();
            }
        }

        // return a wrapped connection
        Connection connection = new JdbcConnection(h2Url, info);
        Connection wrappedConnection = (Connection) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {Connection.class}, new H2ConnectionInvocationHandler(this, portNumber, connection));
        return wrappedConnection;

    }

    public void closing(int portNumber) {
        lock.lock();
        try {
            Integer val = connectionsByPortNumber.get(portNumber);
            // not managed
            if (val == null) {
                return;
            }

            // decrement
            val = val - 1;
            connectionsByPortNumber.put(portNumber, val);

            // zero ? needs to stop the database
            if (val == 0) {
                H2Server server = servers.get(portNumber);
                server.stop();
                servers.remove(portNumber);
                connectionsByPortNumber.remove(portNumber);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url != null) {
            if (url.startsWith(PGH2_START_URL)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return wrappedDriver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return wrappedDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return wrappedDriver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return wrappedDriver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // H2 Driver class does not implement that method
        // We have to implement it our-self to avoid NoSuchMethodError type of Exceptions
        throw new SQLFeatureNotSupportedException("H2 driver does not support this feature");
    }

}
