/**
 * Copyright 2013 Peergreen S.A.S. All rights reserved.
 * Proprietary and confidential.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.peergreen.db.h2.internal;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.h2.tools.Server;

/**
 * Handles the start and the stop of the H2 server.
 * @author Florent Benoit
 */
public class H2Server {

    /**
     * Base directory of the H2 server.
     */
    private final File baseDir;

    /**
     * Port number of this instance of H2 TCP server.
     */
    private final int portNumber;

    /**
     * Instance of the wrapped server.
     */
    private Server server;

    /**
     * List of Users to add on the instance of the database.
     */
    private final List<User> users;

    /**
     * Build a new instance of the H2 server.
     * @param baseDir the base directory
     * @param portNumber the port number
     */
    public H2Server(File baseDir, int portNumber) {
        this.baseDir = baseDir;
        this.portNumber = portNumber;
        this.users = new ArrayList<>();
    }

    /**
     * Starts the H2 server.
     * @throws H2ServerException if H2 server cannot be started
     * @throws SQLException if H2 server cannot be started
     */
    public void start() throws H2ServerException, SQLException {
        // Build list of arguments
        List<String> argList = new ArrayList<>();

        // Specify TCP port
        argList.add("-tcpPort");
        argList.add(String.valueOf(this.portNumber));

        // Specify base directory
        argList.add("-baseDir");
        argList.add(new File(baseDir.getPath(), "tcp-".concat(String.valueOf(portNumber))).getPath());

        // Convert args into array
        String[] args = argList.toArray(new String[argList.size()]);

        // Create server instance
        try {
            server = Server.createTcpServer(args);
        } catch (SQLException e) {
            throw new H2ServerException("Unable to init the server", e);
        }

        server.start();
        if (users.size() > 0) {
            for (User user : users) {
                insertUser(user.getUsername(), user.getPassword(), user.getDatabase());
            }
        }

    }

    /**
     * Insert the given user for the following database
     * @param user the user
     * @param password the password
     * @param database the database for which add the given user
     * @throws SQLException if the user cannot be added
     */
    protected void insertUser(String user, String password, String database) throws SQLException {
        String updatedPassword;
        if (password == null) {
            updatedPassword = "";
        } else {
            updatedPassword = password;
        }
        try (Connection connection = DriverManager.getConnection("jdbc:h2:tcp://localhost:" + portNumber + "/" + database, "", ""); Statement statement = connection.createStatement()) {
            statement.execute("DROP USER " + user + " IF EXISTS");
            statement.execute("Create USER " + user + " PASSWORD '" + updatedPassword + "' ADMIN");
        }
    }


    /**
     * Adds the given user for the following database. Adds can be later added
     * @param user the user
     * @param password the password
     * @param database the database for which add the given user
     * @throws SQLException if the user cannot be added
     */
    public void addUser(String user, String password, String database) throws SQLException {
        if (server != null) {
            insertUser(user, password, database);
        } else {
            users.add(new User(user, password, database));
        }
    }

    /**
     * Stop the instance of the server.
     */
    public void stop() {
        server.stop();
    }
}
