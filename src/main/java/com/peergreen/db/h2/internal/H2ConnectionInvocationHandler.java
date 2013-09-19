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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.Connection;

import com.peergreen.db.h2.H2Driver;

/**
 * Handler for the connection
 * @author Florent Benoit
 */
public class H2ConnectionInvocationHandler implements InvocationHandler {

    private final H2Driver h2Driver;
    private final int portNumber;
    private final Connection wrappedConnection;

    /**
     * Build an handler around the given H2 driver.
     * @param driver the PG driver used to be notified
     * @param portNumber the port number of the database
     * @param connection the connection to wrap
     */
    public H2ConnectionInvocationHandler(H2Driver driver, int portNumber, Connection connection) {
        this.h2Driver = driver;
        this.portNumber = portNumber;
        this.wrappedConnection = connection;

    }

    /**
     * When the close method is called, notify the driver that we've closed a connection.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("close".equals(method.getName())) {
            try {
                return method.invoke(wrappedConnection, args);
            } finally {
                // closing, notifying the H2 driver
                h2Driver.closing(portNumber);
            }
        }
        return method.invoke(wrappedConnection, args);
    }

}
