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

/**
 * User attributes.
 * A user is for a given database.
 * @author Florent Benoit
 */
public class User {

    private final String username;
    private final String password;
    private final String database;

    public User(String username, String password, String database) {
        this.username = username;
        this.password = password;
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
