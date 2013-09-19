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
 * Exception when H2 server is not correctly started/stopped, etc.
 * @author Florent Benoit
 */
public class H2ServerException extends Exception {

    private static final long serialVersionUID = 2790883731773234801L;

    public H2ServerException(String message, Throwable e) {
        super(message, e);
    }

    public H2ServerException(String message) {
        super(message);
    }

}
