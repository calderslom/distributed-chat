package io.github.cpsc559.team16.addressingserver;


/* My idea with this class is that it will only be instantiated when the addressing server has failed, and will only exist
* until the election cycle has completed. Once the election is complete and a new Primary has been elected, the object will be destroyed.
*
* We can definitely do something else entirely, but I think it's good to destroy all information associated with a previous election.
*
*/


import io.github.cpsc559.team16.common.dto.ServerRole;
import io.github.cpsc559.team16.common.dto.AddrServerRecord;

import java.io.IOException;

public final class ElectionHelper {

    /**
     * Handles full promotion steps for a replica becoming the new Primary.
     * @param server the AddressingServer instance being promoted.
     */
    public static void promoteSelf(AddressingServer server) {
        System.out.println("This addressing server has been promoted from REPLICA to PRIMARY...");
        // Shut down the ping manager (only REPLICA's ping their peers)
        server.getPingManager().shutdown();
        // Set internal PID generator to ensure no active processes have their PID re-assigned.
        server.setPidCounterToNetworkMax();
        // Update config to reflect new PRIMARY status
        AddrServerConfig config = server.getConfig();
        config.setRole(ServerRole.PRIMARY);
        // Update the internal registry to reflect the new leadership status
        AddrServerRecord myRecord = server.getAddrServerRegistry().getRecords().get(config.getPID());
        if (myRecord != null) {
            myRecord.setRole(ServerRole.PRIMARY);
        }

        // Update connection details for the primary addressing server.
        server.getConfig().setPrimaryReplicaPort(server.getConfig().getReplicaPort());
        server.getConfig().setPrimaryHostAddress(server.getConfig().getHostAddress());
        // Since we don't have a DNS, the new Primary writes its info to the shared volume NOW.
        try {
            PrimaryDiscoveryManager discovery = server.getDiscoveryManager();
            discovery.publish();
        } catch (IllegalStateException ise) {
            System.err.println("REPLICA attempted to publish host details before promotion: " + ise.getMessage());
        } catch(IOException ioe) {
            System.err.println("New PRIMARY could not publish discovery file: " + ioe.getMessage());
        }

        System.out.println("Promotion Complete. Now serving as PRIMARY.");
    }


}
