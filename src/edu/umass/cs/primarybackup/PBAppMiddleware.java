package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.util.Set;

// TODO: create a Middleware between Paxos and the actual Stateful Application
//  so that the Stateful Application does not need to aware with Packets of the
//  Primary Backup Coordinator. Currently the Stateful Application interact
//  directly with Paxos, as can be seen in the diagram below, making the
//  Application needs to handle packet such as StartEpochPacket in the App's
//  execute method.
//   ┌──────────────┐
//   │   end user   │
//   └────┬───▲─────┘
//        │   │
//  ┌─────▼───┴──────────┐     ┌───────────────┐
//  │                    ├─────►               │
//  │ PrimaryBackupCoor  │     │   Paxos       ├─────►
//  │                    │     │               │▼────┘
//  │                    ◄─────┤               │
//  └────────────────────┘     └─▲───┬─────────┘
//                               │   │
//   ┌────────┐  ┌─────────┐  ┌──┴───▼─┐  ┌────────┐
//   │Stateful│  │ Stateful│  │Stateful│  │        │
//   │App 01  │  │ App 02  │  │App 03  │  │  ...   │
//   └────────┘  └─────────┘  └────────┘  └────────┘
//  .
//  The Middleware should be placed inside the Primary Backup Coordinator
//  so the logic of the coordinator stays outside of the Application. After
//  refactor the message flow should looks like the diagram below. The Primary
//  Backup coordinator should handle the "execute" of StartEpochPacket called
//  by Paxos, after agreement is reached.
//   ┌──────────────┐
//   │   end user   │
//   └────┬───▲─────┘
//        │   │
//  ┌─────▼───┴──────────┐     ┌───────────────┐
//  │                    ├─────►               │
//  │ PrimaryBackupCoor  │     │   Paxos       ├─────►
//  │                    │     │               │▼────┘
//  │                    ◄─────┤               │
//  └──┬───▲─────────────┘     └───────────────┘
//     │   │
//   ┌─▼───┴──┐  ┌─────────┐  ┌────────┐  ┌────────┐
//   │Stateful│  │ Stateful│  │Stateful│  │        │
//   │App 01  │  │ App 02  │  │App 03  │  │  ...   │
//   └────────┘  └─────────┘  └────────┘  └────────┘
//  The Middleware is PBAppMiddleware.

public class PBAppMiddleware implements Replicable {

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }

    @Override
    public boolean execute(Request request) {
        return false;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return false;
    }

    @Override
    public String checkpoint(String name) {
        return null;
    }

    @Override
    public boolean restore(String name, String state) {
        return false;
    }
}
