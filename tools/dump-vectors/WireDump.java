// Dumps representative RapidRequest byte strings for the Phase-0 wire-format
// roundtrip gate. Output: one line per vector, "<name>=<hexbytes>".
//
// Build (run from this directory):
//   javac -cp $RAPID_CLASSES:$PROTOBUF_JAR WireDump.java
// where
//   RAPID_CLASSES=/home/mumoshu/p/rapid-rs/references/rapid-java/rapid/target/classes
//   PROTOBUF_JAR=$HOME/.m2/repository/com/google/protobuf/protobuf-java/3.4.0/protobuf-java-3.4.0.jar
// Run:
//   java -cp .:$RAPID_CLASSES:$PROTOBUF_JAR WireDump
import com.google.protobuf.ByteString;
import com.vrg.rapid.pb.AlertMessage;
import com.vrg.rapid.pb.BatchedAlertMessage;
import com.vrg.rapid.pb.EdgeStatus;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.JoinMessage;
import com.vrg.rapid.pb.LeaveMessage;
import com.vrg.rapid.pb.Metadata;
import com.vrg.rapid.pb.NodeId;
import com.vrg.rapid.pb.PreJoinMessage;
import com.vrg.rapid.pb.ProbeMessage;
import com.vrg.rapid.pb.RapidRequest;

public class WireDump {

    static Endpoint ep(String host, int port) {
        return Endpoint.newBuilder()
                .setHostname(ByteString.copyFromUtf8(host))
                .setPort(port)
                .build();
    }

    static NodeId nid(long high, long low) {
        return NodeId.newBuilder().setHigh(high).setLow(low).build();
    }

    public static void main(String[] args) {
        Endpoint sender = ep("127.0.0.1", 1234);

        // Vector 1: PreJoinMessage
        RapidRequest v1 = RapidRequest.newBuilder()
                .setPreJoinMessage(PreJoinMessage.newBuilder()
                        .setSender(sender)
                        .setNodeId(nid(0x1122334455667788L, 0x99aabbccddeeff00L))
                        .build())
                .build();

        // Vector 2: JoinMessage (carries ringNumbers + configId + metadata)
        Metadata md = Metadata.newBuilder()
                .putMetadata("role", ByteString.copyFromUtf8("web"))
                .build();
        RapidRequest v2 = RapidRequest.newBuilder()
                .setJoinMessage(JoinMessage.newBuilder()
                        .setSender(sender)
                        .setNodeId(nid(1L, 2L))
                        .addRingNumber(0).addRingNumber(3).addRingNumber(7)
                        .setConfigurationId(0x0123456789abcdefL)
                        .setMetadata(md)
                        .build())
                .build();

        // Vector 3: BatchedAlertMessage with 2 AlertMessages
        AlertMessage a1 = AlertMessage.newBuilder()
                .setEdgeSrc(sender)
                .setEdgeDst(ep("10.0.0.5", 4444))
                .setEdgeStatus(EdgeStatus.UP)
                .setConfigurationId(7L)
                .addRingNumber(1)
                .build();
        AlertMessage a2 = AlertMessage.newBuilder()
                .setEdgeSrc(sender)
                .setEdgeDst(ep("10.0.0.6", 4444))
                .setEdgeStatus(EdgeStatus.DOWN)
                .setConfigurationId(7L)
                .addRingNumber(2).addRingNumber(5)
                .build();
        RapidRequest v3 = RapidRequest.newBuilder()
                .setBatchedAlertMessage(BatchedAlertMessage.newBuilder()
                        .setSender(sender)
                        .addMessages(a1).addMessages(a2)
                        .build())
                .build();

        // Vector 4: ProbeMessage with payload
        RapidRequest v4 = RapidRequest.newBuilder()
                .setProbeMessage(ProbeMessage.newBuilder()
                        .setSender(sender)
                        .addPayload(ByteString.copyFromUtf8("ping"))
                        .build())
                .build();

        // Vector 5: LeaveMessage
        RapidRequest v5 = RapidRequest.newBuilder()
                .setLeaveMessage(LeaveMessage.newBuilder()
                        .setSender(sender)
                        .build())
                .build();

        emit("preJoin", v1);
        emit("join", v2);
        emit("batchedAlert", v3);
        emit("probe", v4);
        emit("leave", v5);
    }

    static void emit(String name, RapidRequest req) {
        byte[] bytes = req.toByteArray();
        StringBuilder sb = new StringBuilder();
        sb.append(name).append('=');
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        System.out.println(sb);
    }
}
