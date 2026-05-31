// Dumps golden vectors for Phase-1 parity checks. See parity report for
// the exact format. We use full reflection because MembershipView is
// package-private in com.vrg.rapid.
//
// Build (run from this directory):
//   javac -cp $RAPID_CLASSES:$PROTOBUF_JAR:$HASH_JAR ViewDump.java
// Run:
//   java -cp .:$RAPID_CLASSES:$PROTOBUF_JAR:$HASH_JAR ViewDump

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.vrg.rapid.pb.Endpoint;
import com.vrg.rapid.pb.NodeId;

import net.openhft.hashing.LongHashFunction;

public class ViewDump {

    static Endpoint ep(String host, int port) {
        return Endpoint.newBuilder()
                .setHostname(ByteString.copyFromUtf8(host))
                .setPort(port)
                .build();
    }

    public static void main(String[] args) throws Exception {
        dumpHashIntLongVectors();
        dumpAddressComparatorVectors();

        // Reflection handles for MembershipView (package-private).
        Class<?> mvClass = Class.forName("com.vrg.rapid.MembershipView");
        Constructor<?> mvCtor = mvClass.getDeclaredConstructor(int.class);
        mvCtor.setAccessible(true);

        Method ringAdd = mvClass.getDeclaredMethod("ringAdd", Endpoint.class, NodeId.class);
        ringAdd.setAccessible(true);
        Method getCfgId = mvClass.getDeclaredMethod("getCurrentConfigurationId");
        getCfgId.setAccessible(true);
        Method getObservers = mvClass.getDeclaredMethod("getObserversOf", Endpoint.class);
        getObservers.setAccessible(true);
        Method getSubjects = mvClass.getDeclaredMethod("getSubjectsOf", Endpoint.class);
        getSubjects.setAccessible(true);
        Method getRing = mvClass.getDeclaredMethod("getRing", int.class);
        getRing.setAccessible(true);

        for (int n : new int[] { 3, 10, 100 }) {
            List<Endpoint> endpoints = buildEndpoints(n);
            List<NodeId> nodeIds = buildNodeIds(n);

            Object mview = mvCtor.newInstance(10); // K = 10
            for (int i = 0; i < n; i++) {
                ringAdd.invoke(mview, endpoints.get(i), nodeIds.get(i));
            }

            long cfgId = (long) getCfgId.invoke(mview);
            System.out.printf("configId n=%d -> %016x%n", n, cfgId);

            Endpoint probe = endpoints.get(0);
            @SuppressWarnings("unchecked")
            List<Endpoint> obs = (List<Endpoint>) getObservers.invoke(mview, probe);
            @SuppressWarnings("unchecked")
            List<Endpoint> subs = (List<Endpoint>) getSubjects.invoke(mview, probe);
            System.out.printf("observers n=%d of=%s:%d -> [%s]%n", n,
                    probe.getHostname().toStringUtf8(), probe.getPort(),
                    joinEndpoints(obs));
            System.out.printf("subjects  n=%d of=%s:%d -> [%s]%n", n,
                    probe.getHostname().toStringUtf8(), probe.getPort(),
                    joinEndpoints(subs));

            @SuppressWarnings("unchecked")
            List<Endpoint> ring0 = (List<Endpoint>) getRing.invoke(mview, 0);
            System.out.printf("ring0 n=%d -> [%s]%n", n, joinEndpoints(ring0));
        }
    }

    static void dumpHashIntLongVectors() {
        for (int seed = 0; seed < 2; seed++) {
            LongHashFunction h = LongHashFunction.xx(seed);
            System.out.printf("hashInt seed=%d value=0 -> %016x%n", seed, h.hashInt(0));
            System.out.printf("hashInt seed=%d value=1 -> %016x%n", seed, h.hashInt(1));
            System.out.printf("hashInt seed=%d value=1234 -> %016x%n", seed, h.hashInt(1234));
            System.out.printf("hashInt seed=%d value=-1 -> %016x%n", seed, h.hashInt(-1));
            System.out.printf("hashLong seed=%d value=0 -> %016x%n", seed, h.hashLong(0L));
            System.out.printf("hashLong seed=%d value=1 -> %016x%n", seed, h.hashLong(1L));
            System.out.printf("hashLong seed=%d value=0x1122334455667788 -> %016x%n", seed,
                    h.hashLong(0x1122334455667788L));
            System.out.printf("hashLong seed=%d value=-1 -> %016x%n", seed, h.hashLong(-1L));
        }
    }

    static void dumpAddressComparatorVectors() throws Exception {
        Class<?> ac = Class.forName("com.vrg.rapid.MembershipView$AddressComparator");
        Constructor<?> ctor = ac.getDeclaredConstructor(int.class);
        ctor.setAccessible(true);
        Method computeHash = ac.getDeclaredMethod("computeHash", Endpoint.class);
        computeHash.setAccessible(true);

        Endpoint[] sample = new Endpoint[] {
                ep("127.0.0.1", 1234),
                ep("127.0.0.1", 1),
                ep("127.0.0.2", 2),
                ep("10.0.0.5", 4444),
                ep("a-very-long-hostname.example.com", 65535),
        };
        for (int seed = 0; seed < 4; seed++) {
            Object cmp = ctor.newInstance(seed);
            for (Endpoint e : sample) {
                long hash = (long) computeHash.invoke(cmp, e);
                System.out.printf("addrCmp seed=%d host=%s port=%d -> %016x%n",
                        seed,
                        e.getHostname().toStringUtf8(),
                        e.getPort(),
                        hash);
            }
        }
    }

    static List<Endpoint> buildEndpoints(int n) {
        List<Endpoint> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(ep("127.0.0.1", 10000 + i));
        }
        return out;
    }

    static List<NodeId> buildNodeIds(int n) {
        List<NodeId> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String tag = "127.0.0.1:" + (10000 + i);
            UUID u = UUID.nameUUIDFromBytes(tag.getBytes(StandardCharsets.UTF_8));
            out.add(NodeId.newBuilder()
                    .setHigh(u.getMostSignificantBits())
                    .setLow(u.getLeastSignificantBits())
                    .build());
        }
        return out;
    }

    static String joinEndpoints(List<Endpoint> es) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < es.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(es.get(i).getHostname().toStringUtf8())
                    .append(':')
                    .append(es.get(i).getPort());
        }
        return sb.toString();
    }
}
