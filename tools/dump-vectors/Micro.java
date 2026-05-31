// Phase-0 xxh64 micro-conformance: emit Java LongHashFunction.xx(seed) of
// "hello" with seed=0. The Rust side must match this byte-for-byte.
//
// Build:
//   javac -cp $HOME/.m2/repository/net/openhft/zero-allocation-hashing/0.8/zero-allocation-hashing-0.8.jar Micro.java
// Run:
//   java -cp .:$HOME/.m2/repository/net/openhft/zero-allocation-hashing/0.8/zero-allocation-hashing-0.8.jar Micro
//
// MembershipView.java uses LongHashFunction.xx(seed) — same algorithm
// xxhash-rust calls xxh64.
import net.openhft.hashing.LongHashFunction;

public class Micro {
    public static void main(String[] args) {
        byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long h0 = LongHashFunction.xx(0).hashBytes(bytes);
        long h1 = LongHashFunction.xx(1).hashBytes(bytes);
        // Output hex as unsigned 64-bit, lowercased, zero-padded.
        System.out.printf("xx(0,\"hello\")=%016x%n", h0);
        System.out.printf("xx(1,\"hello\")=%016x%n", h1);
        // A second test vector that exercises a longer input:
        byte[] ep = "127.0.0.1:1234".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int seed = 0; seed < 4; seed++) {
            long h = LongHashFunction.xx(seed).hashBytes(ep);
            System.out.printf("xx(%d,\"127.0.0.1:1234\")=%016x%n", seed, h);
        }
    }
}
