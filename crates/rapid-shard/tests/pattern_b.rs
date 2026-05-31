//! End-to-end Pattern-B demo: 6 nodes, 3 shards (A/B/C), 2 replicas
//! per shard, with shard tags carried in `pb::Metadata`. A
//! [`ShardDirectory`] on the seed node tracks the per-shard replica
//! set and we verify:
//!   1. Initial state — each shard has the expected 2 replicas.
//!   2. Failure removes the failed node from its shards.
//!   3. A new node joining with a `shards` tag appears in those
//!      buckets without disturbing siblings.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use rapid::cluster::{Cluster, ClusterBuilder};
use rapid::messaging::InProcessNetwork;
use rapid::pb;
use rapid_shard::{ShardDirectory, DEFAULT_METADATA_KEY};

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

async fn spawn(
    listen: SocketAddr,
    net: &InProcessNetwork,
    seed: Option<SocketAddr>,
    shards: &str,
) -> Cluster {
    let mut builder = ClusterBuilder::new(listen, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .with_metadata([(DEFAULT_METADATA_KEY, shards.as_bytes().to_vec())]);
    let _ = &mut builder;
    match seed {
        None => builder.start().await.expect("seed bootstraps"),
        Some(s) => builder.join(s).await.expect("joiner converges"),
    }
}

fn ports(eps: &[pb::Endpoint]) -> HashSet<i32> {
    eps.iter().map(|e| e.port).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pattern_b_initial_state_indexes_correctly() {
    let net = InProcessNetwork::new();
    // 6 nodes, 2 replicas per shard A/B/C:
    //   port 40000 → A,B
    //   port 40001 → A,C
    //   port 40002 → B,C
    //   port 40003 → A
    //   port 40004 → B
    //   port 40005 → C
    // Expected:
    //   A: {40000, 40001, 40003}
    //   B: {40000, 40002, 40004}
    //   C: {40001, 40002, 40005}
    let seed_addr = addr(40_000);
    let seed = spawn(seed_addr, &net, None, "A,B").await;
    let n1 = spawn(addr(40_001), &net, Some(seed_addr), "A,C").await;
    let n2 = spawn(addr(40_002), &net, Some(seed_addr), "B,C").await;
    let n3 = spawn(addr(40_003), &net, Some(seed_addr), "A").await;
    let n4 = spawn(addr(40_004), &net, Some(seed_addr), "B").await;
    let n5 = spawn(addr(40_005), &net, Some(seed_addr), "C").await;
    // Let the cluster settle.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let dir = ShardDirectory::new(&seed).await.expect("directory builds");
    assert_eq!(
        ports(&dir.replicas_of("A")),
        HashSet::from([40_000, 40_001, 40_003])
    );
    assert_eq!(
        ports(&dir.replicas_of("B")),
        HashSet::from([40_000, 40_002, 40_004])
    );
    assert_eq!(
        ports(&dir.replicas_of("C")),
        HashSet::from([40_001, 40_002, 40_005])
    );
    assert_eq!(dir.all_shards(), vec!["A", "B", "C"]);
    assert_eq!(dir.total_replica_slots(), 9);

    dir.shutdown().await;
    seed.shutdown().await;
    n1.shutdown().await;
    n2.shutdown().await;
    n3.shutdown().await;
    n4.shutdown().await;
    n5.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pattern_b_node_failure_removes_replicas() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(40_100);
    let seed = spawn(seed_addr, &net, None, "A").await;
    let _n1 = spawn(addr(40_101), &net, Some(seed_addr), "A,B").await;
    let _n2 = spawn(addr(40_102), &net, Some(seed_addr), "B,C").await;
    let n3 = spawn(addr(40_103), &net, Some(seed_addr), "A,C").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let dir = ShardDirectory::new(&seed).await.expect("directory builds");
    let initial_a = ports(&dir.replicas_of("A"));
    let initial_c = ports(&dir.replicas_of("C"));
    assert!(initial_a.contains(&40_103));
    assert!(initial_c.contains(&40_103));

    let pre_cfg = dir.configuration_id();
    // Kill the multi-shard node (40103). Rapid's FD pipeline will
    // detect, propose, and apply a view-change.
    n3.shutdown().await;
    // Wait until the directory's configuration id advances and the
    // failed node is gone from every bucket.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let now_cfg = dir.configuration_id();
        if now_cfg != pre_cfg && !ports(&dir.replicas_of("A")).contains(&40_103) {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "directory did not observe failure within budget; \
             cfg(before)={pre_cfg:?} cfg(now)={now_cfg:?} A={:?}",
            ports(&dir.replicas_of("A"))
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!ports(&dir.replicas_of("A")).contains(&40_103));
    assert!(!ports(&dir.replicas_of("C")).contains(&40_103));
    // Sibling shard B never had 40103 → unchanged.
    assert_eq!(
        ports(&dir.replicas_of("B")),
        HashSet::from([40_101, 40_102])
    );
    dir.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pattern_b_new_joiner_appears_in_its_shards() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(40_200);
    let seed = spawn(seed_addr, &net, None, "A").await;
    let _n1 = spawn(addr(40_201), &net, Some(seed_addr), "A").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let dir = ShardDirectory::new(&seed).await.expect("directory builds");
    let pre_cfg = dir.configuration_id();
    assert_eq!(ports(&dir.replicas_of("A")).len(), 2);
    assert!(dir.replicas_of("Z").is_empty());

    // New node joins, advertising shard Z (fresh) and A (existing).
    let _n2 = spawn(addr(40_202), &net, Some(seed_addr), "A,Z").await;

    // Wait for the directory to fold in the join's view-change.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if dir.configuration_id() != pre_cfg
            && ports(&dir.replicas_of("A")).contains(&40_202)
            && ports(&dir.replicas_of("Z")) == HashSet::from([40_202])
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "join not folded in within budget; A={:?} Z={:?}",
            ports(&dir.replicas_of("A")),
            ports(&dir.replicas_of("Z"))
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(ports(&dir.replicas_of("A")).len(), 3);
    assert_eq!(ports(&dir.replicas_of("Z")), HashSet::from([40_202]));
    assert!(dir.all_shards().contains(&"Z".to_string()));
    dir.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pattern_b_custom_key_and_delimiter() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(40_300);
    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .with_metadata([("shard.placement", b"north|south|east".to_vec())])
        .start()
        .await
        .expect("seed bootstraps");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dir = ShardDirectory::builder()
        .with_key("shard.placement")
        .with_delimiter('|')
        .build(&seed)
        .await
        .expect("directory builds");
    assert_eq!(
        dir.all_shards(),
        vec!["east".to_string(), "north".to_string(), "south".to_string()]
    );
    dir.shutdown().await;
    seed.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pattern_b_no_metadata_means_empty_index() {
    let net = InProcessNetwork::new();
    let seed_addr = addr(40_400);
    // No metadata → no shards tagged.
    let seed = ClusterBuilder::new(seed_addr, net.clone())
        .with_settings(rapid::settings::Settings::for_tests())
        .start()
        .await
        .expect("seed bootstraps");
    let dir = ShardDirectory::new(&seed).await.expect("directory builds");
    assert!(dir.all_shards().is_empty());
    assert!(dir.replicas_of("anything").is_empty());
    dir.shutdown().await;
    seed.shutdown().await;
}
