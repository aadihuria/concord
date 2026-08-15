use proptest::prelude::*;
use serde_json::json;

use concord_crdt::lww::LwwRegister;
use concord_crdt::orset::OrSet;
use concord_crdt::sequence::RgaSequence;

// --- LWW-Register property tests ---

fn arb_lww() -> impl Strategy<Value = LwwRegister> {
    (1u64..1000, "[a-z]{1,4}").prop_map(|(ts, node)| LwwRegister::new(json!(ts), ts, node))
}

proptest! {
    #[test]
    fn lww_merge_commutative(a in arb_lww(), b in arb_lww()) {
        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        prop_assert_eq!(ab.value, ba.value);
        prop_assert_eq!(ab.timestamp, ba.timestamp);
        prop_assert_eq!(ab.node_id, ba.node_id);
    }

    #[test]
    fn lww_merge_associative(a in arb_lww(), b in arb_lww(), c in arb_lww()) {
        // (a merge b) merge c == a merge (b merge c)
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut a_bc = a.clone();
        let mut bc = b.clone();
        bc.merge(&c);
        a_bc.merge(&bc);

        prop_assert_eq!(ab_c.value, a_bc.value);
        prop_assert_eq!(ab_c.timestamp, a_bc.timestamp);
    }

    #[test]
    fn lww_merge_idempotent(a in arb_lww()) {
        let mut merged = a.clone();
        merged.merge(&a);
        merged.merge(&a);

        prop_assert_eq!(merged.value, a.value);
        prop_assert_eq!(merged.timestamp, a.timestamp);
    }
}

// --- OR-Set property tests ---

fn arb_element() -> impl Strategy<Value = String> {
    "[a-z]{1,6}"
}

fn arb_node_id() -> impl Strategy<Value = String> {
    "[a-z]{1,3}"
}

proptest! {
    #[test]
    fn orset_add_is_observable(elem in arb_element(), node in arb_node_id()) {
        let mut set = OrSet::new();
        set.add(elem.clone(), &node);
        prop_assert!(set.contains(&elem));
    }

    #[test]
    fn orset_merge_commutative(
        elems_a in prop::collection::vec(arb_element(), 0..5),
        elems_b in prop::collection::vec(arb_element(), 0..5),
    ) {
        let mut a = OrSet::new();
        let mut b = OrSet::new();

        for e in &elems_a {
            a.add(e.clone(), "node-a");
        }
        for e in &elems_b {
            b.add(e.clone(), "node-b");
        }

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        let mut ab_elems: Vec<_> = ab.elements().into_iter().map(|s| s.to_string()).collect();
        let mut ba_elems: Vec<_> = ba.elements().into_iter().map(|s| s.to_string()).collect();
        ab_elems.sort();
        ba_elems.sort();

        prop_assert_eq!(ab_elems, ba_elems);
    }

    #[test]
    fn orset_merge_idempotent(
        elems in prop::collection::vec(arb_element(), 1..5),
    ) {
        let mut set = OrSet::new();
        for e in &elems {
            set.add(e.clone(), "node-a");
        }

        let original_len = set.len();
        let snapshot = set.clone();

        set.merge(&snapshot);
        set.merge(&snapshot);

        prop_assert_eq!(set.len(), original_len);
    }

    #[test]
    fn orset_concurrent_add_wins(elem in arb_element()) {
        let mut state_a = OrSet::new();
        let mut state_b = OrSet::new();

        // both start with the element
        let op_init = state_a.add(elem.clone(), "init");
        state_b.apply(&op_init);

        // b removes
        let remove_op = state_b.remove(&elem).unwrap();

        // a concurrently adds again
        let add_op = state_a.add(elem.clone(), "node-a");

        // cross-apply
        state_a.apply(&remove_op);
        state_b.apply(&add_op);

        // add-wins: both should still contain the element
        prop_assert!(state_a.contains(&elem));
        prop_assert!(state_b.contains(&elem));
    }
}

// --- RGA Sequence property tests ---

proptest! {
    #[test]
    fn rga_insert_preserves_order(n in 1usize..8) {
        let mut seq = RgaSequence::new();
        for i in 0..n {
            seq.insert(i, json!(i), (i + 1) as u64, "node-a");
        }

        let vals = seq.values();
        prop_assert_eq!(vals.len(), n);

        for (i, v) in vals.iter().enumerate() {
            prop_assert_eq!(*v, &json!(i));
        }
    }

    #[test]
    fn rga_merge_idempotent(n in 1usize..6) {
        let mut seq = RgaSequence::new();
        for i in 0..n {
            seq.insert(i, json!(i), (i + 1) as u64, "a");
        }

        let original_len = seq.len();
        let snapshot = seq.clone();

        seq.merge(&snapshot);
        seq.merge(&snapshot);

        prop_assert_eq!(seq.len(), original_len);
    }

    #[test]
    fn rga_concurrent_inserts_converge(
        ts_a in 1u64..100,
        ts_b in 1u64..100,
    ) {
        let mut state_a = RgaSequence::new();
        let mut state_b = RgaSequence::new();

        // shared root
        let op0 = state_a.insert(0, json!("root"), 1, "init");
        state_b.apply(&op0);

        // concurrent inserts
        let op_a = state_a.insert(1, json!("from-a"), ts_a + 1, "node-a");
        let op_b = state_b.insert(1, json!("from-b"), ts_b + 1, "node-b");

        state_a.apply(&op_b);
        state_b.apply(&op_a);

        let vals_a: Vec<_> = state_a.values().into_iter().cloned().collect();
        let vals_b: Vec<_> = state_b.values().into_iter().cloned().collect();

        prop_assert_eq!(vals_a, vals_b, "replicas diverged after concurrent insert");
    }

    #[test]
    fn rga_delete_is_stable(n in 2usize..6, del_pos in 0usize..5) {
        let mut seq = RgaSequence::new();
        for i in 0..n {
            seq.insert(i, json!(i), (i + 1) as u64, "a");
        }

        let del_pos = del_pos % n;
        seq.delete(del_pos);

        prop_assert_eq!(seq.len(), n - 1);

        // merging with self shouldn't un-delete
        let snapshot = seq.clone();
        seq.merge(&snapshot);
        prop_assert_eq!(seq.len(), n - 1);
    }
}
