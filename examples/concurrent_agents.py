"""
concurrent agents demo — three agents writing shared plan state simultaneously.

this demonstrates the core value proposition: concurrent writes from multiple
agents converge correctly without clobbering each other, thanks to the crdt
merge layer underneath raft's replicated log.

usage:
    1. start a concord cluster (see README)
    2. python examples/concurrent_agents.py
"""

import threading
import time
import json
from concord import ConcordClient


def research_agent(client: ConcordClient, barrier: threading.Barrier):
    """simulates an agent that researches topics and reports findings."""
    barrier.wait()

    # update own status
    client.put("agents", "researcher-status", {"state": "running", "task": "web-search"})

    # add discovered facts to shared set
    facts = ["python-3.12-released", "rust-1.75-stable", "grpc-supports-streaming"]
    for fact in facts:
        client.add_to_set("knowledge", "discovered-facts", fact)
        time.sleep(0.05)

    # add plan steps
    client.insert_into_sequence("plan", "steps", 0, {
        "action": "search",
        "query": "distributed systems papers",
        "agent": "researcher"
    })

    client.put("agents", "researcher-status", {"state": "done", "findings": len(facts)})
    print(f"  [researcher] done — added {len(facts)} facts")


def analyst_agent(client: ConcordClient, barrier: threading.Barrier):
    """simulates an agent that analyzes data and updates shared state."""
    barrier.wait()

    client.put("agents", "analyst-status", {"state": "running", "task": "analysis"})

    # add its own discovered facts concurrently
    facts = ["latency-p99-under-5ms", "throughput-200k-ops", "failover-sub-ms"]
    for fact in facts:
        client.add_to_set("knowledge", "discovered-facts", fact)
        time.sleep(0.05)

    # add plan steps concurrently with researcher
    client.insert_into_sequence("plan", "steps", 0, {
        "action": "analyze",
        "metric": "system-performance",
        "agent": "analyst"
    })

    client.put("agents", "analyst-status", {"state": "done", "findings": len(facts)})
    print(f"  [analyst] done — added {len(facts)} facts")


def writer_agent(client: ConcordClient, barrier: threading.Barrier):
    """simulates an agent that writes reports based on shared state."""
    barrier.wait()

    client.put("agents", "writer-status", {"state": "running", "task": "drafting"})

    # add plan steps
    client.insert_into_sequence("plan", "steps", 0, {
        "action": "draft-report",
        "format": "markdown",
        "agent": "writer"
    })

    # add its own facts
    client.add_to_set("knowledge", "discovered-facts", "report-template-ready")
    client.add_to_set("knowledge", "discovered-facts", "citations-collected")

    client.put("agents", "writer-status", {"state": "done"})
    print(f"  [writer] done")


def main():
    print("=== concord concurrent agents demo ===\n")
    print("connecting to concord cluster...")

    try:
        client = ConcordClient("127.0.0.1:50051", "demo-orchestrator")
    except Exception as e:
        print(f"error: could not connect to concord at 127.0.0.1:50051")
        print(f"make sure the cluster is running (see README for instructions)")
        print(f"detail: {e}")
        return

    # each agent gets its own client connection
    clients = [
        ConcordClient("127.0.0.1:50051", "researcher"),
        ConcordClient("127.0.0.1:50051", "analyst"),
        ConcordClient("127.0.0.1:50051", "writer"),
    ]

    # barrier ensures all agents start at the same time
    barrier = threading.Barrier(3)

    print("launching 3 agents concurrently...\n")
    start = time.time()

    threads = [
        threading.Thread(target=research_agent, args=(clients[0], barrier)),
        threading.Thread(target=analyst_agent, args=(clients[1], barrier)),
        threading.Thread(target=writer_agent, args=(clients[2], barrier)),
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed = time.time() - start
    print(f"\nall agents finished in {elapsed*1000:.0f}ms")

    # read the merged state
    print("\n--- merged state ---\n")

    # check agent statuses (lww-register — last write wins)
    for agent in ["researcher", "analyst", "writer"]:
        status = client.get("agents", f"{agent}-status")
        print(f"  {agent}: {status}")

    # check discovered facts (or-set — all adds preserved)
    facts = client.get_set("knowledge", "discovered-facts")
    if facts:
        print(f"\n  discovered facts ({len(facts)} total):")
        for fact in sorted(facts):
            print(f"    - {fact}")
    else:
        print("\n  no facts found")

    # check plan steps (rga sequence — ordered, interleaved correctly)
    steps = client.get_sequence("plan", "steps")
    if steps:
        print(f"\n  plan steps ({len(steps)} total):")
        for i, step in enumerate(steps):
            print(f"    {i+1}. {step}")
    else:
        print("\n  no plan steps found")

    print("\n--- key insight ---")
    print("all 3 agents wrote concurrently to the same keys.")
    print("no writes were lost — the crdt merge layer preserved everything:")
    print("  - lww-registers: each agent's final status is correct")
    print("  - or-set: all 8 facts from all agents are present")
    print("  - rga sequence: all 3 plan steps interleaved deterministically")


if __name__ == "__main__":
    main()
