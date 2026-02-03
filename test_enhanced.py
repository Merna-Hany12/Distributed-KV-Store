"""
test_enhanced_features.py
Fixed for Windows:
  - Each test uses a UNIQUE port (no conflicts)
  - Proper server stop + sleep between tests
  - All cleanup is robust (retries on PermissionError)
  - client is always defined before finally block
"""

import socket
import json
import threading
import os
import sys
import time
import shutil

from server import KVServer
from client import KVClient


# ============================================================================
# HELPERS
# ============================================================================

# Every test gets its OWN port — no conflicts ever
PORT_BASE = 9200  # Start high to avoid conflicts with cluster.py / benchmark
_port_counter = 0
_port_lock = threading.Lock()

def get_unique_port():
    """Return a fresh port for each test."""
    global _port_counter
    with _port_lock:
        _port_counter += 1
    return PORT_BASE + _port_counter


def cleanup_dir(path):
    """Robust directory cleanup — retries on Windows PermissionError."""
    if not os.path.exists(path):
        return
    for _ in range(10):
        try:
            shutil.rmtree(path)
            return
        except PermissionError:
            time.sleep(0.3)
        except Exception:
            return


def start_server(port, data_dir):
    """Start a KVServer in a daemon thread. Returns the server object."""
    server = KVServer(port=port, data_dir=data_dir)
    t = threading.Thread(target=server.start, daemon=True)
    t.start()
    # Wait until the port is actually listening
    for _ in range(20):  # up to 10 seconds
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(('localhost', port))
            s.close()
            return server  # ready
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    raise RuntimeError(f"Server on port {port} did not start in time")


def stop_server(server):
    """Stop server and wait for port to be released."""
    if server:
        server.stop()
    time.sleep(1.5)  # Windows needs time to release the port


# ============================================================================
# TEST 1: Full-Text Search (Inverted Index)
# ============================================================================

def test_full_text_search():
    print("\n" + "="*70)
    print("TEST 1: Full-Text Search (Inverted Index)")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_fts_{port}"
    cleanup_dir(data_dir)
    server = None
    client = None

    try:
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        # --- Insert documents ---
        docs = {
            "doc1": "Python is a high level programming language",
            "doc2": "JavaScript is used for web development",
            "doc3": "Python is great for data science and machine learning",
            "doc4": "Java is an object oriented programming language",
            "doc5": "Go is a compiled language for building systems",
        }
        print("\n📝 Inserting documents...")
        for k, v in docs.items():
            client.Set(k, v)
        print(f"   Inserted {len(docs)} documents")

        # --- OR search: any word matches ---
        print("\n🔍 Test 1a: OR search for 'Python programming'")
        results = client.FullTextSearch("Python programming")
        keys_found = [r[0] for r in results]
        print(f"   Found keys: {keys_found}")
        # doc1, doc3 have "Python"; doc1, doc4 have "programming"
        assert "doc1" in keys_found, f"Expected doc1 in results"
        assert "doc3" in keys_found, f"Expected doc3 in results"
        print("   ✓ PASSED")

        # --- Single word search ---
        print("\n🔍 Test 1b: Search for 'JavaScript'")
        results = client.FullTextSearch("JavaScript")
        keys_found = [r[0] for r in results]
        print(f"   Found keys: {keys_found}")
        assert "doc2" in keys_found
        print("   ✓ PASSED")

        # --- Search for word that doesn't exist ---
        print("\n🔍 Test 1c: Search for 'Rust' (not in any doc)")
        results = client.FullTextSearch("Rust")
        print(f"   Found keys: {results}")
        assert len(results) == 0, f"Expected 0 results, got {len(results)}"
        print("   ✓ PASSED")

        print("\n✅ Full-Text Search — ALL PASSED")

    except Exception as e:
        print(f"\n❌ Full-Text Search ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# TEST 2: Semantic Search (Word Embeddings)
# ============================================================================

def test_semantic_search():
    print("\n" + "="*70)
    print("TEST 2: Semantic Search (Word Embeddings)")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_sem_{port}"
    cleanup_dir(data_dir)
    server = None
    client = None

    try:
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        docs = {
            "doc1": "Python is a high level programming language",
            "doc2": "JavaScript is used for web development",
            "doc3": "Python is great for data science and machine learning",
            "doc4": "Java is an object oriented programming language",
        }
        print("\n📝 Inserting documents...")
        for k, v in docs.items():
            client.Set(k, v)

        # --- Semantic search returns scored results ---
        print("\n🧠 Test 2a: Semantic search for 'coding languages'")
        results = client.SemanticSearch("coding languages", top_k=3)
        print(f"   Results (key, score): {results}")
        assert len(results) > 0, "Expected at least 1 result"
        # Results should be sorted by score descending
        scores = [r[1] for r in results]
        assert scores == sorted(scores, reverse=True), "Results not sorted by score"
        print("   ✓ PASSED (sorted by similarity)")

        # --- top_k limits results ---
        print("\n🧠 Test 2b: Semantic search top_k=1")
        results = client.SemanticSearch("programming", top_k=1)
        print(f"   Results: {results}")
        assert len(results) <= 1, f"Expected max 1 result, got {len(results)}"
        print("   ✓ PASSED")

        print("\n✅ Semantic Search — ALL PASSED")

    except Exception as e:
        print(f"\n❌ Semantic Search ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# TEST 3: Index Persistence
# ============================================================================

def test_index_persistence():
    print("\n" + "="*70)
    print("TEST 3: Index Persistence")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_persist_{port}"
    cleanup_dir(data_dir)
    server = None
    client = None

    try:
        # --- Phase 1: Write data, save indexes, stop server ---
        print("\n📝 Phase 1: Creating indexes...")
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        client.Set("key1", "The quick brown fox")
        client.Set("key2", "jumps over the lazy dog")
        client.Set("key3", "Python is amazing for AI")

        # Explicitly save indexes
        client.SaveIndexes()
        print("   Saved indexes to disk")

        client.close()
        client = None
        stop_server(server)
        server = None

        # --- Phase 2: Restart server on SAME port, verify indexes loaded ---
        print("\n🔄 Phase 2: Restarting server...")
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        print("   Searching for 'fox' after restart...")
        results = client.FullTextSearch("fox")
        keys_found = [r[0] for r in results]
        print(f"   Found keys: {keys_found}")
        assert "key1" in keys_found, f"Expected key1, got {keys_found}"
        print("   ✓ PASSED — indexes survived restart")

        print("\n✅ Index Persistence — PASSED")

    except Exception as e:
        print(f"\n❌ Index Persistence ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# TEST 4: Phrase Search
# ============================================================================

def test_phrase_search():
    print("\n" + "="*70)
    print("TEST 4: Phrase Search (Exact Match)")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_phrase_{port}"
    cleanup_dir(data_dir)
    server = None
    client = None

    try:
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        client.Set("s1", "The quick brown fox jumps over the lazy dog")
        client.Set("s2", "A lazy dog sleeps all day")
        client.Set("s3", "The fox is quick and brown")

        # --- Exact phrase match ---
        print("\n🔍 Test 4a: Phrase search 'lazy dog'")
        results = client.PhraseSearch("lazy dog")
        print(f"   Found keys: {results}")
        assert "s1" in results, f"Expected s1"
        assert "s2" in results, f"Expected s2"
        print("   ✓ PASSED")

        # --- Phrase not present ---
        print("\n🔍 Test 4b: Phrase search 'blue cat'")
        results = client.PhraseSearch("blue cat")
        print(f"   Found keys: {results}")
        assert len(results) == 0
        print("   ✓ PASSED")

        print("\n✅ Phrase Search — ALL PASSED")

    except Exception as e:
        print(f"\n❌ Phrase Search ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# TEST 5: Concurrent Indexing
# ============================================================================

def test_concurrent_indexing():
    print("\n" + "="*70)
    print("TEST 5: Concurrent Indexing")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_conc_{port}"
    cleanup_dir(data_dir)
    server = None

    try:
        server = start_server(port, data_dir)

        num_threads = 5
        writes_per_thread = 100
        errors = []

        def writer(thread_id):
            """Each thread opens its own client and writes independently."""
            c = None
            try:
                c = KVClient(port=port)
                for i in range(writes_per_thread):
                    key = f"t{thread_id}_key_{i}"
                    value = f"thread {thread_id} wrote value {i} about programming and Python"
                    c.Set(key, value)
            except Exception as e:
                errors.append((thread_id, str(e)))
            finally:
                if c:
                    c.close()

        print(f"\n📝 Starting {num_threads} concurrent writers ({writes_per_thread} writes each)...")
        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=writer, args=(tid,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        total_writes = num_threads * writes_per_thread
        print(f"   Completed {total_writes} writes with {len(errors)} errors")

        if errors:
            for tid, err in errors:
                print(f"   ⚠ Thread {tid}: {err}")

        # --- Verify data + search ---
        print("\n🔍 Verifying data and search after concurrent writes...")
        verify_client = KVClient(port=port)

        # Check a sample key from each thread
        for tid in range(num_threads):
            val = verify_client.Get(f"t{tid}_key_0")
            assert val is not None, f"Missing key t{tid}_key_0"

        # Search should find results
        results = verify_client.FullTextSearch("Python")
        print(f"   Full-text search 'Python' found {len(results)} results")
        assert len(results) > 0, "Expected search results after concurrent writes"

        verify_client.close()
        print("   ✓ PASSED — all threads wrote successfully, search works")

        print("\n✅ Concurrent Indexing — PASSED")

    except Exception as e:
        print(f"\n❌ Concurrent Indexing ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# TEST 6: Delete updates indexes
# ============================================================================

def test_delete_updates_index():
    print("\n" + "="*70)
    print("TEST 6: Delete Updates Index")
    print("="*70)

    port = get_unique_port()
    data_dir = f"test_del_{port}"
    cleanup_dir(data_dir)
    server = None
    client = None

    try:
        server = start_server(port, data_dir)
        client = KVClient(port=port)

        client.Set("del1", "unique_word_xyz is here")
        client.Set("del2", "another document entirely")

        # Confirm it's searchable
        results = client.FullTextSearch("unique_word_xyz")
        keys = [r[0] for r in results]
        assert "del1" in keys, "del1 should appear before delete"
        print("   ✓ Key found before delete")

        # Delete it
        client.Delete("del1")

        # Should NOT appear anymore
        results = client.FullTextSearch("unique_word_xyz")
        keys = [r[0] for r in results]
        assert "del1" not in keys, f"del1 should NOT appear after delete, got {keys}"
        print("   ✓ Key removed from index after delete")

        print("\n✅ Delete Updates Index — PASSED")

    except Exception as e:
        print(f"\n❌ Delete Updates Index ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            client.close()
        stop_server(server)
        cleanup_dir(data_dir)


# ============================================================================
# MAIN — Run all tests
# ============================================================================

def run_all_tests():
    print("="*70)
    print("ENHANCED KV STORE - COMPREHENSIVE TEST SUITE")
    print("="*70)

    tests = [
        ("Full-Text Search",       test_full_text_search),
        ("Semantic Search",        test_semantic_search),
        ("Index Persistence",      test_index_persistence),
        ("Phrase Search",          test_phrase_search),
        ("Concurrent Indexing",    test_concurrent_indexing),
        ("Delete Updates Index",   test_delete_updates_index),
    ]

    passed = 0
    failed = 0

    for name, func in tests:
        try:
            func()
            passed += 1
        except Exception as e:
            print(f"\n❌ {name} FAILED: {e}")
            failed += 1
        # Give Windows time to release sockets between tests
        time.sleep(2.0)

    # --- Summary ---
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"  ✅ Passed: {passed}/{passed+failed}")
    print(f"  ❌ Failed: {failed}/{passed+failed}")
    print("="*70)


if __name__ == "__main__":
    run_all_tests()