package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager has the following:
 * - Shared and exclusive locks
 * - Lock upgrades (READ_ONLY -> READ_WRITE)
 * - Basic deadlock prevention using timeout (FINALLY PASSES WOAHH)
 */
public class LockManager {

    private static class Lock {
        TransactionId tid;
        Permissions perm; // READ_ONLY and READ_WRITE

        Lock(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }
    }

    // PageId, List of locks (shared or exclusive)
    private final Map<PageId, List<Lock>> lockTable = new ConcurrentHashMap<>();

    // TransactionId, Set of pages locked
    private final Map<TransactionId, Set<PageId>> txnLocks = new ConcurrentHashMap<>();

    // Max time to wait for a lock before aborting (ms)
    private static final long LOCK_TIMEOUT = 1000;

    // Wait-for graph for deadlock detection
    Map<TransactionId, Set<TransactionId>> waitForGraph = new HashMap<>();

    /**
     * Acquire a lock for a transaction on a page.
     * Blocks until the lock can be acquired or throws TransactionAbortedException
     * if timeout occurs to prevent deadlock.
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        long start = System.currentTimeMillis();

        synchronized (this) {
            while (!canGrantLock(tid, pid, perm)) {
                List<Lock> existingLocks = lockTable.get(pid);
                for (Lock lock : existingLocks) {
                    if (!lock.tid.equals(tid)) {
                        addEdge(tid, lock.tid);
                    }
                }
                if (detectCycle(tid)) {
                    throw new TransactionAbortedException();
                }
                
                try {
                    wait(50); // Wait for 50ms and re-check
                } catch (InterruptedException e) {
                    throw new TransactionAbortedException();
                }

                if (System.currentTimeMillis() - start > LOCK_TIMEOUT) {
                    throw new TransactionAbortedException();
                }
            }

            // once canGrantLock == true
            grantLock(tid, pid, perm);
            removeTransaction(tid);
        }
    }

    /**
     * Release a lock held by a transaction on a page.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        List<Lock> locks = lockTable.get(pid);

        // runs if there are any locks helf by a transaction on page
        if (locks != null) {
            locks.removeIf(lock -> lock.tid.equals(tid));
            if (locks.isEmpty()) {
                lockTable.remove(pid);
            }
        }

        Set<PageId> pages = txnLocks.get(tid);

        // release page lock
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) {
                txnLocks.remove(tid);
            }
        }

        notifyAll(); // Notify waiting transactions
    }

    /**
     * Release all locks held by a transaction.
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = txnLocks.getOrDefault(tid, Set.of());
        for (PageId pid : new HashSet<>(pages)) {
            releaseLock(tid, pid);
        }
        txnLocks.remove(tid);
        removeTransaction(tid);
    }

    /**
     * Check if a transaction holds a lock on a page.
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        List<Lock> locks = lockTable.get(pid);

        // Guard clause, no locks == easy false
        if (locks == null) {
            return false;
        }

        return locks.stream().anyMatch(lock -> lock.tid.equals(tid));
    }

    /**
     * Determine if we can grant the requested lock to the transaction.
     */
    private boolean canGrantLock(TransactionId tid, PageId pid, Permissions perm) {
        List<Lock> locks = lockTable.get(pid);
        if (locks == null || locks.isEmpty())
            return true;

        boolean alreadyHolding = locks.stream().anyMatch(l -> l.tid.equals(tid));

        if (perm == Permissions.READ_ONLY) {
            // Shared lock can be granted if no exclusive lock is held by another txn
            return locks.stream().allMatch(l -> l.perm == Permissions.READ_ONLY || l.tid.equals(tid));
        } else { // READ_WRITE (exclusive)
            if (alreadyHolding && locks.size() == 1) {
                // This transaction already holds the only lock (safe upgrade)
                return true;
            }
            // Cannot grant exclusive if another transaction holds any lock
            return locks.size() == 1 && locks.get(0).tid.equals(tid) && locks.get(0).perm == Permissions.READ_ONLY;
        }
    }

    /**
     * Grant the lock to the transaction.
     */
    private void grantLock(TransactionId tid, PageId pid, Permissions perm) {
        lockTable.putIfAbsent(pid, new ArrayList<>());

        // Check if this transaction already holds a lock on pid
        List<Lock> locks = lockTable.get(pid);
        Optional<Lock> existingLock = locks.stream().filter(l -> l.tid.equals(tid)).findFirst();

        if (existingLock.isPresent()) {
            // Upgrade READ_ONLY to READ_WRITE
            Lock lock = existingLock.get();
            if (lock.perm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE) {
                lock.perm = Permissions.READ_WRITE;
            }
        } else {
            locks.add(new Lock(tid, perm));
        }

        txnLocks.putIfAbsent(tid, new HashSet<>());
        txnLocks.get(tid).add(pid);
    }

    // Deadlock Detection
    private boolean detectCycle(TransactionId startingTransaction) {
        Set<TransactionId> visitedList = new HashSet<>();
        return dfs(startingTransaction, startingTransaction, visitedList);
    }

    private void addEdge(TransactionId keyId, TransactionId tId) {
        if (!waitForGraph.containsKey(keyId)) {
            waitForGraph.put(keyId, new HashSet<>());
        }
        waitForGraph.get(keyId).add(tId);
    }

    private boolean dfs(TransactionId startingTransaction, TransactionId currentTransaction,
            Set<TransactionId> visitedList) {
        if (!waitForGraph.containsKey(currentTransaction)) {
            return false;
        }

        for (TransactionId neighbour : waitForGraph.get(currentTransaction)) {
            if (neighbour.equals(startingTransaction)) {
                return true;
            }
            if (!visitedList.contains(neighbour)) {
                visitedList.add(neighbour);

                if (dfs(startingTransaction, neighbour, visitedList)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void removeTransaction(TransactionId tId) {
        waitForGraph.remove(tId);
        for (Set<TransactionId> wait : waitForGraph.values()) {
            wait.remove(tId);
        }
    }

}