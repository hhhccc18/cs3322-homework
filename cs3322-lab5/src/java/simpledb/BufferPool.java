package simpledb;

import simpledb.utils.HashablePair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe , all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int PAGE_SIZE = 4096;
	
	private static int pageSize = PAGE_SIZE;
	
	/** Default number of pages passed to the constructor. This is used by
	 other classes. BufferPool should use the numPages argument to the
	 constructor instead. */
	public static final int DEFAULT_PAGES = 50;
	
//	public static class PageItem {
//		PageId pid;
//		TransactionId tid;
//		Permissions perm;
//		Page page;
//
//		public PageItem(PageId pid, TransactionId tid, Permissions perm, Page page) {
//			this.pid = pid;
//			this.tid = tid;
//			this.perm = perm;
//			this.page = page;
//		}
//	}
	
	private final int numPages;
	
//	private final LinkedHashMap<PageId, Page> pageTableById =
//			new LinkedHashMap<>(0, 0.75f, true);
	private final ConcurrentHashMap<PageId, Page> pageTableById =
			new ConcurrentHashMap<>(); // 你干嘛哎哟
	
	private final ConcurrentHashMap<PageId, ReadWriteSemaphore> lockTbl =
			new ConcurrentHashMap<>();
	private final ConcurrentHashMap<HashablePair<TransactionId, PageId>, LockInfo>
			lockInfoTbl = new ConcurrentHashMap<>();
	private final DependencyGraph graph = new DependencyGraph();
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		this.numPages = numPages;
	}
	
	public static int getPageSize() {
		return BufferPool.pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = PAGE_SIZE;
	}
	
	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid the ID of the transaction requesting the page
	 * @param pid the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		ReadWriteSemaphore lock = this.lockTbl.computeIfAbsent(pid, foo -> new ReadWriteSemaphore());
		LockInfo info = this.lockInfoTbl.computeIfAbsent(new HashablePair<>(tid, pid),
				foo -> new LockInfo(tid, lock));
		info.update(perm == Permissions.READ_WRITE);
		
		synchronized (this) {
			if (!this.pageTableById.containsKey(pid)) {
				if (this.pageTableById.size() >= this.numPages)
					this.evictPage();
				
				assert this.pageTableById.size() < this.numPages;
				
				this.pageTableById.put(pid,
						Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
			}
		}
		
		return this.pageTableById.get(pid);
	}
	
	/**
	 * Releases the lock on a page.
	 * Calling this is very risky, and may result in wrong behavior. Think hard
	 * about who needs to call this and why, and why they can run the risk of
	 * calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		LockInfo info = this.lockInfoTbl.get(new HashablePair<>(tid, pid));
		
		if (info != null) {
			info.unlock();
			this.lockInfoTbl.remove(new HashablePair<>(tid, pid));
		}
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		this.transactionComplete(tid, true);
	}
	
	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		return lockInfoTbl.containsKey(new HashablePair<>(tid, p));
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid the ID of the transaction requesting to unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		if (commit)
			this.flushPages(tid);
		else
			this.lockInfoTbl.entrySet().stream()
					.filter(entry -> entry.getKey().first.equals(tid))
					.filter(entry -> entry.getValue().isWrite())
//					.collect(Collectors.toList())
					.forEach(entry -> this.discardPage(entry.getKey().second));
		
		this.lockInfoTbl.entrySet().stream()
				.filter(entry -> entry.getKey().first.equals(tid))
//				.collect(Collectors.toList())
				.forEach(entry -> entry.getValue().unlock());
	}
	
	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2).
	 * May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t)
				.forEach(page -> {
					try {
						this.checkAndUpdate(tid, page);
					} catch (DbException e) {
						e.printStackTrace();
					}
					
					page.markDirty(true, tid);
				});
	}
	
	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		Database.getCatalog().getDatabaseFile(
				t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t)
				.forEach(page -> {
					try {
						this.checkAndUpdate(tid, page);
					} catch (DbException e) {
						e.printStackTrace();
					}
					
					page.markDirty(true, tid);
				});
	}
	
	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 *     break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		new ArrayList<>(this.pageTableById.keySet())
				.forEach(pid -> {
					try {
						this.flushPage(pid);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
	}
	
	/** Remove the specific page id from the buffer pool.
	 Needed by the recovery manager to ensure that the
	 buffer pool doesn't keep a rolled back page in its
	 cache.
	 
	 Also used by B+ tree files to ensure that deleted pages
	 are removed from the cache, so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		this.pageTableById.remove(pid);
	}
	
	/**
	 * Flushes a certain page to disk
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
//		assert this.pageTableById.containsKey(pid);

//		if (!this.pageTableById.containsKey(pid))
//			return;
		
		Page page = this.pageTableById.get(pid);
		if (page != null && page.isDirty() != null) {
			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
			page.markDirty(false, null);
		}
		
	}
	
	/** Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		this.lockInfoTbl.entrySet().stream()
				.filter(entry -> entry.getKey().first.equals(tid))
				.filter(entry -> entry.getValue().isWrite())
				.forEach(entry -> {
					try {
						this.flushPage(entry.getKey().second);
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
	}
	
	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		try {
			PageId scapegoat = this.pageTableById.entrySet().stream()
					.filter(entry -> entry.getValue().isDirty() == null)
					.map(Map.Entry::getKey)
					.findFirst()
					.orElseThrow(() -> new DbException("食不食油饼"));
			
			this.flushPage(scapegoat);
//			this.lockTbl.remove(scapegoat);
			this.discardPage(scapegoat);
		}
		catch (IOException e) {
			e.printStackTrace();
			throw new DbException("Failed to evict page");
		}
	}
	
	private void checkAndUpdate(TransactionId tid, Page page) throws DbException {
		if (!this.pageTableById.containsKey(page.getId()) &&
				this.pageTableById.size() >= this.numPages)
			this.evictPage();
		
		this.pageTableById.put(page.getId(), page);
	}
	
	private static class DependencyGraph {
		private static class SemaphoreEdge {
			public final ReadWriteSemaphore semaphore;
			public final boolean isWrite;
			
			public SemaphoreEdge(ReadWriteSemaphore semaphore, boolean isWrite) {
				this.semaphore = semaphore;
				this.isWrite = isWrite;
			}
		}
		
		private static class TransactionEdge {
			public final TransactionId tid;
			public final boolean isWrite;
			
			public TransactionEdge(TransactionId tid, boolean isWrite) {
				this.tid = tid;
				this.isWrite = isWrite;
			}
			
			@Override
			public boolean equals(Object obj) {
				if (!(obj instanceof TransactionEdge))
					return false;
				
				TransactionEdge o = (TransactionEdge) obj;
				return this.isWrite == o.isWrite && this.tid.equals(o.tid);
			}
			
			@Override
			public int hashCode() {
				return Objects.hash(this.tid, this.isWrite);
			}
		}
		
		private final ConcurrentHashMap<TransactionId, SemaphoreEdge> semaphoreEdges =
				new ConcurrentHashMap<>();
		private final ConcurrentHashMap<ReadWriteSemaphore, Set<TransactionEdge>> transactionEdges =
				new ConcurrentHashMap<>();
		
		public synchronized boolean wait(TransactionId tid, ReadWriteSemaphore semaphore,
		                                 boolean isWrite) {
			this.semaphoreEdges.put(tid, new SemaphoreEdge(semaphore, isWrite));
			
			// Use a BFS to find cycles
			boolean cyclic = false;
			HashSet<TransactionId> vis = new HashSet<TransactionId>() {{ add(tid); }};
			LinkedList<TransactionId> q = new LinkedList<TransactionId>() {{ add(tid); }};
			
			while (!q.isEmpty()) {
				TransactionId cur = q.poll();
				SemaphoreEdge edge = this.semaphoreEdges.get(cur);
				
				if (edge == null)
					continue;
				
				for (TransactionEdge e : this.transactionEdges.getOrDefault(edge.semaphore, new HashSet<>())) {
					if (!(edge.isWrite || e.isWrite) || cur.equals(e.tid))
						continue;
					
					if (vis.contains(e.tid)) {
						cyclic = true;
						break;
					}
					
					vis.add(e.tid);
					q.add(e.tid);
				}
			}
			
			if (cyclic)
				this.semaphoreEdges.remove(tid);
			
			return !cyclic;
		}
		
		public synchronized void acquire(TransactionId tid, ReadWriteSemaphore semaphore,
		                                 boolean isWrite) {
			this.semaphoreEdges.remove(tid);
			this.transactionEdges.computeIfAbsent(semaphore, foo -> new HashSet<>())
					.add(new TransactionEdge(tid, isWrite));
		}
		
		public synchronized void release(TransactionId tid, ReadWriteSemaphore semaphore,
		                                 boolean isWrite) {
			Set<TransactionEdge> st = this.transactionEdges.get(semaphore);
			st.remove(new TransactionEdge(tid, isWrite));
			
			if (st.isEmpty())
				this.transactionEdges.remove(semaphore);
		}
		
		public synchronized void upgrade(TransactionId tid, ReadWriteSemaphore semaphore) {
			this.release(tid, semaphore, false);
			this.acquire(tid, semaphore, true);
		}
	}
	
	private class ReadWriteSemaphore {
		private final Semaphore read, write, upgrade;
		private AtomicInteger count;
		
		public ReadWriteSemaphore() {
			this.read = new Semaphore(1);
			this.write = new Semaphore(1);
			this.upgrade = new Semaphore(1);
			this.count = new AtomicInteger(0);
		}
		
		public void lockRead(TransactionId tid) throws TransactionAbortedException {
			if (!BufferPool.this.graph.wait(tid, this, false))
				throw new TransactionAbortedException();
			
			this.read.acquireUninterruptibly();
			if (this.count.incrementAndGet() == 1)
				this.write.acquireUninterruptibly();
			this.read.release();
			
			BufferPool.this.graph.acquire(tid, this, false);
		}
		
		public void unlockRead(TransactionId tid) {
			this.read.acquireUninterruptibly();
			if (this.count.decrementAndGet() == 0)
				this.write.release();
			this.read.release();
			
			BufferPool.this.graph.release(tid, this, false);
		}
		
		public void lockWrite(TransactionId tid) throws TransactionAbortedException {
			if (!BufferPool.this.graph.wait(tid, this, true))
				throw new TransactionAbortedException();
			
			this.write.acquireUninterruptibly();
			BufferPool.this.graph.acquire(tid, this, true);
		}
		
		public void unlockWrite(TransactionId tid) {
			this.write.release();
			BufferPool.this.graph.release(tid, this, true);
		}
		
		public void upgrade(TransactionId tid) throws TransactionAbortedException {
			if (!BufferPool.this.graph.wait(tid, this, true))
				throw new TransactionAbortedException(); // is this right?
			
			this.read.acquireUninterruptibly();
			this.upgrade.acquireUninterruptibly();
			
			if (this.count.decrementAndGet() == 0)
				this.write.release();
			
			this.read.release();
			this.write.acquireUninterruptibly();
			this.upgrade.release();
			
			BufferPool.this.graph.upgrade(tid, this);
		}
	}
	
	private static class LockInfo {
		private enum State { FREE, READ, WRITE }
		
		private final TransactionId tid;
		private final ReadWriteSemaphore lock;
		private State state;
		
		public LockInfo(TransactionId tid, ReadWriteSemaphore lock) {
			this.tid = tid;
			this.lock = lock;
			this.state = State.FREE;
		}
		
		public void update(boolean isWrite) throws TransactionAbortedException {
			if (this.state == State.FREE) {
				if (!isWrite) {
					this.lock.lockRead(this.tid);
					this.state = State.READ;
				}
				else {
					this.lock.lockWrite(tid);
					this.state = State.WRITE;
				}
			}
			else if (this.state == State.READ && isWrite) {
				this.lock.upgrade(this.tid);
				this.state = State.WRITE;
			}
		}
		
		public boolean isWrite() {
			return this.state == State.WRITE;
		}
		
		public void unlock() {
			if (this.state == State.READ)
				this.lock.unlockRead(this.tid);
			else if (this.state == State.WRITE)
				this.lock.unlockWrite(this.tid);
			
			this.state = State.FREE;
		}
	}
}
