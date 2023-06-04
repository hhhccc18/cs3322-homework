package simpledb;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author ?
 */
public class HeapFile implements DbFile {
	
	private final File file;
	private final TupleDesc tupleDesc;
	
	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.file = f;
		this.tupleDesc = td;
	}
	
	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return this.file;
	}
	
	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return this.file.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}
	
	// see DbFile.java for javadocs
	public Page readPage(PageId pid) throws IllegalArgumentException {
		if (!(pid instanceof HeapPageId))
			throw new IllegalArgumentException("pid should be HeapPageId");
		
		int size = BufferPool.getPageSize();
		
		try (RandomAccessFile f = new RandomAccessFile(this.file, "r")) {
			f.seek((long) size * pid.pageNumber());
			
			byte[] data = new byte[size];
			f.read(data, 0, size);
			f.close();
			
			return new HeapPage((HeapPageId) pid, data);
		}
		catch (IOException e) {
			throw new DbException("readPage: IOException");
		}
	}
	
	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		if (!(page instanceof HeapPage))
			throw new IllegalArgumentException("page should be HeapPage");
		
		int size = BufferPool.getPageSize();
		
		try (RandomAccessFile f = new RandomAccessFile(this.file, "rw")) {
			f.seek((long) size * page.getId().pageNumber());
			
			f.write(page.getPageData()); // fuck
		}
		catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) this.file.length() / BufferPool.getPageSize();
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		final boolean[] isNewPage = {false};
		
		HeapPage page = this.getPagesStream(tid, Permissions.READ_WRITE)
				.filter(foo -> foo.getNumEmptySlots() > 0)
				.findFirst()
				.orElseGet(() -> {
					try {
						isNewPage[0] = true;
						return new HeapPage(
								new HeapPageId(this.getId(), this.numPages()),
								HeapPage.createEmptyPageData());
					} catch (IOException e) {
						throw new DbException("IOException: " + e.getMessage());
					}
				});
		
		if (page == null)
			throw new DbException("No empty page");
		
		page.insertTuple(t);
		
		if (isNewPage[0])
			this.writePage(page);
		
		return new ArrayList<Page>() {{ this.add(page); }};
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(
				tid, pid, Permissions.READ_WRITE);
		
		page.deleteTuple(t);
		return new ArrayList<Page>() {{ this.add(page); }};
	}
	
	public class HeapFileIterator implements DbFileIterator {
		private final TransactionId tid;
		private Iterator<Stream<Tuple>> st;
		private Iterator<Tuple> it;
//		private int cur;
		
		public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
			this.close(); // Notice that the iterator is closed by default
		}
		
		public void open() throws TransactionAbortedException, DbException {
			this.st = HeapFile.this.getPagesStream(this.tid, Permissions.READ_ONLY)
					.map(HeapPage::stream)
					.iterator();
			
//			this.hasNext();
			// Stream runs much faster than which was written myself
			// Why???
			
//			this.cur = 0;
//			this.hasNext();
		}
		
		public boolean hasNext() throws TransactionAbortedException, DbException {
//			if (this.cur == -1 || this.cur >= HeapFile.this.numPages())
//				return false;
//
//			if (this.it != null && this.it.hasNext())
//				return true;
//
//			if (this.it != null)
//				this.cur++;
//
//			while (this.cur < HeapFile.this.numPages()) {
//				this.it = ((HeapPage) Database.getBufferPool().getPage(
//						this.tid,
//						new HeapPageId(HeapFile.this.getId(), this.cur),
//						Permissions.READ_ONLY
//				)).iterator();
//
//				if (!this.it.hasNext()) {
//					this.it = null;
//					this.cur++;
//				}
//				else
//					return true;
// 			}
//
//			this.it = null;
//
//			return false;
			
			while (!this.it.hasNext() && this.st.hasNext())
				this.it = this.st.next().iterator();
			
			return this.it.hasNext();
		}
		
		public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
			if (!this.hasNext())
				throw new NoSuchElementException();
			
			return this.it.next();
		}
		
		public void rewind() throws TransactionAbortedException, DbException {
			this.close();
			this.open();
		}
		
		public void close() {
//			this.cur = -1;
//			this.it = null;
			this.st = Collections.emptyIterator();
			this.it = Collections.emptyIterator();
		}
	}
	
	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid);
	}
	
	private Stream<HeapPage> getPagesStream(TransactionId tid, Permissions perm)
			throws TransactionAbortedException, DbException {
		return IntStream.range(0, HeapFile.this.numPages())
				.mapToObj(i -> (HeapPage) Database.getBufferPool().getPage(
								tid,
								new HeapPageId(HeapFile.this.getId(), i),
								perm))
//				.filter(foo -> { assert foo != null; return true; });
				.filter(Objects::nonNull); // 好怪喔
	}
	
}

