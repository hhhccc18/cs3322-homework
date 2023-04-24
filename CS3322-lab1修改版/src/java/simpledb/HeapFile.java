package simpledb;

import java.io.*;
import java.util.*;
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
			
			return new HeapPage((HeapPageId) pid, data);
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// TODO not necessary for lab1
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
		// TODO not necessary for lab1
		return null;
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		// TODO not necessary for lab1
		return null;
	}
	
	public class HeapFileIterator implements DbFileIterator {
		private final TransactionId tid;
		private Iterator<Tuple> it;
		
		public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
			this.close(); // Notice that the iterator is closed by default
		}
		
		public void open() {
			this.it = IntStream.range(0, HeapFile.this.numPages())
					.mapToObj(i -> {
						try {
							return ((HeapPage) Database.getBufferPool().getPage(
									this.tid,
									new HeapPageId(HeapFile.this.getId(), i),
									Permissions.READ_ONLY
							)).stream();
						} catch (TransactionAbortedException | DbException e) {
							e.printStackTrace();
							return null;
						}
					})
					.filter(Objects::nonNull)
					.reduce(Stream::concat)
					.orElse(Stream.empty())
					.iterator();
		}
		
		public boolean hasNext() {
			return this.it != null && this.it.hasNext();
		}
		
		public Tuple next() throws NoSuchElementException {
			if (this.it == null || !this.it.hasNext())
				throw new NoSuchElementException();
			
			return this.it.next();
		}
		
		public void rewind() {
			this.close();
			this.open();
		}
		
		public void close() {
			this.it = Stream.empty().map(foo -> (Tuple) null).iterator();
		}
	}
	
	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid);
	}
	
}

