package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];
            raf.seek(pid.pageNumber() * pageSize);
            int len = raf.read(data);
            assert len == pageSize;
            raf.close();
            assert pid instanceof HeapPageId;
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(page.getId().pageNumber() * BufferPool.getPageSize());
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int length = (int) f.length();
        assert length % BufferPool.getPageSize() == 0;
        return length / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtiedPages = new ArrayList<>();
        int numPages = numPages();
        for (int i = 0; i < numPages; ++i) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                dirtiedPages.add(page);
                return dirtiedPages;
            }
        }
        // create a new page
        HeapPageId pid = new HeapPageId(getId(), numPages);
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        writePage(page);
        // read the newly created page
        page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        dirtiedPages.add(page);
        return dirtiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        assert rid != null;
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);

        ArrayList<Page> dirtiedPages = new ArrayList<>();
        dirtiedPages.add(page);
        return dirtiedPages;
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        private final TransactionId tid;
        private int pgNo;
        private Iterator<Tuple> iterator = null;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        private PageId currentPageId() {
            return new HeapPageId(getId(), pgNo);
        }

        private Iterator<Tuple> currentTupleIterator() throws DbException, TransactionAbortedException {
            Page page = Database.getBufferPool().getPage(tid, currentPageId(), Permissions.READ_ONLY);
            assert page instanceof HeapPage;
            return ((HeapPage) page).iterator();
        }

        public void open() throws DbException, TransactionAbortedException {
            pgNo = 0;
            iterator = currentTupleIterator();
        }

        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator == null) {
                return null;
            }
            if (iterator.hasNext()) {
                return iterator.next();
            }
            if (pgNo == numPages() - 1) {
                return null;
            }
            ++pgNo;
            iterator = currentTupleIterator();
            return readNext();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            if (iterator != null) {
                open();
            }
        }

        public void close() {
            iterator = null;
            super.close();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

