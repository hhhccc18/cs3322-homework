package simpledb;

import simpledb.Predicate.Op;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @see simpledb.BTreeLeafPage#BTreeLeafPage
 * @see simpledb.BTreeInternalPage#BTreeInternalPage
 * @see simpledb.BTreeHeaderPage#BTreeHeaderPage
 * @see simpledb.BTreeRootPtrPage#BTreeRootPtrPage
 * @author .
 */
public class BTreeFile implements DbFile {

	private final File f;
	private final TupleDesc tupleDesc;
	private final int tableId;
	private int keyField;

	/**
	 * Constructs a B+ tree file backed by the specified file.
	 *
	 * @param f - the file that stores the on-disk backing store for this B+ tree
	 *            file.
	 * @param key - the field which index is keyed on
	 * @param tupleDesc - the tuple descriptor of tuples in the file
	 */
	public BTreeFile(File f, int key, TupleDesc tupleDesc) {
		this.f = f;
		this.tableId = f.getAbsoluteFile().hashCode();
		this.keyField = key;
		this.tupleDesc = tupleDesc;
	}

	/**
	 * Returns the File backing this BTreeFile on disk.
	 */
	public File getFile() {
		return this.f;
	}

	/**
	 * Returns an ID uniquely identifying this BTreeFile. Implementation note:
	 * you will need to generate this tableId somewhere and ensure that each
	 * BTreeFile has a "unique id," and that you always return the same value for
	 * a particular BTreeFile. We suggest hashing the absolute file name of the
	 * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this BTreeFile.
	 */
	public int getId() {
		return this.tableId;
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}

	/**
	 * Read a page from the file on disk. This should not be called directly
	 * but should be called from the BufferPool via getPage()
	 *
	 * @param pid - the id of the page to read from disk
	 * @return the page constructed from the contents on disk
	 */
	public Page readPage(PageId pid) {
		BTreePageId id = (BTreePageId) pid;
		BufferedInputStream bis = null;

		try {
			bis = new BufferedInputStream(new FileInputStream(f));
			if (id.pgcateg() == BTreePageId.ROOT_PTR) {
				byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
				int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BTreeRootPtrPage.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				BTreeRootPtrPage p = new BTreeRootPtrPage(id, pageBuf);
				return p;
			}
			else {
				byte[] pageBuf = new byte[BufferPool.getPageSize()];
				if (bis.skip(BTreeRootPtrPage.getPageSize() + (id.pageNumber() - 1) * BufferPool.getPageSize()) !=
						BTreeRootPtrPage.getPageSize() + (id.pageNumber() - 1) * BufferPool.getPageSize()) {
					throw new IllegalArgumentException(
							"Unable to seek to correct place in BTreeFile");
				}
				int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
				if (retval == -1) {
					throw new IllegalArgumentException("Read past end of table");
				}
				if (retval < BufferPool.getPageSize()) {
					throw new IllegalArgumentException("Unable to read "
							+ BufferPool.getPageSize() + " bytes from BTreeFile");
				}
				Debug.log(1, "BTreeFile.readPage: read page %d", id.pageNumber());
				if (id.pgcateg() == BTreePageId.INTERNAL) {
					BTreeInternalPage p = new BTreeInternalPage(id, pageBuf, keyField);
					return p;
				}
				else if (id.pgcateg() == BTreePageId.LEAF) {
					BTreeLeafPage p = new BTreeLeafPage(id, pageBuf, keyField);
					return p;
				}
				else { // id.pgcateg() == BTreePageId.HEADER
					BTreeHeaderPage p = new BTreeHeaderPage(id, pageBuf);
					return p;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			// Close the file on success or error
			try {
				if (bis != null)
					bis.close();
			} catch (IOException ioe) {
				// Ignore failures closing the file
			}
		}
	}

	/**
	 * Write a page to disk.  This should not be called directly but should
	 * be called from the BufferPool when pages are flushed to disk
	 *
	 * @param page - the page to write to disk
	 */
	public void writePage(Page page) throws IOException {
		BTreePageId id = (BTreePageId) page.getId();
		
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		if (id.pgcateg() == BTreePageId.ROOT_PTR) {
			rf.write(data);
			rf.close();
		}
		else {
			rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().pageNumber() - 1) * BufferPool.getPageSize());
			rf.write(data);
			rf.close();
		}
	}
	
	/**
	 * Returns the number of pages in this BTreeFile.
	 */
	public int numPages() {
		// we only ever write full pages
		return (int) ((this.f.length() - BTreeRootPtrPage.getPageSize())/ BufferPool.getPageSize());
	}

	/**
	 * Returns the index of the field that this B+ tree is keyed on
	 */
	public int keyField() {
		return this.keyField;
	}

	/**
	 * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
	 * the left-most page possibly containing the key field f. It locks all internal
	 * nodes along the path to the leaf node with READ_ONLY permission, and locks the
	 * leaf node with permission perm.
	 * <p>
	 * If value is null, it finds the left-most leaf page -- used for the iterator
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param value - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 *
	 */
	private BTreeLeafPage findLeafPage(TransactionId tid, HashMap<PageId, Page> dirtyPages,
	                                   BTreePageId pid, Permissions perm, Field value)
			throws DbException, TransactionAbortedException {
		Page page = this.getPage(tid, dirtyPages, pid, perm);
		if (page instanceof BTreeLeafPage)
			return (BTreeLeafPage) page;
		
		BTreeInternalPage cur = (BTreeInternalPage) page;
		if (!cur.iterator().hasNext())
			return null;

		assert cur.iterator().hasNext();
//		if (!cur.reverseIterator().hasNext()) {
//			throw new DbException(cur.debug() + "\n" +
//					String.format("cur.getNumEntries() = %d, stream() = %s, reversedStream() = %s",
//					cur.getNumEntries(),
//					cur.entryStream().map(BTreeEntry::toString)
//							.collect(Collectors.joining(", "))
//			, cur.reversedEntryStream().map(BTreeEntry::toString)
//					.collect(Collectors.joining(", "))));
//		}
		assert cur.reverseIterator().hasNext();
		assert cur.reverseIterator().next().getRightChild() != null;
		
		BTreePageId nextPage = cur.entryStream()
				.filter(o -> (value == null || value.compare(Op.LESS_THAN_OR_EQ, o.getKey())))
				.map(BTreeEntry::getLeftChild)
				.findFirst()
				.orElse(cur.getRightmostChild());
				
		if (nextPage == null)
			throw new DbException(String.format("wtf, cur.getRightmostChild = %s, cur.iterator.next() = %s", cur.getRightmostChild().toString(), cur.iterator().next().toString()));
		
		return this.findLeafPage(tid, dirtyPages, nextPage, perm, value);
	}
	
	/**
	 * Convenience method to find a leaf page when there is no dirtyPages HashMap.
	 * Used by the BTreeFile iterator.
	 * @see #findLeafPage(TransactionId, HashMap, BTreePageId, Permissions, Field)
	 *
	 * @param tid - the transaction id
	 * @param pid - the current page being searched
	 * @param perm - the permissions with which to lock the leaf page
	 * @param f - the field to search for
	 * @return the left-most leaf page possibly containing the key field f
	 *
	 */
	BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid, Permissions perm,
			Field f)
					throws DbException, TransactionAbortedException {
		return this.findLeafPage(tid, new HashMap<>(), pid, perm, f);
	}

	/**
	 * Split a leaf page to make room for new tuples and recursively split the parent node
	 * as needed to accommodate a new entry. The new entry should have a key matching the key field
	 * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
	 * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
	 * pointers as needed.
	 * <p>
	 * Return the leaf page into which a new tuple with key field "field" should be inserted.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page to split
	 * @param field - the key field of the tuple to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 *
	 * @return the leaf page into which the new tuple should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected BTreeLeafPage splitLeafPage(TransactionId tid, HashMap<PageId, Page> dirtyPages, BTreeLeafPage page, Field field)
			throws DbException, IOException, TransactionAbortedException {
        // Split the leaf page by adding a new page on the right of the existing
		// page and moving half of the tuples to the new page. Copy the middle key up
		// into the parent page, and recursively split the parent as needed to accommodate
		// the new entry. getParentWithEmptySlots() will be useful here. Don't forget to update
		// the sibling pointers of all the affected leaf pages. Return the page into which a
		// tuple with the given key field should be inserted.
		
		assert(page.getNumEmptySlots() == 0);
		int right = page.getNumTuples() / 2, left = page.getNumTuples() - right;
		
		BTreeInternalPage par = this.getParentWithEmptySlots(tid, dirtyPages, page.getParentId(), field);
		BTreeLeafPage other = (BTreeLeafPage) this.getEmptyPage(tid, dirtyPages, BTreePageId.LEAF);
		
		dirtyPages.put(page.getId(), page);
		dirtyPages.put(other.getId(), other);
		dirtyPages.put(par.getId(), par);
		
		page.reversedStream()
				.limit(right)
				.collect(Collectors.toList()) // collect() is to force the stream to be evaluated
				.forEach(t -> {
					try {
						page.deleteTuple(t);
						other.insertTuple(t);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		BTreeEntry mid = new BTreeEntry(
				page.reverseIterator().next().getField(this.keyField),
				page.getId(), other.getId());
		par.insertEntry(mid);
		
		other.setParentId(par.getId());
		other.setLeftSiblingId(page.getId());
		other.setRightSiblingId(page.getRightSiblingId());
		
		if (page.getRightSiblingId() != null)
			((BTreeLeafPage) this
					.getPage(tid, dirtyPages, page.getRightSiblingId(), Permissions.READ_WRITE))
					.setLeftSiblingId(other.getId());
		
		page.setRightSiblingId(other.getId());
		
		return field.compare(Op.LESS_THAN_OR_EQ, mid.getKey()) ? page : other;
	}
	
	/**
	 * Split an internal page to make room for new entries and recursively split its parent page
	 * as needed to accommodate a new entry. The new entry for the parent should have a key matching
	 * the middle key in the original internal page being split (this key is "pushed up" to the parent).
	 * The child pointers of the new parent entry should point to the two internal pages resulting
	 * from the split. Update parent pointers as needed.
	 * <p>
	 * Return the internal page into which an entry with key field "field" should be inserted
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page to split
	 * @param field - the key field of the entry to be inserted after the split is complete. Necessary to know
	 * which of the two pages to return.
	 * @see #getParentWithEmptySlots(TransactionId, HashMap, BTreePageId, Field)
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 *
	 * @return the internal page into which the new entry should be inserted
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected BTreeInternalPage splitInternalPage(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreeInternalPage page, Field field)
					throws DbException, IOException, TransactionAbortedException {
		// Similar to splitLeafPage
		
		assert(page.getNumEmptySlots() == 0);
		int right = page.getNumEntries() / 2, left = page.getNumEntries() - right;
		
		BTreeInternalPage par = this.getParentWithEmptySlots(tid, dirtyPages, page.getParentId(), field);
		BTreeInternalPage other =
				(BTreeInternalPage) this.getEmptyPage(tid, dirtyPages, BTreePageId.INTERNAL);
		
		dirtyPages.put(page.getId(), page);
		dirtyPages.put(other.getId(), other);
		dirtyPages.put(par.getId(), par);
		
		page.reversedEntryStream()
				.limit(right)
				.collect(Collectors.toList())
				.forEach(o -> {
					try {
						page.deleteKeyAndRightChild(o);
						other.insertEntry(o);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		BTreeEntry mid = new BTreeEntry(
				page.reverseIterator().next().getKey(),
				page.getId(), other.getId());
		
		page.deleteKeyAndRightChild(page.reverseIterator().next()); // wssb
		par.insertEntry(mid);
		
		other.setParentId(par.getId());
		this.updateParentPointers(tid, dirtyPages, other);
		
		return field.compare(Op.LESS_THAN_OR_EQ, mid.getKey()) ? page : other;
	}
	
	/**
	 * Method to encapsulate the process of getting a parent page ready to accept new entries.
	 * This may mean creating a page to become the new root of the tree, splitting the existing
	 * parent page if there are no empty slots, or simply locking and returning the existing parent page.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param parentId - the id of the parent. Maybe an internal page or the RootPtr page
	 * @param field - the key of the entry which will be inserted. Needed in case the parent must be split
	 * to accommodate the new entry
	 * @return the parent page, guaranteed to have at least one empty slot
	 * @see #splitInternalPage(TransactionId, HashMap, BTreeInternalPage, Field)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {
		
		BTreeInternalPage parent = null;
		
		// create a parent node if necessary
		// this will be the new root of the tree
		if (parentId.pgcateg() == BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getEmptyPage(tid, dirtyPages, BTreePageId.INTERNAL);

			// update the root pointer
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtyPages,
					BTreeRootPtrPage.getId(tableId), Permissions.READ_WRITE);
			BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
			rootPtr.setRootId(parent.getId());

			// update the previous root to now point to this new root.
			BTreePage prevRootPage = (BTreePage)getPage(tid, dirtyPages, prevRootId, Permissions.READ_WRITE);
			prevRootPage.setParentId(parent.getId());
		}
		else {
			// lock the parent page
			parent = (BTreeInternalPage) getPage(tid, dirtyPages, parentId,
					Permissions.READ_WRITE);
		}

		// split the parent if needed
		if (parent.getNumEmptySlots() == 0) {
			parent = splitInternalPage(tid, dirtyPages, parent, field);
		}

		return parent;

	}

	/**
	 * Helper function to update the parent pointer of a node.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - id of the parent node
	 * @param child - id of the child node to be updated with the parent pointer
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointer(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
			throws DbException, IOException, TransactionAbortedException {

		BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

		if (!p.getParentId().equals(pid)) {
			p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
			p.setParentId(pid);
		}

	}
	
	/**
	 * Update the parent pointer of every child of the given page so that it correctly points to
	 * the parent
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the parent page
	 * @see #updateParentPointer(TransactionId, HashMap, BTreePageId, BTreePageId)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void updateParentPointers(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page)
			throws DbException, IOException, TransactionAbortedException{
		Iterator<BTreeEntry> it = page.iterator();
		BTreePageId pid = page.getId();
		BTreeEntry e = null;
		while (it.hasNext()) {
			e = it.next();
			updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
		}
		if (e != null) {
			updateParentPointer(tid, dirtypages, pid, e.getRightChild());
		}
	}
	
	/**
	 * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
	 * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
	 * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
	 * presumably they will soon be dirtied by this transaction.
	 *
	 * This method is needed to ensure that page updates are not lost if the same pages are
	 * accessed multiple times.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pid - the id of the requested page
	 * @param perm - the requested permissions on the page
	 * @return the requested page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	Page getPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
			throws DbException, TransactionAbortedException {
		if (dirtypages.containsKey(pid)) {
			return dirtypages.get(pid);
		}
		else {
			Page p = Database.getBufferPool().getPage(tid, pid, perm);
			if (perm == Permissions.READ_WRITE) {
				dirtypages.put(pid, p);
			}
			return p;
		}
	}

	/**
	 * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
	 * May cause pages to split if the page where tuple t belongs is full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, HashMap, BTreeLeafPage, Field)
	 */
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HashMap<PageId, Page> dirtyPages = new HashMap<PageId, Page>();

		// get a read lock on the root pointer page and use it to locate the root page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtyPages);
		BTreePageId rootId = rootPtr.getRootId();

		if (rootId == null) { // the root has just been created, so set the root pointer to point to it
			rootId = new BTreePageId(tableId, numPages(), BTreePageId.LEAF);
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtyPages, BTreeRootPtrPage.getId(tableId), Permissions.READ_WRITE);
			rootPtr.setRootId(rootId);
		}

		// find and lock the left-most leaf page corresponding to the key field,
		// and split the leaf page if there are no more slots available
		BTreeLeafPage leafPage = this.findLeafPage(tid, dirtyPages, rootId, Permissions.READ_WRITE, t.getField(keyField));
		assert(leafPage != null);
		if (leafPage.getNumEmptySlots() == 0) {
			leafPage = splitLeafPage(tid, dirtyPages, leafPage, t.getField(keyField));
		}

		// insert the tuple into the leaf page
		leafPage.insertTuple(t);
		
		return new ArrayList<>(dirtyPages.values());
	}
	
	/**
	 * Handle the case when a B+ tree page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the page which is less than half full
	 * @see #handleMinOccupancyLeafPage(TransactionId, HashMap, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 * @see #handleMinOccupancyInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePage page)
			throws DbException, IOException, TransactionAbortedException {
		BTreePageId parentId = page.getParentId();
		BTreeEntry leftEntry = null;
		BTreeEntry rightEntry = null;
		BTreeInternalPage parent = null;

		// find the left and right siblings through the parent so we make sure they have
		// the same parent as the page. Find the entries in the parent corresponding to
		// the page and siblings
		if (parentId.pgcateg() != BTreePageId.ROOT_PTR) {
			parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
			for (BTreeEntry e : parent) {
				if (e.getLeftChild().equals(page.getId())) {
					rightEntry = e;
					break;
				}
				else if (e.getRightChild().equals(page.getId())) {
					leftEntry = e;
				}
			}
		}
		
		if (page.getId().pgcateg() == BTreePageId.LEAF) {
			handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
		}
		else { // BTreePageId.INTERNAL
			handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
		}
	}
	
	/**
	 * Handle the case when a leaf page becomes less than half full due to deletions.
	 * If one of its siblings has extra tuples, redistribute those tuples.
	 * Otherwise merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the leaf page which is less than half full
	 * @param parent - the parent of the leaf page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeLeafPages(TransactionId, HashMap, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage,  BTreeEntry, boolean)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page,
			BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
			throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();
		
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if (leftSiblingId != null) {
			BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
			}
		}
		else if (rightSiblingId != null) {
			BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some tuples from it
			if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
			}
		}
	}
	
	/**
	 * Steal tuples from a sibling and copy them to the given page so that both pages are at least
	 * half full.  Update the parent's entry so that the key matches the key field of the first
	 * tuple in the right-hand page.
	 *
	 * @param page - the leaf page which is less than half full
	 * @param sibling - the sibling which has tuples to spare
	 * @param parent - the parent of the two leaf pages
	 * @param entry - the entry in the parent pointing to the two leaf pages
	 * @param isRightSibling - whether the sibling is a right-sibling
	 *
	 * @throws DbException
	 */
	protected void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
			BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
		// Move some of the tuples from the sibling to the page so
		// that the tuples are evenly distributed.
		// Be sure to update the corresponding parent entry.
		
		int pageSize = page.getNumTuples(), siblingSize = sibling.getNumTuples();
		assert pageSize * 2 < page.getMaxTuples() && siblingSize * 2 > sibling.getMaxTuples();
		
		(isRightSibling ? sibling.stream() : sibling.reversedStream())
				.limit((siblingSize - pageSize) / 2)
				.collect(Collectors.toList())
				.forEach(t -> {
					try {
						sibling.deleteTuple(t);
						page.insertTuple(t);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		entry.setKey((isRightSibling ? page : sibling)
				.reverseIterator().next().getField(this.keyField));
		parent.updateEntry(entry);
	}

	/**
	 * Handle the case when an internal page becomes less than half full due to deletions.
	 * If one of its siblings has extra entries, redistribute those entries.
	 * Otherwise, merge with one of the siblings. Update pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param parent - the parent of the internal page
	 * @param leftEntry - the entry in the parent pointing to the given page and its left-sibling
	 * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
	 * @see #mergeInternalPages(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromLeftInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 * @see #stealFromRightInternalPage(TransactionId, HashMap, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void handleMinOccupancyInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages,
			BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
					throws DbException, IOException, TransactionAbortedException {
		BTreePageId leftSiblingId = null;
		BTreePageId rightSiblingId = null;
		if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
		if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();
		
		int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries()/2; // ceiling
		if (leftSiblingId != null) {
			BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
			// if the left sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
			}
			else {
				stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
			}
		}
		else if (rightSiblingId != null) {
			BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
			// if the right sibling is at minimum occupancy, merge with it. Otherwise
			// steal some entries from it
			if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
				mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
			else {
				stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
			}
		}
	}
	
	/**
	 * Steal entries from the left sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
	 * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param leftSibling - the left sibling which has entries to spare
	 * @param parent - the parent of the two internal pages
	 * @param parentEntry - the entry in the parent pointing to the two internal pages
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected void stealFromLeftInternalPage(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent,
			BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {
		
		dirtyPages.put(page.getId(), page);
		dirtyPages.put(leftSibling.getId(), leftSibling);
		dirtyPages.put(parent.getId(), parent);
		
		int pageSize = page.getNumEntries(), siblingSize = leftSibling.getNumEntries();
		assert pageSize * 2 < page.getMaxEntries() && siblingSize * 2 > leftSibling.getMaxEntries();
		
		page.insertEntry(new BTreeEntry(
				parentEntry.getKey(),
				leftSibling.reverseIterator().next().getRightChild(),
				page.iterator().next().getLeftChild())); // insert the old entry
		
		leftSibling.reversedEntryStream()
				.limit((siblingSize - pageSize) / 2 - 1)
				.collect(Collectors.toList())
				.forEach(o -> {
					try {
						leftSibling.deleteKeyAndRightChild(o);
						page.insertEntry(o);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		BTreeEntry mid = leftSibling.reverseIterator().next();
		parentEntry.setKey(mid.getKey());
		parent.updateEntry(parentEntry);
		leftSibling.deleteKeyAndRightChild(mid);
		
		this.updateParentPointers(tid, dirtyPages, page);
	}
	
	/**
	 * Steal entries from the right sibling and copy them to the given page so that both pages are at least
	 * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
	 * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
	 * to the parent.  Update parent pointers as needed.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param page - the internal page which is less than half full
	 * @param rightSibling - the right sibling which has entries to spare
	 * @param parent - the parent of the two internal pages
	 * @param parentEntry - the entry in the parent pointing to the two internal pages
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected void stealFromRightInternalPage(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent,
			BTreeEntry parentEntry) throws DbException, IOException, TransactionAbortedException {
		
		dirtyPages.put(page.getId(), page);
		dirtyPages.put(rightSibling.getId(), rightSibling);
		dirtyPages.put(parent.getId(), parent);
		
		int pageSize = page.getNumEntries(), siblingSize = rightSibling.getNumEntries();
		assert pageSize * 2 < page.getMaxEntries() && siblingSize * 2 > rightSibling.getMaxEntries();
		
		page.insertEntry(new BTreeEntry(
				parentEntry.getKey(),
				page.reverseIterator().next().getRightChild(),
				rightSibling.iterator().next().getLeftChild())); // insert the old entry
		
		rightSibling.entryStream()
				.limit((siblingSize - pageSize) / 2 - 1)
				.collect(Collectors.toList())
				.forEach(o -> {
					try {
						rightSibling.deleteKeyAndLeftChild(o);
						page.insertEntry(o);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		BTreeEntry mid = rightSibling.iterator().next();
		parentEntry.setKey(mid.getKey());
		parent.updateEntry(parentEntry);
		rightSibling.deleteKeyAndLeftChild(mid);
		
		this.updateParentPointers(tid, dirtyPages, page);
	}
	
	/**
	 * Merge two leaf pages by moving all tuples from the right page to the left page.
	 * Delete the corresponding key and right child pointer from the parent, and recursively
	 * handle the case when the parent gets below minimum occupancy.
	 * Update sibling pointers as needed, and make the right page available for reuse.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left leaf page
	 * @param rightPage - the right leaf page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected void mergeLeafPages(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {
		// Move all the tuples from the right page to the left page, update
		// the sibling pointers, and make the right page available for reuse.
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here
		
		// Just the reversal of splitLeafPage
		assert leftPage.getNumTuples() + rightPage.getNumTuples() <= leftPage.getMaxTuples();
		
		dirtyPages.put(leftPage.getId(), leftPage);
		dirtyPages.put(parent.getId(), parent);
		
		rightPage.stream()
				.collect(Collectors.toList())
				.forEach(t -> {
					try {
						rightPage.deleteTuple(t);
						leftPage.insertTuple(t);
					} catch (DbException e) {
						e.printStackTrace();
					}
				});
		
		this.deleteParentEntry(tid, dirtyPages, leftPage, parent, parentEntry);
		
		leftPage.setRightSiblingId(rightPage.getRightSiblingId());
		if (rightPage.getRightSiblingId() != null) {
			((BTreeLeafPage) getPage(tid, dirtyPages,
					rightPage.getRightSiblingId(), Permissions.READ_WRITE))
					.setLeftSiblingId(leftPage.getId());
		}
		
		this.setEmptyPage(tid, dirtyPages, rightPage.getId().pageNumber());
	}

	/**
	 * Merge two internal pages by moving all entries from the right page to the left page
	 * and "pulling down" the corresponding key from the parent entry.
	 * Delete the corresponding key and right child pointer from the parent, and recursively
	 * handle the case when the parent gets below minimum occupancy.
	 * Update parent pointers as needed, and make the right page available for reuse.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the left internal page
	 * @param rightPage - the right internal page
	 * @param parent - the parent of the two pages
	 * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
	 * @see #deleteParentEntry(TransactionId, HashMap, BTreePage, BTreeInternalPage, BTreeEntry)
	 * @see #updateParentPointers(TransactionId, HashMap, BTreeInternalPage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected void mergeInternalPages(TransactionId tid, HashMap<PageId, Page> dirtyPages,
			BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {
        // Move all the entries from the right page to the left page, update
		// the parent pointers of the children in the entries that were moved,
		// and make the right page available for reuse
		// Delete the entry in the parent corresponding to the two pages that are merging -
		// deleteParentEntry() will be useful here
		
		// Just the reversal of splitInternalPage
		
		assert leftPage.getNumEntries() + rightPage.getNumEntries() < leftPage.getMaxEntries();
		
		dirtyPages.put(leftPage.getId(), leftPage);
		dirtyPages.put(parent.getId(), parent);
		
		BTreeEntry mid = new BTreeEntry(parentEntry.getKey(),
				leftPage.reverseIterator().next().getRightChild(), // fuck
				rightPage.iterator().next().getLeftChild());
		leftPage.insertEntry(mid);
		
		rightPage.entryStream()
				.collect(Collectors.toList())
				.forEach(o -> {
					try {
						rightPage.deleteKeyAndRightChild(o);
						leftPage.insertEntry(o);
					} catch (DbException e1) {
						e1.printStackTrace();
					}
				});
		
		this.deleteParentEntry(tid, dirtyPages, leftPage, parent, parentEntry);
		this.updateParentPointers(tid, dirtyPages, leftPage);
		
		this.setEmptyPage(tid, dirtyPages, rightPage.getId().pageNumber());
	}
	
	/**
	 * Method to encapsulate the process of deleting an entry (specifically the key and right child)
	 * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
	 * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
	 * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
	 * merge with a sibling.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @param leftPage - the child remaining after the key and right child are deleted
	 * @param parent - the parent containing the entry to be deleted
	 * @param parentEntry - the entry to be deleted
	 * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private void deleteParentEntry(TransactionId tid, HashMap<PageId, Page> dirtypages,
			BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
					throws DbException, IOException, TransactionAbortedException {
		
		// delete the entry in the parent.  If
		// the parent is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		parent.deleteKeyAndRightChild(parentEntry);
		int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries()/2; // ceiling
		if (parent.getNumEmptySlots() == parent.getMaxEntries()) {
			// This was the last entry in the parent.
			// In this case, the parent (root node) should be deleted, and the merged
			// page will become the new root
			BTreePageId rootPtrId = parent.getParentId();
			if (rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
				throw new DbException("attempting to delete a non-root node");
			}
			BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
			leftPage.setParentId(rootPtrId);
			rootPtr.setRootId(leftPage.getId());

			// release the parent page for reuse
			setEmptyPage(tid, dirtypages, parent.getId().pageNumber());
		}
		else if (parent.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtypages, parent);
		}
	}

	/**
	 * Delete a tuple from this BTreeFile.
	 * May cause pages to merge or redistribute entries/tuples if the pages
	 * become less than half full.
	 *
	 * @param tid - the transaction id
	 * @param t - the tuple to delete
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node merges.
	 * @see #handleMinOccupancyPage(TransactionId, HashMap, BTreePage)
	 */
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		HashMap<PageId, Page> dirtyPages = new HashMap<PageId, Page>();

		BTreePageId pageId = new BTreePageId(tableId, t.getRecordId().getPageId().pageNumber(),
				BTreePageId.LEAF);
		BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtyPages, pageId, Permissions.READ_WRITE);
		page.deleteTuple(t);

		// if the page is below minimum occupancy, get some tuples from its siblings
		// or merge with one of the siblings
		int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples()/2; // ceiling
		if (page.getNumEmptySlots() > maxEmptySlots) {
			handleMinOccupancyPage(tid, dirtyPages, page);
		}
		
		return new ArrayList<>(dirtyPages.values());
	}

	/**
	 * Get a read lock on the root pointer page. Create the root pointer page and root page
	 * if necessary.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the root pointer page
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	BTreeRootPtrPage getRootPtrPage(TransactionId tid, HashMap<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
		synchronized(this) {
			if (f.length() == 0) {
				// create the root pointer page and the root page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
				byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
				bw.write(emptyRootPtrData);
				bw.write(emptyLeafData);
				bw.close();
			}
		}

		// get a read lock on the root pointer page
		return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableId), Permissions.READ_ONLY);
	}

	/**
	 * Get the page number of the first empty page in this BTreeFile.
	 * Creates a new page if none of the existing pages are empty.
	 *
	 * @param tid - the transaction id
	 * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
	 * @return the page number of the first empty page
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected int getEmptyPageNo(TransactionId tid, HashMap<PageId, Page> dirtypages)
			throws DbException, IOException, TransactionAbortedException {
		// get a read lock on the root pointer page and use it to locate the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
		BTreePageId headerId = rootPtr.getHeaderId();
		int emptyPageNo = 0;

		if (headerId != null) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
			int headerPageCount = 0;
			// try to find a header page with an empty slot
			while (headerPage != null && headerPage.getEmptySlot() == -1) {
				headerId = headerPage.getNextPageId();
				if (headerId != null) {
					headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
					headerPageCount++;
				}
				else {
					headerPage = null;
				}
			}

			// if headerPage is not null, it must have an empty slot
			if (headerPage != null) {
				headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
				int emptySlot = headerPage.getEmptySlot();
				headerPage.markSlotUsed(emptySlot, true);
				emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
			}
		}

		// at this point if headerId is null, either there are no header pages
		// or there are no free slots
		if (headerId == null) {
			synchronized(this) {
				// create the new page
				BufferedOutputStream bw = new BufferedOutputStream(
						new FileOutputStream(f, true));
				byte[] emptyData = BTreeInternalPage.createEmptyPageData();
				bw.write(emptyData);
				bw.close();
				emptyPageNo = numPages();
			}
		}

		return emptyPageNo;
	}
	
	/**
	 * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
	 * and creates a new page if none are available.  It wipes the page on disk and in the cache and
	 * returns a clean copy locked with read-write permission
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param pgcateg - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
	 * @return the new empty page
	 * @see #getEmptyPageNo(TransactionId, HashMap)
	 * @see #setEmptyPage(TransactionId, HashMap, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	private Page getEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtyPages, int pgcateg)
			throws DbException, IOException, TransactionAbortedException {
		// create the new page
		int emptyPageNo = this.getEmptyPageNo(tid, dirtyPages);
		BTreePageId newPageId = new BTreePageId(tableId, emptyPageNo, pgcateg);
		
		// write empty page to disk
		RandomAccessFile rf = new RandomAccessFile(f, "rw");
		rf.seek(BTreeRootPtrPage.getPageSize() + (emptyPageNo - 1) * BufferPool.getPageSize());
		rf.write(BTreePage.createEmptyPageData());
		rf.close();
		
		// make sure the page is not in the buffer pool	or in the local cache
		Database.getBufferPool().discardPage(newPageId);
		dirtyPages.remove(newPageId);
		
		return getPage(tid, dirtyPages, newPageId, Permissions.READ_WRITE);
	}

	/**
	 * Mark a page in this BTreeFile as empty. Find the corresponding header page
	 * (create it if needed), and mark the corresponding slot in the header page as empty.
	 *
	 * @param tid - the transaction id
	 * @param dirtyPages - the list of dirty pages which should be updated with all new dirty pages
	 * @param emptyPageNo - the page number of the empty page
	 * @see #getEmptyPage(TransactionId, HashMap, int)
	 *
	 * @throws DbException
	 * @throws IOException
	 * @throws TransactionAbortedException
	 */
	protected void setEmptyPage(TransactionId tid, HashMap<PageId, Page> dirtyPages, int emptyPageNo)
			throws DbException, IOException, TransactionAbortedException {

		// if this is the last page in the file (and not the only page), just
		// truncate the file
		// @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
//		synchronized(this) {
//			if (emptyPageNo == numPages()) {
//				if (emptyPageNo <= 1) {
//					// if this is the only page in the file, just return.
//					// It just means we have an empty root page
//					return;
//				}
//				long newSize = f.length() - BufferPool.getPageSize();
//				FileOutputStream fos = new FileOutputStream(f, true);
//				FileChannel fc = fos.getChannel();
//				fc.truncate(newSize);
//				fc.close();
//				fos.close();
//				return;
//			}
//		}

		// otherwise, get a read lock on the root pointer page and use it to locate
		// the first header page
		BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtyPages);
		BTreePageId headerId = rootPtr.getHeaderId();
		BTreePageId prevId = null;
		int headerPageCount = 0;

		// if there are no header pages, create the first header page and update
		// the header pointer in the BTreeRootPtrPage
		if (headerId == null) {
			rootPtr = (BTreeRootPtrPage) getPage(tid, dirtyPages, BTreeRootPtrPage.getId(tableId), Permissions.READ_WRITE);
			
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtyPages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			rootPtr.setHeaderId(headerId);
		}

		// iterate through all the existing header pages to find the one containing the slot
		// corresponding to emptyPageNo
		while (headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtyPages, headerId, Permissions.READ_ONLY);
			prevId = headerId;
			headerId = headerPage.getNextPageId();
			headerPageCount++;
		}

		// at this point headerId should either be null or set with
		// the headerPage containing the slot corresponding to emptyPageNo.
		// Add header pages until we have one with a slot corresponding to emptyPageNo
		while ((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
			BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtyPages, prevId, Permissions.READ_WRITE);
			
			BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtyPages, BTreePageId.HEADER);
			headerId = headerPage.getId();
			headerPage.init();
			headerPage.setPrevPageId(prevId);
			prevPage.setNextPageId(headerId);
			
			headerPageCount++;
			prevId = headerId;
		}

		// now headerId should be set with the headerPage containing the slot corresponding to
		// emptyPageNo
		BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtyPages, headerId, Permissions.READ_WRITE);
		int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
		headerPage.markSlotUsed(emptySlot, false);
	}

	/**
	 * get the specified tuples from the file based on its IndexPredicate value on
	 * behalf of the specified transaction. This method will acquire a read lock on
	 * the affected pages of the file, and may block until the lock can be
	 * acquired.
	 *
	 * @param tid - the transaction id
	 * @param ipred - the index predicate value to filter on
	 * @return an iterator for the filtered tuples
	 */
	public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
		return new BTreeSearchIterator(this, tid, ipred);
	}

	/**
	 * Get an iterator for all tuples in this B+ tree file in sorted order. This method
	 * will acquire a read lock on the affected pages of the file, and may block until
	 * the lock can be acquired.
	 *
	 * @param tid - the transaction id
	 * @return an iterator for all the tuples in this file
	 */
	public DbFileIterator iterator(TransactionId tid) {
		return new BTreeFileIterator(this, tid);
	}

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curPage = null;

	TransactionId tid;
	BTreeFile f;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 */
	public BTreeFileIterator(BTreeFile f, TransactionId tid) {
		this.f = f;
		this.tid = tid;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		curPage = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		it = curPage.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples or
	 * from the next page by following the right sibling pointer.
	 *
	 * @return the next tuple, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		if (it != null && !it.hasNext())
			it = null;

		while (it == null && curPage != null) {
			BTreePageId nextp = curPage.getRightSiblingId();
			if (nextp == null) {
				curPage = null;
			}
			else {
				curPage = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curPage.iterator();
				if (!it.hasNext())
					it = null;
			}
		}

		if (it == null)
			return null;
		return it.next();
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
		curPage = null;
	}
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

	Iterator<Tuple> it = null;
	BTreeLeafPage curp = null;

	TransactionId tid;
	BTreeFile f;
	IndexPredicate ipred;

	/**
	 * Constructor for this iterator
	 * @param f - the BTreeFile containing the tuples
	 * @param tid - the transaction id
	 * @param ipred - the predicate to filter on
	 */
	public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
		this.f = f;
		this.tid = tid;
		this.ipred = ipred;
	}

	/**
	 * Open this iterator by getting an iterator on the first leaf page applicable
	 * for the given predicate operation
	 */
	public void open() throws DbException, TransactionAbortedException {
		BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
				tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
		BTreePageId root = rootPtr.getRootId();
		if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
				|| ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, ipred.getField());
		}
		else {
			curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
		}
		it = curp.iterator();
	}

	/**
	 * Read the next tuple either from the current page if it has more tuples matching
	 * the predicate or from the next page by following the right sibling pointer.
	 *
	 * @return the next tuple matching the predicate, or null if none exists
	 */
	@Override
	protected Tuple readNext() throws TransactionAbortedException, DbException,
	NoSuchElementException {
		while (it != null) {

			while (it.hasNext()) {
				Tuple t = it.next();
				if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
					return t;
				}
				else if (ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
					// if the predicate was not satisfied and the operation is less than, we have
					// hit the end
					return null;
				}
				else if (ipred.getOp() == Op.EQUALS &&
						t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
					// if the tuple is now greater than the field passed in and the operation
					// is equals, we have reached the end
					return null;
				}
			}

			BTreePageId nextp = curp.getRightSiblingId();
			// if there are no more pages to the right, end the iteration
			if (nextp == null) {
				return null;
			}
			else {
				curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
						nextp, Permissions.READ_ONLY);
				it = curp.iterator();
			}
		}

		return null;
	}

	/**
	 * rewind this iterator back to the beginning of the tuples
	 */
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();
	}

	/**
	 * close the iterator
	 */
	public void close() {
		super.close();
		it = null;
	}
}
