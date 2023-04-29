package simpledb;

import java.util.Arrays;
import java.util.stream.IntStream;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
	
	private final int tableId;
	private final int pageNumber;
	
	/**
	 * Constructor. Create a page id structure for a specific page of a
	 * specific table.
	 *
	 * @param tableId The table that is being referenced
	 * @param pgNo The page number in that table.
	 */
	public HeapPageId(int tableId, int pgNo) {
		this.tableId = tableId;
		this.pageNumber = pgNo;
	}
	
	/** @return the table associated with this PageId */
	public int getTableId() {
		return this.tableId;
	}
	
	/**
	 * @return the page number in the table getTableId() associated with
	 *   this PageId
	 */
	public int pageNumber() {
		return this.pageNumber;
	}
	
	/**
	 * @return a hash code for this page, represented by the concatenation of
	 *   the table number and the page number (needed if a PageId is used as a
	 *   key in a hash table in the BufferPool, for example.)
	 * @see BufferPool
	 */
	public int hashCode() {
		return java.util.Objects.hash(this.tableId, this.pageNumber);
	}
	
	/**
	 * Compares one PageId to another.
	 *
	 * @param o The object to compare against (must be a PageId)
	 * @return true if the objects are equal (e.g., page numbers and table
	 *   ids are the same)
	 */
	public boolean equals(Object o) {
		if (!(o instanceof PageId))
			return false;
		
		PageId other = (PageId) o;
		return this.tableId == other.getTableId()
				&& this.pageNumber == other.pageNumber();
	}
	
	/**
	 *  Return a representation of this object as an array of
	 *  integers, for writing to disk.  Size of returned array must contain
	 *  number of integers that corresponds to number of args to one of the
	 *  constructors.
	 */
	public int[] serialize() {
		return new int[] { this.tableId, this.pageNumber };
	}
	
}
