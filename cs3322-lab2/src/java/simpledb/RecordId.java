package simpledb;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private PageId pid;
	private int index;
	
	/**
	 * Creates a new RecordId referring to the specified PageId and tuple
	 * number.
	 *
	 * @param pid
	 *            the PageId of the page on which the tuple resides
	 * @param tupleno
	 *            the tuple number within the page.
	 */
	public RecordId(PageId pid, int tupleno) {
		this.pid = pid;
		this.index = tupleno;
	}
	
	/**
	 * @return the tuple number this RecordId references.
	 */
	public int tupleno() {
		return this.index;
	}
	
	/**
	 * @return the page id this RecordId references.
	 */
	public PageId getPageId() {
		return this.pid;
	}
	
	/**
	 * Two RecordId objects are considered equal if they represent the same
	 * tuple.
	 *
	 * @return True if this and o represent the same tuple
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof RecordId))
			return false;
		
		RecordId other = (RecordId) o;
		return this.pid.equals(other.pid) && this.index == other.index;
	}
	
	/**
	 * You should implement the hashCode() so that two equal RecordId instances
	 * (with respect to equals()) have the same hashCode().
	 *
	 * @return An int that is the same for equal RecordId objects.
	 */
	@Override
	public int hashCode() {
		return Objects.hash(this.pid, this.index);
	}
	
	/**
	 * @return a copy of this RecordId.
	 */
	@Deprecated
	public RecordId makeCopy() {
		return new RecordId(this.pid, this.index);
	}
}
