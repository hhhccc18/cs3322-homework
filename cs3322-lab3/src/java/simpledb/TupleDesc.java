package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	/**
	 * A help class to facilitate organizing the information of each field
	 * */
	public static class TDItem implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		/**
		 * The type of the field
		 * */
		public final Type fieldType;
		
		/**
		 * The name of the field
		 * */
		public final String fieldName;
		
		public TDItem(Type t, String n) {
			this.fieldType = t;
			this.fieldName = n;
		}
		
		@Override
		public String toString() {
			return this.fieldType.toString() + (this.fieldName != null ? "(" + fieldType + ")" : "");
		}
	}
	
	private final ArrayList<TDItem> tdItems;
	
	/**
	 * @return
	 *        An iterator which iterates over all the field TDItems
	 *        that are included in this TupleDesc
	 * */
	public Iterator<TDItem> iterator() {
		return tdItems.iterator();
	}
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. **Note that names may
	 *            be null.**
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
//		if (typeAr == null || typeAr.length == 0)
//			throw new IllegalArgumentException("typeAr must contain at least one entry");
	
		this.tdItems = IntStream.range(0, typeAr.length)
				.mapToObj(i -> new TDItem(typeAr[i], fieldAr[i]))
				.collect(Collectors.toCollection(ArrayList::new));
	}
	
	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		this.tdItems = Arrays.stream(typeAr)
				.map(type -> new TDItem(type, null))
				.collect(Collectors.toCollection(ArrayList::new));
	}
	
	/**
	 * Constructor. This is used in merge(), otherwise should not be used directly.
	 */
	private TupleDesc(ArrayList<TDItem> tdItems) {
		this.tdItems = tdItems.stream()
				.map(item -> new TDItem(item.fieldType, item.fieldName))
				.collect(Collectors.toCollection(ArrayList::new));
	}
	
	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return this.tdItems.size();
	}
	
	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if @i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		try {
			return this.tdItems.get(i).fieldName;
		}
		catch (IndexOutOfBoundsException e) {
			throw new NoSuchElementException("i out of range");
		}
	}
	
	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if @i is not a valid field reference.
	 */
	public Type getFieldType(int i) throws NoSuchElementException {
		if (i < 0 || i >= this.numFields())
			throw new NoSuchElementException("i out of range");
		
		return this.tdItems.get(i).fieldType;
	}
	
	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int fieldNameToIndex(String name) throws NoSuchElementException {
		if (name == null)
			throw new NoSuchElementException("name is null");

		return IntStream.range(0, this.numFields())
				.filter(i -> name.equals(this.tdItems.get(i).fieldName))
				.findFirst()
				.orElseThrow(() -> new NoSuchElementException("Field not found"));
	}
	
	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		return this.tdItems.stream()
				.mapToInt(item -> item.fieldType.getLen())
				.sum();
	}
	
	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 *
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
		return new TupleDesc(
				Stream.concat(td1.tdItems.stream(), td2.tdItems.stream())
						.collect(Collectors.toCollection(ArrayList::new)));
	}
	
	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof TupleDesc))
			return false;
		
		TupleDesc other = (TupleDesc) o;
		if (this.getSize() != other.getSize())
			return false;

		return IntStream.range(0, this.tdItems.size())
				.allMatch(i -> this.tdItems.get(i).fieldType
						== other.tdItems.get(i).fieldType);
	}
	
	@Override
	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
//		throw new UnsupportedOperationException("unimplemented");
		
		return Arrays.hashCode(this.tdItems.stream()
				.map(item -> item.fieldType)
				.toArray());
	}
	
	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 *
	 * @return String describing this descriptor.
	 */
	@Override
	public String toString() {
		return this.tdItems.stream()
				.map(TDItem::toString)
				.collect(Collectors.joining(", "));
	}
	
	/**
	 * @return a copy of this TupleDesc.
	 */
	@Deprecated
	public TupleDesc makeCopy() {
		return new TupleDesc(this.tdItems);
	}
	// Don't make a copy, or it will cost very much time and memory.
}
