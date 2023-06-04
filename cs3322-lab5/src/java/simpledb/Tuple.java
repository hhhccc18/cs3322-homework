package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private TupleDesc tupleDesc;
	private final ArrayList<Field> fields;
	// Although this is final, but calling set() is still allowed.
	private RecordId recordId;
	
	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc
	 *            instance with at least one field.
	 */
	public Tuple(TupleDesc td) {
		this.tupleDesc = td; // .makeCopy();
		this.fields = new ArrayList<>(Arrays.asList(new Field[td.numFields()]));
	}
	
	private Tuple(TupleDesc td, ArrayList<Field> fields) {
		assert td.numFields() == fields.size();
		this.tupleDesc = td;
		this.fields = fields;
	}
	
	public static Tuple fromArrayList(TupleDesc td, ArrayList<Field> fields) {
		return new Tuple(td, fields);
	}
	
	public static Tuple fromArray(TupleDesc td, Field[] fields) {
		return new Tuple(td, new ArrayList<>(Arrays.asList(fields)));
	}
	
	/**
	 * @return The length of the tuple.
	 */
	public int length() {
		return this.tupleDesc.numFields();
	}
	
	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		return this.tupleDesc;
	}
	
	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 *         be null.
	 */
	public RecordId getRecordId() {
		return this.recordId;
	}
	
	/**
	 * Set the RecordId information for this tuple.
	 *
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
//		this.recordId = rid == null ? null : rid.makeCopy();
		this.recordId = rid;
	}
	
	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f) {
		this.fields.set(i, f);
	}
	
	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i) {
		return this.fields.get(i);
	}
	
	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 * <p>
	 * column1 \t column2 \t column3 \t...\t columnN
	 * <p>
	 * where \t is any whitespace (except a newline)
	 */
	public String toString() {
		return this.fields.stream()
				.map(Field::toString)
				.collect(Collectors.joining(" "));
	}
	
	/**
	 * @return
	 *        An iterator which iterates over all the fields of this tuple
	 * */
	public Iterator<Field> fieldsIterator() {
		return this.fields.iterator();
	}
	
	public Stream<Field> fieldsStream() {
		return this.fields.stream();
	}
	
	/**
	 * reset the TupleDesc of the tuple
	 * */
	public void resetTupleDesc(TupleDesc td) {
		this.tupleDesc = td; // .makeCopy();
	}
	
	public static Tuple merge(Tuple a, Tuple b) {
		return new Tuple(TupleDesc.merge(a.getTupleDesc(), b.getTupleDesc()),
				new ArrayList<Field>() {{
					this.addAll(a.fields);
					this.addAll(b.fields);
				}});
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Tuple))
			return false;
		
		Tuple o = (Tuple) obj;
		return this.tupleDesc.numFields() == o.tupleDesc.numFields()
				&& this.tupleDesc.equals(o.tupleDesc)
				&& this.fields.equals(o.fields);
	}
}
