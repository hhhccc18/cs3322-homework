package simpledb;

import java.io.Serializable;
import java.util.*;

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
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final TDItem[] TDItems;
    private final Map<String, Integer> name2index = new HashMap<>();
    private final int tupleSize;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(TDItems).iterator();
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
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        TDItems = new TDItem[typeAr.length];
        int x = 0;
        for (int i = 0; i < typeAr.length; ++i) {
            String name = null;
            if (fieldAr != null) {
                name = fieldAr[i];
                name2index.put(name, i);
            }
            TDItems[i] = new TDItem(typeAr[i], name);
            x += typeAr[i].getLen();
        }
        this.tupleSize = x;
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
        this(typeAr, null);
    }

    private TupleDesc(TupleDesc td1, TupleDesc td2) {
        this.TDItems = new TDItem[td1.numFields() + td2.numFields()];
        System.arraycopy(td1.TDItems, 0, this.TDItems, 0, td1.numFields());
        System.arraycopy(td2.TDItems, 0, this.TDItems, td1.numFields(), td2.numFields());
        this.name2index.putAll(td1.name2index);
        for (Map.Entry<String, Integer> entry : td2.name2index.entrySet()) {
            this.name2index.put(entry.getKey(), td1.numFields() + entry.getValue());
        }
        this.tupleSize = td1.tupleSize + td2.tupleSize;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDItems.length;
    }

    private void checkRange(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields())
            throw new NoSuchElementException();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        checkRange(i);
        return TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        checkRange(i);
        return TDItems[i].fieldType;
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
        Integer index = name2index.get(name);
        if (index == null) {
            throw new NoSuchElementException();
        }
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return tupleSize;
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
        return new TupleDesc(td1, td2);
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
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (getClass() != o.getClass())
            return false;
        TupleDesc other = (TupleDesc) o;
        if (numFields() != other.numFields())
            return false;
        for (int i = 0; i < numFields(); ++i) {
            if (!TDItems[i].fieldType.equals(other.TDItems[i].fieldType)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < numFields(); ++i) {
            s.append(TDItems[i].fieldType).append("(").append(TDItems[i].fieldName).append(")");
            if (i < numFields() - 1) {
                s.append(", ");
            }
        }
        return s.toString();
    }
}
