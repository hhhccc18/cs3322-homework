package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private static final TupleDesc TD = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of deleted tuples"});

    private final TransactionId tid;
    private DbIterator child;
    private boolean fetched = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param tid
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, DbIterator child) {
        this.tid = tid;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return TD;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        fetched = false;
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        fetched = false;
        // ensure that Operator.next == null
        super.close();
        super.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (fetched) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().deleteTuple(tid, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ++count;
        }
        Tuple result = new Tuple(TD);
        result.setField(0, new IntField(count));
        fetched = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }

}
