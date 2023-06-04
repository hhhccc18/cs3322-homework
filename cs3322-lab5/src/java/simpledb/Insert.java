package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
	private static final long serialVersionUID = 1L;
	private static final TupleDesc resultTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
	
	private final TransactionId tid;
	private DbIterator child;
	private final int tableId;
	
	private boolean isDone = false;
	
	/**
	 * Constructor.
	 *
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableId
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableId)
			throws DbException {
		if (child != null && // ???
				!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
			throw new DbException("TupleDesc mismatch");
		
		this.tid = t;
		this.child = child;
		this.tableId = tableId;
	}
	
	public TupleDesc getTupleDesc() {
		return Insert.resultTupleDesc;
	}
	
//	@Override
//	public void open() throws DbException, TransactionAbortedException {
//		super.open();
//	}
//
//	@Override
//	public void close() {
//		super.close();
//	}
	
	public void rewind() throws DbException, TransactionAbortedException {
//		this.child.rewind(); // but why are you doing this?
	}
	
	/**
	 * Inserts tuples read from child into the tableId specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if (this.isDone)
			return null;
		this.isDone = true;
		
		this.child.open();
		
		int count = 0;
		boolean hasException = false;
		while (this.child.hasNext()) {
			Tuple t = this.child.next();
			try {
				Database.getBufferPool().insertTuple(this.tid, this.tableId, t);
				count++;
			} catch (IOException e) {
				hasException = true;
			}
		}
		
		this.child.close();
		
		if (hasException)
			throw new DbException("IOException");
		
//		if (count == 0)
//			return null;
		
		return Tuple.fromArray(Insert.resultTupleDesc,
				IntField.fromInts(count));
	}
	
	@Override
	public DbIterator[] getChildren() {
		return new DbIterator[] { this.child };
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		if (children.length != 1)
			throw new IllegalArgumentException("Insert requires 1 child");
		
		this.child = children[0];
		this.isDone = false;
	}
}
