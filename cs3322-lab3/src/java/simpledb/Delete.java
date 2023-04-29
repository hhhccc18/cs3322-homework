package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	private static final long serialVersionUID = 1L;
	private static final TupleDesc resultTupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
	
	private final TransactionId tid;
	private DbIterator child;
	
	private boolean isDone = false;
	
	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 *
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		this.tid = t;
		this.child = child;
	}
	
	public TupleDesc getTupleDesc() {
		return Delete.resultTupleDesc; // fuck
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
//		this.child.rewind(); // Why are you doing this?
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
		if (this.isDone)
			return null;
		this.isDone = true;
		
		this.child.open();
		
		int count = 0;
		boolean hasException = false;
		while (this.child.hasNext()) {
			Tuple t = this.child.next();
			try {
				Database.getBufferPool().deleteTuple(this.tid, t);
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
		// fuck
		
		return Tuple.fromArray(Delete.resultTupleDesc,
				IntField.fromInts(count));
	}
	
	public DbIterator[] getChildren() {
		return new DbIterator[] { this.child };
	}
	
	public void setChildren(DbIterator[] children) {
		if (children.length != 1)
			throw new IllegalArgumentException("Delete requires 1 child");
		
		this.child = children[0];
		this.isDone = false;
	}
	
}
