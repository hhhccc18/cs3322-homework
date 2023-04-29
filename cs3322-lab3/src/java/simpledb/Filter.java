package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	private static final long serialVersionUID = 1L;
	
	private final Predicate p;
	private DbIterator child;
	
	/**
	 * Constructor accepts a predicate to apply and a child operator to read
	 * tuples to filter from.
	 *
	 * @param p
	 *            The predicate to filter tuples with
	 * @param child
	 *            The child operator
	 */
	public Filter(Predicate p, DbIterator child) {
		this.p = p;
		this.child = child;
		this.close();
	}
	
	public Predicate getPredicate() {
		return this.p;
	}
	
	public TupleDesc getTupleDesc() {
		return this.child.getTupleDesc();
	}
	
	@Override
	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		super.open();
		this.child.open();
	}
	
	@Override
	public void close() {
		super.close();
		this.child.close();
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		this.child.rewind();
	}
	
	/**
	 * AbstractDbIterator.readNext implementation. Iterates over tuples from the
	 * child operator, applying the predicate to them and returning those that
	 * pass the predicate (i.e. for which the Predicate.filter() returns true.)
	 *
	 * @return The next tuple that passes the filter, or null if there are no
	 *         more tuples
	 * @see Predicate#filter
	 */
	protected Tuple fetchNext() throws NoSuchElementException,
			TransactionAbortedException, DbException {
		while (this.child.hasNext()) {
			Tuple t = this.child.next();
			if (this.p.filter(t))
				return t;
		}
		
//		throw new NoSuchElementException();
		return null;
	}
	
	@Override
	public DbIterator[] getChildren() {
		return new DbIterator[] { this.child };
	}
	
	@Override
	public void setChildren(DbIterator[] children) throws DbException {
		if (children.length != 1) {
			throw new DbException("Filter requires 1 child");
		}
		
		this.child = children[0];
	}
	
}
