package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/** Constants used for return codes in Field.compare */
	public enum Op implements Serializable {
		EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
		
		/**
		 * Interface to access operations by integer value for command-line
		 * convenience.
		 *
		 * @param i
		 *            a valid integer Op index
		 */
		public static Op getOp(int i) {
			return Op.values()[i];
		}
		
		public String toString() {
			if (this == EQUALS)
				return "=";
			else if (this == GREATER_THAN)
				return ">";
			else if (this == LESS_THAN)
				return "<";
			else if (this == LESS_THAN_OR_EQ)
				return "<=";
			else if (this == GREATER_THAN_OR_EQ)
				return ">=";
			else if (this == LIKE)
				return "LIKE";
			else if (this == NOT_EQUALS)
				return "<>";
			else
				throw new IllegalStateException("impossible to reach here");
		}
		
//		public Op reversal() { // swap the side of the comparison
//			if (this == EQUALS)
//				return EQUALS;
//			else if (this == GREATER_THAN)
//				return LESS_THAN;
//			else if (this == LESS_THAN)
//				return GREATER_THAN;
//			else if (this == LESS_THAN_OR_EQ)
//				return GREATER_THAN_OR_EQ;
//			else if (this == GREATER_THAN_OR_EQ)
//				return LESS_THAN_OR_EQ;
//			else if (this == LIKE)
//				return LIKE;
//			else if (this == NOT_EQUALS)
//				return NOT_EQUALS;
//			else
//				throw new IllegalStateException("impossible to reach here");
//		}
//
//		public static Op reversal(Op op) {
//			return op.reversal();
//		}
	}
	
	private final int index;
	private final Op op;
	private final Field operand;
	
	/**
	 * Constructor.
	 *
	 * @param field
	 *            field number of passed in tuples to compare against.
	 * @param op
	 *            operation to use for comparison
	 * @param operand
	 *            field value to compare passed in tuples to
	 */
	public Predicate(int field, Op op, Field operand) {
		this.index = field;
		this.op = op;
		this.operand = operand;
	}
	
	/**
	 * @return the field number
	 */
	public int getField() {
		return this.index;
	}
	
	/**
	 * @return the operator
	 */
	public Op getOp() {
		return this.op;
	}
	
	/**
	 * @return the operand
	 */
	public Field getOperand() {
		return this.operand;
	}
	
	/**
	 * Compares the field number of t specified in the constructor to the
	 * operand field specified in the constructor using the operator specific in
	 * the constructor. The comparison can be made through Field's compare
	 * method.
	 *
	 * @param t
	 *            The tuple to compare against
	 * @return true if the comparison is true, false otherwise.
	 */
	public boolean filter(Tuple t) {
		if (t.length() <= this.index)
			return false;
		
		return t.getField(this.index).compare(this.op, this.operand);
	}
	
	/**
	 * Returns something useful, like "f = field_id op = op_string operand =
	 * operand_string
	 */
	public String toString() {
		return String.format("f = %d op = %s operand = %s",
				this.index, this.op.toString(), this.operand.toString());
	}
}
