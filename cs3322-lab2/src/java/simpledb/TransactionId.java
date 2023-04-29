package simpledb;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TransactionId is a class that contains the identifier of a transaction.
 */
public class TransactionId implements Serializable {

    private static final long serialVersionUID = 1L;

    static AtomicLong counter = new AtomicLong(0);
    final long myid;

    public TransactionId() {
        this.myid = TransactionId.counter.getAndIncrement();
    }

    public long getId() {
        return this.myid;
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TransactionId other = (TransactionId) obj;
	    return this.myid == other.myid;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		return prime + (int) (this.myid ^ (this.myid >>> 32));
	}
}
