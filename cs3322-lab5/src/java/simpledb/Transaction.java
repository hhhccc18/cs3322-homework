package simpledb;

import java.io.*;

/**
 * Transaction encapsulates information about the state of
 * a transaction and manages transaction commit / abort.
 */

public class Transaction {
	private final TransactionId tid;
	volatile boolean started = false;
	
	public Transaction() {
		this.tid = new TransactionId();
	}
	
	/** Start the transaction running */
	public void start() {
		this.started = true;
		try {
			Database.getLogFile().logXactionBegin(this.tid);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public TransactionId getId() {
		return this.tid;
	}
	
	/** Finish the transaction */
	public void commit() throws IOException {
		this.transactionComplete(false);
	}
	
	/** Finish the transaction */
	public void abort() throws IOException {
		this.transactionComplete(true);
	}
	
	/** Handle the details of transaction commit / abort */
	public void transactionComplete(boolean abort) throws IOException {
		
		if (this.started) {
			//write commit / abort records
			if (abort) {
				Database.getLogFile().logAbort(this.tid); //does rollback too
			} else {
				//write all the dirty pages for this transaction out
				Database.getBufferPool().flushPages(this.tid);
				Database.getLogFile().logCommit(this.tid);
			}
			
			try {
				Database.getBufferPool().transactionComplete(this.tid, !abort); // release locks
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			//setting this here means we could possibly write multiple abort records -- OK?
			this.started = false;
		}
	}
}
