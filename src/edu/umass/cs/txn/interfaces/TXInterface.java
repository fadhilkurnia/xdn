package edu.umass.cs.txn.interfaces;

/**
 * @author arun
 *     <p>The Iterable<TxOp> interface should return an iterator over the individual ordered
 *     operations constituting this transaction.
 */
public interface TXInterface extends TXRequest {
  /**
   * @return Transaction ID.
   */
  public String getTXID();
}
