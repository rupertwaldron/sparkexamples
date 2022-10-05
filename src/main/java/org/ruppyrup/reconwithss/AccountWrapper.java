package org.ruppyrup.reconwithss;

import java.io.Serializable;

public class AccountWrapper implements Serializable {
  private Account account;
  private int windowId;
  private int counter;

  public AccountWrapper(final Account account, final int windowId, final int counter) {
    this.account = account;
    this.windowId = windowId;
    this.counter = counter;
  }

  public Account getAccount() {
    return account;
  }

  public void setAccount(final Account account) {
    this.account = account;
  }

  public int getWindowId() {
    return windowId;
  }

  public void setWindowId(final int windowId) {
    this.windowId = windowId;
  }

  public int getCounter() {
    return counter;
  }

  public void setCounter(final int counter) {
    this.counter = counter;
  }
}
