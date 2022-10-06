package org.ruppyrup.reconwithss;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccountWindow implements Serializable {

  private final int windowSize;

  private final List<AccountWrapper> accountWrappers = new ArrayList<>();

  public AccountWindow() {
    windowSize = 10;
  }

  public AccountWindow(final int windowSize) {
    this.windowSize = windowSize;
  }

  public void addAccountWrapper(AccountWrapper event) {
    accountWrappers.add(event);
  }

  public Collection<AccountWrapper> getAccountWrappers() {
    return accountWrappers;
  }

  public boolean isComplete() {
    return accountWrappers.size() == windowSize;
  }

  public int getEventCount() {
    return accountWrappers.size();
  }

  @Override
  public String toString() {
    return "AccountWindow{" +
               "windowSize=" + windowSize +
               ", accountWrappers=" + accountWrappers +
               '}';
  }
}
