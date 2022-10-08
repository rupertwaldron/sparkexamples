package org.ruppyrup.reconwithss;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccountWindow implements Serializable {

  private int windowId;
  private List<AccountWrapper> accountWrappers = new ArrayList<>();

  public AccountWindow() {
  }

  public List<AccountWrapper> getAccountWrappers() {
    return accountWrappers;
  }

  public void setAccountWrappers(final List<AccountWrapper> accountWrappers) {
    this.accountWrappers = accountWrappers;
  }

  public int getWindowId() {
    return windowId;
  }

  public void setWindowId(final int windowId) {
    this.windowId = windowId;
  }

  @Override
  public String toString() {
    return "AccountWindow{" +
               "windowId=" + windowId +
               ", accountWrappers=" + accountWrappers +
               '}';
  }
}
