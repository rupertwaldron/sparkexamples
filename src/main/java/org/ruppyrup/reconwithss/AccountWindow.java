package org.ruppyrup.reconwithss;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccountWindow implements Serializable {
  private List<AccountWrapper> accountWrappers = new ArrayList<>();

  public AccountWindow() {
  }

  public List<AccountWrapper> getAccountWrappers() {
    return accountWrappers;
  }

  public void setAccountWrappers(final List<AccountWrapper> accountWrappers) {
    this.accountWrappers = accountWrappers;
  }

  @Override
  public String toString() {
    return "AccountWindow{" +
               "accountWrappers=" + accountWrappers +
               '}';
  }
}
