package org.ruppyrup.reconwithss;

import java.io.Serializable;

public class WindowResult implements Serializable {

  private AccountWindow accountWindow;
  private String accountNotification;

  public WindowResult(final AccountWindow accountWindow, final String accountNotification) {
    this.accountWindow = accountWindow;
    this.accountNotification = accountNotification;
  }

  public WindowResult() {
  }

  @Override
  public String toString() {
    return "WindowResult{" +
               "accountWindow=" + accountWindow +
               ", accountNotification='" + accountNotification + '\'' +
               '}';
  }

  public AccountWindow getAccountWindow() {
    return accountWindow;
  }

  public void setAccountWindow(final AccountWindow accountWindow) {
    this.accountWindow = accountWindow;
  }

  public String getAccountNotification() {
    return accountNotification;
  }

  public void setAccountNotification(final String accountNotification) {
    this.accountNotification = accountNotification;
  }
}
