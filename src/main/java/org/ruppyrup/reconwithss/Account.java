package org.ruppyrup.reconwithss;

import java.io.Serializable;

public class Account implements Serializable {

  private String accountNumber;
  private String accountName;
  private double balance;

  public Account() {
  }

  public Account(final String accountNumber, final String accountName, final double balance) {
    this.accountNumber = accountNumber;
    this.accountName = accountName;
    this.balance = balance;
  }

  public String getAccountNumber() {
    return accountNumber;
  }

  public void setAccountNumber(final String accountNumber) {
    this.accountNumber = accountNumber;
  }

  public String getAccountName() {
    return accountName;
  }

  public void setAccountName(final String accountName) {
    this.accountName = accountName;
  }

  public double getBalance() {
    return balance;
  }

  public void setBalance(final double balance) {
    this.balance = balance;
  }
}
