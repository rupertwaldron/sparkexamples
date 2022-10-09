package org.ruppyrup.reconwithss;

import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AccountWrapper implements Serializable {
  private Account account;
  private int windowId;
  private int counter;

  private String timestamp;

  public AccountWrapper() {
  }

  public AccountWrapper(final Account account, final int windowId, final int counter) {
    this.account = account;
    this.windowId = windowId;
    this.counter = counter;
    this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS"));
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

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final String timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return "AccountWrapper{" +
               "account=" + account +
               ", windowId=" + windowId +
               ", counter=" + counter +
               ", timestamp=" + timestamp +
               '}';
  }
}
