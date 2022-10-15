package org.ruppyrup.twitter;

import org.ruppyrup.reconwithss.Account;

import java.io.Serializable;

public class TwitterDto implements Serializable {
  private String created_at;
  private long id;
  private String id_str;
  private String text;

  public TwitterDto() {
  }

  public TwitterDto(final String created_at, final long id, final String id_str, final String text) {
    this.created_at = created_at;
    this.id = id;
    this.id_str = id_str;
    this.text = text;
  }

  public String getCreated_at() {
    return created_at;
  }

  public void setCreated_at(final String created_at) {
    this.created_at = created_at;
  }

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getId_str() {
    return id_str;
  }

  public void setId_str(final String id_str) {
    this.id_str = id_str;
  }

  public String getText() {
    return text;
  }

  public void setText(final String text) {
    this.text = text;
  }
}
