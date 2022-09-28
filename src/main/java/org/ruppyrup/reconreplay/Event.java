package org.ruppyrup.reconreplay;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

public class Event implements Serializable  {
  private final String value;
  private final Integer key;
  private final String topic;

  public Event(final ConsumerRecord<Integer, String> record) {
    value = record.value();
    key = record.key();
    topic = record.topic();
  }

  @Override
  public String toString() {
    return "Event{" +
               "value='" + value + '\'' +
               ", key=" + key +
               ", topic='" + topic + '\'' +
               '}';
  }
}
