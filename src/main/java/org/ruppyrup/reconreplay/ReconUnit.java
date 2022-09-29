package org.ruppyrup.reconreplay;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReconUnit implements Serializable {

  private final int windowSize;


  private final List<Event> allEvents = new ArrayList<>();

  public ReconUnit(final int windowSize) {
    this.windowSize = windowSize;
  }

  public void addEvent(Event event) {
    allEvents.add(event);
  }

  public Collection<Event> getAllEvents() {
    return allEvents;
  }

  public boolean isComplete() {
    return allEvents.size() == windowSize;
  }

  public int getEventCount() {
    return allEvents.size();
  }

  @Override
  public String toString() {
    return "ReconUnit{" +
               "allEvents=" + allEvents +
               '}';
  }
}
