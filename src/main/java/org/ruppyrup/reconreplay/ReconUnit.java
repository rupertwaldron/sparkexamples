package org.ruppyrup.reconreplay;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds list of all events for a particular windowId
 */
public class ReconUnit implements Serializable {

  private final int windowSize;
  private boolean timedOut = false;


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

  public synchronized Boolean hasTimedOut() {
    return timedOut;
  }

  public synchronized void setTimedOut() {
    timedOut = true;
  }

  @Override
  public String toString() {
    return "ReconUnit{" +
               "allEvents=" + allEvents +
               '}';
  }
}
