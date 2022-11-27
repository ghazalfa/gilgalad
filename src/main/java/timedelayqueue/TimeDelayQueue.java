package timedelayqueue;

import java.security.Timestamp;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.*;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TimeDelayQueue {

    // Store all current messages in a list
    private List<PubSubMessage> messages;

    // Store the total number of messages added (irrespective of those that have been removed)
    long totalMessageCount = 0;

    // Store the delay of the TimeDelayQueue (initialized in constructor)
    int delay;

    // Store all operations that have occurred and the time in which they occurred
    //      K: Timestamp
//          V: Number of operations at the timestamp
    Map<Long, Integer> history;

    // a comparator to sort messages
    private class PubSubMessageComparator implements Comparator<PubSubMessage> {
        public int compare(PubSubMessage msg1, PubSubMessage msg2) {
            return msg1.getTimestamp().compareTo(msg2.getTimestamp());
        }
    }

    /**
     * Create a new TimeDelayQueue
     * @param delay the delay, in milliseconds, that the queue can tolerate, >= 0
     */
    public TimeDelayQueue(int delay) {
        this.delay = delay;
        this.messages = new ArrayList<>();
        this.history = new HashMap<>();
    }

    // add() and getNext() are equally worth in load
    // increment the count at a specific timestamp
    private void addToHistory() {
        long t = System.currentTimeMillis();
        // If key does not exist, put a 1
        // Else increment value at the key
        history.merge(t, 1, Integer::sum);
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public boolean add(PubSubMessage msg) {
        addToHistory();
        
        if (!messages.contains(msg)) {
            messages.add(msg);
            Collections.sort(messages, new PubSubMessageComparator());
            totalMessageCount++;
            return true;
        }
        return false;
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     * @return
     */
    public long getTotalMsgCount() {
        return -1;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
    public PubSubMessage getNext() {
        addToHistory();
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        
        PubSubMessage nextMsg = messages.get(0);
        if (currentTimestamp.getTime() - nextMsg.getTimestamp().getTime() >= delay) {
            messages.remove(nextMsg);
            return nextMsg;
        }
        return PubSubMessage.NO_MSG;
    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {



        return -1;
    }

}
