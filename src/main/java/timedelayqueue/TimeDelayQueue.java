package timedelayqueue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Timestamp;
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


    // Store all operations that have occurred by storing the timestamp in a list
    // Assume that multiple operations cannot happen at the same millisecond
    List<Long> history;

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
        this.history = new ArrayList<>();
    }

    private synchronized void addToHistory() {
        history.add(System.currentTimeMillis());
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public synchronized boolean add(PubSubMessage msg) {
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());

        if (msg.isTransient()) {
            removeTransientMsg((TransientPubSubMessage) msg, currentTimestamp);
        }
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
        return this.totalMessageCount;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
    public synchronized PubSubMessage getNext() {
        addToHistory();
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());

        PubSubMessage nextMsg = messages.get(0);
        if (nextMsg.isTransient()) {
            removeTransientMsg((TransientPubSubMessage) nextMsg, currentTimestamp);
            nextMsg = messages.get(0);
        }

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
    public synchronized int getPeakLoad(int timeWindow) {
        int temp = 0;
        int count = 0;
        long timestamp;
        int highest = 0;

        for(int i = 0; i<history.size(); i++){
            count = 0;
            timestamp = history.get(i) +timeWindow;
            temp = i;
            while(temp< history.size()&& history.get(temp)<=timestamp){
                    count++;
                    temp++;
            }

            if(count>highest){
                highest = count;
            }
        }

        return highest;
    }

    /**
     * Remove a TransientPubSubMessage from this.messages
     * if its time within the TimeDelayQueue exceeds the TransientPubSubMessage's lifetime
     * @param msg the TransientPubSubMessage
     * @param currentTimestamp the current system time
     */
    public synchronized void removeTransientMsg(TransientPubSubMessage msg, Timestamp currentTimestamp) {
        if (currentTimestamp.getTime() >= msg.getTimestamp().getTime() + msg.getLifetime()) {
            messages.remove(msg);
        }
    }

    public static void main(String[] args) {

    }

}
