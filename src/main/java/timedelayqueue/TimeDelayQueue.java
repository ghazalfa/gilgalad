package timedelayqueue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.*;

// Description: A TimeDelayQueue stores messages, the total number of messages ever added
//              and a history of the operations performed on the queue (add/getNext).
//              The TimeDelayQueue uses TimeStamps and System time to determine when
//              a message can be removed and when transient message are out of their lifetime


// Representation Invariant:


// Abstraction Function:


// Thread safety: We use the synchronized keyword on the methods below so that
//                threads operate in a mutually exclusive manner, using the class itself
//                as the mutex for synchronization
//
//                The methods, add(), getTotalMsgCount(), getNext(), getPeakLoad(), removeTransientMsg():
//                could face concurrent modification and require synchronization of their shared resources
//                mainly the list of messages and the history list.
//
//                For example, getPeakLoad() could be iterating over the history list
//                while another thread is modifying the history list by adding a message to the queue.

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
    public synchronized long getTotalMsgCount() {
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

    public synchronized void removeTransientMsg(TransientPubSubMessage msg, Timestamp currentTimestamp) {
        if (currentTimestamp.getTime() >= msg.getTimestamp().getTime() + msg.getLifetime()) {
            messages.remove(msg);
        }
    }

    public static void main(String[] args) {
//        TimeDelayQueue tdq = new TimeDelayQueue(DELAY);
//
//        UUID sndID     = UUID.randomUUID();
//        UUID rcvID     = UUID.randomUUID();
//        String msgText = gson.toJson("test");
//        TransientPubSubMessage msg1 = new TransientPubSubMessage(sndID, rcvID, msgText, MSG_LIFETIME);
//        PubSubMessage          msg2 = new PubSubMessage(sndID, rcvID, msgText);
//        tdq.add(msg1);
//        tdq.add(msg2);
//        try {
//            Thread.sleep(MSG_LIFETIME + 1);
//        }
//        catch (InterruptedException ie) {
//            throw new RuntimeException();
//        }
//        PubSubMessage get = tdq.getNext();


    }

}
