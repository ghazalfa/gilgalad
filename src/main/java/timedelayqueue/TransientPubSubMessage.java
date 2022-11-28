package timedelayqueue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// Description: A transient PubSubMessage is a subclass of PubSubMessage that is automatically
//              removed from a TimeDelayQueue after a specifided lifetime field
// RI:
// AF:
// Thread safety:
public class TransientPubSubMessage extends PubSubMessage {
    private final int     lifetime;
    private final boolean isTransient = true;

    // create a TransientPubSubMessage instance with explicit args;
    // content should be in JSON format to accommodate a variety of
    // message types (e.g., TweetData)
    public TransientPubSubMessage(UUID id, Timestamp timestamp,
                         UUID sender, UUID receiver, String content, MessageType type, int lifetime) {
        super(id, timestamp, sender, receiver, content, type);
        this.lifetime = lifetime;
    }

    // create a PubSubMessage instance with explicit args
    // a message may be intended for more than one user
    public TransientPubSubMessage(UUID id, Timestamp timestamp,
                         UUID sender, List<UUID> receiver, String content, MessageType type, int lifetime) {
        super(id, timestamp, sender, receiver, content, type);
        this.lifetime = lifetime;
    }

    public TransientPubSubMessage(UUID sender, List<UUID> receiver, String content, int lifetime) {
        this(
                UUID.randomUUID(),
                new Timestamp(System.currentTimeMillis()),
                sender, receiver,
                content,
                BasicMessageType.SIMPLEMSG,
                lifetime
        );
    }

    public TransientPubSubMessage(UUID sender, UUID receiver, String content, int lifetime) {
        super(sender, receiver, content);
        this.lifetime = lifetime;
    }

    public int getLifetime() {
        return lifetime;
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }
}
