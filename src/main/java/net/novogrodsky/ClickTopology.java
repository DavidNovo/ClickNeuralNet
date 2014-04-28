package net.novogrodsky;

/**
 * Created by david.j.novogrodsky on 4/27/2014.
 * Defines the topology of the solution
 * there will be a way to launch the topology in local
 * or cluster mode
 */
public class ClickTopology {

    // encapsulating the topology into a method
    // so it can be called in local or cluster mode
    public void clickTopology() {
        builder.setSpout("clickSpout", new ClickTopology(), 10);

        // this layer operates on the stream first

    }

}
