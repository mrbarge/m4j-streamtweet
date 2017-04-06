import com.cycling74.max.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Max plugin for real-time streaming of tweets
 */
public class StreamTweet extends MaxObject {

    /** Endpoint OAuth configuration */
    private String OAuthAccessToken;
    private String OAuthAccessSecret;
    private String OAuthConsumerKey;
    private String OAuthConsumerSecret;

    /** List of bounding-box locations to filter tweets from */
    private List<Location> locations;

    /** HBC endpoint configuration */
    private static int ENDPOINT_CAPACITY = 100;
    private BlockingQueue<String> msgQueue;
    private static String CLIENT_NAME = "M4JStreamTweet";
    private Client streamClient;

    /** Running thread for streaming tweets */
    private Thread thread;

    public StreamTweet() {

        this.thread = new Thread();
        this.locations = new ArrayList<Location>();
        this.msgQueue = new LinkedBlockingQueue<String>(StreamTweet.ENDPOINT_CAPACITY);

        declareInlets(new int[] {
            DataTypes.ALL
        });
        declareOutlets(new int[] {
            DataTypes.MESSAGE,          // screen name
            DataTypes.MESSAGE,          // text
            DataTypes.MESSAGE,          // timestamp
            DataTypes.MESSAGE           // tweet as dict
        });
        declareAttribute("OAuthAccessToken");
        declareAttribute("OAuthAccessSecret");
        declareAttribute("OAuthConsumerKey");
        declareAttribute("OAuthConsumerSecret");
        declareAttribute("LocationFilter", null, "addLocationFilter");
    }

    /**
     * Adds a location filter to the Twitter streaming endpoint based on
     * longitude/latitude bounding box coordinates.
     *
     * The bounding box is defined as four coordinates in order of:
     *  south-west longitude,
     *  south-west latitude,
     *  north-east longitude
     *  north-east latitude
     *
     * If the input arguments are empty all locations in the filter are
     * deleted. This can be used to clear the filter.
     *
     * @param args array representing bounding box coordinates (SWLongitude,SWLatitude,NELongitude,NELatitude)
     */
    private void addLocationFilter(Atom[] args) {

        if (args.length >= 4) {
            if (args[0].isFloat() && args[1].isFloat() && args[2].isFloat() && args[3].isFloat()) {
                double swLongitude = args[0].toDouble();
                double swLatitude = args[1].toDouble();
                double neLongitude = args[2].toDouble();
                double neLatitude = args[3].toDouble();

                Location.Coordinate swCoord = new Location.Coordinate(swLongitude, swLatitude);
                Location.Coordinate neCoord = new Location.Coordinate(neLongitude, neLatitude);
                Location loc = new Location(swCoord, neCoord);
                this.locations.add(loc);
            }
        } else {
            this.locations = new ArrayList<Location>();
        }
    }

    /**
     * Handle the input of a toggle output into the inlet,
     * indicating that either the running thread should stop (0)
     * or that it should start/remain running (1)
     */
    @Override
    protected void inlet(int value) {
        if (value == 0) {
            this.stopThread();
        } else {
            this.startThread();
        }
    }

    private void stopThread() {
        this.thread.interrupt();
    }

    private void startThread() {
        if (!this.thread.isAlive()) {
            System.out.println("Starting thread..");
            this.thread = new Thread() {
                public void run() {
                    setupClient();
                    streamClient.connect();
                    try {
                        while (!streamClient.isDone()) {
                            String msg = msgQueue.take();
                            JsonObject obj = new JsonParser().parse(msg).getAsJsonObject();
                            String username = obj.get("user").getAsJsonObject().get("screen_name").getAsString();
                            String tweet = obj.get("text").getAsString();
                            String timeline = obj.get("created_at").getAsString();

                            outlet(0, username);
                            outlet(1, tweet);
                            outlet(2, timeline);
                            outlet(3, msg);
                        }
                    } catch (InterruptedException e) {
                            streamClient.stop();
                    }
                }
            };
            thread.start();
        }
    }

    /**
     * Setup the Twitter streaming endpoint
     */
    private void setupClient() {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.locations(this.locations);

        Authentication hosebirdAuth = new OAuth1(this.OAuthConsumerKey, this.OAuthConsumerSecret,
                this.OAuthAccessToken, this.OAuthAccessSecret);

        ClientBuilder builder = new ClientBuilder()
                .name(StreamTweet.CLIENT_NAME)                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        this.streamClient = builder.build();
    }

    /**
     * Handles a bang arriving into the inlet which should reverse the client's
     * current behaviour.
     */
    @Override
    protected void bang() {
        if (thread.isAlive()) {
            this.stopThread();
        } else {
            this.startThread();
        }
    }

}
