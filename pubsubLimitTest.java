import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class pubsubLimitTest {
    Publisher publisher;
    Map<UUID,Data> map;
    String message;


    void init(String credentialUrl) throws IOException {
        publisher = Publisher
                .newBuilder("projects/data-platform-246911/topics/michael-signumingest-oom-test")
                .setCredentialsProvider(
                        FixedCredentialsProvider.create(
                                ServiceAccountCredentials.fromStream(
                                        new FileInputStream(
                                                new File(credentialUrl)
                                        )
                                )
                        )
                )
                .build();
        map=new HashMap<>();
        message="";
        Random r = new Random();
        for (int i = 0; i <1024 ; i++) {
            message+=(char)(r.nextInt(26) + 'a');
        }
    }

    void run(int threadCount, int sleep, String outputFile, String credentialUrl) throws ExecutionException, InterruptedException, IOException {
        init(credentialUrl);
        List<PublisherThread> publisherThreads = new ArrayList<>();

        while(threadCount-->0){
            PublisherThread publisherThread = new PublisherThread(publisher,map,message);
            publisherThreads.add(publisherThread);
            publisherThread.start();
        }

        Thread.sleep(sleep);
        for (PublisherThread thread:publisherThreads
             ) {
            thread.stop();
        }
        FileWriter myFile = new FileWriter(outputFile);
        myFile.write("request_id,publish_start_ts,publish_acked_ts,message_size_in_bytes\n");

        for (Map.Entry<UUID,Data> entry : map.entrySet()){
            myFile.write(
                    String.format(
                            "%s,%d,%d,%d\n",
                            entry.getKey(),
                            entry.getValue().send,
                            entry.getValue().receive,
                            entry.getValue().message.size()
                    )
            );
        }
        myFile.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        pubsubLimitTest self=new pubsubLimitTest();
        self.run(Integer.parseInt(args[3]),Integer.parseInt(args[1]),args[2],args[0]);
    }
}

class PublisherThread extends Thread {
    Publisher publisher;
    Map<UUID,Data> map;
    String message;
    PublisherThread(Publisher publisher, Map<UUID,Data> map, String message){
        this.publisher = publisher;
        this.map=map;
        this.message=message;
    }

    public void run() {
        while(true) {
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage message = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();
            long time = System.currentTimeMillis();
            ApiFuture<String> result = publisher.publish(message);
            try {
                result.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            map.put(
                    UUID.randomUUID(),
                    new Data(time,System.currentTimeMillis(),data)
            );
        }
    }
}

class Data{
    long send;
    long receive;
    ByteString message;

    Data(long send, long receive, ByteString message){
        this.send=send;
        this.receive=receive;
        this.message=message;
    }
}
