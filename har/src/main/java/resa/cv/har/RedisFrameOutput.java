package resa.cv.har;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.bytedeco.javacpp.opencv_core;

import java.util.Map;

/**
 * Created by Tom Fu
 * <p>
 * This is usually the last bolt for displaying or outputting finished frames
 * In the current implementation, it uses RedisStreamProducerBeta instance for outputting as jpg figures to redis output queue
 * RedisStreamProducerBeta keeps a timer for each frame, if the expected frame is late, it starts the time and wait until timeout,
 * then it simply drops this frame and come to the next expected frame. (suitable for loss insensitive application)
 * <p>
 * The alternation can be RedisStreamProducer, which maintains a fix length sorted queue as a buffer for sorting, and re-ordering
 * dump out the data at head only when new data arrive at the tail.
 */
public class RedisFrameOutput extends BaseRichBolt implements Constants {
    OutputCollector collector;

    RedisStreamProducer producer;
    //RedisStreamProducer producer;

    private String host;
    private int port;
    private String queueName;
    private int sleepTime;
    private int startFrameID;
    private int maxWaitCount;

    //private int accumulateFrameSize;

    //private HashMap<Integer, Serializable.Mat> rawFrameMap;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.host = (String) map.get("redis.host");
        this.port = ConfigUtil.getInt(map, "redis.port", 6379);
        this.queueName = (String) map.get("redis.queueName");

        this.sleepTime = ConfigUtil.getInt(map, "sleepTime", 10);
        this.startFrameID = ConfigUtil.getInt(map, "startFrameID", 1);
        this.maxWaitCount = ConfigUtil.getInt(map, "maxWaitCount", 4);

//        accumulateFrameSize = ConfigUtil.getInt(map, "accumulateFrameSize", 1);
//        rawFrameMap = new HashMap<>();

        producer = new RedisStreamProducer(host, port, queueName, startFrameID, maxWaitCount, sleepTime);
        //producer = new RedisStreamProducer(host, port, queueName, accumulateFrameSize);
        new Thread(producer).start();

    }

    // Fields("frameId", "frameMat", "patchCount")
    // Fields("frameId", "foundRectList")
    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        Protocal.Mat sMat = (Protocal.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        producer.addFrame(new StreamFrame(frameId, sMat.toJavaCVMat()));

        System.out.println("producerAdd: " + System.currentTimeMillis() + ":" + frameId);
        collector.ack(tuple);
    }
}
