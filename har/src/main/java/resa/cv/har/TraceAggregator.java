package resa.cv.har;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import resa.cv.har.Protocal.*;

import java.util.*;

import static org.bytedeco.javacpp.opencv_core.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * We design the delta version, to make traceAgg distributable, i.e., each instance takes a subset of all the traces,
 * partitioned by traceID
 */
public class TraceAggregator extends BaseRichBolt implements Constants {
    OutputCollector collector;

    ///private HashMap<Integer, HashSet<String>> traceMonitor;
    private HashMap<Integer, Integer> traceMonitor;
    private HashMap<String, List<PointDesc>> traceData;
    private HashMap<Integer, Queue<Object>> messageQueue;
    private HashMap<Integer, List<TwoIntegers>> newPointsWHInfo;

    int maxTrackerLength;
    DescInfo mbhInfo;
    double min_distance;
    int init_counter;

    static int patch_size = 32;
    static int nxy_cell = 2;
    static int nt_cell = 3;
    static float min_flow = 0.4f * 0.4f;

    String traceGenBoltNameString;
    int traceGenBoltTaskNumber;
    List<Integer> flowTrackerTasks;
    String flowTrackerName;

    public TraceAggregator(String traceGenBoltNameString, String flowTrackerName) {
        this.traceGenBoltNameString = traceGenBoltNameString;
        this.flowTrackerName = flowTrackerName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        traceMonitor = new HashMap<>();
        traceData = new HashMap<>();
        messageQueue = new HashMap<>();
        newPointsWHInfo = new HashMap<>();

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.maxTrackerLength = ConfigUtil.getInt(map, "maxTrackerLength", 15);
        this.mbhInfo = new DescInfo(8, 0, 1, patch_size, nxy_cell, nt_cell, min_flow);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        this.traceGenBoltTaskNumber = topologyContext.getComponentTasks(traceGenBoltNameString).size();
        flowTrackerTasks = topologyContext.getComponentTasks(flowTrackerName);

        IplImage fk = new IplImage();
    }

    //tuple format:  STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_BYTES)
    @Override
    public void execute(Tuple tuple) {

        String streamId = tuple.getSourceStreamId();
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        //if (streamId.equals(STREAM_EXIST_TRACE) || streamId.equals(STREAM_REMOVE_TRACE)) {
        if (streamId.equals(STREAM_EXIST_REMOVE_TRACE)) {
            List<Object> messages = (List<Object>) tuple.getValueByField(FIELD_TRACE_CONTENT);
            messageQueue.computeIfAbsent(frameId, k -> new LinkedList<>()).addAll(messages);

        } else if (streamId.equals(STREAM_REGISTER_TRACE)) {
            //List<String> registerTraceIDList = (List<String>) tuple.getValueByField(FIELD_TRACE_CONTENT);
            Integer registerTraceCnt = tuple.getIntegerByField(FIELD_TRACE_CONTENT);
            TwoIntegers wh = (TwoIntegers) tuple.getValueByField(FIELD_WIDTH_HEIGHT);
            //newPointsWHInfo.put(frameId, wh);
            newPointsWHInfo.computeIfAbsent(frameId, k -> new ArrayList<TwoIntegers>()).add(wh);

            ///TODO: to deal with special case when registerTraceIDList is empty!!!
            ///TODO: one point to optimize, the register for feedback traces are not necessary, can directly added in this bolt.
            //HashSet<String> traceIDset = new HashSet<>();
            //registerTraceIDList.forEach(k -> traceIDset.add(k));
            //traceMonitor.put(frameId, traceIDset);
            if (frameId == 1 && !traceMonitor.containsKey(frameId)) {
                //traceMonitor.put(frameId, new HashSet<>());
                traceMonitor.put(frameId, 0);
            }
            if (!traceMonitor.containsKey(frameId)) {
                throw new IllegalArgumentException("!traceMonitor.containsKey(frameId), frameID: " + frameId);
            }
            //traceMonitor.get(frameId).addAll(traceIDset);
            traceMonitor.computeIfPresent(frameId, (k, v) -> v + registerTraceCnt);
//            System.out.println("Register frame: " + frameId
//                    //+ ", registerTraceListCnt: " + registerTraceIDList.size()
//                    + ", registerTraceCnt: " + registerTraceCnt
//                    //+ ", traceSetSize: " + traceIDset.size()
//                    + ", traceMonitorCnt: " + traceMonitor.size()
//                    + ", messageQueueSize: " + messageQueue.size()
//                    + ", newPointsWHInfoSize: " + newPointsWHInfo.size()
//                    //+ ", totalRegistered: " + traceMonitor.get(frameId).size()
//                    );
        }

        if (traceMonitor.containsKey(frameId) && messageQueue.containsKey(frameId)
                && newPointsWHInfo.containsKey(frameId)
                && newPointsWHInfo.get(frameId).size() == traceGenBoltTaskNumber) {
            aggregateTraceRecords(frameId);
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID, FIELD_TRACE_RECORD));
        //outputFieldsDeclarer.declareStream(STREAM_CACHE_CLEAN, new Fields(FIELD_FRAME_ID));
        outputFieldsDeclarer.declareStream(STREAM_RENEW_TRACE, true, new Fields(FIELD_FRAME_ID, FIELD_TRACE_META_LAST_POINT));
        outputFieldsDeclarer.declareStream(STREAM_INDICATOR_TRACE, new Fields(FIELD_FRAME_ID, FIELD_COUNTERS_INDEX));
    }

    public void aggregateTraceRecords(int frameId) {
        Queue<Object> messages = messageQueue.get(frameId);
        TwoIntegers wh = newPointsWHInfo.get(frameId).get(0);
        traceMonitor.computeIfPresent(frameId, (k, v) -> v - messages.size());

        while (!messages.isEmpty()) {
            Object m = messages.poll();
            if (m instanceof TraceMetaAndLastPoint) {
                ///m  is from Exist_trace
                TraceMetaAndLastPoint trace = (TraceMetaAndLastPoint) m;
                traceData.computeIfAbsent(trace.traceID, k -> new ArrayList<>()).add(new PointDesc(mbhInfo, trace.lastPoint));
            } else if (m instanceof String) {
                String traceID2Remove = (String) m;
                traceData.computeIfPresent(traceID2Remove, (k, v) -> traceData.remove(k));
            }
        }

        if (traceMonitor.get(frameId) == 0) {
            List<List<PointDesc>> traceRecords = new ArrayList<List<PointDesc>>(traceData.values());
            collector.emit(STREAM_PLOT_TRACE, new Values(frameId, traceRecords));
            //collector.emit(STREAM_CACHE_CLEAN, new Values(frameId));

            List<Integer> feedbackIndicators = new ArrayList<>();
            //HashSet<String> traceToRegister = new HashSet<>();
            int traceToRegisterCnt = 0;
            List<String> traceToRemove = new ArrayList<>();
            int width = wh.getV1();
            int height = wh.getV2();
            int nextFrameID = frameId + 1;

            List<List<TraceMetaAndLastPoint>> renewTraces = new ArrayList<>();
            for (int i = 0; i < flowTrackerTasks.size(); i++) {
                renewTraces.add(new ArrayList<>());
            }

            for (Map.Entry<String, List<PointDesc>> trace : traceData.entrySet()) {
                int traceLen = trace.getValue().size();
                if (traceLen > maxTrackerLength) {
                    traceToRemove.add(trace.getKey());
                } else {
                    traceToRegisterCnt++;
                    Protocal.CvPoint2D32f point = new Protocal.CvPoint2D32f(trace.getValue().get(traceLen - 1).sPoint);
                    TraceMetaAndLastPoint fdPt = new TraceMetaAndLastPoint(trace.getKey(), point);

                    int x = cvFloor(point.x() / min_distance);
                    int y = cvFloor(point.y() / min_distance);
                    int ywx = y * width + x;

                    if (point.x() < min_distance * width && point.y() < min_distance * height) {
                        feedbackIndicators.add(ywx);
                    }

                    int q = Math.min(Math.max(cvRound(point.y()), 0), height - 1);
                    //int tID = flowTrackerTasks.get(q % flowTrackerTasks.size());
                    //collector.emitDirect(tID, STREAM_RENEW_TRACE, new Values(nextFrameID, fdPt));
                    renewTraces.get(q % flowTrackerTasks.size()).add(fdPt);
                }
            }

            for (int i = 0; i < flowTrackerTasks.size(); i++) {
                int tID = flowTrackerTasks.get(i);
                collector.emitDirect(tID, STREAM_RENEW_TRACE, new Values(nextFrameID, renewTraces.get(i)));
            }

            ///leave this for the next bolt!

            traceToRemove.forEach(item -> traceData.remove(item));
            traceMonitor.remove(frameId);
            messageQueue.remove(frameId);
            newPointsWHInfo.remove(frameId);

            if (nextFrameID % init_counter == 0) {
                collector.emit(STREAM_INDICATOR_TRACE, new Values(nextFrameID, feedbackIndicators));
            } else {
                List<TwoIntegers> empty = new ArrayList<>();
                for (int i = 0; i < traceGenBoltTaskNumber; i++) {
                    empty.add(new TwoIntegers(wh.getV1(), wh.getV2()));
                }
                newPointsWHInfo.put(nextFrameID, empty);
            }

            traceMonitor.put(nextFrameID, traceToRegisterCnt);

//            System.out.println("ef: " + frameId + ", tMCnt: " + traceMonitor.size()
//                    + ", mQS: " + messageQueue.size() + ", nPWHS: " + newPointsWHInfo.size()
//                    + "tDS: " + traceData.size() + ", removeSize: " + traceToRemove.size()
//                    + ", exisSize: " + traceToRegisterCnt
//            );
        }
    }
}
