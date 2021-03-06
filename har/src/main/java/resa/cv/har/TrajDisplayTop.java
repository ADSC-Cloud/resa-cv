package resa.cv.har;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Created by Tom Fu on Jan 29, 2015.
 * TODO: Notes:
 * traceGenerator 是否可以并行？ 这样需要feedback分开，register也要分开
 * 扩展，如果有2个scale的话，需要对当前程序扩展！
 * 产生光流是bottleneck-> done
 * 此版本暂时通过测试
 * 尝试将optFlowGen and optFlowAgg 分布式化->done
 * 在echo 版本中， optFlowTracker也作了细分，大大降低了传输的network cost
 * 在echo 版本中，ImgPrep在把eig frame传给traceGen的时候，也做了划分，减少了network cost
 * 在echo 版本的基础上，使用batch传输策略，即，将所有的trace的点，收集之后再传给下一个bolt，
 * 这样的batch似乎有效的提高了传输和处理的效率
 * Test 通过，现在18-3能到25fps，具体performance可以查询笔记
 */
public class TrajDisplayTop implements Constants {

    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = ConfigUtil.readConfig(new File(args[0]));

        TopologyBuilder builder = new TopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = Utils.getInt(conf.get("redis.port"));
        String queueName = (String) conf.get("redis.sourceQueueName");

        String spoutName = "TrajSpout";
        String imgPrepareBolt = "TrajImgPrep";
        String optFlowGenBolt = "TrajOptFlowGen";
        String traceGenBolt = "TrajTraceGen";
        String optFlowTrans = "TrajOptFlowTrans";
        String optFlowTracker = "TrajOptFlowTracker";
        String traceAggregator = "TrajTraceAgg";
        String frameDisplay = "TrajDisplay";
        String redisFrameOut = "RedisFrameOut";

        builder.setSpout(spoutName, new FrameImplImageSource(host, port, queueName), getParallelism(conf, spoutName))
                .setNumTasks(getNumTasks(conf, spoutName));

        builder.setBolt(imgPrepareBolt, new ImagePrepare(traceGenBolt), getParallelism(conf, imgPrepareBolt))
                .shuffleGrouping(spoutName, STREAM_FRAME_OUTPUT)
                .setNumTasks(getNumTasks(conf, imgPrepareBolt));

        builder.setBolt(optFlowGenBolt, new OptlFlowGeneratorMultiOptFlow(), getParallelism(conf, optFlowGenBolt))
                .shuffleGrouping(imgPrepareBolt, STREAM_GREY_FLOW)
                .setNumTasks(getNumTasks(conf, optFlowGenBolt));

        builder.setBolt(optFlowTrans, new OptlFlowTrans(optFlowTracker), getParallelism(conf, optFlowTrans))
                .shuffleGrouping(optFlowGenBolt, STREAM_OPT_FLOW)
                .setNumTasks(getNumTasks(conf, optFlowTrans));

        builder.setBolt(traceGenBolt, new TraceGenerator(traceAggregator, optFlowTracker),
                getParallelism(conf, traceGenBolt))
                //.allGrouping(imgPrepareBolt, STREAM_EIG_FLOW)
                .directGrouping(imgPrepareBolt, STREAM_EIG_FLOW)
                .allGrouping(traceAggregator, STREAM_INDICATOR_TRACE)
                .setNumTasks(getNumTasks(conf, traceGenBolt));

        builder.setBolt(optFlowTracker, new OptFlowTracker(traceAggregator), getParallelism(conf, optFlowTracker))
                .directGrouping(traceGenBolt, STREAM_NEW_TRACE)
                .directGrouping(traceAggregator, STREAM_RENEW_TRACE)
                .directGrouping(optFlowTrans, STREAM_OPT_FLOW)
                .allGrouping(frameDisplay, STREAM_CACHE_CLEAN)
                .setNumTasks(getNumTasks(conf, optFlowTracker));

        builder.setBolt(traceAggregator, new TraceAggregator(traceGenBolt, optFlowTracker),
                getParallelism(conf, traceAggregator))
                .directGrouping(traceGenBolt, STREAM_REGISTER_TRACE)
                        //.directGrouping(optFlowTracker, STREAM_EXIST_TRACE)
                        //.directGrouping(optFlowTracker, STREAM_REMOVE_TRACE)
                .directGrouping(optFlowTracker, STREAM_EXIST_REMOVE_TRACE)
                .setNumTasks(getNumTasks(conf, traceAggregator));

        builder.setBolt(frameDisplay, new FrameDisplay(traceAggregator), getParallelism(conf, frameDisplay))
                .fieldsGrouping(imgPrepareBolt, STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(traceAggregator, STREAM_PLOT_TRACE, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getNumTasks(conf, frameDisplay));

        builder.setBolt(redisFrameOut, new RedisFrameOutput(), getParallelism(conf, redisFrameOut))
                .globalGrouping(frameDisplay, STREAM_FRAME_DISPLAY)
                .setNumTasks(getNumTasks(conf, redisFrameOut));

        StormTopology topology = builder.createTopology();

        int min_dis = ConfigUtil.getInt(conf, "min_distance", 1);
        int init_counter = ConfigUtil.getInt(conf, "init_counter", 1);
        for (Class c : Protocal.getAllClasses()) {
            conf.registerSerialization(c);
        }
        StormSubmitter.submitTopology("tTrajTopEchoBatch-" + init_counter + "-" + min_dis, conf, topology);
    }

    private static int getNumTasks(Config conf, String comp) {
        return ConfigUtil.getInt(conf, comp + ".tasks", getParallelism(conf, comp));
    }

    private static int getParallelism(Config conf, String comp) {
        return ConfigUtil.getInt(conf, comp + ".parallelism", 1);
    }

    private static int getInt(Config conf, String key) {
        return ConfigUtil.getInt(conf, key, 1);
    }

}
