package resa.cv.har;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_imgproc;

import javax.imageio.stream.ImageInputStream;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;

/**
 * Created by Tom Fu on Jan 28, 2015
 * This version is an alternative implementation, which every time emit two consecutive raw frames,
 * it ease the processing of imagePrepare and opticalFlow calculation, at the cost of more network transmissions
 */
public class FrameImplImageSource extends RedisQueueSpout implements Constants {

    private int frameId;
    //private String idPrefix;
    private int nChannel = 3;
    private int nDepth = 8;
    private int inHeight = 640;
    private int inWidth = 480;

    private Protocal.Mat sMatPrev;

    public FrameImplImageSource(String host, int port, String queue) {
        super(host, port, queue, true);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.collector = collector;
        frameId = 0;
        sMatPrev = null;
        nChannel = ConfigUtil.getInt(conf, "nChannel", 3);
        nDepth = ConfigUtil.getInt(conf, "nDepth", 8);
        inWidth = ConfigUtil.getInt(conf, "inWidth", 640);
        inHeight = ConfigUtil.getInt(conf, "inHeight", 480);
    }

    @Override
    protected void emitData(Object data) {
        String id = String.valueOf(frameId);
        byte[] imgBytes = (byte[]) data;
        ImageInputStream iis = null;

        try {
            //iis = ImageIO.createImageInputStream(new ByteArrayInputStream(imgBytes));
            //BufferedImage img = ImageIO.read(iis);
            //opencv_core.IplImage image = opencv_core.IplImage.createFrom(img);

            //byte[] imgBytes = (byte[]) data.getValueByField(FIELD_FRAME_BYTES);
            IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));

            IplImage frame = cvCreateImage(cvSize(inWidth, inHeight), nDepth, nChannel);
            opencv_imgproc.cvResize(image, frame, opencv_imgproc.CV_INTER_AREA);

            Mat mat = new Mat(image);
            Protocal.Mat sMat = new Protocal.Mat(mat);

            if (frameId > 0 && sMatPrev != null) {
                collector.emit(STREAM_FRAME_OUTPUT, new Values(frameId, sMat, sMatPrev), id);

                long nowTime = System.currentTimeMillis();
                System.out.printf("Sendout: " + nowTime + "," + frameId);
            }
            frameId++;
            sMatPrev = new Protocal.Mat(mat);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_FRAME_MAT_PREV));
    }
}
