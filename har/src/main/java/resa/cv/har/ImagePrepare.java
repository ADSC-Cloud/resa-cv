package resa.cv.har;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;

/**
 * Created by Tom Fu
 * Input is raw video frames, output optical flow results between every two consecutive frames.
 * Maybe use global grouping and only one task/executor
 * Similar to frame producer, maintain an ordered list of frames
 * <p>
 * Modified on Feb 7, level I improvement
 */
public class ImagePrepare extends BaseRichBolt implements Constants {
    OutputCollector collector;

    IplImage image, prev_image, grey, prev_grey;
    IplImagePyramid grey_pyramid, prev_grey_pyramid;
    IplImage eig;
    IplImagePyramid eig_pyramid;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static int ixyScale = 0;

    double min_distance;
    double quality;
    int init_counter;

    List<Integer> traceGeneratorTasks;
    String traceGeneratorName;

    public ImagePrepare(String traceGeneratorName) {
        this.traceGeneratorName = traceGeneratorName;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.image = null;
        this.prev_image = null;
        this.grey = null;
        this.prev_grey = null;

        this.grey_pyramid = null;
        this.prev_grey_pyramid = null;

        this.min_distance = ((Number) conf.getOrDefault("min_distance", 5.0)).doubleValue();
        this.quality = ((Number) conf.getOrDefault("quality", 0.001)).doubleValue();
        this.init_counter = Utils.getInt(conf.get("init_counter"), 1);
        traceGeneratorTasks = topologyContext.getComponentTasks(traceGeneratorName);

        IplImage imageFK = new IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        Protocal.Mat sMat = (Protocal.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        IplImage frame = sMat.toJavaCVMat().asIplImage();

        collector.emit(STREAM_FRAME_OUTPUT, tuple, new Values(frameId, sMat));

        Protocal.Mat sMatPrev = (Protocal.Mat) tuple.getValueByField(FIELD_FRAME_MAT_PREV);
        IplImage framePrev = sMatPrev.toJavaCVMat().asIplImage();

        if (this.image == null || frameId == 1) { //only first time
            image = cvCreateImage(cvGetSize(frame), 8, 3);
            image.origin(frame.origin());

            prev_image = cvCreateImage(cvGetSize(frame), 8, 3);
            prev_image.origin(frame.origin());

            grey = cvCreateImage(cvGetSize(frame), 8, 1);
            grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            prev_grey = cvCreateImage(cvGetSize(frame), 8, 1);
            prev_grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            eig_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(this.grey), 32, 1);
        }

        cvCopy(frame, image, null);
        opencv_imgproc.cvCvtColor(image, grey, opencv_imgproc.CV_BGR2GRAY);
        grey_pyramid.rebuild(grey);

        IplImage grey_temp = cvCloneImage(grey_pyramid.getImage(ixyScale));

        Mat gMat = new Mat(grey_temp);
        Protocal.Mat sgMat = new Protocal.Mat(gMat);

        cvCopy(framePrev, prev_image, null);
        opencv_imgproc.cvCvtColor(prev_image, prev_grey, opencv_imgproc.CV_BGR2GRAY);
        prev_grey_pyramid.rebuild(prev_grey);

        IplImage prev_grey_temp = cvCloneImage(prev_grey_pyramid.getImage(ixyScale));

        Mat gMatPrev = new Mat(prev_grey_temp);
        Protocal.Mat sgMatPrev = new Protocal.Mat(gMatPrev);

        collector.emit(STREAM_GREY_FLOW, tuple, new Values(frameId, sgMat, sgMatPrev));

        int width = cvFloor(grey.width() / min_distance);
        int height = cvFloor(grey.height() / min_distance);
        if (frameId == 1 || frameId % init_counter == 0) {

            this.eig = cvCloneImage(eig_pyramid.getImage(ixyScale));
            double[] maxVal = new double[1];
            maxVal[0] = 0.0;
            opencv_imgproc.cvCornerMinEigenVal(grey, this.eig, 3, 3);
            cvMinMaxLoc(eig, null, maxVal, null, null, null);
            double threshold = maxVal[0] * quality;
            int offset = cvFloor(min_distance / 2.0);

            List<List<float[]>> group = new ArrayList<>();

            for (int i = 0; i < traceGeneratorTasks.size(); i++) {
                List<float[]> subGroup = new ArrayList<>();
                group.add(subGroup);
            }

            int floatArraySize = grey.width() + offset + 1;
            for (int i = 0; i < height; i++) {
                int y = opencv_core.cvFloor(i * min_distance + offset);

                FloatBuffer floatBuffer = eig.getByteBuffer(y * eig.widthStep()).asFloatBuffer();
                float[] floatArray = new float[floatArraySize];
                floatBuffer.get(floatArray);

                int index = i % traceGeneratorTasks.size();
                group.get(index).add(floatArray);
            }

            //Mat eigMat = new Mat(eig);
            //Serializable.Mat sEigMat = new Serializable.Mat(eigMat);
            //collector.emit(STREAM_EIG_FLOW, tuple, new Values(frameId, sEigMat, new EigRelatedInfo(width, height, offset, threshold)));
            for (int i = 0; i < traceGeneratorTasks.size(); i++) {
                int tID = traceGeneratorTasks.get(i);
                //System.out.println("i: " + i + ", tID: " + tID + ", size: " + group.get(i).size() + ",w: "+ width + ", h: " + height + ",off: " + offset + ", min_dis:" + minDistance);
                collector.emitDirect(tID, STREAM_EIG_FLOW, tuple, new Values(frameId, group.get(i),
                        new Protocal.EigRelatedInfo(width, height, offset, threshold)));
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_GREY_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT,
                FIELD_FRAME_MAT_PREV));
        outputFieldsDeclarer.declareStream(STREAM_EIG_FLOW, true, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT,
                FIELD_EIG_INFO));
        outputFieldsDeclarer.declareStream(STREAM_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT));
    }
}
