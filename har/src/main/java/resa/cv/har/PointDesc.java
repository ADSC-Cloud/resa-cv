package resa.cv.har;

import org.bytedeco.javacpp.opencv_core;

/**
 * Created by Tom.fu on 2/12/2014.
 */
public class PointDesc implements java.io.Serializable{
    public float[] mbhX;
    public float[] mbhY;
    //public opencv_core.CvPoint2D32f point;
    public Protocal.CvPoint2D32f sPoint;

    PointDesc(DescInfo mbhInfo, opencv_core.CvPoint2D32f point){
        //mbhX = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        //mbhY = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        this.sPoint = new Protocal.CvPoint2D32f(point);
    }

    PointDesc(DescInfo mbhInfo, Protocal.CvPoint2D32f point) {
        //mbhX = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        //mbhY = new float[mbhInfo.nxCells * mbhInfo.nyCells * mbhInfo.nBins];
        this.sPoint = new Protocal.CvPoint2D32f(point);
    }
}