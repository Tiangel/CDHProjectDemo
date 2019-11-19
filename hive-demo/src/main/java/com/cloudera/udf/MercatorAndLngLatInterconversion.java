package com.cloudera.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;


/**
 * 经纬度和墨卡托相互转换
 */
@Description(name = "mercator_lnglat_convert",
        value = "mercator_lnglat_convert(String a , String b, boolean flag) ==> return result ",
        extended = "Example:\n"
                + " mercator_lnglat_convert(\"12727039.383734727\", \"3579066.6894065146\") ==> \"114.3289400||30.5857480\" \n"
                + " mercator_lnglat_convert(\"12727039.383734727\", \"3579066.6894065146\", true) ==> \"114.3289400||30.5857480\" \n"
                + " mercator_lnglat_convert(\"12727039.383734727\", \"3579066.6894065146\", 7) ==> \"114.3289400||30.5857480\" \n"
                + " mercator_lnglat_convert(\"12727039.383734727\", \"3579066.6894065146\", true, 9) ==> \"114.328940016||30.585748004\" \n"
                + " mercator_lnglat_convert(\"114.32894\", \"30.585748\", false, 9) ==> \"12727039.383734727||3404789.892891386\";"
)
public class MercatorAndLngLatInterconversion extends UDF {

    /**
     * 地球半径
     */
    private final static double EARTH_RADIUS = 6378137.0;

    private final static double EARTH_SEMI_PERIMETER = 20037508.34;

    private final static int DEFAULT_LEN = 7;

    private final Text result = new Text();

    /**
     * @param flag   true 为经纬度转墨卡托，false  为墨卡托转经纬度
     * @param num   保留小数点后面的位数，默认7位
     */
    public Text evaluate(String value1, String value2, boolean flag, int num) {
        String x = "";
        String y = "";
        try {
            String pattern = "#." + String.format("%0" + num + "d", 0);
            DecimalFormat format = new DecimalFormat(pattern);
            if (flag) {
                x = format.format(getLng(Double.valueOf(value1)));
                y = format.format(getLat(Double.valueOf(value2)));
            } else {
                x = format.format(getMercatorX(Double.valueOf(value1)));
                y = format.format(getMercatorX(Double.valueOf(value2)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        result.set(x + "||" + y);
        return result;
    }

    public Text evaluate(String value1, String value2) {
        return evaluate(value1, value2, true, DEFAULT_LEN);
    }

    public Text evaluate(String value1, String value2, int num) {
        return evaluate(value1, value2, true, num);
    }
    public Text evaluate(String value1, String value2, boolean flag) {
        return evaluate(value1, value2, flag, DEFAULT_LEN);
    }

    /**
     * 经度转墨卡托坐标 X
     */
    private double getMercatorX(double lng) {
        double mercator_X = lng * Math.PI / 180 * EARTH_RADIUS;
        return mercator_X;
    }

    /**
     * 纬度转墨卡托坐标 Y
     */
    private double getMercatorY(double lat) {
        double a = lat * Math.PI / 180;
        double mercator_Y = EARTH_RADIUS / 2 * Math.log((1.0 + Math.sin(a)) / (1.0 - Math.sin(a)));
        return mercator_Y;
    }

    /**
     * 墨卡托转经度
     */
    private double getLng(double poi_X) {
        double lng = poi_X / EARTH_SEMI_PERIMETER * 180;
        return lng;
    }

    /**
     * 墨卡托转纬度
     */
    private double getLat(double poi_Y) {
        double mmy = poi_Y / EARTH_SEMI_PERIMETER * 180;
        double lat = 180 / Math.PI * (2 * Math.atan(Math.exp(mmy * Math.PI / 180)) - Math.PI / 2);
        return lat;
    }

    public static void main(String[] args) {
        MercatorAndLngLatInterconversion a = new MercatorAndLngLatInterconversion();
        double mercatorX = a.getMercatorX(114.32894);
        double mercatorY = a.getMercatorY(30.585748);
        double lng = a.getLng(12727039.383734727);
        double lat = a.getLat(3579066.6894065146);
        System.out.println(mercatorX + "========" + mercatorY);
        System.out.println(lng + "========" + lat);

        Text evaluate = a.evaluate("12727039.383734727", "3579066.6894065146", true, 9);
        System.out.println(evaluate);
        Text evaluate1 = a.evaluate("12727039.383734727", "3579066.6894065146");
        System.out.println(evaluate1);
        Text evaluate2 = a.evaluate("12727039.383734727", "3579066.6894065146", 7);
        System.out.println(evaluate2);
        Text evaluate3 = a.evaluate("114.32894", "30.585748", false);
        System.out.println(evaluate3);
        Text evaluate4 = a.evaluate("114.32894", "30.585748", false, 9);
        System.out.println(evaluate4);
    }
}
