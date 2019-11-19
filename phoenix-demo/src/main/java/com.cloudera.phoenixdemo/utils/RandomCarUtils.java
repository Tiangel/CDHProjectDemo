package com.cloudera.phoenixdemo.utils;

import com.alibaba.fastjson.JSON;

import java.util.*;

/**
 * @author Charles
 * @package com.hdjt.bigdata.util
 * @classname CarUtil
 * @description 随机生成车辆相关属性
 * @date 2019-5-13 10:54
 */
public class RandomCarUtils {

    private static final List<String> BRAND_LIST = Arrays.asList(
            "奔驰", "奥迪", "宝马", "林肯", "劳斯莱斯", "玛莎拉蒂", "法拉利","马自达", "凯迪拉克", "本田", "丰田",
            "比亚迪", "标致", "别克", "宾利", "宝骏", "大众", "哈佛", "吉利", "长安", "长城", "雷克萨斯", "沃尔沃"
    );
    private static final List<String> COLOR_LIST = Arrays.asList("白色", "银色", "黑色", "灰色", "蓝色", "红色", "棕色", "绿色", "其他");
    private static final List<String> MODE_LIST = Arrays.asList("小车", "大车", "超大车", "摩托车");
    private static final List<String> TYPE_LIST = Arrays.asList("普通车", "特殊车");


    /**
     * @param
     * @return java.lang.String
     * @description 随机生成牌照
     * 车牌号的组成一般为：省份+地区代码+5位数字/字母
     * @author Charles
     * @date 2019-5-13
     */
    public static String generateRandomCarID() {
        char[] provinceAbbr = { // 省份简称 4+22+5+3
                '京', '津', '沪', '渝',
                '冀', '豫', '云', '辽', '黑', '湘', '皖', '鲁', '苏', '浙', '赣',
                '鄂', '甘', '晋', '陕', '吉', '闽', '贵', '粤', '青', '川', '琼',
                '宁', '新', '藏', '桂', '蒙',
                '港', '澳', '台'
        };
        String alphas = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"; // 26个字母 + 10个数字

        Random random = new Random(); // 随机数生成器

        String carID = "";

        // 省份+地区代码+·  如 湘A· 这个点其实是个传感器，不过加上美观一些
        carID += provinceAbbr[random.nextInt(34)]; // 注意：分开加，因为加的是2个char
        carID += alphas.charAt(random.nextInt(26)) + "-";

        // 5位数字/字母
        for (int i = 0; i < 5; i++) {
            carID += alphas.charAt(random.nextInt(36));
        }
        return carID;
    }

    /**
     * @param carNum
     * @return java.lang.String
     * @description TODO
     * @author Charles
     * @date 2019-5-13
     */
    public static String generateCarList(int carNum) {

        List list = new ArrayList<Map>();

        for (int i = 0; i < carNum; i++) {
            Map map = new HashMap();
            map.put("plateNum", generateRandomCarID());
            map.put("carBrand", BRAND_LIST.get(RandomUserUtils.getNum(0, BRAND_LIST.size() - 1)));
            map.put("carColor", COLOR_LIST.get(RandomUserUtils.getNum(0, COLOR_LIST.size() - 1)));
            map.put("carMode", MODE_LIST.get(RandomUserUtils.getNum(0, MODE_LIST.size() - 1)));
            map.put("carType", TYPE_LIST.get(RandomUserUtils.getNum(0, TYPE_LIST.size() - 1)));
            list.add(map);
        }
        return JSON.toJSONString(list);
    }

    public static void main(String[] args) {

        System.out.println(generateCarList(4));

//        System.out.println(DigestUtils.md5Hex("465465"));

    }
}
