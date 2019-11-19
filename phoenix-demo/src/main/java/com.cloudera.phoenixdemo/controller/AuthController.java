package com.cloudera.phoenixdemo.controller;

import com.cloudera.phoenixdemo.dao.outer.UserMapper;
import com.cloudera.phoenixdemo.entity.Auth;
import com.cloudera.phoenixdemo.entity.User;
import com.cloudera.phoenixdemo.service.AuthService;
import com.cloudera.phoenixdemo.shiro.JWTUtil;
import com.cloudera.phoenixdemo.utils.ResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Map;


@RestController()
@RequestMapping("/auth")
@Slf4j
public class AuthController {

    @Value("${auth.access.token.expiretime}")
    private String expireTime;

    @Autowired
    private AuthService authService;
//    @Autowired
//    KafkaConsumerListener kafkaConsumerListener;

    @Autowired
    private UserMapper userMapper;

    @RequestMapping("gateway.do")
    public ResponseDto gateWayDo(Map<String, String> params) {
        return authService.gatewayDo(params);
    }

    @PostMapping("/login")
    public ResponseDto login(
            @RequestBody Auth user
    ) throws Exception {
        long timeStamp = System.currentTimeMillis();
//        System.out.println(timeStamp/1000);
//        System.out.println(user.getTimestamp());
//        System.out.println(expireTime);
        System.out.println(user.getAppId());
        if(user.getAppId() == null || user.getSig() == null || user.getTimestamp() == null) {
            return ResponseDto.fallR("参数错误");
        }
        // 验证时间戳过期
        if (( timeStamp / 1000 - Long.valueOf(user.getTimestamp())) > Long.valueOf(expireTime)) {
            return ResponseDto.fallR("认证参数已过期");
        }

        User userTemp = userMapper.getOne(user.getAppId());
        // 验证sig
        MessageDigest messageDigest = MessageDigest.getInstance("md5");
        System.out.println(userTemp.getUserName() + userTemp.getPassword() + user.getTimestamp());
        messageDigest.update((userTemp.getUserName() + userTemp.getPassword() + user.getTimestamp()).getBytes());
        //将加密后的数据转换为16进制数字
        String md5code = new BigInteger(1, messageDigest.digest()).toString(16);// 16进制数字
        // 如果生成数字未满32位，需要前面补0
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        System.out.println(md5code);
        if (!user.getSig().equals(md5code)) {
            return ResponseDto.fallR("认证失败");
        }
        // 生成token
        String token = JWTUtil.sign(Long.valueOf(userTemp.getId()), String.valueOf(userTemp.getUserName()));
        return new ResponseDto(token);
    }
//    @GetMapping("/saveLastInfo")
//    public ResponseDto getHive() {
//        kafkaConsumerListener.saveLastKafkaProfileData();
//        return new ResponseDto();
//    }
//    /**
//     * 参数校验
//     *
//     * @param params
//     */
//    void valideParams(Map<String, String> params) {
//        if (params == null) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20001);
//        }
//        if (!params.containsKey("app_id")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20002);
//        }
//        if (!params.containsKey("method")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20003);
//        }
//        if (!params.containsKey("timestamp")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20004);
//        } else {
//            String timestamp = params.get("timestamp");
//            try {
//                DateUtils.dateString2Date(timestamp, "yyyy-MM-dd HH:mm:ss");
//            } catch (Exception e) {
//                log.error("timestamp 参数格式错误");
//                throw new CommonException(ErrorCodeConstant.ERROR_20004);
//            }
//        }
//        if (!params.containsKey("sign")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20005);
//        }
//        if (!params.containsKey("version")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20006);
//        }
//        if (!params.containsKey("biz_content")) {
//            throw new CommonException(ErrorCodeConstant.ERROR_20008);
//        }
//    }
}
