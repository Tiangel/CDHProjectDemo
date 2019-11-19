

package com.cloudera.phoenixdemo.utils;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 响应信息主体
 *
 * @param <T>
 * @author nisan
 */
@ToString
public class ResponseDto<T> implements Serializable {
    private static final int SUCCESS = 200;
    private static final int FAIL = 201;
    private static final int AUTHF = 403;
    // 操作成功
    public static final String DEFAULT_SUCCESS_MESSAGE = "success";
    // 操作失败
    public static final String DEFAULT_FAILED_MESSAGE = "failed";
    private static final long serialVersionUID = 1L;
    @Getter
    @Setter
    private String msg = DEFAULT_SUCCESS_MESSAGE;

    @Getter
    @Setter
    private int code = SUCCESS;

    @Getter
    @Setter
    private Long time;

    @Getter
    @Setter
    private T data;

    public ResponseDto() {
        super();
    }

    public ResponseDto(T data) {
        super();
        this.data = data;
        this.time = System.currentTimeMillis();
    }

    public ResponseDto(T data, String msg) {
        super();
        this.data = data;
        this.msg = msg;
    }

    public static ResponseDto fallR(String msg) {
        return new ResponseDto(FAIL, msg);
    }

    public static ResponseDto authFail(String msg) {
        return new ResponseDto(AUTHF,msg);
    }

    public ResponseDto(Throwable e) {
        super();
        this.msg = e.getMessage();
        this.code = FAIL;
    }

    public ResponseDto(int code, String message) {
        super();
        this.msg = message;
        this.code = code;
    }
}
