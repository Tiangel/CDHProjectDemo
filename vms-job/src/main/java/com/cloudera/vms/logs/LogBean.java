package com.cloudera.vms.logs;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
public @interface LogBean {
	String name() default "";//名称
	int size();// key size
	char placeholder() ;// 占位符
	char separator() ;//分隔符
	String pattern() default "";//格式
	String description() default "";//描述
}
