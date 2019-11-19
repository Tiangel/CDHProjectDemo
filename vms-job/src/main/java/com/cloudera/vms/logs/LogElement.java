package com.cloudera.vms.logs;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
@Documented
public @interface LogElement {
	String name() default "";

	int rank() default -1;

	String description() default "";
}
