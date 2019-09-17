package com.sofkyle.elasticjob.annotation;

import java.lang.annotation.*;

/**
 * @author: Kyle
 */
@Inherited
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ElasticScriptJob {

    String jobName() default "";

    String cron() default "";

    int shardingTotalCount() default 1;

    String shardingItemParameters() default "";

    String jobParameter() default "";

    boolean failover() default false;

    boolean misfire() default false;

    String description() default "";

    String scriptCommandLine() default "";

    boolean overwrite() default false;
}
