package com.sofkyle.elasticjob.autoconfiguration;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.api.script.ScriptJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbConfiguration;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.sofkyle.elasticjob.annotation.ElasticDataflowJob;
import com.sofkyle.elasticjob.annotation.ElasticScriptJob;
import com.sofkyle.elasticjob.annotation.ElasticSimpleJob;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.Map;
import java.util.Optional;

/**
 * @author: Kyle
 */
@Configuration
@ConditionalOnExpression("'${elasticjob.zookeeper.server-list}'.length() > 0")
public class ElasticJobAutoConfiguration {
    @Value("${elastic-job.zookeeper.server-list}")
    private String serverList;

    @Value("${elastic-job.zookeeper.namespace}")
    private String namespace;

    @Value("${elastic-job.datasource}")
    private String datasource;

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Zookeeper Registry Center
     */
    private ZookeeperRegistryCenter registryCenter;

    @PostConstruct
    public void initElasticJob(){
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration(serverList, namespace);
        zookeeperConfiguration.setConnectionTimeoutMilliseconds(10000);
        registryCenter = new ZookeeperRegistryCenter(zookeeperConfiguration);
        registryCenter.init();

        // simple job
        initSimpleJob();

        // dataflow job
        initDataFlowJob();

        // script job
        initScriptJob();
    }

    /**
     * init simple job
     */
    private void initSimpleJob() {
        Map<String, SimpleJob> map = applicationContext.getBeansOfType(SimpleJob.class);

        for(Map.Entry<String, SimpleJob> entry : map.entrySet()){
            SimpleJob simpleJob = entry.getValue();
            ElasticSimpleJob elasticSimpleJob = simpleJob.getClass().getAnnotation(ElasticSimpleJob.class);

            String cron = Optional.of(elasticSimpleJob.cron()).get();
            String jobName = StringUtils.defaultIfBlank(elasticSimpleJob.jobName(), simpleJob.getClass().getName());
            JobCoreConfiguration jobCoreConfiguration = JobCoreConfiguration
                    .newBuilder(jobName, cron, elasticSimpleJob.shardingTotalCount())
                    .shardingItemParameters(elasticSimpleJob.shardingItemParameters())
                    .jobParameter(elasticSimpleJob.jobParameter())
                    .failover(elasticSimpleJob.failover())
                    .misfire(elasticSimpleJob.misfire())
                    .description(elasticSimpleJob.description())
                    .build();

            SimpleJobConfiguration simpleJobConfiguration = new SimpleJobConfiguration(jobCoreConfiguration,
                    simpleJob.getClass().getCanonicalName());

            LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(simpleJobConfiguration)
                    .overwrite(elasticSimpleJob.overwrite()).build();

            registry(simpleJob, liteJobConfiguration);
        }
    }

    /**
     * init data flow job
     */
    private void initDataFlowJob() {
        Map<String, DataflowJob> map = applicationContext.getBeansOfType(DataflowJob.class);

        for(Map.Entry<String, DataflowJob> entry : map.entrySet()){
            DataflowJob dataflowJob = entry.getValue();

            ElasticDataflowJob elasticDataflowJob = dataflowJob.getClass().getAnnotation(ElasticDataflowJob.class);

            String cron = Optional.of(elasticDataflowJob.cron()).get();
            String jobName = StringUtils.defaultIfBlank(elasticDataflowJob.jobName(), dataflowJob.getClass().getName());
            JobCoreConfiguration jobCoreConfiguration = JobCoreConfiguration
                    .newBuilder(jobName, cron, elasticDataflowJob.shardingTotalCount())
                    .shardingItemParameters(elasticDataflowJob.shardingItemParameters())
                    .jobParameter(elasticDataflowJob.jobParameter())
                    .failover(elasticDataflowJob.failover())
                    .misfire(elasticDataflowJob.misfire())
                    .description(elasticDataflowJob.description())
                    .build();

            DataflowJobConfiguration dataflowJobConfiguration = new DataflowJobConfiguration(jobCoreConfiguration,
                    dataflowJob.getClass().getCanonicalName(), elasticDataflowJob.streamingProcess());

            LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(dataflowJobConfiguration)
                    .overwrite(elasticDataflowJob.overwrite()).build();

            registry(dataflowJob, liteJobConfiguration);
        }
    }

    /**
     * init script job
     */
    private void initScriptJob() {
        Map<String, ScriptJob> map = applicationContext.getBeansOfType(ScriptJob.class);

        for(Map.Entry<String, ScriptJob> entry : map.entrySet()){
            ScriptJob scriptJob = entry.getValue();

            ElasticScriptJob elasticScriptJob = scriptJob.getClass().getAnnotation(ElasticScriptJob.class);

            String cron = Optional.of(elasticScriptJob.cron()).get();
            String jobName = StringUtils.defaultIfBlank(elasticScriptJob.jobName(), scriptJob.getClass().getName());
            JobCoreConfiguration jobCoreConfiguration = JobCoreConfiguration
                    .newBuilder(jobName, cron, elasticScriptJob.shardingTotalCount())
                    .shardingItemParameters(elasticScriptJob.shardingItemParameters())
                    .jobParameter(elasticScriptJob.jobParameter())
                    .failover(elasticScriptJob.failover())
                    .misfire(elasticScriptJob.misfire())
                    .description(elasticScriptJob.description())
                    .build();

            ScriptJobConfiguration scriptJobConfiguration = new ScriptJobConfiguration(jobCoreConfiguration,
                    elasticScriptJob.scriptCommandLine());

            LiteJobConfiguration liteJobConfiguration = LiteJobConfiguration.newBuilder(scriptJobConfiguration)
                    .overwrite(elasticScriptJob.overwrite()).build();

            registry(scriptJob, liteJobConfiguration);
        }
    }

    /**
     * registry all type of elastic jobs
     *  @param elasticJob
     * @param liteJobConfiguration
     */
    private void registry(ElasticJob elasticJob, LiteJobConfiguration liteJobConfiguration) {
        if(StringUtils.isNotBlank(datasource)){
            if(!applicationContext.containsBean(datasource)){
                throw new RuntimeException("not exist datasource [" + datasource + "] !");
            }

            DataSource dataSource = (DataSource) applicationContext.getBean(datasource);
            JobEventRdbConfiguration jobEventRdbConfiguration = new JobEventRdbConfiguration(dataSource);
            SpringJobScheduler jobScheduler = new SpringJobScheduler(elasticJob, registryCenter, liteJobConfiguration,jobEventRdbConfiguration);
            jobScheduler.init();
        }else{
            SpringJobScheduler jobScheduler = new SpringJobScheduler(elasticJob, registryCenter, liteJobConfiguration);
            jobScheduler.init();
        }
    }
}
