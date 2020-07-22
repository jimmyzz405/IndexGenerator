package com.index.generator.generator;


import com.index.generator.config.ElasticSearchConfig;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class IndexGenerator {



    public static void main(String[] args) {

        //传入参数，1=data_instance, 2=instance_relation, 3=attribute_relation
        String index = args[0];
        String serverIp=args[1];

        ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig();
        elasticSearchConfig.init(serverIp);
        RestHighLevelClient client = elasticSearchConfig.getClient();


        System.out.println("连接 es Cluster ==>" + serverIp);
        while (client == null) {
            try {
                System.out.println("init es client...");
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        String type = "";
        if (index.contains("1")) {
            type = "data_instance";
        }
        if (index.contains("2")) {
            type = "instance_relation";
        }
        if (index.contains("3")) {
            type = "attribute_relation";
        }

        System.out.println("当前要创建的index : " + type);
        String indexName=type;

        createIndex(indexName, type,serverIp);

        System.out.println("process finished!");
        elasticSearchConfig.destroy();
        System.exit(1);
    }


    private static void createIndex(String indexName,  String type, String serverIp) {


            try {
                CreateIndexRequest request = new CreateIndexRequest(indexName);
                request.settings(Settings.builder()
                        //设置数据分片数
                        .put("index.number_of_shards", 1)
                        //设置数据副本数量
                        .put("index.number_of_replicas", 0)
                        //索引刷新频率（刷新时间间隔）
                        .put("index.refresh_interval", "59s")
                        //是否在每次index，delete，update，bulk请求之后立即同步并提交translog
                        //request ：（默认）每次request后都进行一次fsync和commit操作
                        //async ：在后台每sync_interval时间进行一次fsync和commit
                        .put("index.translog.durability", "async")
                        //translog多久被同步到磁盘并提交一次
                        .put("index.translog.sync_interval", "600s")
                        //当操作达到多大时执行刷新
                        .put("index.translog.flush_threshold_size", "1024mb")
                        //merge最大合并数
                        .put("index.merge.scheduler.max_merge_count", 100)
                        //执行merge所用的线程数
                        .put("index.merge.scheduler.max_thread_count", 1)
                        //每层所允许的分段数
                        .put("index.merge.policy.segments_per_tier", 50)
                        //开启或关闭每个请求的缓存
                        .put("index.requests.cache.enable", "true"))
                        .mapping("_doc", createMapping(type));
                ElasticSearchConfig elasticSearchConfig = new ElasticSearchConfig();
                elasticSearchConfig.init(serverIp);
                RestHighLevelClient client = elasticSearchConfig.getClient();
                CreateIndexResponse response = client.indices()
                        .create(request, RequestOptions.DEFAULT);
                if (response.isAcknowledged()) {
                    elasticSearchConfig.destroy();
                    System.out.println("创建 index: " + indexName + " 成功！ ");
                }
            } catch (Exception e) {

                System.out.println("创建index : " + indexName + "失败！" );
                System.out.println("reason : " +e.getMessage());


            }

    }

    private static XContentBuilder createMapping(String type) {
        // type= data_instance
        if ("data_instance".equalsIgnoreCase(type)) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {


                        builder.startArray("dynamic_templates");
                        /*第一个mapping*/
                        builder.startObject();
                        {
                            builder.startObject("time_info");
                            {
                                builder.field("match_mapping_type", "string");
                                builder.field("match", "*_time");
                                builder.field("unmatch", "extra_*");
                                builder.startObject("mapping");
                                {
                                    builder.field("type", "date");
                                    builder.field("format", "yyyy-MM-dd HH:mm:ss");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                    builder.endObject();

                        /*第二个mapping*/
                        builder.startObject();
                        {
                            builder.startObject("extra_info");
                            {
                                builder.field("match_mapping_type", "string");
                                builder.field("match", "*extra_*");
                                builder.field("unmatch", "*_time");
                                builder.startObject("mapping");
                                {
                                    builder.field("type", "keyword");

                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                     /* 第三个mapping*/
                        builder.startObject();
                        {
                            builder.startObject("stat_info");
                            {
                                builder.field("match_mapping_type", "string");
                                builder.field("match", "stat_*");
                                builder.startObject("mapping");
                                {
                                    builder.field("type", "long");
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();



                        builder.endArray();

                        //properties
                        builder.startObject("properties");
                    {

                        builder.startObject("project");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("person_in_charge");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("topic");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();
                        builder.startObject("classification");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("tags");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("instance_name");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("comment");
                        {
                            builder.field("type","keyword");
                        }
                        builder.endObject();

                        builder.startObject("attributes");
                        {
                            builder.field("type","nested");
                            builder.startObject("properties");
                            {

                                builder.startObject("attribute_id");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();

                                builder.startObject("attribute_name");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();

                                builder.startObject("attribute_cn_name");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();

                                builder.startObject("attribute_type");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();

                                builder.startObject("attribute_length");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();

                                builder.startObject("comment");
                                {
                                    builder.field("type","keyword");
                                }
                                builder.endObject();


                            }
                            builder.endObject();
                        }
                        builder.endObject();





                    //ascription
                    builder.startObject("ascription");
                    {
                        builder.startObject("properties");
                        {
                            builder.startObject("type");
                            {
                                builder.field("type","keyword");
                            }
                            builder.endObject();

                            builder.startObject("partition");
                            {
                                builder.field("type","keyword");
                            }
                            builder.endObject();

                            builder.startObject("is_tmp");
                            {
                                builder.field("type","boolean");
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    }
                    builder.endObject();

                }
                builder.endObject();
//                builder.endObject();
                return builder;
            } catch (IOException e) {
                System.out.println("构造mapping 失败! reason" + e.getMessage());
            }



            //type = attribute_relation
        } else if("attribute_relation".equalsIgnoreCase(type)) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {

                       builder.startObject("properties");
                        {
                            builder.startObject("process_definition_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("task_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("instance_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("source_attribute_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("target_attribute_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();
                        }
                       builder.endObject();

                }
                builder.endObject();
                return builder;
            } catch (IOException e) {
                System.out.println("构造mapping 失败! reason" + e.getMessage());
            }

        }



         //type = instance_relation
        else if("instance_relation".equalsIgnoreCase(type)) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {

                        builder.startObject("properties");
                        {
                            builder.startObject("process_definition_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("task_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();

                            builder.startObject("source_instance_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();


                            builder.startObject("target_instance_id");
                            {
                                builder.field("type", "keyword");
                            }
                            builder.endObject();
                        }
                        builder.endObject();

                }
                builder.endObject();
                return builder;
            } catch (IOException e) {
                System.out.println("构造mapping 失败! reason" + e.getMessage());
            }

        }
        return null;
    }


}
