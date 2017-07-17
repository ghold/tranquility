/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.metamx.tranquility.kafka;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.kafka.curator.CuratorClient;
import com.metamx.tranquility.kafka.model.PropertiesBasedKafkaConfig;
import com.metamx.tranquility.kafka.writer.WriterController;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * tranquility-kafka main.
 */
@Command(name = "tranquility-kafka", description = "Kafka consumer which pushes events to Druid through Tranquility")
public class KafkaMain //implements IService
{
  private static final Logger log = new Logger(KafkaMain.class);

  @Inject
  public HelpOption helpOption;

  @Option(name = {"-f", "-configFile"}, description = "Path to configuration property file")
  public String propertiesFile;

  @Option(name = {"-z", "-zookeeper"}, description = "zookeeper for tranquility management")
  public String zookeeperConnect;

  @Option(name = {"-n", "-namespace"}, description = "tranquility namespace")
  public String namespace = "/tranquility/instances";

  @Option(name = {"-i", "-instance"}, description = "tranquility instance name")
  public String instance;

  private KafkaConsumer kafkaConsumer;
  private static KafkaMain main;
  private static CuratorClient curator;
  private static String instancePath;

  public static void main(String[] args) throws Exception
  {
//    KafkaMain main;
    try {
      main = SingleCommand.singleCommand(KafkaMain.class).parse(args);
    }
    catch (Exception e) {
      log.error(e, "Exception parsing arguments");
      Help.help(SingleCommand.singleCommand(KafkaMain.class).getCommandMetadata());
      return;
    }

    if (main.helpOption.showHelpIfRequested()) {
      return;
    }

    main.run();
    Thread.sleep(Long.MAX_VALUE);

  }

  public void run() throws InterruptedException
  {
    instancePath = main.namespace + "/" + main.instance;
    try {
      curator = new CuratorClient(main.zookeeperConnect, instancePath,10, 5000);
      curator.getClient().getChildren().usingWatcher(watcherBuilder()).forPath(instancePath);
    } catch (Exception e) {
      throw new InterruptedException(e.getLocalizedMessage());
    }
    if (propertiesFile == null || propertiesFile.isEmpty()) {
      helpOption.help = true;
      helpOption.showHelpIfRequested();

      log.warn("Missing required parameters, aborting.");
      return;
    }

    TranquilityConfig<PropertiesBasedKafkaConfig> config = null;
    try (InputStream in = new FileInputStream(propertiesFile)) {
      config = TranquilityConfig.read(in, PropertiesBasedKafkaConfig.class);
    }
    catch (IOException e) {
      log.error("Could not read config file: %s, aborting.", propertiesFile);
      Throwables.propagate(e);
    }

    PropertiesBasedKafkaConfig globalConfig = config.globalConfig();
    Map<String, DataSourceConfig<PropertiesBasedKafkaConfig>> dataSourceConfigs = Maps.newHashMap();
    for (String dataSource : config.getDataSources()) {
      dataSourceConfigs.put(dataSource, config.getDataSource(dataSource));
    }

    // find all properties that start with 'kafka.' and pass them on to Kafka
    final Properties kafkaProperties = new Properties();
    for (String propertyName : config.globalConfig().properties().stringPropertyNames()) {
      if (propertyName.startsWith("kafka.")) {
        kafkaProperties.setProperty(
            propertyName.replaceFirst("kafka\\.", ""),
            config.globalConfig().properties().getProperty(propertyName)
        );
      }
    }

    // set the critical Kafka configs again from TranquilityKafkaConfig so it picks up the defaults
    kafkaProperties.setProperty("group.id", globalConfig.getKafkaGroupId());
    kafkaProperties.setProperty("zookeeper.connect", globalConfig.getKafkaZookeeperConnect());
    if (kafkaProperties.setProperty(
        "zookeeper.session.timeout.ms",
        Long.toString(globalConfig.zookeeperTimeout().toStandardDuration().getMillis())
    ) != null) {
      throw new IllegalArgumentException(
          "Set zookeeper.timeout instead of setting kafka.zookeeper.session.timeout.ms"
      );
    }

    final WriterController writerController = new WriterController(dataSourceConfigs);
      final KafkaConsumer kafkaConsumer = new KafkaConsumer(
        globalConfig,
        kafkaProperties,
        dataSourceConfigs,
        writerController
    );

    try {
      this.setKafkaConsumer(kafkaConsumer);
      kafkaConsumer.start();
    }
    catch (Throwable t) {
      log.error(t, "Error while starting up. Exiting.");
      System.exit(1);
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Initiating shutdown...");
                curator.close(instancePath);
                kafkaConsumer.stop();
              }
            }
        )
    );

    kafkaConsumer.join();
  }

    public KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public KafkaMain getMain() {
        return main;
    }

    public void setMain(KafkaMain main) {
        this.main = main;
    }

//    @Override
  private void restart() throws RemoteException,InterruptedException {
    log.info("Initiating restart...");
    log.info("Initiating shutdown...");
    curator.close(instancePath);
    this.kafkaConsumer.stop();
    log.info("Initiating start...");
    this.getMain().run();
  }

  private static CuratorWatcher watcherBuilder() {
    return new CuratorWatcher() {
      @Override
      public void process(WatchedEvent event) throws Exception {
        switch (event.getType()) {
          case NodeChildrenChanged:
            try {
              KafkaMain.messageHandle(event.getPath());
            } catch (Exception e) {
              e.printStackTrace();
            }
            break;
        }
      }
    };
  }

  private static void messageHandle(String watchPath) throws Exception {
    List<String> children = curator.getClient().getChildren().forPath(watchPath);
    for (String a : children) {
      String childrenPath = watchPath + "/" + a;
      curator.getClient().delete().forPath(childrenPath);
    }
    main.restart();
  }
}
