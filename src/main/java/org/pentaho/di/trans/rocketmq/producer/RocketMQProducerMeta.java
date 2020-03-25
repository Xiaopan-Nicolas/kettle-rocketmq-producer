package org.pentaho.di.trans.rocketmq.producer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * @Author: cunxiaopan
 * @Date: 2020/2/21 3:49 PM
 * @Description:
 */
@Step(
    id = "RocketMQProducer",
    image = "org/pentaho/di/trans/rocketmq/producer/resources/RocketMQProducer.svg",
    i18nPackageName="org.pentaho.di.trans.rocketmq.producer",
    name="RocketMQProducerDialog.Shell.Title",
    description = "RocketMqProducerDialog.Shell.Tooltip",
    categoryDescription="i18n:org.pentaho.di.trans.step:BaseStep.Category.Message")
public class RocketMQProducerMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String[] ROCKET_MQ_PROPERTIES_NAMES = new String[] { "server.connect","group.name","tag.name","producer.type"};

  public static final Map<String, String> ROCKETMQ_PROPERTIES_DEFAULTS = new HashMap<String, String>();
  static {
    ROCKETMQ_PROPERTIES_DEFAULTS.put("server.connect", "localhost:9876");
    ROCKETMQ_PROPERTIES_DEFAULTS.put("producer.type", "sync");
  }

  private Properties rocketMqProperties = new Properties();
  private String topic;
  private String messageField;
  private String keyField;

  public Properties getRocketMqProperties() {
    return rocketMqProperties;
  }

  /**
   * @return Kafka topic name
   */
  public String getTopic() {
    return topic;
  }

  /**
   * @param topic
   *            Kafka topic name
   */
  public void setTopic(String topic) {
    this.topic = topic;
  }

  /**
   * @return Target key field name in Kettle stream
   */
  public String getKeyField() {
    return keyField;
  }

  /**
   * @param field
   *            Target key field name in Kettle stream
   */
  public void setKeyField(String field) {
    this.keyField = field;
  }

  /**
   * @return Target message field name in Kettle stream
   */
  public String getMessageField() {
    return messageField;
  }

  /**
   * @param field
   *            Target message field name in Kettle stream
   */
  public void setMessageField(String field) {
    this.messageField = field;
  }

  public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String input[], String output[], RowMetaInterface info) {

    if (isEmpty(topic)) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQProducerMeta.Check.InvalidTopic"), stepMeta));
    }
    if (isEmpty(messageField)) {
      remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
          Messages.getString("RocketMQProducerMeta.Check.InvalidMessageField"), stepMeta));
    }
  }

  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans) {
    return new RocketMQStepProducer(stepMeta, stepDataInterface, cnr, transMeta, trans);
  }

  public StepDataInterface getStepData() {
    return new RocketMQProducerData();
  }

  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleXMLException {

    try {
      topic = XMLHandler.getTagValue(stepnode, "TOPIC");
      messageField = XMLHandler.getTagValue(stepnode, "FIELD");
      keyField = XMLHandler.getTagValue(stepnode, "KEYFIELD");
      Node kafkaNode = XMLHandler.getSubNode(stepnode, "KAFKA");
      String[] kafkaElements = XMLHandler.getNodeElements(kafkaNode);
      if (kafkaElements != null) {
        for (String propName : kafkaElements) {
          String value = XMLHandler.getTagValue(kafkaNode, propName);
          if (value != null) {
            rocketMqProperties.put(propName, value);
          }
        }
      }
    } catch (Exception e) {
      throw new KettleXMLException(Messages.getString("RocketMQProducerMeta.Exception.loadXml"), e);
    }
  }

  public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if (topic != null) {
      retval.append("    ").append(XMLHandler.addTagValue("TOPIC", topic));
    }
    if (messageField != null) {
      retval.append("    ").append(XMLHandler.addTagValue("FIELD", messageField));
    }
    if (keyField != null) {
      retval.append("    ").append(XMLHandler.addTagValue("KEYFIELD", keyField));
    }
    retval.append("    ").append(XMLHandler.openTag("KAFKA")).append(Const.CR);
    for (String name : rocketMqProperties.stringPropertyNames()) {
      String value = rocketMqProperties.getProperty(name);
      if (value != null) {
        retval.append("      " + XMLHandler.addTagValue(name, value));
      }
    }
    retval.append("    ").append(XMLHandler.closeTag("KAFKA")).append(Const.CR);
    return retval.toString();
  }

  public void readRep(Repository rep, ObjectId stepId, List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleException {
    try {
      topic = rep.getStepAttributeString(stepId, "TOPIC");
      messageField = rep.getStepAttributeString(stepId, "FIELD");
      keyField = rep.getStepAttributeString(stepId, "KEYFIELD");
      String kafkaPropsXML = rep.getStepAttributeString(stepId, "KAFKA");
      if (kafkaPropsXML != null) {
        rocketMqProperties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
      }
      // Support old versions:
      for (String name : ROCKET_MQ_PROPERTIES_NAMES) {
        String value = rep.getStepAttributeString(stepId, name);
        if (value != null) {
          rocketMqProperties.put(name, value);
        }
      }
    } catch (Exception e) {
      throw new KettleException("RocketMQProducerMeta.Exception.loadRep", e);
    }
  }

  public void saveRep(Repository rep, ObjectId transformationId, ObjectId stepId) throws KettleException {
    try {
      if (topic != null) {
        rep.saveStepAttribute(transformationId, stepId, "TOPIC", topic);
      }
      if (messageField != null) {
        rep.saveStepAttribute(transformationId, stepId, "FIELD", messageField);
      }
      if (keyField != null) {
        rep.saveStepAttribute(transformationId, stepId, "KEYFIELD", keyField);
      }
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      rocketMqProperties.storeToXML(buf, null);
      rep.saveStepAttribute(transformationId, stepId, "KAFKA", buf.toString());
    } catch (Exception e) {
      throw new KettleException("RocketMQProducerMeta.Exception.saveRep", e);
    }
  }

  public void setDefault() {
  }

  public static boolean isEmpty(String str) {
    return str == null || str.length() == 0;
  }
}
