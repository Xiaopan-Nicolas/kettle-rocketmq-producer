package org.pentaho.di.trans.rocketmq.producer;

import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * @Author: cunxiaopan
 * @Date: 2020/2/21 4:07 PM
 * @Description:
 */
public class RocketMQStepProducer extends BaseStep implements StepInterface {


  private final static byte[] getUTFBytes(String source) {
    if (source == null) {
      return null;
    }
    try {
      return source.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      return null;
    }
  }

  public RocketMQStepProducer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    RocketMQProducerData data = (RocketMQProducerData) sdi;
    if (data.producer != null) {
      data.producer.shutdown();
      data.producer = null;
    }
    super.dispose(smi, sdi);
  }

  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    Object[] r = getRow();
    if (r == null) {
      setOutputDone();
      return false;
    }

    RocketMQProducerMeta meta = (RocketMQProducerMeta) smi;
    RocketMQProducerData data = (RocketMQProducerData) sdi;

    RowMetaInterface inputRowMeta = getInputRowMeta();

    if (first) {
      first = false;
      // Initialize Kafka client:
      if (data.producer == null) {
        Properties substProperties = new Properties();
        Properties properties = meta.getRocketMqProperties();
        for (Entry<Object, Object> e : properties.entrySet()) {
          substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
        }
        logBasic("Creating RocketMQ Producer Send Message To " + substProperties.getProperty("server.connect"));
        data.producer = new DefaultMQProducer(substProperties.getProperty("group.name"));
        data.producer.setNamesrvAddr(substProperties.getProperty("server.connect"));
        data.producer.setSendMsgTimeout(6000);
        try {
          data.producer.start();
        } catch (MQClientException e) {
          logError(">>>>>启动 Rocket MQ Producer 失败！", e);
          throw new KettleException(e);
        }
        logBasic(">>>>>启动 Rocket MQ Producer！");
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

      int numErrors = 0;

      String messageField = environmentSubstitute(meta.getMessageField());

      if (RocketMQProducerMeta.isEmpty(messageField)) {
        logError(Messages.getString("RocketMQProducerStep.Log.MessageFieldNameIsNull"));
        numErrors++;
      }
      data.messageFieldNr = inputRowMeta.indexOfValue(messageField);
      if (data.messageFieldNr < 0) {
        logError(Messages.getString("RocketMQProducerStep.Log.CouldntFindField", messageField));
        numErrors++;
      }
      if (!inputRowMeta.getValueMeta(data.messageFieldNr).isBinary()
          && !inputRowMeta.getValueMeta(data.messageFieldNr).isString()) {
        logError(Messages.getString("RocketMQProducerStep.Log.FieldNotValid", messageField));
        numErrors++;
      }
      data.messageIsString = inputRowMeta.getValueMeta(data.messageFieldNr).isString();
      data.messageFieldMeta = inputRowMeta.getValueMeta(data.messageFieldNr);

      String keyField = environmentSubstitute(meta.getKeyField());

      if (!RocketMQProducerMeta.isEmpty(keyField)) {
        logBasic(Messages.getString("RocketMQProducerStep.Log.UsingKey", keyField));

        data.keyFieldNr = inputRowMeta.indexOfValue(keyField);

        if (data.keyFieldNr < 0) {
          logError(Messages.getString("RocketMQProducerStep.Log.CouldntFindField", keyField));
          numErrors++;
        }
        if (!inputRowMeta.getValueMeta(data.keyFieldNr).isBinary()
            && !inputRowMeta.getValueMeta(data.keyFieldNr).isString()) {
          logError(Messages.getString("RocketMQProducerStep.Log.FieldNotValid", keyField));
          numErrors++;
        }
        data.keyIsString = inputRowMeta.getValueMeta(data.keyFieldNr).isString();
        data.keyFieldMeta = inputRowMeta.getValueMeta(data.keyFieldNr);

      } else {
        data.keyFieldNr = -1;
      }

      if (numErrors > 0) {
        setErrors(numErrors);
        stopAll();
        return false;
      }
    }

    try {
      byte[] message = null;
      if (data.messageIsString) {
        logBasic("message:" + data.messageFieldMeta.getString(r[data.messageFieldNr]));
        message = getUTFBytes(data.messageFieldMeta.getString(r[data.messageFieldNr]));
      } else {
        message = data.messageFieldMeta.getBinary(r[data.messageFieldNr]);
      }
      String topic = environmentSubstitute(meta.getTopic());
      if (isRowLevel()) {
        logDebug(Messages.getString("RocketMQProducerStep.Log.SendingData", topic));
        logRowlevel(data.messageFieldMeta.getString(r[data.messageFieldNr]));
      }

      Message msg = null;
      Properties properties = meta.getRocketMqProperties();
      String tagName = properties.getProperty("tag.name");
      if (data.keyFieldNr < 0) {
        msg = new Message(meta.getTopic(), tagName, message);
      } else {
        byte[] key = null;
        if (data.keyIsString) {
          key = getUTFBytes(data.keyFieldMeta.getString(r[data.keyFieldNr]));
        } else {
          key = data.keyFieldMeta.getBinary(r[data.keyFieldNr]);
        }
        msg = new Message(meta.getTopic(), tagName, key);
      }

      try {
        if ("async".equals(properties.getProperty("producer.type"))) {
          // 可靠异步
          logBasic(">>>>>使用可靠异步方式发送消息！");
          data.producer.send(msg, new SendCallback() {
            public void onSuccess(SendResult sendResult) {
              logBasic(">>>>>发送消息成功！msgId=" + sendResult.getMsgId());
            }

            public void onException(Throwable e) {
              logError("发送消息失败！", e);
            }
          });
        } else if ("oneway".equals(properties.getProperty("producer.type"))) {
          // 单向发送
          logBasic(">>>>>使用单向方式发送消息！");
          data.producer.sendOneway(msg);
        } else {
          //可靠同步
          logBasic(">>>>>使用可靠同步方式发送消息！");
          SendResult sendResult = data.producer.send(msg);
          System.out.printf("%s%n", sendResult);
        }
      } catch (Exception e) {
        logError("发送消息失败！", e);
        throw new KettleException(e);
      }

      incrementLinesOutput();
    } catch (KettleException e) {
      if (!getStepMeta().isDoingErrorHandling()) {
        logError(Messages.getString("RocketMQProducerStep.ErrorInStepRunning", e.getMessage()));
        setErrors(1);
        stopAll();
        setOutputDone();
        return false;
      }
      putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
    }
    return true;
  }

  public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    RocketMQProducerData data = (RocketMQProducerData) sdi;
    if (data.producer != null) {
      data.producer.shutdown();
      data.producer = null;
    }
    super.stopRunning(smi, sdi);
  }
}
