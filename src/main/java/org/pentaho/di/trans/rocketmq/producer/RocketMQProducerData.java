package org.pentaho.di.trans.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * @Author: cunxiaopan
 * @Date: 2020/2/21 3:45 PM
 * @Description:
 */
public class RocketMQProducerData extends BaseStepData implements StepDataInterface {

  DefaultMQProducer producer;
  RowMetaInterface outputRowMeta;
  int messageFieldNr;
  int keyFieldNr;
  boolean messageIsString;
  boolean keyIsString;
  ValueMetaInterface messageFieldMeta;
  ValueMetaInterface keyFieldMeta;

}
