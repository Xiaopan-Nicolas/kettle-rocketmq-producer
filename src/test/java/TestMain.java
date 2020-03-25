import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Author: cunxiaopan
 * @Date: 2020/2/24 3:24 PM
 * @Description:
 */
public class TestMain {

  /*public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException, MQBrokerException {
    //Instantiate with a producer group name.
    DefaultMQProducer producer = new
        DefaultMQProducer("please_rename_unique_group_name_4");
    producer.setSendMsgTimeout(6000);
    producer.setNamesrvAddr("cunmq.com:9876");
    System.out.println(System.getProperty("NAMESRV_ADDR"));
    //Launch the instance.
    producer.start();
    for (int i = 0; i < 10; i++) {
      //Create a message instance, specifying topic, tag and message body.
      Message msg = new Message("TopicTest" ,
          "TagA" ,
          ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
          *//* Message body *//*
      );
      //Call send message to deliver message to one of brokers.
      SendResult sendResult = producer.send(msg);
      System.out.printf("%s%n", sendResult);
    }
    //Shut down once the producer instance is not longer in use.
    producer.shutdown();
  }*/
}
