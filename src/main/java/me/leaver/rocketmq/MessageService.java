package me.leaver.rocketmq;

import com.alibaba.fastjson.JSON;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.Charsets;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;

@Component
public class MessageService {
    Map<String, DefaultMQAdminExt> defaultMQAdminExts = new HashMap<>();

    Map<String, DefaultMQProducer> defaultProducers = new HashMap<>();

    @Tool(description = "通过topic和消息id查询消息", name = "查询消息")
    MessageRecord queryMessageById(String nameserver, String ak, String sk, String topic, String messageId) {

        if (defaultMQAdminExts.get(nameserver) == null) {
            AclClientRPCHook rpcHook = new AclClientRPCHook(new SessionCredentials(ak, sk));
            DefaultMQAdminExt defaultMQAdminExt;
            defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
            defaultMQAdminExt.setNamesrvAddr(nameserver);
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                defaultMQAdminExt.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
            defaultMQAdminExts.put(nameserver, defaultMQAdminExt);
        }

        DefaultMQAdminExt defaultMQAdminExt = defaultMQAdminExts.get(nameserver);

        QueryResult result = null;
        try {
            result = defaultMQAdminExt.queryMessageByUniqKey(topic, messageId, 10, MessageClientIDSetter.getNearlyTimeFromID(messageId).getTime() - 1000 * 60 * 60 * 13L,
                Long.MAX_VALUE);
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (result.getMessageList().size() > 0) {
            MessageExt ext = result.getMessageList().get(0);
            return new MessageRecord(ext.getTopic(), messageId, new String(ext.getBody(), Charsets.UTF_8), JSON.toJSONString(ext.getProperties()));
        } else {
            return null;
        }
    }

    @Tool(description = "给指定topic发送消息", name = "发送消息")
    MessageResult sendMessage(String nameserver, String ak, String sk, String topic, String body) {

        if (defaultMQAdminExts.get(nameserver) == null) {
            AclClientRPCHook rpcHook = new AclClientRPCHook(new SessionCredentials(ak, sk));
            DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ReSendMsgById", rpcHook);
            defaultMQProducer.setNamesrvAddr(nameserver);
            defaultMQProducer.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                defaultMQProducer.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }

            defaultProducers.put(nameserver, defaultMQProducer);
        }
        DefaultMQProducer defaultMQProducer = defaultProducers.get(nameserver);
        SendResult sendResult;
        try {
            sendResult = defaultMQProducer.send(new org.apache.rocketmq.common.message.Message(topic, body.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new MessageResult(topic, sendResult.getMsgId());
    }

    record MessageRecord(String topic, String messageId, String body, String properties) {
    }

    record MessageResult(String topic, String messageId) {
    }
}