import com.ibm.mq.jms.*;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by user on 05.10.2018.
 */

class JmsClass extends Thread {

    String host = "localhost"; // хост, где расположен MQ-сервер
    int port = 1414; // порт для работы с MQ-сервером
    String mqQManager = "kracoz"; // менеджер очередей MQ
    String mqQChannel = "SYSTEM.DEF.SVRCONN"; // Канал для подключения к MQ-серверу
    String in; // Очередь входящих сообщений
    String out; // Очередь исходящих сообщений

    MQQueueConnection mqConn;
    MQQueueConnectionFactory mqCF;
    MQQueueSession mqQSession;
    MQQueue mqIn;
    MQQueue mqOut;
    MQQueueSender mqSender;
    MQQueueReceiver mqReceiver;
    MessageProducer replyProd;

    public JmsClass(String in, String out) {
        this.in = in;
        this.out = out;
        try {
            mqCF = new MQQueueConnectionFactory();
            mqCF.setHostName(host);
            mqCF.setPort(port);
            mqCF.setQueueManager(mqQManager);
            mqCF.setChannel(mqQChannel);
            mqCF.setTransportType(WMQConstants.TIME_TO_LIVE_UNLIMITED);

//            Устанавливаем соединение с сервером MQ:
            mqConn = (MQQueueConnection) mqCF.createQueueConnection();
            mqQSession = (MQQueueSession) mqConn.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
            mqIn = (MQQueue) mqQSession.createQueue(in); // входная
            mqOut = (MQQueue) mqQSession.createQueue(out); //выходная
            mqSender = (MQQueueSender) mqQSession.createSender(mqOut);
            mqReceiver = (MQQueueReceiver) mqQSession.createReceiver(mqIn);
            this.replyProd = this.mqQSession.createProducer(null);
            this.replyProd.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        } catch (JMSException ex) {
            Logger.getLogger(JmsClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void sendAnswer(Message msg) {
        try {
            TextMessage answer = mqQSession.createTextMessage("ответ из sendAnswer()");

            answer.setJMSCorrelationID(msg.getJMSMessageID());
            mqSender.send(answer);
            mqQSession.commit(); //Подтверждаем отправку сообщения
        } catch (JMSException ex) {
            Logger.getLogger(JmsClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() //Этот метод будет выполнен в побочном потоке
    {
        while (true) {
            try //Этот метод будет выполнен в побочном потоке
            {
                mqConn.start();
                MessageListener Listener = msg -> {
                    if (msg instanceof TextMessage) {
                        try {
                            TextMessage tMsg = (TextMessage) msg;
                            String msgText = tMsg.getText();
                            mqQSession.commit(); //Подтверждаем что мы прочитали это сообщение
                            sendAnswer(msg); //Отправляем ответное сообзение
                        } catch (JMSException ex) {
                            Logger.getLogger(JmsClass.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                };
                mqReceiver.setMessageListener(Listener);
                System.out.println("in:" + in);
                System.out.println("out:" + out);
                System.out.println("");
            } catch (JMSException ex) {
                Logger.getLogger(JmsClass.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}

public class TestJmsClass {

//    static String mqQIn = "MQ.Incoming"; // Очередь входящих сообщений
//    static String mqQOut = "MQ.Outgoing"; // Очередь исходящих сообщений
    static String mqQIn = "In"; // Очередь входящих сообщений
    static String mqQOut = "Out"; // Очередь исходящих сообщений

    public static void main(String[] args) {
        JmsClass oneThJms = new JmsClass(mqQIn, mqQOut);    //Создание потока
        JmsClass secondThJms = new JmsClass(mqQOut, mqQIn);

            oneThJms.start();//Запуск потока
            secondThJms.start();


        System.out.println("Главный поток завершён...");
    }
}