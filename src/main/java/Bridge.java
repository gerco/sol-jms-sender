import java.io.FileInputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class Bridge {

  public static void main(String[] args) throws Exception {
    if(args.length != 6) {
      System.out.println("Usage: Bridge <source properties> <source cf> <source queue name> <target properties> <target cf> <target queue name>");
      System.exit(1);
    }
    
    Properties sourceProps = new Properties();
    sourceProps.load(new FileInputStream(args[0]));
    
    Properties targetProps = new Properties();
    targetProps.load(new FileInputStream(args[3]));
    
    System.out.println("Opening source connection...");
    Connection sourceConn = createConnection(sourceProps, args[1]);
    Session sourceSession = sourceConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    MessageConsumer source = sourceSession.createConsumer(sourceSession.createQueue(args[2]));
    sourceConn.start();
    
    System.out.println("Opening target connection...");
    Connection targetConn = createConnection(targetProps, args[4]);
    Session targetSession = targetConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
    MessageProducer target = targetSession.createProducer(targetSession.createQueue(args[5]));
    targetConn.start();

    long count = 0;
    long receiveNanos = 0;
    long processNanos = 0;
    long sendNanos = 0;
    long ackNanos = 0;

    long secStart = System.currentTimeMillis();
    while(true) {
      long t1 = System.nanoTime();
      Message sm = source.receive(1000);
      long t2 = System.nanoTime();
 
      if(sm != null) { 
        if(!(sm instanceof TextMessage)) {
          System.out.println("Message is not TextMessage: " + sm);
        } else {
          // Translate message (don't bother with headers)      
          TextMessage tm = targetSession.createTextMessage();
          tm.setText(((TextMessage)sm).getText());
          
          // Send target message
          long t3 = System.nanoTime();
          target.send(tm, DeliveryMode.PERSISTENT, 4, 0);
          long t4 = System.nanoTime();
          sm.acknowledge();
          long t5 = System.nanoTime();
          
          receiveNanos += t2-t1;
          processNanos += t3-t2;
          sendNanos += t4-t3;
          ackNanos += t5-t4;
          count++;
        }
      }
      
      // If a second has passed, print stats
      long t6 = System.currentTimeMillis();
      long el = t6-secStart;
      if(el > 1000) {
        if(count > 0) {
          String msg = String.format("%4dms elapsed, %5d messages processed. Receive: %4dms, Processing: %4dms, Send: %4dms, Acknowledge: %4dms", 
              el, count, receiveNanos/1000000, processNanos/1000000, sendNanos/1000000, ackNanos/1000000);
          System.out.println(msg);
        }
        
        count = 0;
        receiveNanos = 0;
        processNanos = 0;
        sendNanos = 0;
        ackNanos = 0;
        secStart = t6;
      }
    }
    
  }
  
  private static void blah() {
    String a = Context.INITIAL_CONTEXT_FACTORY;
    String b = Context.PROVIDER_URL;
    String c = Context.SECURITY_PRINCIPAL;
  }
  
  private static Connection createConnection(Properties props, String lookupName) throws NamingException, JMSException {
    InitialContext ctx = new InitialContext(props);
    ConnectionFactory cf = (ConnectionFactory)ctx.lookup(lookupName);
    return cf.createConnection();
  }
    
}
