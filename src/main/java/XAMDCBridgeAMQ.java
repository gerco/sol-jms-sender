import java.io.FileInputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;

import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.atomikos.jms.extra.MessageDrivenContainer;
import com.atomikos.jms.extra.SingleThreadedJmsSenderTemplate;

public class XAMDCBridgeAMQ {

  public static void main(String[] args) throws Exception {
    if(args.length != 6) {
      System.out.println("Usage: Bridge <source properties> <source cf> <source queue name> <target properties> <target cf> <target queue name>");
      System.exit(1);
    }

    Properties sourceProps = new Properties();
    sourceProps.load(new FileInputStream(args[0]));
    
    Properties targetProps = new Properties();
    targetProps.load(new FileInputStream(args[3]));

    System.out.println("Opening target connection...");
    AtomikosConnectionFactoryBean targetAcf = new AtomikosConnectionFactoryBean();
    targetAcf.setUniqueResourceName("target");
    targetAcf.setXaConnectionFactory(createConnectionFactory(targetProps, args[4]));
    targetAcf.setPoolSize(1);

    final SingleThreadedJmsSenderTemplate st = new SingleThreadedJmsSenderTemplate();
    st.setAtomikosConnectionFactoryBean(targetAcf);
    st.setDestinationName(args[5]);
    
    
    System.out.println("Opening source connection...");
    AtomikosConnectionFactoryBean sourceAcf = new AtomikosConnectionFactoryBean();
    sourceAcf.setUniqueResourceName("source");
    sourceAcf.setXaConnectionFactory(createConnectionFactory(sourceProps, args[1]));
    
    MessageDrivenContainer mdc = new MessageDrivenContainer();
    mdc.setAtomikosConnectionFactoryBean(sourceAcf);
    mdc.setDestinationName(args[2]);
    mdc.setMessageListener(new MessageListener() {
      long secStart = System.currentTimeMillis();
      long count = 0;
      
      @Override
      public void onMessage(Message sm) {
        if(!(sm instanceof TextMessage)) {
          System.out.println("Message is not TextMessage: " + sm);
        } else {
          try {
            st.sendTextMessage(((TextMessage)sm).getText());
            count++;
          } catch (JMSException e) {
            e.printStackTrace();
            System.exit(1);
          }
        }
        
        // If a second has passed, print stats
        long t6 = System.currentTimeMillis();
        long el = t6-secStart;
        if(el > 1000) {
          if(count > 0) {
            String msg = String.format("%4dms elapsed, %5d messages processed.", el, count);
            System.out.println(msg);
          }
          
          count = 0;
          secStart = t6;
        }
      }
    });
    mdc.start();
    
    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("tcp://localhost:61616");
    Connection c = cf.createConnection();
    Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer p = s.createProducer(s.createQueue(args[2]));
    while(true) {
      p.send(s.createTextMessage("some content"));
    }
  }

  private static XAConnectionFactory createConnectionFactory(Properties props, String lookupName) throws NamingException, JMSException {  
    ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory("tcp://localhost:61616");
    return cf;
  }
    
}
