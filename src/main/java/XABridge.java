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
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import com.atomikos.datasource.xa.jms.JmsTransactionalResource;
import com.atomikos.icatch.config.UserTransactionService;
import com.atomikos.icatch.config.UserTransactionServiceImp;
import com.atomikos.icatch.jta.UserTransactionManager;

public class XABridge {

  public static void main(String[] args) {
    if(args.length != 6) {
      System.out.println("Usage: Bridge <source properties> <source cf> <source queue name> <target properties> <target cf> <target queue name>");
      System.exit(1);
    }

    try {
      doMain(args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    
  }
  
  private static void doMain(String[] args) throws Exception {
    Properties sourceProps = new Properties();
    sourceProps.load(new FileInputStream(args[0]));
    
    Properties targetProps = new Properties();
    targetProps.load(new FileInputStream(args[3]));
    
    UserTransactionManager txm = new UserTransactionManager();
    txm.init();
    
    System.out.println("Opening source connection...");
    XAConnection sourceConn = createConnection(sourceProps, args[1]);
    XASession sourceSession = sourceConn.createXASession();
    MessageConsumer source = sourceSession.createConsumer(sourceSession.createQueue(args[2]));
    sourceConn.start();
    
    System.out.println("Opening target connection...");
    XAConnection targetConn = createConnection(targetProps, args[4]);
    XASession targetSession = targetConn.createXASession();
    MessageProducer target = targetSession.createProducer(targetSession.createQueue(args[5]));
    targetConn.start();

    long count = 0;
    long begintxNanos = 0;
    long receiveNanos = 0;
    long processNanos = 0;
    long sendNanos = 0;
    long commitNanos = 0;

    txm.setTransactionTimeout(1);
    
    long secStart = System.currentTimeMillis();
    while(true) {
      long t1 = System.nanoTime();
      txm.begin();
      Transaction tx = txm.getTransaction();
      tx.enlistResource(sourceSession.getXAResource());
      
      long t2 = System.nanoTime();      
      Message sm = source.receive(1000);
      long t3 = System.nanoTime();
 
      if(sm != null) { 
        if(!(sm instanceof TextMessage)) {
          System.out.println("Message is not TextMessage: " + sm);
        } else {
          // Translate message (don't bother with headers)      
          TextMessage tm = targetSession.createTextMessage();
          tm.setText(((TextMessage)sm).getText());
          
          // Send target message
          long t4 = System.nanoTime();
          tx.enlistResource(targetSession.getXAResource());
          target.send(tm, DeliveryMode.PERSISTENT, 4, 0);
          tx.delistResource(targetSession.getXAResource(), XAResource.TMSUCCESS);
          long t5 = System.nanoTime();
          
          tx.delistResource(sourceSession.getXAResource(), XAResource.TMSUCCESS);
          tx.commit();
          long t6 = System.nanoTime();

          begintxNanos += t2-t1;
          receiveNanos += t3-t2;
          processNanos += t4-t3;
          sendNanos += t5-t4;
          commitNanos += t6-t5;
          count++;
        }
      }
      
      // If a second has passed, print stats
      long t6 = System.currentTimeMillis();
      long el = t6-secStart;
      if(el > 1000) {
        if(count > 0) {
          String msg = String.format("%4dms elapsed, %5d messages processed. Begin transaction: %4dms, Receive: %4dms, Processing: %4dms, Send: %4dms, Commit: %4dms", 
              el, count, begintxNanos/1000000, receiveNanos/1000000, processNanos/1000000, sendNanos/1000000, commitNanos/1000000);
          System.out.println(msg);
        }
        
        count = 0;
        begintxNanos = 0;
        receiveNanos = 0;
        processNanos = 0;
        sendNanos = 0;
        commitNanos = 0;
        secStart = t6;
      }
    }

  }

  private static XAConnectionFactory createConnectionFactory(Properties props, String lookupName) throws NamingException, JMSException {  
    InitialContext ctx = new InitialContext(props);
    return (XAConnectionFactory)ctx.lookup(lookupName);
  }
  
  private static XAConnection createConnection(Properties props, String lookupName) throws NamingException, JMSException {
    return createConnectionFactory(props, lookupName).createXAConnection();
  }
    
}
