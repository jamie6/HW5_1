/*
    1.	Producer/Consumer with Bounded Buffer 
        You will run each implementation twice with the following:
            a)	5 producers and 2 consumers 
            b)	2 producers and 5 consumers 
        NOTE:   Each producer must produce 100 items
                Buffer max size is 10 items
                Consumer will sleep for 1 second when consuming an item 

	IMPLEMENT THE MODEL ABOVE USING THE FOLLOWING CONSTRUCTS
	NOTE: Timestamp execution time of each implementation
            i)	Implement the model above using Locks.  
            ii)	Implement the model above using Isolated Sections
            iii)	Implement the model above using Atomics
            iv)	Implement the model above using Actors
            v)	Compare speed of all implementations and describe in one paragraph why you got your results.
*/
package com.mycompany.cs3700_hw5_1;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;
import akka.actor.ActorSystem;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;

/**
 *
 * @author Jamie
 */
public class HW5_1
{
    public static void main(String[] args)
    {
        int producerCount = 2;
        int consumerCount = 5;
        int maxItemsToProduce = 100;
        //locksTest(producerCount, consumerCount, maxItemsToProduce);
        //isolatedSectionTest(producerCount, consumerCount, maxItemsToProduce);
        //atomicsTest(producerCount, consumerCount, maxItemsToProduce);
        actorsTest(producerCount, consumerCount, maxItemsToProduce);
    }
    
    // Isolated Section 
    public static void isolatedSectionTest(int producerCount, int consumerCount, int maxItemsToProduce)
    {
        long startTime, endTime;
        Thread[] producers = new Thread[producerCount];
        Thread[] consumers = new Thread[consumerCount];
        final ProducerConsumerIsolatedSection producerConsumer = new ProducerConsumerIsolatedSection(maxItemsToProduce);

        // producer
        for ( int i = 0; i < producers.length; i++ )
        {
            producers[i] = new Thread(()->
            {
                try {producerConsumer.produce();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            producers[i].setName("Producer " + i);
        }

        // consumer
        for ( int i = 0; i < consumers.length; i++ )
        {
            consumers[i] = new Thread(()->
            {
                try {producerConsumer.consume();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            consumers[i].setName("Consumer " + i);
        }
        
        startTime = System.currentTimeMillis();
        for ( int i = 0; i < producers.length; i++ ) producers[i].start();
        for ( int i = 0; i < consumers.length; i++ ) consumers[i].start();
        try
        {
            for ( int i = 0; i < producers.length; i++ ) producers[i].join();
            for ( int i = 0; i < consumers.length; i++ ) consumers[i].join();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        System.out.println("Producers: " + producers.length);
        System.out.println("Consumers: " + consumers.length);
        System.out.println("Time: " + (endTime-startTime) + " ms.");
    }
    
    // Isolated Sections
    public static class ProducerConsumerIsolatedSection
    {
        LinkedList<Integer> list = new LinkedList<>(); // store items to consume
        int maxItemsToProduce;
        int maxBufferSize = 10;
        int item = 0;
        
        public ProducerConsumerIsolatedSection(int maxItemsToProduce)
        {
            this.maxItemsToProduce = maxItemsToProduce;
        }

        public void produce() throws InterruptedException 
        {
            while (item < maxItemsToProduce) 
            {
                synchronized (this) 
                {
                    while (list.size()==maxBufferSize) wait();
                    if ( item < maxItemsToProduce )
                    {
                        System.out.println(Thread.currentThread().getName() + " produced item: " + item);
                        list.add(item++); 
                        notify();
                    }
                    else break;
                }
            }
        }
        
        public void consume() throws InterruptedException 
        {
            while (item < maxItemsToProduce || !list.isEmpty()) 
            {
                boolean isConsumed = false;
                synchronized (this) 
                {
                    while (list.isEmpty() && item < maxItemsToProduce) wait(); 
                    if ( !list.isEmpty())
                    {
                        System.out.println(Thread.currentThread().getName() + " consumed item: " + list.removeFirst());
                        isConsumed = true;
                        notify(); 
                    }
                }
                if ( isConsumed ) 
                {
                    Thread.sleep(1000); // sleep for 1 second when consuming item
                }
            }
        }
    }
    
    // Locks
    public static void locksTest(int producerCount, int consumerCount, int maxItemsToProduce)
    {
        long startTime, endTime;
        Thread[] producers = new Thread[producerCount];
        Thread[] consumers = new Thread[consumerCount];
        final ProducerConsumerLocks producerConsumer = new ProducerConsumerLocks(maxItemsToProduce);

        // producer
        for ( int i = 0; i < producers.length; i++ )
        {
            producers[i] = new Thread(()->
            {
                try {producerConsumer.produce();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            producers[i].setName("Producer " + i);
        }

        // consumer
        for ( int i = 0; i < consumers.length; i++ )
        {
            consumers[i] = new Thread(()->
            {
                try {producerConsumer.consume();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            consumers[i].setName("Consumer " + i);
        }
        
        startTime = System.currentTimeMillis();
        for ( int i = 0; i < producers.length; i++ ) producers[i].start();
        for ( int i = 0; i < consumers.length; i++ ) consumers[i].start();
        try
        {
            for ( int i = 0; i < producers.length; i++ ) producers[i].join();
            for ( int i = 0; i < consumers.length; i++ ) consumers[i].join();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        System.out.println("Producers: " + producers.length);
        System.out.println("Consumers: " + consumers.length);
        System.out.println("Time: " + (endTime-startTime) + " ms.");
    }
    
    // Locks
    public static class ProducerConsumerLocks
    {
        ReentrantLock lock = new ReentrantLock();
        LinkedList<Integer> list = new LinkedList<>(); // store items to consume
        int maxItemsToProduce;
        int maxBufferSize = 10;
        int item = 0; 
        
        public ProducerConsumerLocks(int maxItemsToProduce)
        {
            this.maxItemsToProduce = maxItemsToProduce;
        }

        public void produce() throws InterruptedException 
        {
            while (item < maxItemsToProduce) 
            {
                lock.lock();
                if ( list.size() < maxBufferSize )
                {
                    System.out.println(Thread.currentThread().getName() + " produced item: " + item);
                    list.add(item++);
                }
                lock.unlock();
            }
        }
        
        public void consume() throws InterruptedException 
        {
            while (item < maxItemsToProduce || !list.isEmpty()) 
            {
                if ( !list.isEmpty() )
                {
                    lock.lock();
                    System.out.println(Thread.currentThread().getName() + " consumed item: " + list.removeFirst());
                    lock.unlock();
                    Thread.sleep(1000); // sleep for 1 second when consuming item
                }
            }
        }
    }
    
    // Atomics
    public static void atomicsTest(int producerCount, int consumerCount, int maxItemsToProduce)
    {
        long startTime, endTime;
        Thread[] producers = new Thread[producerCount];
        Thread[] consumers = new Thread[consumerCount];
        final ProducerConsumerAtomics producerConsumer = new ProducerConsumerAtomics(maxItemsToProduce);

        // producer
        for ( int i = 0; i < producers.length; i++ )
        {
            producers[i] = new Thread(()->
            {
                try {producerConsumer.produce();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            producers[i].setName("Producer " + i);
        }

        // consumer
        for ( int i = 0; i < consumers.length; i++ )
        {
            consumers[i] = new Thread(()->
            {
                try {producerConsumer.consume();}
                catch(InterruptedException e) {e.printStackTrace();}
            });
            consumers[i].setName("Consumer " + i);
        }
        
        startTime = System.currentTimeMillis();
        for ( int i = 0; i < producers.length; i++ ) producers[i].start();
        for ( int i = 0; i < consumers.length; i++ ) consumers[i].start();
        try
        {
            for ( int i = 0; i < producers.length; i++ ) producers[i].join();
            for ( int i = 0; i < consumers.length; i++ ) consumers[i].join();
        }
        catch(InterruptedException e)
        {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        System.out.println("Producers: " + producers.length);
        System.out.println("Consumers: " + consumers.length);
        System.out.println("Time: " + (endTime-startTime) + " ms.");
    }
    
    // Atomics
    public static class ProducerConsumerAtomics
    {
        ConcurrentLinkedQueue list = new ConcurrentLinkedQueue(); // store items to consume
        int maxItemsToProduce;
        int maxBufferSize = 10;
        AtomicInteger item = new AtomicInteger();
        
        public ProducerConsumerAtomics(int maxItemsToProduce)
        {
            this.maxItemsToProduce = maxItemsToProduce;
        }

        public void produce() throws InterruptedException 
        {
            while (item.get() < maxItemsToProduce) 
            {
                synchronized (this) 
                {
                    while (list.size()==maxBufferSize) wait();
                    if ( item.get() < maxItemsToProduce )
                    {
                        System.out.println(Thread.currentThread().getName() + " produced item: " + item);
                        list.add(item.getAndIncrement()); 
                        notify();
                    }
                    else break;
                }
            }
        }
        
        public void consume() throws InterruptedException 
        {
            while (item.get() < maxItemsToProduce || !list.isEmpty()) 
            {
                boolean isConsumed = false;
                synchronized (this) 
                {
                    while (list.isEmpty() && item.get() < maxItemsToProduce) wait(); 
                    if ( !list.isEmpty())
                    {
                        System.out.println(Thread.currentThread().getName() + " consumed item: " + list.remove());
                        isConsumed = true;
                        notify(); 
                    }
                }
                if ( isConsumed ) 
                {
                    Thread.sleep(1000); // sleep for 1 second when consuming item
                }
            }
        }
    }    
    
    // Actors
    public static void actorsTest(int producerCount, int consumerCount, int maxItemsToProduce)
    {
        ActorSystem system = ActorSystem.create("actors");
        Object[] args = {producerCount, consumerCount, maxItemsToProduce};
        final ActorRef producerConsumer = system.actorOf(ProducerConsumerActors.props(args), "producerConsumer");
        long startTime, endTime=0;
        
        startTime = System.currentTimeMillis();
        while ( endTime-startTime < 1000*maxItemsToProduce)
        {
            for ( int i = 0; i < producerCount; i++ ) new Thread(()-> producerConsumer.tell(new ProducerConsumerActors.Message("producer"), ActorRef.noSender())).start();
            for ( int i = 0; i < consumerCount; i++ ) new Thread(()-> producerConsumer.tell(new ProducerConsumerActors.Message("consumer"), ActorRef.noSender())).start();
            endTime = System.currentTimeMillis();
        }
        try
        {
            Thread.sleep(1000);
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        system.terminate();
    }
    
    // Actors
    public static class ProducerConsumerActors extends AbstractLoggingActor
    {
        private LinkedList<Integer> list = new LinkedList<>(); // store items to consume
        final private int maxItemsToProduce, maxProducerCount, maxConsumerCount, maxBufferSize = 10;
        private int item = 0, producerCount = 0, consumerCount = 0;
        private long startTime, endTime;
        boolean isFinished = false;
        
        public ProducerConsumerActors(int producerCount, int consumerCount, int maxItemsToProduce)
        {
            this.maxProducerCount = producerCount;
            this.maxConsumerCount = consumerCount;
            this.maxItemsToProduce = maxItemsToProduce;
            startTime = System.currentTimeMillis();
        }
        
        @Override
        public Receive createReceive()
        {
            return ReceiveBuilder.create().match(Message.class, this::onMessage).build();
        }
        
        static class Message
        {
            String id; // producer, consumer
            public Message(String id)
            {
                this.id = id;
            }
        }
        
        private void onMessage(Message message) throws InterruptedException
        {
            if ( message.id.equals("producer") )
            {
                if ( item < maxItemsToProduce && producerCount < maxProducerCount && list.size() < maxBufferSize )
                {
                    producerCount++;
                    log().info("produced item: " + item);
                    list.add(item);
                    producerCount--;
                    item++;
                }
            }
            else 
            {
                if ( !list.isEmpty() && consumerCount < maxConsumerCount)
                {
                    consumerCount++;
                    log().info("consumed item: " + list.removeFirst());
                    Thread.sleep(1000);
                    consumerCount--;
                }
            }
            if ( item == maxItemsToProduce && list.isEmpty() && !isFinished)
            {
                endTime = System.currentTimeMillis();
                isFinished = true;
                log().info("\nProducers: " + maxProducerCount + "\nConsumers: " + maxConsumerCount + "\nTime: " + (endTime-startTime) + " ms.");
            }
        }
        
        public static Props props(Object[] args)
        {
            return Props.create(ProducerConsumerActors.class, args);
        }
    }
}
