package org.apache.jmeter.protocol.rocketmq.sampler;

import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class RocketMQSampler extends AbstractSampler implements TestStateListener {
//	private static final long serialVersionUID = 1L;
	private static final String ROCKETMQ_NAMESRV = "rocketmq.nameserver";
	private static final String ROCKETMQ_TOPIC = "rocketmq.topic";
	private static final String ROCKETMQ_TAG = "rocketmq.key";
	private static final String ROCKETMQ_MESSAGE = "rocketmq.message";
//	private static final String ROCKETMQ_MESSAGE_SERIALIZER = "kafka.message.serializer";
//	private static final String ROCKETMQ_KEY_SERIALIZER = "kafka.key.serializer";
	private static ConcurrentHashMap<String, DefaultMQProducer> producers = new ConcurrentHashMap<>();
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public RocketMQSampler() {
		setName("RocketMQ sampler");
	}
	
	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		try {
			result.sampleStart();
			DefaultMQProducer producer = getProducer();
//			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(getTopic(), getMessage());
			Message msg = new Message(getTopic() /* Topic */,
                    getTag() /* Tag */,
                    (getMessage()).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
			producer.send(msg);
			result.sampleEnd(); 
			result.setSuccessful(true);
			result.setResponseCodeOK();
		} catch (Exception e) {
			result.sampleEnd(); // stop stopwatch
			result.setSuccessful(false);
			result.setResponseMessage("Exception: " + e);
			// get stack trace as a String to return as document data
			java.io.StringWriter stringWriter = new java.io.StringWriter();
			e.printStackTrace(new java.io.PrintWriter(stringWriter));
			result.setResponseData(stringWriter.toString(), null);
			result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
			result.setResponseCode("FAILED");
		}
		return result;
	}

	private DefaultMQProducer getProducer() throws MQClientException {
		String threadGrpName = getThreadName();
		DefaultMQProducer producer = producers.get(threadGrpName);
		synchronized (this) {
			int threadCount = producers.size();
			StringBuilder groupName = new StringBuilder();
			groupName.append("jmeterGroup");
			if(producer == null) {
			log.info(MessageFormat.format("Cannot find the producer for {0}, going to create a new producer.", threadGrpName));
			producer = new DefaultMQProducer(groupName.append(threadCount).toString());
			producer.setNamesrvAddr(getNameSrv());
			producer.start();
			producers.put(threadGrpName, producer);
			}
		}

		return producer;
	}
	
	public String getNameSrv() {
		return getPropertyAsString(ROCKETMQ_NAMESRV);
	}

	public void setNameSrv(String namesrv) {
		setProperty(ROCKETMQ_NAMESRV, namesrv);
	}

	public String getTopic() {
		return getPropertyAsString(ROCKETMQ_TOPIC);
	}

	public void setTopic(String topic) {
		setProperty(ROCKETMQ_TOPIC, topic);
	}

	public String getTag() {
		return getPropertyAsString(ROCKETMQ_TAG);
	}

	public void setTag(String key) {
		setProperty(ROCKETMQ_TAG, key);
	}

	public String getMessage() {
		return getPropertyAsString(ROCKETMQ_MESSAGE);
	}

	public void setMessage(String message) {
		setProperty(ROCKETMQ_MESSAGE, message);
	}


	@Override
	public void testEnded() {
		this.testEnded("local");
	}

	@Override
	public void testEnded(String arg0) {
//		Iterator<Producer<String, String>> it = producers.values().iterator();
//		while(it.hasNext()) {
//			Producer<String, String> producer = it.next();
//			producer.close();
//		}
	}

	@Override
	public void testStarted() {
	}

	@Override
	public void testStarted(String arg0) {
	}

	
}
