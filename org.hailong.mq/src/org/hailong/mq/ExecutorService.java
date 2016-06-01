package org.hailong.mq;


public class ExecutorService implements Recipient {

	private final java.util.concurrent.ExecutorService _executor;
	
	public ExecutorService(java.util.concurrent.ExecutorService executor) {
		_executor = executor;
	}
	
	@Override
	public void onMessage(final Message message, final MQ mq, final MQ from) {
		
		_executor.execute(new Runnable(){

			@Override
			public void run() {
				onExecutorMessage(message,mq,from);
			}});
		
	}

	protected void onExecutorMessage(Message message, MQ mq, MQ from) {
		
	}
}
