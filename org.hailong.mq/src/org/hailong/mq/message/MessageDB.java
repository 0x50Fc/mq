package org.hailong.mq.message;

import org.hailong.mq.Message;

public interface MessageDB {

	/**
	 * 验证授权
	 * @param token
	 * @return 用户ID
	 */
	public long auth(String token) throws Throwable ;
	
	/**
	 * 拉取消息
	 * @param 用户ID
	 * @param minObjectId
	 * @param limit
	 * @return
	 * @throws Throwable
	 */
	public Message[] pull(long uid, long minObjectId,long limit) throws Throwable ;
	
	/**
	 * 添加消息
	 * @param uid
	 * @param fuid
	 * @param mettingId
	 * @param type
	 * @param bytes
	 * @throws Throwable
	 * @return 
	 */
	public long[] add(long uid,long fuid,long mettingId,String type, byte[] bytes) throws Throwable ;
	
}
