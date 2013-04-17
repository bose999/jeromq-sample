package jp.techie.jeromq.sample.one2many;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.msgpack.MessagePack;
import org.msgpack.type.NilValue;
import org.msgpack.type.ValueFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * JeroMQ Sample Worker
 * @author bose999
 * 
 */
public class WorkerMaker {
	
	/**
	 * Brokerの生成とポーリング実行
	 * @param arg
	 */
	public static void main(String[] args) {
		String replyAddressHead = "tcp://127.0.0.1:";
		int replyPort = 10001;
		for (int i = 0; i < 30; i++) {
			// 30スレッド作ってポートを一意にしてWorkerとして処理を行わせる
			WorkerExecuter workerExecuter = new WorkerExecuter();
			String replyAddress = replyAddressHead + replyPort;
			workerExecuter.setReplyAddress(replyAddress);
			new Thread(workerExecuter).start();
			replyPort++;
		}
	}
	
	/**
	 * Worker複数スレッド実行
	 * ファイルの擬似ストリーム転送試行バージョン
	 * @author bose999
	 * 
	 */
	private static class WorkerExecuter implements Runnable {
		
		/**
		 * Worker TCP Address
		 */
		private String replyAddress = null;
		
		/**
		 * setter
		 * @param replyAddress
		 */
		public void setReplyAddress(String replyAddress) {
			this.replyAddress = replyAddress;
		}
		
		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Runnable#run()
		 */
		public void run() {
			System.out.println("worker - replyAddress:" + this.replyAddress);
			Context context = ZMQ.context(1);
			Socket responder = context.socket(ZMQ.REP);
			responder.connect(this.replyAddress);
			System.out.println("worker - replyAddress:" + this.replyAddress);
			
			while (!Thread.currentThread().isInterrupted()) {
				// clientから受信したMessagePackシリアライズ
				byte[] recvBytes = responder.recv(0);
				MessagePack msgpack = new MessagePack();
				String recvString = "";
				InputStream inputStream = null;
				try {
					// clientからbyte配列を受信してMessagePackでデシリアライズ
					recvString = msgpack.read(recvBytes, String.class);
					System.out.println("worker - Received request ( replyAddress: " + this.replyAddress + "): " + "["
							+ recvString + "]");
					
					FileSystem fileSystem = FileSystems.getDefault();
					Path path = fileSystem.getPath("/Users/matakeda/Desktop/001.jpg");
					inputStream = Files.newInputStream(path);
					
					int outputByesLength = 10240;
					byte[] sendBytes = new byte[outputByesLength];
					int result = 1;
					
					while (result > 0) {
						result = inputStream.read(sendBytes);
						if(result != -1){
							if(outputByesLength != result){
								// 切り出した桁数が少ない場合に切り出した桁数で入れ替える
								// MessagePackがnewした桁数で転送するので暫定対応
								byte[] sendBytesEdit = new byte[result];
								for(int i=0;i<result ;i++){
									sendBytesEdit[i] = sendBytes[i];
								}
								byte[] sendMessageBytes = msgpack.write(sendBytesEdit);
								responder.send(sendMessageBytes, ZMQ.SNDMORE);
							} else {
								byte[] sendMessageBytes = msgpack.write(sendBytes);
								responder.send(sendMessageBytes, ZMQ.SNDMORE);
							}
							System.out.println("sendmore");
						}
						System.out.println(result);
					}
					NilValue nilValue = ValueFactory.createNilValue();
					byte[] sendMessageBytes = msgpack.write(nilValue);
					responder.send(sendMessageBytes, 0);
					System.out.println("sendlast");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			responder.close();
			context.term();
			System.out.println("worker - socket close ( replyAddress: " + this.replyAddress + ")");
		}
	}
}
