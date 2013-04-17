package jp.techie.jeromq.sample.one2many;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

/**
 * JeroMQ Sample Brocker
 * @author bose999
 *
 */
public class BrockerMaker {

    /**
     * Brokerの生成とポーリング実行
     * @param args none
     */
    public static void main(String[] args) {
        Context context = ZMQ.context(1);
        Socket router = context.socket(ZMQ.ROUTER);
        String routerAddress = "tcp://127.0.0.1:9001";
        System.out.println("broker - routerAddress:" + routerAddress);
        router.bind(routerAddress);

        Socket dealer = context.socket(ZMQ.DEALER);
        String dealerAddressHead = "tcp://127.0.0.1:";
        int dealerPort = 10001;

        for (int i = 0; i < 30; i++) {
            String backEndAddress = dealerAddressHead + dealerPort;
            dealer.bind(backEndAddress);
            System.out.println("broker - backEndAddress:" + backEndAddress);
            dealerPort++;
        }

        Poller items = new Poller(2);
        items.register(router, Poller.POLLIN);
        items.register(dealer, Poller.POLLIN);

        boolean more = false;
        byte[] message;

        while (!Thread.currentThread().isInterrupted()) {
            items.poll();
            if (items.pollin(0)) {
                while (true) {
                    message = router.recv(0);
                    more = router.hasReceiveMore();
                    dealer.send(message, more ? ZMQ.SNDMORE : 0);
                    if (!more) {
                        break;
                    }
                }
            }
            if (items.pollin(1)) {
                while (true) {
                    message = dealer.recv(0);
                    more = dealer.hasReceiveMore();
                    router.send(message, more ? ZMQ.SNDMORE : 0);
                    if (!more) {
                        break;
                    }
                }
            }
        }
        router.close();
        System.out.println("broker - close router.");
        dealer.close();
        System.out.println("broker - close dealer.");
    }
}
