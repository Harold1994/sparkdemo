import org.apache.log4j.Logger;

public class Log4jTest {
    public static Logger logger = Logger.getLogger(Log4jTest.class);
    public static void main(String[] args) throws InterruptedException {
        int index = 0;
        while (true) {
            Thread.sleep(1000);
            logger.info("value: " + index++);
        }
    }
}
