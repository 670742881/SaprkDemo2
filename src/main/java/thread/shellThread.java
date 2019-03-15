package thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @created by imp ON 2019/2/19
 */
public class shellThread extends Thread {
    @Override
    public void run() {
        ThreadPoolExecutor pool= new ThreadPoolExecutor(5,10, 3, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        pool.execute(new Runnable() {
            @Override
            public void run() {
                BufferedReader br=null;
                InputStream processIn=null;
                Process process=null;
                try {
                   process =Runtime.getRuntime().exec("dir C:\\WINDOWS\\system32\\");
                    int exitValue = process.waitFor(); //进程没有结束的话，会阻塞等待状态

                    if (exitValue == 0) {
                        System.out.println("Success!!" + exitValue);
                    } else {
                        System.out.println("Failure!!" + exitValue);
                       processIn = process.getInputStream();
                        br= new BufferedReader(new InputStreamReader(processIn));
                        String line = null;
                        System.out.println("====Error Msg====");
                        while ((line = br.readLine()) != null) {
                            System.out.println(line);
                        }
                        System.out.println("====Error Msg====");
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }finally{
                    try {
                        if(br!=null){
                            br.close();
                            br=null;
                        }
                        if(processIn!=null){
                            processIn.close();
                            processIn=null;
                        }
                        if(process!=null){
                            br=null;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

System.setProperty("user","");//设置classpath

        }
        });
    }
}
