package com.ywf;

import com.ywf.interfaces.YWFModel;

/**
 * ClassName App
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2019-04-16 16:33
 * Version 1.0
 **/
public class App {
    public static void main(String[] args) throws Exception {
        long start_time = System.currentTimeMillis();
        YWFModel mainClass = (YWFModel) Class.forName(args[0]).newInstance();
        String[] codeArgs = new String[args.length - 1];
        for (int i = 0; i < codeArgs.length; i++) {
            codeArgs[i] = args[i + 1];
        }
        mainClass.execute(codeArgs);
        long end_time = System.currentTimeMillis();
        System.out.println("job running cost time is " + (end_time - start_time) / 1000 + " s!");
    }
}
