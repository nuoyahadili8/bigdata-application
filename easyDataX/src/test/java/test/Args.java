package test;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @Project:
 * @Description:
 * @Version 1.0.0
 * @Throws SystemException:
 * @Author: <li>2019/8/30/030 Administrator Create 1.0
 * @Copyright ©2018-2019 al.github
 * @Modified By:
 */
public class Args {

    @Option(required = true, name = "-arg1", usage = "arg1, desc")
    private String arg1;

    @Option(required = true, name = "-arg2", usage = "arg2, desc")
    private String arg2;

    @Option(required = true, name = "-arg3", usage = "arg3, desc")
    private String arg3;

    public static Args parseArgs(String[] args) {
        Args myArgs = new Args();
        CmdLineParser parser = new CmdLineParser(myArgs);

        if (args.length > 0 && args[0].trim().equals("-h")) {
            System.out.println("Usage descrition. options: ");
            parser.printUsage(System.out);
            return null;
        }

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            myArgs = null;
        }

        return myArgs;
    }

    public String getArg1() {
        return arg1;
    }

    public void setArg1(String arg1) {
        this.arg1 = arg1;
    }

    public String getArg2() {
        return arg2;
    }

    public void setArg2(String arg2) {
        this.arg2 = arg2;
    }

    public String getArg3() {
        return arg3;
    }

    public void setArg3(String arg3) {
        this.arg3 = arg3;
    }
}
