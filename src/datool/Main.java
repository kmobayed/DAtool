package datool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


public class Main {

   public static final int GIT_LOG=1;
    public static final int Mercurial_LOG=2;
    public static final int Bazaar_LOG=3;
    public static final String dateFormatLog="yyyy-MM-dd HH:mm:ss Z";
    public static final String dateFormatJena="yyyy-MM-dd'T'HH:mm:ss'Z'";

    public static void main( String[] args ) throws FileNotFoundException, IOException, java.text.ParseException
    {
        if (args.length<3)
        {
                System.err.println("Usage: java -jar scho.jar <TDB_folder> <step_in_seconds> <output_file> [start_date]");
                System.exit(0);
        }

        String DBdirectory = args[0] ;
        Jena J= new Jena(DBdirectory);

        System.out.println("Number of triple(s) = "+J.getTripleCount());
        System.out.println("Number of site(s) = "+J.getSiteCount());
        System.out.println("Number of commit(s) = "+J.getCommitCount());
        System.out.println("Number of merge(s) = "+J.getMergeCount());
        System.out.println("Number of author(s) = "+J.getAuthorCount());

        System.out.print("Calculating divergence awareness ... ");
        long startTime = System.currentTimeMillis();
        Main.calculateDA(J,args);
        long endTime = System.currentTimeMillis();
        System.out.println("DONE");
        System.out.println("Divergence awareness calcualtion time (seconds):"+ (endTime-startTime)/1000);
        J.close();

    }

    public static void calculateDA(Jena J, String[] args) throws  FileNotFoundException, IOException, java.text.ParseException
    {
        FileHandler hand = new FileHandler(args[2]);
        hand.setFormatter(new LoggingSimpleFormatter());
        Logger log = Logger.getLogger("scho_log");
        log.addHandler(hand);
        LogRecord rec2 =null;
        String date;
        if (args.length==4)
        {
            date=args[3];
        }else
        {
            ChangeSet FCS=J.getFirstCS();
            date = FCS.getDate();
        }
        Date D;
        SimpleDateFormat sdf1 = new SimpleDateFormat(dateFormatJena);
        sdf1.setTimeZone(TimeZone.getTimeZone("GMT"));
        D = sdf1.parse(date);
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("GMT"));
        cal.setTime(D);
        int step=Integer.valueOf(args[1]); //in seconds

        ArrayList <ChangeSet> AL2=new ArrayList <ChangeSet>();
        boolean more=true;

        while (more)
        {
            AL2=J.getCStillDate(cal.getTime());
            int RM=0;
            int LM=0;
            for (ChangeSet o : AL2)
            {
                if (!o.isPublished())
                {
                    if (J.inPushFeed(o,cal.getTime()))
                    {
                        o.publish();
                        J.publishChangeSet(o);
                    }
                    else
                    {
                        if (J.inPullFeed(o,cal.getTime()))
                        {
                            if (J.isPullHead(o,cal.getTime())) //pull head
                            {
                                //publish parents
                                RM++;
                                Date D2=sdf1.parse(J.getNextCS(o.getChgSetID()).get(0).getDate());
                                if (D2.before(cal.getTime()))
                                {
                                    J.publishParents(o,cal.getTime());
                                    J.publishChangeSet(o);
                                }
                            }
                            else
                            {
                                RM++;
                            }
                        }
                        else
                        {
                            LM++;
                        }
                    }

                    if (J.getNextCS(o.getChgSetID()).isEmpty())
                    {
                        more = false;
                    }
                }
            }

            rec2 = new LogRecord(Level.INFO,cal.getTime().getTime()+"\t"+LM+"\t"+RM);
            hand.publish(rec2);
            cal.add(Calendar.SECOND, step);
        }
        hand.close();
    }

}
