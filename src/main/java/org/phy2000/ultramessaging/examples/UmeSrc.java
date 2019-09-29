package org.phy2000.ultramessaging.examples;

import com.latencybusters.lbm.*;
import java.util.*;
import java.text.NumberFormat;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import gnu.getopt.*;
import java.io.*;
/*
   Copyright (c) 2005-2019 Informatica Corporation  Permission is granted to licensees to use
   or alter this software for any purpose, including commercial applications,
   according to the terms laid out in the Software License Agreement.

   This source code example is provided by Informatica for educational
   and evaluation purposes only.

   THE SOFTWARE IS PROVIDED "AS IS" AND INFORMATICA DISCLAIMS ALL WARRANTIES
   EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION, ANY IMPLIED WARRANTIES OF
   NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR
   PURPOSE.  INFORMATICA DOES NOT WARRANT THAT USE OF THE SOFTWARE WILL BE
   UNINTERRUPTED OR ERROR-FREE.  INFORMATICA SHALL NOT, UNDER ANY CIRCUMSTANCES, BE
   LIABLE TO LICENSEE FOR LOST PROFITS, CONSEQUENTIAL, INCIDENTAL, SPECIAL OR
   INDIRECT DAMAGES ARISING OUT OF OR RELATED TO THIS AGREEMENT OR THE
   TRANSACTIONS CONTEMPLATED HEREUNDER, EVEN IF INFORMATICA HAS BEEN APPRISED OF
   THE LIKELIHOOD OF SUCH DAMAGES.
 */


public class UmeSrc {

    private static int msgs = 10000000;
    private static int stats_sec = 0;
    private static int verbose = 0;
    private static boolean sequential = true;
    private static boolean dereg = false;
    public static int flightsz = 0;
    public static int appsent = 0;
    public static int stablerecv = 0;
    public static int store_behaviour = LBM.SRC_TOPIC_ATTR_UME_STORE_BEHAVIOR_RR;
    private static int pause_ivl = 0;
    private static int msgs_per_ivl = 1;
    public static long last_clientd_sent = 0;
    public static long last_clientd_stable = 0;
    public static long sleep_before_sending = 0;
    private static int msgs_per_sec = 0;
    private static String purpose = "Purpose: Send messages on a single topic.";
    private static String usage =
	"Usage: umesrc [options] topic\n"+
	"Available options:\n"+
	"  -c filename = read config parameters from filename\n"+
	"  -C filename = read context config parameters from filename\n"+
	"  -D = Send deregistration after sending 1000 messages\n"+
	"  -e = use LBM embedded mode\n"+
	"  -f NUM = allow NUM unstabilized messages in flight (determines message rate)"+
	"  --flight-size = See -f above"+
	"  -h = help\n"+
	"  -j = turn on UME late join\n"+
	"  -l len = send messages of len bytes\n"+
	"  -L linger = linger for linger seconds before closing context\n"+
	"  -m NUM = send at NUM messages per second (trumped by -f)"+
	"  --message-rate = See -m above"+
	"  -M msgs = send msgs number of messages\n"+
	"  -N = display sequence number information source events\n"+
	"  -n = use non-blocking I/O\n"+
	"  -P msec = pause after each send msec milliseconds\n"+
	"  -R [UM]DATA/RETR = Set transport type to LBT-R[UM], set data rate limit to\n"+
	"                     DATA bits per second, and set retransmit rate limit to\n"+
	"                     RETR bits per second.  For both limits, the optional\n"+
	"                     k, m, and g suffixes may be used.  For example,\n"+
	"                     '-R 1m/500k' is the same as '-R 1000000/500000'\n"+
	"  -S ip:port = use UME store at the specified address and port\n"+
	"  -s sec = print stats every sec seconds\n"+
	"  -t storename = use UME store with name storename\n"+
	"  -v = bump verbose level\n"+
	"\nMonitoring options:\n"+
	"  --monitor-ctx NUM = monitor context every NUM seconds\n"+
	"  --monitor-src NUM = monitor source every NUM seconds\n"+
	"  --monitor-transport TRANS = use monitor transport module TRANS\n"+
	"                              TRANS may be `lbm', `udp', or `lbmsnmp', default is `lbm'\n"+
	"  --monitor-transport-opts OPTS = use OPTS as transport module options\n"+
	"  --monitor-format FMT = use monitor format module FMT\n"+
	"                         FMT may be `csv'\n"+
	"  --monitor-format-opts OPTS = use OPTS as format module options\n"+
	"  --monitor-appid ID = use ID as application ID string\n"
	;

    public static void main(String[] args)
    {
	char protocol = '\0';
	int send_rate = 0;
	int retrans_rate = 0;
	int linger = 5;
	int monitor_context_ivl = 0;
	int monitor_source_ivl = 0;
	boolean monitor_context = false;
	boolean monitor_source = false;
	boolean latejoin = false;
	String storeip = null;
	String storeport = null;
	String storename = null;
	int mon_transport = LBMMonitor.TRANSPORT_LBM;
	int mon_format = LBMMonitor.FORMAT_CSV;
	String mon_format_options = "";
	String mon_transport_options = "";
	String application_id = null;
	StringTokenizer tokens;
	boolean seqnum_info = false;
	boolean stability = false;
	LBMObjectRecycler objRec = new LBMObjectRecycler();

	LBM lbm = null;
	try
	{
	    lbm = new LBM();
	}
	catch (LBMException ex)
	{
	    System.err.println("Error initializing LBM: " + ex.toString());
	    System.exit(1);
	}
	org.apache.log4j.Logger logger;
	logger = org.apache.log4j.Logger.getLogger("umesrc");
	org.apache.log4j.BasicConfigurator.configure();
	log4jLogger lbmlogger = new log4jLogger(logger);
	lbm.setLogger(lbmlogger);
	String conffname = null;
	String cconffname = null;

	LongOpt[] longopts = new LongOpt[9];
	final int OPTION_MONITOR_CTX = 2;
	final int OPTION_MONITOR_SRC = 3;
	final int OPTION_MONITOR_TRANSPORT = 4;
	final int OPTION_MONITOR_TRANSPORT_OPTS = 5;
	final int OPTION_MONITOR_FORMAT = 6;
	final int OPTION_MONITOR_FORMAT_OPTS = 7;
	final int OPTION_MONITOR_APPID = 8;
	final int OPTION_FLIGHT_SIZE = 9;
	final int OPTION_MESSAGE_RATE = 10;

	longopts[0] = new LongOpt("monitor-ctx", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_CTX);
	longopts[1] = new LongOpt("monitor-src", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_SRC);
	longopts[2] = new LongOpt("monitor-transport", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_TRANSPORT);
	longopts[3] = new LongOpt("monitor-transport-opts", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_TRANSPORT_OPTS);
	longopts[4] = new LongOpt("monitor-format", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_FORMAT);
	longopts[5] = new LongOpt("monitor-format-opts", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_FORMAT_OPTS);
	longopts[6] = new LongOpt("monitor-appid", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MONITOR_APPID);
	longopts[7] = new LongOpt("flight-size", LongOpt.REQUIRED_ARGUMENT, null, OPTION_FLIGHT_SIZE);
	longopts[8] = new LongOpt("message-rate", LongOpt.REQUIRED_ARGUMENT, null, OPTION_MESSAGE_RATE);
	Getopt gopt = new Getopt("umesrc", args, "+C:Dec:f:hjL:l:m:M:nP:R:S:s:t:vN", longopts);
	int c = -1;
	int msglen = 25;
	long bytes_sent = 0;
	boolean error = false;
	boolean block = true;
	while ((c = gopt.getopt()) != -1)
	{
	    try
	    {
		switch (c)
		{
		    case OPTION_MONITOR_APPID:
			application_id = gopt.getOptarg();
			break;
		    case OPTION_MONITOR_CTX:
			monitor_context = true;
			monitor_context_ivl = Integer.parseInt(gopt.getOptarg());
			break;
		    case OPTION_MONITOR_SRC:
			monitor_source = true;
			monitor_source_ivl = Integer.parseInt(gopt.getOptarg());
			break;
		    case OPTION_MONITOR_TRANSPORT:
			if (gopt.getOptarg().compareToIgnoreCase("lbm") == 0)
			{
			    mon_transport = LBMMonitor.TRANSPORT_LBM;
			}
			else
			{
			    if (gopt.getOptarg().compareToIgnoreCase("udp") == 0)
			    {
				mon_transport = LBMMonitor.TRANSPORT_UDP;
			    }
			    else
			    {
				if (gopt.getOptarg().compareToIgnoreCase("lbmsnmp") == 0)
				{
				    mon_transport = LBMMonitor.TRANSPORT_LBMSNMP;
				}
				else
				{
				    error = true;
				}
			    }
			}
			break;
		    case OPTION_MONITOR_TRANSPORT_OPTS:
			mon_transport_options += gopt.getOptarg();
			break;
		    case OPTION_MONITOR_FORMAT:
			if (gopt.getOptarg().compareToIgnoreCase("csv") == 0)
			    mon_format = LBMMonitor.FORMAT_CSV;
			else
			    error = true;
			break;
		    case OPTION_MONITOR_FORMAT_OPTS:
			mon_format_options += gopt.getOptarg();
			break;
		    case 'c':
			conffname = gopt.getOptarg();
			break;
		    case 'C':
			cconffname = gopt.getOptarg();
			break;
		    case 'D':
			dereg = true;
			break;
		    case 'e':
			sequential = false;
			break;
		    case 'f':
		    case OPTION_FLIGHT_SIZE:
			flightsz = Integer.parseInt(gopt.getOptarg());
			break;
		    case 'h':
			print_help_exit(0);
			break;
		    case 'j':
			latejoin = true;
			break;
		    case 'l':
			msglen = Integer.parseInt(gopt.getOptarg());
			break;
		    case 'n':
			block = false;
			break;
		    case 'N':
			seqnum_info = true;
			break;
		    case 'L':
			linger = Integer.parseInt(gopt.getOptarg());
			break;
		    case 'm':
		    case OPTION_MESSAGE_RATE:
			msgs_per_sec = Integer.parseInt(gopt.getOptarg());
			break;

		    case 'M':
			msgs = Integer.parseInt(gopt.getOptarg());
			break;
		    case 'P':
			pause_ivl = Integer.parseInt(gopt.getOptarg());
			break;
		    case 'R':
			ParseRateVars parseRateVars = lbmExampleUtil.parseRate(gopt.getOptarg());
			if (parseRateVars.error) {
			    print_help_exit(1);
			}
			protocol = parseRateVars.protocol;
			send_rate = parseRateVars.rate;
			retrans_rate = parseRateVars.retrans;
			break;
		    case 'S':
			tokens = new StringTokenizer(gopt.getOptarg(), ":");
			if (tokens.countTokens() > 2)
			{
			    error = true;
			    break;
			}
			storeip = tokens.nextToken();
			try {
			    InetAddress iaddr = InetAddress.getByName(storeip);
			    storeip = iaddr.getHostAddress();
			}
			catch (UnknownHostException ex)
			{
			    System.err.println("Host " + storeip + " unknown.");
			    System.exit(1);
			}
			if (tokens.countTokens() == 1)
			{
			    storeport = tokens.nextToken();
			}
			break;
		    case 's':
			stats_sec = Integer.parseInt(gopt.getOptarg());
			break;
		    case 't':
			storename = gopt.getOptarg();
			break;
		    case 'v':
			verbose++;
			break;
		    default:
			error = true;
			break;
		}
		if (error)
		    break;
	    } catch (Exception e) {
		/* type conversion exception */
		System.err.println("umesrc: error\n" + e);
		print_help_exit(1);
	    }
	}
	if (error || gopt.getOptind() >= args.length)
	{
	    print_help_exit(1);
	}
	/* Verify the Message Rate and Flight Time args */
	if(msgs_per_sec > 0 && pause_ivl > 0) {
	    System.err.println("-m and -P are conflicting options");
	    System.err.println(usage);
	    System.exit(1);
	}

	byte [] message = new byte[msglen];
	if (conffname != null)
	{
	    try
	    {
		LBM.setConfiguration(conffname);
	    }
	    catch (LBMException ex)
	    {
		System.err.println("Error setting LBM configuration: " + ex.toString());
		System.exit(1);
	    }
	}
	LBMContextAttributes cattr = null;
	try
	{
	    cattr = new LBMContextAttributes();
	    cattr.setObjectRecycler(objRec, null);
	    // Since we are manually validating attributes, retrieve any XML
	    // configuration attributes set for this context.
	    cattr.setFromXml(cattr.getValue("context_name"));
	    // Set UMP liveness detection callbacks
	    cattr.setReceiverLivenessNotificationCallbacks(new UMERcvrLivenessCreationCb(), new UMERcvrLivenessDeletionCb(), null);
	}
	catch (LBMException ex)
	{
	    System.err.println("Error creating context attributes: " + ex.toString());
	    System.exit(1);
	}
	UMESrcCB srccb = new UMESrcCB(verbose);
	LBMSourceAttributes sattr = null;
	try
	{
	    sattr = new LBMSourceAttributes();
	    sattr.setObjectRecycler(objRec, null);
	    // Since we are manually validating attributes, retrieve any XML
	    // configuration attributes set for this topic.
	    sattr.setFromXml(cattr.getValue("context_name"), args[gopt.getOptind()]);
	}
	catch (LBMException ex)
	{
	    System.err.println("Error creating source attributes: " + ex.toString());
	    System.exit(1);
	}
	LongObject cd = new LongObject();
	sattr.setMessageReclamationCallback(srccb, cd);
	if (send_rate != 0)
	{
	    try
	    {
		sattr.setProperty("transport", "LBTR" + protocol);
		cattr.setProperty("transport_lbtrm_data_rate_limit",
			Integer.toString(send_rate));
		cattr.setProperty("transport_lbtrm_retransmit_rate_limit",
			Integer.toString(retrans_rate));
	    }
	    catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting LBTRM rate: " + ex.toString());
		System.exit(1);
	    }
	}

	if (flightsz > 0) {
	    try
	    {
		sattr.setProperty("ume_flight_size", Integer.toString(flightsz));
	    } catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting flight size: " + ex.toString());
		System.exit(1);
	    }
	}


	try
	{
	    System.out.println("Allowing " + sattr.getValue("ume_flight_size") + " in-flight messages.");
	} catch (LBMException ex) {
	    System.err.println("Error getting flight size: " + ex.toString());
	    System.exit(1);
	}

	/* Set the command line store IP and port to the config */
	if (storeip != null)
	{
	    try
	    {
		sattr.setProperty("ume_store", storeip + ":" + storeport);
	    }
	    catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting UME store: " + ex.toString());
		System.exit(1);
	    }
	} else if (storename != null) {
	    try
	    {
		sattr.setProperty("ume_store_name", storename);
	    }
	    catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting UME storename: " + ex.toString());
		System.exit(1);
	    }
	}

	/* Get the store address and port from the current config.
	 * If the command line specified the address/port it will
	 * have set the config above overriding the config file
	 */
	try {
	    int colon;

	    storeip = sattr.getValue("ume_store");

	    colon = storeip.indexOf(':');
	    if(colon > 0) {
		storeport = storeip.substring(colon, storeip.length());
		storeip = storeip.substring(0,colon - 1);
	    }

	    if (storeip.equals("0.0.0.0"))
		storeip = null;
	}
	catch (LBMException ex)
	{
	    System.err.println("Error fetching source attributes: " + ex.toString());
	    System.exit(1);
	}

	/* Calculate the appropriate message rate */
	if(msgs_per_sec > 0) {
	    calc_rate_vals();

	    System.out.println(msgs_per_sec + " msgs/sec -> " + msgs_per_ivl + " msgs/ivl, "
		    + pause_ivl + " msec ivl ");
	}
	if (latejoin)
	{
	    try
	    {
		sattr.setProperty("ume_late_join", "1");
	    }
	    catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting latejoin: " + ex.toString());
		System.exit(1);
	    }
	}

	if (cconffname != null)
	{
	    try
	    {
		FileInputStream f = new FileInputStream(cconffname);
		cattr.load(f);
	    }
	    catch (IOException e)
	    {
		System.err.println(e.toString());
		System.exit(1);
	    }
	    catch (LBMRuntimeException ex)
	    {
		System.err.println("Error setting context configuration: " + ex.toString());
		System.exit(1);
	    }
	}

	/* Check to see what is set and what is not for UME settings. */
	/* If no UME stores have been specified, exit program. */
	if (check_ume_store_config(sattr) == -1)
	    System.exit(1);

	try
	{
	    if (sattr.getValue("ume_late_join").equals("1"))
		System.out.println("Using UME Late Join.");
	    else
		System.out.println("Not using UME Late Join.");
	    if (sattr.getValue("ume_confirmed_delivery_notification").equals("1"))
	    {
		System.out.print("Using UME Confirmed Delivery Notification. ");
		if (verbose == 1)
		    System.out.println("Will display only confirmed delivery events.");
		else if (verbose > 1)
		    System.out.println("Will display confirmed delivery events and message stability events.");
		else
		    System.out.println("Will not display events.");
	    }
	    else
		System.out.println("Not using UME Confirmed Delivery Notification.");

	    if (sattr.getValue("ume_message_stability_notification").equals("1")) {
		System.out.print("Using UME Message Stability Notification. ");
		if (verbose >= 1)
		    System.out.println("Will display message stability events. ");
		else
		    System.out.println(" Will not display events. ");
		stability = true;
	    }
	    if (sattr.getValue("ume_message_stability_notification").equals("2")) {
		System.out.print("Using UME Message Stability Notification. ");
		if (verbose >= 1)
		    System.out.println("Will display message stability events. ");
		else
		    System.out.println(" Will not display events. ");
		stability = true;
	    }
	    if (sattr.getValue("ume_message_stability_notification").equals("3")) {
		System.out.print("Using UME Message Stability Notification. ");
		if (verbose >= 1)
		    System.out.println("Will display message stability events. ");
		else
		    System.out.println(" Will not display events. ");
		stability = true;
	    }
	}
	catch (LBMException ex)
	{
	    System.err.println("Error fetching source attributes: " + ex.toString());
	    System.exit(1);
	}
	try
	{
	    if (sequential)
	    {
		cattr.setProperty("operational_mode", "sequential");
	    }
	    else
	    {
		// The default for operational_mode is embedded, but set it
		// explicitly in case a configuration file was specified with
		// a different value.
		cattr.setProperty("operational_mode", "embedded");
	    }
	}
	catch (LBMRuntimeException ex)
	{
	    System.err.println("Error setting operational_mode: " + ex.toString());
	    System.exit(1);
	}
	LBMContext ctx = null;
	try
	{
	    ctx = new LBMContext(cattr);
	}
	catch (LBMException ex)
	{
	    System.err.println("Error creating context: " + ex.toString());
	    System.exit(1);
	}

	LBMTopic topic = null;
	try
	{
	    topic =  ctx.allocTopic(args[gopt.getOptind()], sattr);
	}
	catch (LBMException ex)
	{
	    System.err.println("Error allocating topic: " + ex.toString());
	    System.exit(1);
	}
	LBMSource src = null;
	LBMContextThread ctxthread = null;
	if (sequential)
	{
	    // create thread to handle event processing
	    ctxthread = new LBMContextThread(ctx);
	    ctxthread.start();
	}
	if (sequential)
	{
	    System.err.println("Sequential mode");
	}
	else
	{
	    System.err.println("Embedded mode");
	}
	try
	{
	    src = ctx.createSource(topic, srccb, null, null);
	}
	catch (LBMException ex)
	{
	    System.err.println("Error creating source: " + ex.toString());
	    System.exit(1);
	}

	try
	{
	    Thread.sleep(1000);
	}
	catch (InterruptedException e) { }
	LBMMonitorSource lbmmonsrc = null;
	if (monitor_context || monitor_source)
	{
	    try
	    {
		lbmmonsrc = new LBMMonitorSource(mon_format, mon_format_options, mon_transport, mon_transport_options);
	    }
	    catch (LBMException ex)
	    {
		System.err.println("Error creating monitor source: " + ex.toString());
		System.exit(1);
	    }
	    try
	    {
		if (monitor_context)
		    lbmmonsrc.start(ctx, application_id, monitor_context_ivl);
		else
		    lbmmonsrc.start(src, application_id, monitor_source_ivl);
	    }
	    catch (LBMException ex)
	    {
		System.err.println("Error starting monitoring: " + ex.toString());
		System.exit(1);
	    }
	}

	System.out.println("Sending "
		+ msgs
		+ " messages of size "
		+ msglen
		+ " bytes to topic ["
		+ args[gopt.getOptind()]
		+ "]");
	System.out.flush();
	long start_time = System.currentTimeMillis();
	boolean regProblem = false;
	LBMSourceSendExInfo exinfo = new LBMSourceSendExInfo();
	for (long count = 0; count < msgs; )
	{
	    if( ( count == 1000) && (dereg == true))
	    {
		System.out.println("Just before sending Dergistration");
		System.out.flush();
		try {
		    System.out.println("Sending DEREGISTRATION");
		    src.umederegister();
		    dereg = false;
		} catch (LBMException ex)
		{
		    System.err.println("Error Deregistering source: " + ex.toString());
		    System.exit(1);
		}
	    }
	    for(int ivlcount = 0; ivlcount < msgs_per_ivl; ivlcount++) {
		if (seqnum_info || stability)
		{
		    //exinfo.setClientObject(new Long..valueOf((long)count + 1));
            exinfo.setClientObject(count + 1);
		    last_clientd_sent = count + 1;
		}
		try
		{
		    int xflag = 0;
		    srccb.blocked = true;
		    if (seqnum_info) {
			exinfo.setFlags(LBM.SRC_SEND_EX_FLAG_SEQUENCE_NUMBER_INFO);
		    }
		    try {
			/* Sleep a bit after a re-registration
			 * to allow topic resolution to take place. */
			if (sleep_before_sending > 0) {
			    Thread.sleep(sleep_before_sending);
			    sleep_before_sending = 0;
			}
		    } catch (InterruptedException ex) {
			/* Interrupted - retry */
		    }
		    src.send(message, msglen, (block ? 0 : LBM.SRC_NONBLOCK) | xflag, exinfo);
		    srccb.blocked = false;
		    count++;
		    appsent++;
		}
		catch (LBMEWouldBlockException ex)
		{
		    while (srccb.blocked)
		    {
			try
			{
			    Thread.sleep(100);
			}
			catch (InterruptedException e) { }
		    }
		    continue;
		}
		catch(UMENoRegException ex)
		{
		    if(verbose > 0)
		    {
			System.out.println("UMENoRegException: "+ex.getMessage());
		    }
		    if (!regProblem)
		    {
			regProblem = true;
			System.out.println("Send unsuccessful. Waiting...");
			System.out.flush();
		    }
		    try
		    {
			Thread.sleep(1000);
			UmeSrc.appsent--;
		    }
		    catch (InterruptedException e) { }
		    continue;
		}
		catch(UMENoQueueException ex)
		{
		    if(verbose > 0)
		    {
			System.out.println("UMENoQueueException: "+ex.getMessage());
		    }
		    if (!regProblem)
		    {
			regProblem = true;
			System.out.println("Queue: Send unsuccessful. Waiting...");
			System.out.flush();
		    }
		    try
		    {
			Thread.sleep(1000);
			UmeSrc.appsent--;
		    }
		    catch (InterruptedException e) { }
		    continue;
		}
		catch(UMENoStoreException ex)
		{
		    if(verbose > 0)
		    {
			System.out.println("UMENoStoreException: "+ex.getMessage());
		    }
		    if (!regProblem)
		    {
			regProblem = true;
			System.out.println("Store: Send unsuccessful. Waiting...");
			System.out.flush();
		    }
		    try
		    {
			Thread.sleep(1000);
			UmeSrc.appsent--;
		    }
		    catch (InterruptedException e) { }
		    continue;
		}
		catch (LBMException ex)
		{
		    System.err.println("Error sending message: " + ex.toString());
		}
		if (regProblem)
		{
		    regProblem = false;
		    System.out.println("Send OK. Continuing.");
		    System.out.flush();
		}
		bytes_sent += msglen;
	    } /* for(ivlcount) */
	    if (pause_ivl > 0)
	    {
		try
		{
		    Thread.sleep(pause_ivl);
		}
		catch (InterruptedException e) { }
	    }
	}
	long end_time = System.currentTimeMillis();
	double secs = (end_time - start_time) / 1000.;
	System.out.println("Sent "
		+ msgs
		+ " messages of size "
		+ msglen
		+ " bytes in "
		+ secs
		+ " seconds.");
	print_bw(secs, msgs, bytes_sent);
	System.out.flush();
	if (linger > 0)
	{
	    System.out.println("Lingering for "
		    + linger
		    + " seconds...");
	    try
	    {
		Thread.sleep(linger * 1000);
	    }
	    catch (InterruptedException e) { }
	}
	if (sequential)
	{
	    ctxthread.terminate();
	}
	if (lbmmonsrc != null)
	{
	    try
	    {
		lbmmonsrc.close();
	    }
	    catch (LBMException ex)
	    {
		System.err.println("Error closing monitor source: " + ex.toString());
	    }
	}
	objRec.close();
	try
	{
	    src.close();
	}
	catch (LBMException ex)
	{
	    System.err.println("Error closing source: " + ex.toString());
	}
	ctx.close();
	cd.done();
    }

    /*
     * Function that determines how to pace sending of messages to obtain a given
     * rate.  Given messages per second, calculates number of messages to send in
     * a particular interval and the number of milliseconds to pause between
     * intervals.
     */
    private static void calc_rate_vals()
    {
	int intervals_per_sec = 1000;

	pause_ivl = 20;
	intervals_per_sec = 1000/(pause_ivl);

	while(pause_ivl <= 1000 && msgs_per_sec % intervals_per_sec != 0)
	{
	    pause_ivl++;
	    while(((1000 % pause_ivl) != 0) && pause_ivl <= 1000)
		pause_ivl++;
	    intervals_per_sec = 1000/pause_ivl;
	}
	msgs_per_ivl = msgs_per_sec/intervals_per_sec;
    }

    private static void print_help_exit(int exit_value){
	System.err.println(LBM.version());
	System.err.println(purpose);
	System.err.println(usage);
	System.exit(exit_value);
    }

    private static void print_bw(double sec, int msgs, long bytes)
    {
	double mps = 0;
	double bps = 0;
	double kscale = 1000;
	double mscale = 1000000;
	char mgscale = 'K';
	char bscale = 'K';

	if (sec == 0) return; /* avoid division by zero */

	mps = msgs/sec;
	bps = bytes*8/sec;
	if (mps <= mscale)
	{
	    mgscale = 'K';
	    mps /= kscale;
	}
	else
	{
	    mgscale = 'M';
	    mps /= mscale;
	}
	if (bps <= mscale)
	{
	    bscale = 'K';
	    bps /= kscale;
	}
	else
	{
	    bscale = 'M';
	    bps /= mscale;
	}
	NumberFormat nf = NumberFormat.getInstance();
	nf.setMaximumFractionDigits(3);
	System.out.println(sec
		+ " secs. "
		+ nf.format(mps)
		+ " " + mgscale + "msgs/sec. "
		+ nf.format(bps)
		+ " " + bscale + "bps");
    }

    private static int check_ume_store_config(LBMSourceAttributes sattr)
    {
	// flag whether a store name is present
	String store_name = null;
	try
	{
	    store_name = sattr.getValue("ume_store_name");
	}
	catch (LBMException ex)
	{
	    System.err.println("Error getting source attribute: " + ex.toString());
	}
	boolean hasStoreName = (store_name.length() == 0 || store_name == null) ? false : true;

	UMEStoreEntry[] stores = sattr.getStores();
	UMEStoreGroupEntry[] groups = sattr.getStoreGroups();
	InetSocketAddress addr = null;

	if (stores.length < 1 && !hasStoreName)
	{
	    System.err.println("No UME stores specified. To send without a store, please use lbmsrc.");
	    return -1; /* exit program */
	}
	try {
	    String storeBehavior = sattr.getValue("ume_store_behavior");
	    if (storeBehavior.equals("round-robin")) {
		for (int i = 0; i < stores.length; i++) {
		    addr = stores[i].address();
		    System.out.print("Store " + i + ": ");
		    if(stores[i].isNamed()) {
			/* If the IP is 0.0.0.0, this store is specified by name. */
			System.out.print(stores[i].name() + " DomainID " + stores[i].domainId() + " ");
		    } else {
			System.out.print(addr.toString() + " DomainID " + stores[i].domainId() + " ");
		    }
		    if (stores[i].registrationId() != 0) {
			System.out.print("RegID " + stores[i].registrationId());
		    }
		    System.out.println();
		}
	    }
	    else {
		UmeSrc.store_behaviour = LBM.SRC_TOPIC_ATTR_UME_STORE_BEHAVIOR_QC;

		if (groups.length > 0) {
		    for (int j = 0; j < groups.length; j++) {
			System.out.println("Group " + j + ": Size " + groups[j].groupSize());
			for (int i = 0; i < stores.length; i++) {
			    if (stores[i].groupIndex() == groups[j].index()) {
				addr = stores[i].address();
				System.out.print("Store " + i + ": ");
				if(stores[i].isNamed()) {
				    /* If the IP is 0.0.0.0, this store is specified by name. */
				    System.out.print(stores[i].name() + " DomainID " + stores[i].domainId() + " ");
				} else {
				    System.out.print(addr.toString() + " DomainID " + stores[i].domainId() + " ");
				}
				if (stores[i].registrationId() != 0) {
				    System.out.print("RegID " + stores[i].registrationId());
				}
				System.out.println();
			    }
			}
		    }
		}
		else {
		    System.out.println("Group None: Number of Stores: " + stores.length);
		    for (int i = 0; i < stores.length; i++) {
			addr = stores[i].address();
			System.out.print("Store " + i + ": ");
			if(stores[i].isNamed()) {
			    /* If the IP is 0.0.0.0, this store is specified by name. */
			    System.out.print(stores[i].name() + " DomainID " + stores[i].domainId() + " ");
			} else {
			    System.out.print(addr.toString() + " DomainID " + stores[i].domainId() + " ");
			}
			if (stores[i].registrationId() != 0) {
			    System.out.print("RegID " + stores[i].registrationId());
			}
			System.out.println();
		    }
		}
	    }
	} catch (LBMException ex) {
	    System.err.println("Error getting source attributes: " + ex.toString());
	}
	System.out.flush();
	return 0;
    }
}

class LongObject
{
    public long value = 0;

    public void done()
    {
    }
}

/* Handle UMP liveness receiver detection */
class UMERcvrLivenessCreationCb implements UMEReceiverLivenessCreationCallback
{
    public Object onNewReceiver(UMEReceiverLivenessCallbackInfo info, Object cbArg)
    {
	Object source_clientd = null;
	System.out.println("Receiver detected: regid " + info.userRcvRegId() + ", session_id " + info.sessionId());
	System.out.flush();
	return source_clientd;
    }
}

/* Handle UMP liveness receiver lost */
class UMERcvrLivenessDeletionCb implements UMEReceiverLivenessDeletionCallback
{
    public int onReceiverDelete(UMEReceiverLivenessCallbackInfo info, Object cbArg, Object sourceCbArg)
    {
	System.out.print("Receiver declared dead: regid " + info.userRcvRegId() + ", session_id " + info.sessionId() + ", reason ");
	if ((info.flags() & LBM.LBM_UME_LIVENESS_RECEIVER_UNRESPONSIVE_FLAG_EOF) != 0)
	{
	    System.out.println("EOF");
	}
	else if ((info.flags() & LBM.LBM_UME_LIVENESS_RECEIVER_UNRESPONSIVE_FLAG_TMO) != 0)
	{
	    System.out.println("TIMEOUT");
	}
	System.out.flush();
	return 0;
    }
}

class UMESrcCB implements LBMSourceEventCallback, LBMMessageReclamationCallback
{
    public boolean blocked = false;
    private int _verbose;
    private int force_reclaim_total = 0;

    public UMESrcCB(int verbose)
    {
	_verbose = verbose;
    }

    public int onSourceEvent(Object arg, LBMSourceEvent sourceEvent)
    {
	switch (sourceEvent.type()) {
	    case LBM.SRC_EVENT_CONNECT:
		System.out.println("Receiver connect " + sourceEvent.dataString());
		break;
	    case LBM.SRC_EVENT_DISCONNECT:
		System.out.println("Receiver disconnect " + sourceEvent.dataString());
		break;
	    case LBM.SRC_EVENT_WAKEUP:
		blocked = false;
		break;
	    case LBM.SRC_EVENT_UME_REGISTRATION_ERROR:
		System.out.println("Error registering source with UME store: "
			+ sourceEvent.dataString());
		break;
	    case LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS:
		System.out.println("UME store registration success. RegID "
			+ sourceEvent.registrationId());
		break;
	    case LBM.SRC_EVENT_UME_DEREGISTRATION_SUCCESS_EX:
		System.out.print("UME_DEREGISTRATION_SUCCESS_EX ");
		System.out.println();
		break;
	    case LBM.SRC_EVENT_UME_DEREGISTRATION_COMPLETE_EX:
		System.out.print("UME_DEREGISTRATION_COMPLETE_EX ");
		System.out.println();
		break;

	    case LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX:
		UMESourceEventRegistrationSuccessInfo reg = sourceEvent.registrationSuccessInfo();
		System.out.print("UME store " + reg.storeIndex() + ": " + reg.store()
			+ " registration success. RegID " + reg.registrationId() + ". Flags "
			+ reg.flags() + " ");
		if (((reg.flags() & LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX_FLAG_OLD)) != 0) {
		    System.out.print("OLD[SQN " + reg.sequenceNumber() + "] ");
		}
		if (((reg.flags() & LBM.SRC_EVENT_UME_REGISTRATION_SUCCESS_EX_FLAG_NOACKS)) != 0) {
		    System.out.print("NOACKS ");
		}
		System.out.println();
		break;
	    case LBM.SRC_EVENT_UME_REGISTRATION_COMPLETE_EX:
		UMESourceEventRegistrationCompleteInfo regcomp = sourceEvent.registrationCompleteInfo();

		UmeSrc.sleep_before_sending = 1000;

		System.out.print("UME registration complete. SQN " + regcomp.sequenceNumber()
			+ ". Flags " + regcomp.flags() + " ");
		if ((regcomp.flags() & LBM.SRC_EVENT_UME_REGISTRATION_COMPLETE_EX_FLAG_QUORUM) != 0) {
		    System.out.print("QUORUM ");
		}
		System.out.println();
		break;
	    case LBM.SRC_EVENT_UME_MESSAGE_STABLE:
		if (_verbose >= 2)
		    System.out.println("UME message stable - sequence number "
			    + Long.toHexString(sourceEvent.sequenceNumber())
			    + " (cd "
			    + Long.toHexString(((Long) sourceEvent.clientObject()).longValue())
			    + ")");

		/* Peg the counter for the received stable message */
            UmeSrc.stablerecv++;
		break;
	    case LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE:
		UMESourceEventAckInfo nstaInfo = sourceEvent.ackInfo();
		if (_verbose >= 2) {
		    System.out.print("UME store " + nstaInfo.storeIndex() + ": "
			    + nstaInfo.store() + " message NOT stable!! SQN " + nstaInfo.sequenceNumber()
			    + " (cd " + nstaInfo.clientObject() + "). Flags " + nstaInfo.flags() + " ");
		    if ((nstaInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE_FLAG_LOSS) != 0) {
			System.out.print("LOSS");
		    } else if ((nstaInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_NOT_STABLE_FLAG_TIMEOUT) != 0) {
			System.out.print("TIMEOUT");
		    }
		    System.out.println();
		}
		break;
	    case LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX:
		UMESourceEventAckInfo staInfo = sourceEvent.ackInfo();
		if (_verbose >= 2) {
		    System.out.print("UME store " + staInfo.storeIndex() + ": "
			    + staInfo.store() + " message stable. SQN " + staInfo.sequenceNumber()
			    + " (cd " + staInfo.clientObject() + "). Flags " + staInfo.flags() + " ");
		    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_INTRAGROUP_STABLE) != 0) {
			System.out.print("IA ");
		    }
		    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_INTERGROUP_STABLE) != 0) {
			System.out.print("IR ");
		    }
		    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE) != 0) {
			System.out.print("STABLE ");
		    }
		    if ((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STORE) != 0) {
			System.out.print("STORE ");
		    }
		    System.out.println();
		}

		if (UmeSrc.store_behaviour == LBM.SRC_TOPIC_ATTR_UME_STORE_BEHAVIOR_RR ||
			((staInfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE) == LBM.SRC_EVENT_UME_MESSAGE_STABLE_EX_FLAG_STABLE)) {

		    /* Peg the counter for the received stable message */
            UmeSrc.stablerecv++;
		}
		break;
	    case LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION:
		if (_verbose > 0)
		    System.out.println("UME delivery confirmation - sequence number "
			    + Long.toHexString(sourceEvent.sequenceNumber())
			    + " Rcv RegID "
			    + sourceEvent.registrationId()
			    + " (cd "
			    + Long.toHexString(((Long) sourceEvent.clientObject()).longValue())
			    + ")");
		break;
	    case LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX:
		UMESourceEventAckInfo cdelvinfo = sourceEvent.ackInfo();
		if (_verbose > 0) {
		    System.out.print("UME delivery confirmation. SQN " + cdelvinfo.sequenceNumber()
			    + ", RcvRegID " + cdelvinfo.receiverRegistrationId() + " (cd "
			    + cdelvinfo.clientObject() + "). Flags " + cdelvinfo.flags() + " ");
		    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_UNIQUEACKS) != 0) {
			System.out.print("UNIQUEACKS ");
		    }
		    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_UREGID) != 0) {
			System.out.print("UREGID ");
		    }
		    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_OOD) != 0) {
			System.out.print("OOD ");
		    }
		    if ((cdelvinfo.flags() & LBM.SRC_EVENT_UME_DELIVERY_CONFIRMATION_EX_FLAG_EXACK) != 0) {
			System.out.print("EXACK ");
		    }
		    System.out.println();
		}
		break;
	    case LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED:
		if (_verbose > 0)
		    System.out.println("UME message reclaimed - sequence number "
			    + Long.toHexString(sourceEvent.sequenceNumber())
			    + " (cd "
			    + Long.toHexString(((Long) sourceEvent.clientObject()).longValue())
			    + ")");
		break;
	    case LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED_EX:
		UMESourceEventAckInfo reclaiminfo = sourceEvent.ackInfo();

		if (_verbose > 0) {
		    if (reclaiminfo.clientObject() != null) {
			System.out.print("UME message reclaimed (ex) - sequence number "
				+ Long.toHexString(reclaiminfo.sequenceNumber())
				+ " (cd "
				+ Long.toHexString(((Long) reclaiminfo.clientObject()).longValue())
				+ "). Flags 0x"
				+ reclaiminfo.flags());
		    } else {
			System.out.print("UME message reclaimed (ex) - sequence number "
				+ Long.toHexString(reclaiminfo.sequenceNumber())
				+ " Flags 0x"
				+ reclaiminfo.flags());
		    }
		    if ((reclaiminfo.flags() & LBM.SRC_EVENT_UME_MESSAGE_RECLAIMED_EX_FLAG_FORCED) != 0) {
			System.out.print(" FORCED");
		    }
		    System.out.println();
		}
		break;
	    case LBM.SRC_EVENT_UME_STORE_UNRESPONSIVE:
		System.out.println("UME store: "
			+ sourceEvent.dataString());
		break;
	    case LBM.SRC_EVENT_SEQUENCE_NUMBER_INFO:
		LBMSourceEventSequenceNumberInfo info = sourceEvent.sequenceNumberInfo();
		if (info.firstSequenceNumber() != info.lastSequenceNumber()) {
		    System.out.println("SQN [" + info.firstSequenceNumber()
			    + "," + info.lastSequenceNumber() + "] (cd "
			    + info.clientObject() + ")");
		} else {
		    System.out.println("SQN " + info.lastSequenceNumber()
			    + " (cd " + info.clientObject() + ")");
		}
		break;
	    case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION:
		if (_verbose > 0) {
		    LBMSourceEventFlightSizeNotification note = sourceEvent.flightSizeNotification();
		    System.out.print("Flight Size Notification. Type ");
		    switch (note.type()) {
			case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_UME:
			    System.out.print("UME");
			    break;
			case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_ULB:
			    System.out.print("ULB");
			    break;
			case LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_TYPE_UMQ:
			    System.out.print("UMQ");
			    break;
			default:
			    System.out.print("unknown");
			    break;
		    }
		    System.out.println(". Inflight is "
			    + (note.state() == LBM.SRC_EVENT_FLIGHT_SIZE_NOTIFICATION_STATE_OVER ? "OVER" : "UNDER")
			    + " specified flight size");
		}
		break;
	    default:
		System.out.println("Unknown source event "
			+ sourceEvent.type());
		break;
	}

	sourceEvent.dispose();
	System.out.flush();
	return 0;
    }

    public void onMessageReclaim(Object clientd, String topic, long sqn)
    {
	LongObject t = (LongObject)clientd;
	if (t == null)
	{
	    System.err.println("WARNING: source for topic \"" + topic + "\" forced reclaim 0x" + Long.toString(sqn, 16));
	}
	else
	{
	    long endt = System.currentTimeMillis();
	    endt -= t.value;
	    force_reclaim_total++;
	    if (endt > 5000)
	    {
		System.err.println("WARNING: source for topic \"" + topic + "\" forced reclaim. Total " + force_reclaim_total);
		t.value = System.currentTimeMillis();
	    }
	}
    }
}
