package org.phy2000.ultramessaging.examples;

import java.util.StringTokenizer;


/*
"Utility functions for UM example programs.
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

class lbmExampleUtil {
    public static ParseRateVars parseRate(String s) {
        ParseRateVars parseRateVars = new ParseRateVars();
        String strRate = "";
        char protocol;
        parseRateVars.protocol = 'M';

        StringTokenizer tokens = new StringTokenizer(s, "/");
        if (tokens.countTokens() != 2) {
            parseRateVars.error = true;
            return parseRateVars;
        }

        // Get protocol if one was specified
        strRate = tokens.nextToken();
        if (Character.isLetter(strRate.charAt(0))) {
            protocol = strRate.charAt(0);
            switch (protocol) {
                case 'm': case 'M':
                    break;				// Already set to multicast by default - no action required
                case 'u': case 'U':
                    parseRateVars.protocol = 'U';
                    break;
                default:
                    parseRateVars.error = true;
                    return parseRateVars;
            }
            strRate = strRate.substring(1, strRate.length());
        }

        // Calculate transmission rate
        parseRateVars = expandSuffix(parseRateVars, strRate);
        if (parseRateVars.error) {
            return parseRateVars;
        }
        try {
            parseRateVars.rate = Integer.parseInt(parseRateVars.str_rate) * parseRateVars.mult_rate;
        } catch (Exception e) {
            parseRateVars.error = true;
            return parseRateVars;
        }

        // Calculate retransmission rate
        parseRateVars.mult_rate = 1;
        parseRateVars = expandSuffix(parseRateVars, tokens.nextToken());
        if (parseRateVars.error) {
            return parseRateVars;
        }
        try {
            parseRateVars.retrans = Integer.parseInt(parseRateVars.str_rate) * parseRateVars.mult_rate;
        } catch (Exception e) {
            parseRateVars.error = true;
            return parseRateVars;
        }

        return parseRateVars;
    }

    private static ParseRateVars expandSuffix(ParseRateVars parseRateVars, String tmpRateStr) {
        char mult = 0;

        if (!Character.isDigit(tmpRateStr.charAt(tmpRateStr.length()-1))) {
            mult = tmpRateStr.charAt(tmpRateStr.length()-1);
            tmpRateStr = tmpRateStr.substring(0, tmpRateStr.length()-1);

            switch(mult) {
                case 'k': case 'K':
                    parseRateVars.mult_rate = 1000;
                    break;
                case 'm': case 'M':
                    parseRateVars.mult_rate = 1000000;
                    break;
                case 'g': case 'G':
                    parseRateVars.mult_rate = 1000000000;
                    break;
                case '%':
                    System.out.println("\n** ERROR - Please reference the updated usage.  Retransmission \n" +
                            "**         rate no longer allows % symbol as a shortcut.\n");
                default:				// Any other letter in mult results in an error
                    parseRateVars.error = true;
                    return parseRateVars;
            }
        }

        parseRateVars.str_rate = tmpRateStr;

        return parseRateVars;
    }
}

class ParseRateVars {
    public boolean error = false;
    public int rate = 1;
    public int retrans = 1;
    public int mult_rate = 1;
    public char protocol = '\0';
    public String str_rate = "";
}