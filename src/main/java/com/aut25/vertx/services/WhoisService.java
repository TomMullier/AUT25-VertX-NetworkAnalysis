package com.aut25.vertx.services;

import org.apache.commons.net.whois.WhoisClient;
import java.io.IOException;

/**
 * WhoisService provides methods to perform WHOIS lookups.
 */
public class WhoisService {
        /**
         * Perform a WHOIS lookup for the given IP address.
         * 
         * @param ip The IP address to look up.
         * @return The WHOIS information for the IP address.
         * @throws IOException if an error occurs during the lookup.
         */
        public String lookupBlocking(String ip) throws IOException {
                WhoisClient whois = new WhoisClient();
                whois.connect(WhoisClient.DEFAULT_HOST);
                String result = whois.query(ip + "\r\n");
                whois.disconnect();
                return parseWhoisOrg(result);
        }

        

        /**
         * Parse the organization name from the raw WHOIS response.
         * 
         * @param whoisRaw The raw WHOIS response.
         * @return The organization name or "No match" if not found.
         */
        private String parseWhoisOrg(String whoisRaw) {
                if (whoisRaw == null)
                        return "N/A";
                String[] lines = whoisRaw.split("\n");
                for (String line : lines) {
                        line = line.trim();
                        if (line.startsWith("OrgName:") || line.startsWith("Organization:")) {
                                return line.split(":", 2)[1].trim();
                        }
                }
                return "Unknown";
        }
}
