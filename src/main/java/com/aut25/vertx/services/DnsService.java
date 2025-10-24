package com.aut25.vertx.services;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * DnsService provides methods to perform DNS lookups.
 */
public class DnsService {
        /**
         * Reverse DNS lookup for the given IP address.
         * 
         * @param ip The IP address to look up.
         * @return The canonical host name associated with the IP address.
         * @throws UnknownHostException if the IP address is not valid.
         */
        public String reverseLookupBlocking(String ip) throws UnknownHostException {

                if (!isValidIP(ip)) {
                        return "N/A";
                }
                return InetAddress.getByName(ip).getCanonicalHostName();
        }

        /**
         * Validate the given IP address.
         * 
         * @param ip The IP address to validate.
         * @return true if the IP address is valid, false otherwise.
         */
        private boolean isValidIP(String ip) {
                try {
                        InetAddress.getByName(ip);
                        return true;
                } catch (UnknownHostException e) {
                        return false;
                }
        }
}
