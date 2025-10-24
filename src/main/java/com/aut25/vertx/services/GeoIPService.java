package com.aut25.vertx.services;

import java.io.File;
import java.net.InetAddress;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * GeoIPService provides methods to get geographical information about an IP
 * address
 * using the MaxMind GeoLite2 database.
 */
public class GeoIPService {
        private DatabaseReader reader;

        /**
         * Constructor that initializes the DatabaseReader with the GeoLite2-City.mmdb
         * database.
         * 
         * @throws IOException if the database file cannot be read.
         */
        public GeoIPService() throws IOException {
                File database = new File("src/main/resources/GeoLite2-City.mmdb");
                reader = new DatabaseReader.Builder(database).build();
        }

        /**
         * Get the country name associated with the given IP address.
         * 
         * @param ip The IP address to look up.
         * @return Country name as a String.
         */
        public String getCountryByIP(String ip) {
                if (!isValidIP(ip)) {
                        return "N/A";
                }
                if (isPrivateIp(ip)) {
                        return "Private IP";
                }
                try {
                        InetAddress ipAddress = InetAddress.getByName(ip);
                        CityResponse response = reader.city(ipAddress);
                        return response.getCountry().getName();
                } catch (Exception e) {
                        e.printStackTrace();
                        return "Unknown";
                }
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

        /**
         * Check if the given IP address is a private IP.
         * 
         * @param ip The IP address to check.
         * @return true if the IP address is private, false otherwise.
         */
        private boolean isPrivateIp(String ip) {
                if (ip.startsWith("10."))
                        return true;
                if (ip.startsWith("192.168."))
                        return true;
                if (ip.startsWith("172.")) {
                        int secondOctet = Integer.parseInt(ip.split("\\.")[1]);
                        return secondOctet >= 16 && secondOctet <= 31;
                }
                return false;
        }

}
