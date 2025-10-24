package com.aut25.vertx.services;

import java.io.File;
import java.net.InetAddress;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import java.io.IOException;
import java.net.UnknownHostException;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.AsnResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * GeoIPService provides methods to get geographical information about an IP
 * address
 * using the MaxMind GeoLite2 database.
 */

public class GeoIPService {
        private final DatabaseReader cityReader;
        private final DatabaseReader asnReader;

        public GeoIPService(String cityDbPath, String asnDbPath) throws IOException {
                this.cityReader = new DatabaseReader.Builder(new File(cityDbPath)).build();
                this.asnReader = new DatabaseReader.Builder(new File(asnDbPath)).build();
        }

        public String getCountryByIP(String ip) {

                if (!isValidIP(ip)) {
                        return "Invalid IP";
                }
                if (isPrivateIp(ip)) {
                        return "Private IP";
                }
                try {
                        InetAddress inet = InetAddress.getByName(ip);
                        CityResponse city = cityReader.city(inet);
                        return city.getCountry().getName();
                } catch (AddressNotFoundException e) {
                        return "Private IP";
                } catch (Exception e) {
                        return "Unknown";
                }
        }

        public String getOrgByIP(String ip) {

                if (!isValidIP(ip)) {
                        return "Invalid IP";
                }
                if (isPrivateIp(ip)) {
                        return "Private IP";
                }
                try {
                        InetAddress inet = InetAddress.getByName(ip);
                        AsnResponse asn = asnReader.asn(inet);
                        return asn.getAutonomousSystemOrganization();
                } catch (AddressNotFoundException e) {
                        return "Private IP";
                } catch (Exception e) {
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
