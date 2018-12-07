package uk.co.essarsoftware.backup.upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

class GlacierConfiguration
{

    private static final Logger _LOG = LoggerFactory.getLogger(GlacierConfiguration.class);
    private static final Properties props = new Properties();

    static {

        try {

            // Load from file
            props.load(GlacierConfiguration.class.getResourceAsStream("/glacier.properties"));

        } catch (IOException ioe) {

            _LOG.warn("Unable to load properties: {}", ioe.getMessage());

        }

        // Allow override by system properties
        props.putAll(System.getProperties());

    }

    static final String awsProfile = props.getProperty("aws.profile");
    static final String vaultName = props.getProperty("glacier.vault.name");

}
