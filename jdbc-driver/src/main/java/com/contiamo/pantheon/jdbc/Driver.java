package com.contiamo.pantheon.jdbc;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Driver extends org.apache.calcite.avatica.remote.Driver {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.calcite.avatica.remote.Driver.class);

    public static final String CONNECT_STRING_PREFIX = "jdbc:pantheon:";

    static final String URL_REGEX = "^//([.a-zA-Z0-9-]+):(\\d+)/([a-zA-Z0-9-]+)/([a-zA-Z0-9-]+)\\??";
    static final Pattern URL_PATTERN = Pattern.compile(URL_REGEX);

    static {
        new Driver().register();
    }

    public Driver() {
        super();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion(
                "Pantheon JDBC Driver",
                "0.1", // TODO replace version
                "Pantheon",
                "1.14", // TODO replace version
                true,
                0,
                1,
                1,
                14
        );
    }

    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }

    private Properties parseUrl(String url, Properties info) throws SQLException {
        final String prefix = getConnectStringPrefix();
        assert url.startsWith(prefix);
        final String urlSuffix = url.substring(prefix.length());

        Matcher matcher = URL_PATTERN.matcher(urlSuffix);
        if (!matcher.matches()) throw new SQLException("Invalid URL: " + url);
        final String host = matcher.group(1);
        final String port = matcher.group(2);
        final String catalogId = matcher.group(3);
        final String schema = matcher.group(4);

        final String params = urlSuffix.substring(matcher.end());
        final Properties info2 = ConnectStringParser.parse(params, info);
        info2.setProperty("url", connectionUrl(host, port));
        info2.setProperty("catalogId", catalogId);
        info2.setProperty("schemaName", schema);

        return info2;
    }

    private static String connectionUrl(String host, String port) {
        return "http://" + host + ":" + port;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        final Properties info2 = parseUrl(url, info);
        if (!info2.containsKey(BuiltInConnectionProperty.SERIALIZATION.camelName())) {
            info2.setProperty(BuiltInConnectionProperty.SERIALIZATION.camelName(), "protobuf");
        }
        final AvaticaConnection connection =
                factory.newConnection(this, factory, url, info2);

        handler.onConnectionInit(connection);

        Service service = connection.getService();
        assert null != service;

        service.apply(
                new Service.OpenConnectionRequest(connection.id,
                        Service.OpenConnectionRequest.serializeProperties(info2)));

        return connection;
    }
}
