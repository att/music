/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.eelf.logging;

import static com.att.eelf.configuration.Configuration.MDC_SERVER_FQDN;
import static com.att.eelf.configuration.Configuration.MDC_SERVER_IP_ADDRESS;
import static com.att.eelf.configuration.Configuration.MDC_SERVICE_INSTANCE_ID;
import static com.att.eelf.configuration.Configuration.MDC_SERVICE_NAME;
import java.net.InetAddress;
import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;
import com.att.eelf.configuration.SLF4jWrapper;

public class EELFLoggerDelegate extends SLF4jWrapper implements EELFLogger {

    public static final EELFLogger errorLogger = EELFManager.getInstance().getErrorLogger();
    public static final EELFLogger applicationLogger =
                    EELFManager.getInstance().getApplicationLogger();
    public static final EELFLogger auditLogger = EELFManager.getInstance().getAuditLogger();
    public static final EELFLogger metricsLogger = EELFManager.getInstance().getMetricsLogger();
    public static final EELFLogger debugLogger = EELFManager.getInstance().getDebugLogger();

    private String className;
    private static ConcurrentMap<String, EELFLoggerDelegate> classMap = new ConcurrentHashMap<>();

    public EELFLoggerDelegate(final String className) {
        super(className);
        this.className = className;
    }

    /**
     * Convenience method that gets a logger for the specified class.
     * 
     * @see #getLogger(String)
     * 
     * @param clazz
     * @return Instance of EELFLoggerDelegate
     */
    public static EELFLoggerDelegate getLogger(Class<?> clazz) {
        return getLogger(clazz.getName());
    }

    /**
     * Gets a logger for the specified class name. If the logger does not already exist in the map,
     * this creates a new logger.
     * 
     * @param className If null or empty, uses EELFLoggerDelegate as the class name.
     * @return Instance of EELFLoggerDelegate
     */
    public static EELFLoggerDelegate getLogger(final String className) {
        String classNameNeverNull = className == null || "".equals(className)
                        ? EELFLoggerDelegate.class.getName()
                        : className;
        EELFLoggerDelegate delegate = classMap.get(classNameNeverNull);
        if (delegate == null) {
            delegate = new EELFLoggerDelegate(className);
            classMap.put(className, delegate);
        }
        return delegate;
    }

    /**
     * Logs a message at the lowest level: trace.
     * 
     * @param logger
     * @param msg
     */
    public void trace(EELFLogger logger, String msg) {
        if (logger.isTraceEnabled()) {
            logger.trace(msg);
        }
    }

    /**
     * Logs a message with parameters at the lowest level: trace.
     * 
     * @param logger
     * @param msg
     * @param arguments
     */
    public void trace(EELFLogger logger, String msg, Object... arguments) {
        if (logger.isTraceEnabled()) {
            logger.trace(msg, arguments);
        }
    }

    /**
     * Logs a message and throwable at the lowest level: trace.
     * 
     * @param logger
     * @param msg
     * @param th
     */
    public void trace(EELFLogger logger, String msg, Throwable th) {
        if (logger.isTraceEnabled()) {
            logger.trace(msg, th);
        }
    }

    /**
     * Logs a message at the second-lowest level: debug.
     * 
     * @param logger
     * @param msg
     */
    public void debug(EELFLogger logger, String msg) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg);
        }
    }

    /**
     * Logs a message with parameters at the second-lowest level: debug.
     * 
     * @param logger
     * @param msg
     * @param arguments
     */
    public void debug(EELFLogger logger, String msg, Object... arguments) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg, arguments);
        }
    }

    /**
     * Logs a message and throwable at the second-lowest level: debug.
     * 
     * @param logger
     * @param msg
     * @param th
     */
    public void debug(EELFLogger logger, String msg, Throwable th) {
        if (logger.isDebugEnabled()) {
            logger.debug(msg, th);
        }
    }

    /**
     * Logs a message at info level.
     * 
     * @param logger
     * @param msg
     */
    public void info(EELFLogger logger, String msg) {
        logger.info(className + " - "+msg);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger
     * @param msg
     * @param arguments
     */
    public void info(EELFLogger logger, String msg, Object... arguments) {
        logger.info(msg, arguments);
    }

    /**
     * Logs a message and throwable at info level.
     * 
     * @param logger
     * @param msg
     * @param th
     */
    public void info(EELFLogger logger, String msg, Throwable th) {
        logger.info(msg, th);
    }

    /**
     * Logs a message at warn level.
     * 
     * @param logger
     * @param msg
     */
    public void warn(EELFLogger logger, String msg) {
        logger.warn(msg);
    }

    /**
     * Logs a message with parameters at warn level.
     * 
     * @param logger
     * @param msg
     * @param arguments
     */
    public void warn(EELFLogger logger, String msg, Object... arguments) {
        logger.warn(msg, arguments);
    }

    /**
     * Logs a message and throwable at warn level.
     * 
     * @param logger
     * @param msg
     * @param th
     */
    public void warn(EELFLogger logger, String msg, Throwable th) {
        logger.warn(msg, th);
    }

    /**
     * Logs a message at error level.
     * 
     * @param logger
     * @param msg
     */
    public void error(EELFLogger logger, String msg) {
        logger.error(className+ " - " + msg);
    }

    /**
     * Logs a message with parameters at error level.
     * 
     * @param logger
     * @param msg
     * @param arguments
     */
    public void error(EELFLogger logger, String msg, Object... arguments) {
        logger.warn(msg, arguments);
    }

    /**
     * Logs a message and throwable at error level.
     * 
     * @param logger
     * @param msg
     * @param th
     */
    public void error(EELFLogger logger, String msg, Throwable th) {
        logger.warn(msg, th);
    }

    /**
     * Logs a message with the associated alarm severity at error level.
     * 
     * @param logger
     * @param msg
     * @param severtiy
     */
    public void error(EELFLogger logger, String msg, Object /* AlarmSeverityEnum */ severtiy) {
        logger.error(msg);
    }

    /**
     * Initializes the logger context.
     */
    public void init() {
        setGlobalLoggingContext();
        final String msg =
                        "############################ Logging is started. ############################";
        // These loggers emit the current date-time without being told.
        info(applicationLogger, msg);
        error(errorLogger, msg);
        debug(debugLogger, msg);
        info(auditLogger, msg);
        info(metricsLogger, msg);
    }

    /**
     * Builds a message using a template string and the arguments.
     * 
     * @param message
     * @param args
     * @return
     */
    private String formatMessage(String message, Object... args) {
        StringBuilder sbFormattedMessage = new StringBuilder();
        if (args != null && args.length > 0 && message != null && message != "") {
            MessageFormat mf = new MessageFormat(message);
            sbFormattedMessage.append(mf.format(args));
        } else {
            sbFormattedMessage.append(message);
        }

        return sbFormattedMessage.toString();
    }

    /**
     * Loads all the default logging fields into the MDC context.
     */
    private void setGlobalLoggingContext() {
        MDC.put(MDC_SERVICE_INSTANCE_ID, "");
        try {
            MDC.put(MDC_SERVER_FQDN, InetAddress.getLocalHost().getHostName());
            MDC.put(MDC_SERVER_IP_ADDRESS, InetAddress.getLocalHost().getHostAddress());
        } catch (Exception e) {
            errorLogger.error("setGlobalLoggingContext failed", e);
        }
    }

    public static void mdcPut(String key, String value) {
        MDC.put(key, value);
    }

    public static String mdcGet(String key) {
        return MDC.get(key);
    }

    public static void mdcRemove(String key) {
        MDC.remove(key);
    }

    /**
     * Loads the RequestId/TransactionId into the MDC which it should be receiving with an each
     * incoming REST API request. Also, configures few other request based logging fields into the
     * MDC context.
     * 
     * @param req
     * @param appName
     */
    public void setRequestBasedDefaultsIntoGlobalLoggingContext(HttpServletRequest req,
                    String appName) {
        // Load the default fields
        setGlobalLoggingContext();

        // Load the request based fields
        if (req != null) {
            // Rest Path
            MDC.put(MDC_SERVICE_NAME, req.getServletPath());

            // Client IPAddress i.e. IPAddress of the remote host who is making
            // this request.
            String clientIPAddress = req.getHeader("X-FORWARDED-FOR");
            if (clientIPAddress == null) {
                clientIPAddress = req.getRemoteAddr();
            }
        }
    }
}
