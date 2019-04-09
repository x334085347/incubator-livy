/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.rsc.operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.livy.rsc.RSCConf;

/**
 * LogManager
 */
public class LogManager {

    private final static Log LOG = LogFactory.getLog(LogManager.class.getName());

    public static final String prefix = "livy-";

    private String rootDirPath = null;

    private boolean isOperationLogEnabled = false;

    private int numRetainedStatements;

    private HiveConf hiveConf;

    private Map<String, OperationLog> logs = Collections.synchronizedMap(new LinkedHashMap<String, OperationLog>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, OperationLog> eldest) {
            boolean isRemove = size() > numRetainedStatements;
            if (isRemove) {
                eldest.getValue().close();
            }
            return isRemove;
        }
    });

    public List<String> readLog(String identifier, Long maxRow) throws SQLException {
        OperationLog tl = this.getLog(identifier) ;
        if(tl!=null){
            return tl.readOperationLog(false, maxRow) ;
        }
        return null;
    }

    public OperationLog getLog(String identifier) {
        return logs.get(identifier);
    }

    public LogManager(RSCConf rscConf) {
        initialize(rscConf);
    }

    private void initialize(RSCConf rscConf) {
        String operationLogLocation = rscConf.get(RSCConf.Entry.LOGGING_OPERATION_LOG_LOCATION);
        this.rootDirPath = operationLogLocation + File.separator + prefix + rscConf.get(RSCConf.Entry.SESSION_ID);
        isOperationLogEnabled = rscConf.getBoolean(RSCConf.Entry.LOGGING_OPERATION_ENABLED) && new File(operationLogLocation).isDirectory();
        numRetainedStatements = rscConf.getInt(RSCConf.Entry.RETAINED_STATEMENTS);
        String logLevel = rscConf.get(RSCConf.Entry.LOGGING_OPERATION_LEVEL);
        hiveConf = new HiveConf();
        hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, isOperationLogEnabled);
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION.varname, operationLogLocation);
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, logLevel);
        if(isOperationLogEnabled){
            File rootDir = new File(this.rootDirPath);
            if (!rootDir.exists()) {
                rootDir.mkdir();
            }
        }
    }

    public void removeCurrentOperationLog() {
        OperationLog.removeCurrentOperationLog();
    }

    public void registerOperationLog(String identifier) {
        if (isOperationLogEnabled) {
            OperationLog operationLog = logs.get(identifier);
            if (operationLog == null) {
                synchronized (this) {
                    File operationLogFile = new File(
                            rootDirPath,
                            identifier);
                    // create log file
                    try {
                        if (operationLogFile.exists()) {
                            LOG.warn("The operation log file should not exist, but it is already there: " +
                                    operationLogFile.getAbsolutePath());
                            operationLogFile.delete();
                        }
                        if (!operationLogFile.createNewFile()) {
                            // the log file already exists and cannot be deleted.
                            // If it can be read/written, keep its contents and use it.
                            if (!operationLogFile.canRead() || !operationLogFile.canWrite()) {
                                LOG.warn("The already existed operation log file cannot be recreated, " +
                                        "and it cannot be read or written: " + operationLogFile.getAbsolutePath());
                                return;
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("Unable to create operation log file: " + operationLogFile.getAbsolutePath(), e);
                        return;
                    }

                    // create OperationLog object with above log file
                    try {
                        operationLog = new OperationLog(identifier, operationLogFile, hiveConf);
                    } catch (FileNotFoundException e) {
                        LOG.warn("Unable to instantiate OperationLog object for operation: " +
                                identifier, e);
                        return;
                    }
                    logs.put(identifier, operationLog);
                }
            }
            // register this operationLog to current thread
            OperationLog.setCurrentOperationLog(operationLog);
        }
    }

    public void shutdown() {
        if (isOperationLogEnabled) {
            logs.forEach((k, v) -> v.close());
            File rootDirFile = new File(rootDirPath);
            if (rootDirFile.isDirectory()) {
                rootDirFile.deleteOnExit();
            }
        }
    }

    public static OperationLog getOperationLogByThread() {
        return OperationLog.getCurrentOperationLog();
    }

}
