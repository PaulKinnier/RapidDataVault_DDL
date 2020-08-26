importPackage(java.lang);
importPackage(java.util);
importPackage(com.ads.api.util);
importPackage(com.ads.api.beans.common);
importPackage(com.ads.api.beans.mm);
importPackage(com.ads.api.beans.sm);
importPackage(com.ads.mm.etl.xml.mapping);
importPackage(com.ads.services.webservice);
importPackage(com.icc.util);
importPackage(org.json);
importPackage(org.apache.commons.io);
importPackage(java.util.Properties);

var mmUtil = new MappingManagerUtil(AUTH_TOKEN);
var smUtil = new SystemManagerUtil(AUTH_TOKEN);
var debug = new java.lang.StringBuffer();

function execute() {
    var sb = new java.lang.StringBuffer();    
    sb.append(CreateDataVault());
    return sb;
}

function CreateDataVault() {
    var cs = new java.lang.StringBuffer();
    var now = new Date();
    if (sqlType=="SqlServer") {
        cs.append("USE [" + dbName + "];\nGO\n");
        cs.append("/***** Script Date: "+now+" ******/\n");
    } else if (sqlType=="Snowflake") {
        cs.append("USE " + dbName + ";\n");
        cs.append("/***** Script Date: "+now+" ******/\n");
    } else if (sqlType=="Oracle") {
        if (liquibase!=false) { cs.append("--liquibase formatted sql\n"); }
        cs.append("/***** Script Date: "+now+" ******/\n");
        //cs.append("USE " + dbName + ";\n");
    } else if (sqlType=="Teradata") {
        cs.append("USE " + dbName + ";\n");
        cs.append("/***** Script Date: "+now+" ******/\n");
    }
    cs.append(SMS());
    return cs;
}

function SMS() {
    var ms = new java.lang.StringBuffer();
    var stg = new java.lang.StringBuffer();
    var hub = new java.lang.StringBuffer();
    var lnk = new java.lang.StringBuffer();
    var sat = new java.lang.StringBuffer();
    var fkt = new java.lang.StringBuffer();
    var rhub = new java.lang.StringBuffer();
    var rlink = new java.lang.StringBuffer();
    var rsat = new java.lang.StringBuffer();
    var rlsat = new java.lang.StringBuffer();
    var sourceMetadataString = metaDataSelection1.replace("[","");
    sourceMetadataString = sourceMetadataString.substring(0, sourceMetadataString.length-1);
    var obj = JSON.parse(sourceMetadataString);
    for (var i=0; i<obj.data.length;i++) {
        var pkCount=0;
        for (var j=0;j<obj.data[i].data.length;j++) {
            //ms.append(obj.data[i].data[j].tableName+"\n");
            SystemManagerInfo(obj.systemName,obj.data[i].environmentName,obj.data[i].data[j].tableName.split(".")[0],obj.data[i].data[j].tableName.split(".")[1],hub,lnk,sat,fkt,stg,rhub,rlink,rsat,rlsat);
        }
    }
    ms.append(stg);
    ms.append(hub);
    ms.append(lnk);
    ms.append(sat);
    ms.append(rhub);
    ms.append(rlink);
    ms.append(rsat);
    ms.append(rlsat);
    ms.append(fkt);
    //ms.append(debug);
    return ms;
}

function loadEnvironment(systemName, environmentName) {
    return smUtil.loadEnvironment(smUtil.getSystemId(systemName), environmentName, false);
}

function SystemManagerInfo(system,environment,schema,table,hub,lnk,sat,fkt,stg,rhub,rlink,rsat,rlsat) {
    var smi = new java.lang.StringBuffer();
    var dbEnvironment = loadEnvironment(system, environment);
    var schemaArr = dbEnvironment.getSchemas().values().toArray();

    for (var h=0;h<schemaArr.length;h++) {
        var dbSchema = schemaArr[h];
        if (dbSchema.getSchemaName().equals(schema)) {
            var tableMap = dbSchema.getTableMap();
            var tabArr = tableMap.values().toArray();
            for (var k=0; k<tabArr.length; k++) {
                var dbTable = tabArr[k];
                var tableName = dbTable.getTableName().replace(dbSchema.getSchemaName()+".","");
                var tableClass = dbTable.getTableClass();
                var j=0;
                var colArr = dbTable.getColumnMap().values().toArray();
                insertOrderSort(colArr);
                if (table.equals(tableName)) {
                    var satAttribute = false;
                    var bkeyFlag = false;
                    var rbkeyFlag = false;
                    var splitSatellite = false;
                    //Column Properties
                    /*
                    for (var l=0; l<colArr.length; l++) {
                        var column = colArr[l];
                        var columnName = column.getColumnName().replaceAll(" ","_");
                        var fKeyColumn = column.getForeignKeyColumnName();
                        var fKeyTable = column.getForeignKeyTableName();
                        var pkey = column.isPrimaryKeyFlag();
                        var bkey = column.getBusinessKeyFlag();
                        var columnClass = column.getColumnClass();
                        var referenceTable = false;
                        if (bkey=="Y" || columnClass == "BKEY") {
                            bkeyFlag=true;
                        } 
                        if (bkey=="Y" || columnClass == "BKEY" || pkey==true) {
                            rbkeyFlag=true;
                        } 
                        if (fKeyColumn!=""&&fKeyColumn!=null&&tableClassType(system,environment,fKeyTable)!="REF"&&columnClass!="RKEY") {
                            fKeyCount++;
                        }
                        if (fKeyColumn!=""&&fKeyColumn!=null&&(tableClassType(system,environment,fKeyTable)=="REF"||columnClass=="RKEY")) {
                            rKeyCount++;
                        }
                        if (pkey==false && (bkey!="Y" || columnClass != "BKEY") && (fKeyColumn=="" || fKeyColumn==null)) {
                           satAttribute=true;
                        }
                        if (columnClass!=null) {
                            if (columnClass.contains("SPL_")) {
                                splitSatellite = true;
                            }
                        }
                        //debug.append("Table="+table+", pkey="+pkey+", bkey="+bkeyFlag+", fkey="+fKeyColumn+", TableClassType="+tableClassType(system,environment,fKeyTable)+", SatAttribute="+satAttribute+"\n");
                        //if (fKeyColumn!=""&&fKeyColumn!=null) {
                        //    debug.append("Table="+table+",FTable="+column.getForeignKeyTableName()+"="+tableClassType(system,environment,column.getForeignKeyTableName())+"\n");
                        //}
                        */
                    if (tableClass=="STG") {
                        createDDL(system,environment,schema,table,dbTable,stg,fkt,tableClass);
                    }
                    else if (tableClass=="HUB" || tableClass=="RHUB") {
                        createDDL(system,environment,schema,table,dbTable,hub,fkt,tableClass);
                    } 
                    else if (tableClass=="LINK" || tableClass=="RLNK") {
                        createDDL(system,environment,schema,table,dbTable,lnk,fkt,tableClass);
                    }
                    else if (tableClass=="SAT" || tableClass=="RSAT" || tableClass=="LSAT" || tableClass=="LRSAT" || tableClass=="MAS") {
                        createDDL(system,environment,schema,table,dbTable,sat,fkt,tableClass);
                    }
                }
            }
        }
    }
}

function insertOrderSort(colArr) {
    for (var i=0;i<colArr.length;i++) {
        for (var j=0; j<colArr.length;j++) {
            if (j<colArr.length-1) { 
                if (colArr[j].getInsertOrder()>colArr[j+1].getInsertOrder()) {
                    var tmp = colArr[j]; 
                    colArr[j] = colArr[j+1];
                    colArr[j+1] = tmp;
                }
            }
        }
    }
    return;
}

function dvDataType(column,mType) {
    // Defaults to character data type nvarchar
    var dataType = "";
    if (sqlType=="SqlServer") {
        if ((column.getDatatype().toLowerCase().contains("char") || column.getDatatype().toLowerCase()=="binary") && column.getDatatype().toLowerCase()!="varchar(max)" && column.getDatatype().toLowerCase()!="varbinary(max)") {
            dataType = column.getDatatype()+"("+(column.getLength()==""?"50":column.getLength())+")";
        }
        else {
            dataType = column.getDatatype();
        }
    }
    else if (sqlType=="Oracle") {
        if (column.getDatatype().equals("RAW")) {
            dataType = column.getDatatype()+"("+(column.getLength()==""?"16":column.getLength())+")";
        } else if (column.getDatatype().toLowerCase().contains("char") || column.getDatatype().equals("Name") || column.getDatatype().equals("AccountNumber") || column.getDatatype().equals("OrderNumber")) {
            if (mType=="meta") {
                dataType = column.getDatatype();
            } else {
                dataType = column.getDatatype()+"("+(column.getLength()==""?"50":column.getLength())+" CHAR)";
            }
        } else if (column.getDatatype().equals("Flag") || column.getDatatype().equals("NameStyle")) {
            dataType = "bit";
        } else if (column.getDatatype().equals("Phone")) {
            if (mType=="meta") {
                dataType = "VARCHAR2";
            } else {
                dataType = "VARCHAR2(50)";
            }
        } else if (column.getDatatype().equals("BYTE")) {
            if (mType=="meta") {
                dataType = "varbinary";
            } else {
                dataType = "varbinary";
            }
        } else {
            dataType = column.getDatatype();
        }
    }
    else if (sqlType=="Snowflake") {
        if (column.getDatatype().equals("RAW")) {
            dataType = column.getDatatype()+"("+(column.getLength()==""?"16":column.getLength())+")";
        } else if (column.getDatatype().toLowerCase().contains("char") || column.getDatatype().equals("Name") || column.getDatatype().equals("AccountNumber") || column.getDatatype().equals("OrderNumber")) {
            if (mType=="meta") {
                dataType = column.getDatatype();
            } else {
                dataType = column.getDatatype()+"("+(column.getLength()==""?"50":column.getLength())+")";
            }
        } else if (column.getDatatype().equals("Flag") || column.getDatatype().equals("NameStyle")) {
            dataType = "bit";
        } else if (column.getDatatype().equals("Phone")) {
            if (mType=="meta") {
                dataType = "VARCHAR2";
            } else {
                dataType = "VARCHAR2(50)";
            }
        } else if (column.getDatatype().equals("BYTE")) {
            if (mType=="meta") {
                dataType = "varbinary";
            } else {
                dataType = "varbinary";
            }
        } else {
            dataType = column.getDatatype();
        }
    }
    return dataType;
}

function createDDL(system,environment,schema,table,dbTable,hub,fkt,classType) {
    var colArr = dbTable.getColumnMap().values().toArray();
    insertOrderSort(colArr);
    var bkeyFlag = false;
    // Manually assigned business key check
    for (var l=0; l<colArr.length; l++) {
            var column = colArr[l];
            if (column.getBusinessKeyFlag() == "Y" || column.getColumnClass() == "BKEY") {
                bkeyFlag = true;
            }
    }
    // SQL Types
    if (sqlType=="SqlServer") {
        // DDL Type Sql Server
        //Description Header and ANSI Settings
        hub.append("/****** Object:  Table ");
        hub.append(table.replaceAll(" ","_") + " ******/\n");
        if (liquibase!=false) { hub.append("--changeset${author}:create_table_"+table.replaceAll(" ","_")+"\n"); }
        //Create Table Function For Each Designated Hub table
        hub.append("CREATE TABLE ");
        if (classType=="STG") {
            if (stageSchemaName) { 
                hub.append(stageSchemaName+".");
            }
        } else {
            if (targetSchemaName) { 
            hub.append(targetSchemaName+".");
            }
        }
        hub.append(table.replaceAll(" ","_") + " (\n");
        //Append Fields
        for (var l=0; l<colArr.length; l++) {
            var column = colArr[l];
            var nullAble = "";
            if (column.isNullableFlag()==true) {
                nullAble = "NULL";
            } else {
                nullAble = "NOT NULL";
            }
            hub.append("    "+column.getColumnName() + " "+dvDataType(column)+" "+nullAble+",\n");
        }
        hub.deleteCharAt(hub.length()-2);
        hub.append("    )");
        
        //Create Primary Key Constraint
        if (classType=="STG") {
            //if (dvDataTablespace) { hub.append(" tablespace "+stgDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+stageSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //hub.append("\nCREATE UNIQUE INDEX ");
            //if (schema) { hub.append(schema+"."); }
            //hub.append("IDX_"+table.replaceAll(" ","_")+"\n    ON ");
            //if (schema) { hub.append(schema+"."); }
            //hub.append(table.replaceAll(" ","_"));
            //hub.append("(");
            //for (var l=0; l<colArr.length; l++) {
            //    var column = colArr[l];
            //    if (column.getColumnClass()=="SQN") {
            //        hub.append(column.getColumnName());
            //    }
            //}
            //hub.append(")\n");
            //hub.append("    tablespace "+stgIndexTablespace+";\n\n");
            hub.append("ALTER TABLE ");
            if (stageSchemaName) { hub.append(stageSchemaName+"."); }
            hub.append(table.replaceAll(" ","_"));
            hub.append(" ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+"\n");
            hub.append("    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="SQN") {
                    hub.append(column.getColumnName());
                }
            }
            hub.append(");");
            //USING INDEX ");
            //if (stageSchemaName) { hub.append(stageSchemaName+"."); }
            //hub.append("IDX_"+table.replaceAll(" ","_")+";\n\n");
            ////hub.append("    tablespace "+stgIndexTablespace+";\n\n");
            //hub.append("GRANT SELECT ON "+stageSchemaName+"."+table+" TO "+targetSchemaName+";\n");
            //hub.append("GRANT SELECT, INSERT ON "+stageSchemaName+"."+table+" TO APP_ETL_ROLE;\n\n\n");
        } 
        else if (classType=="HUB" || classType=="RHUB") {
            //if (dvDataTablespace) { hub.append(" tablespace "+dvDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+targetSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //hub.append("\nCREATE UNIQUE INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table.replaceAll(" ","")+ "\n");
            //hub.append("    ON ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append(table.replaceAll(" ",""));
            //for (var l=0; l<colArr.length; l++) {
            //    var column = colArr[l];
            //    if (classType=="HUB") {
            //        if (column.getColumnClass()=="HKEY") {
            //            hub.append("("+column.getColumnName()+")\n");
            //        }
            //    }
            //    else if (classType=="RHUB") {
            //        if (column.getColumnClass()=="BKEY") {
            //            hub.append("("+column.getColumnName()+")\n");
            //        }
            //    }
            //}
            //hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_") + " ADD CONSTRAINT PK_"+table+"\n    PRIMARY KEY");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="HUB") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append("("+column.getColumnName()+")");
                    }
                }
                else if (classType=="RHUB") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append("("+column.getColumnName()+")");
                    }
                }
            }
            //hub.append(" USING INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table + ")");
            hub.append(";\n\n");
            //hub.append("GRANT SELECT ON "+schema+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            //hub.append("GRANT SELECT, INSERT ON "+schema+"."+table+" TO APP_ETL_ROLE;\n\n\n");
        } 
        else if (classType=="LINK" || classType=="RLNK") {
            if (dvDataTablespace) { hub.append("\n    --tablespace "+dvDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+targetSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //Create Primary Key Constraint
            //hub.append("\nCREATE UNIQUE INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table.replaceAll(" ","_")+ "\n");
            //hub.append("    ON ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append(table.replaceAll(" ","_"));
            //if (classType=="LINK") {
            //    for (var l=0; l<colArr.length; l++) {
            //        var column = colArr[l];
            //        if (column.getColumnClass()=="LHKEY") {
            //            hub.append("("+column.getColumnName()+")\n");
            //        }
            //    }
            //}
            //else if (classType=="RLNK") {
            //    hub.append("(");
            //    for (var l=0; l<colArr.length; l++) {
            //        var column = colArr[l];
            //        if (column.getColumnClass()=="BKEY") {
            //            hub.append(column.getColumnName()+",");
            //        }
            //    }
            //    hub.deleteCharAt(hub.length()-1);
            //    hub.append(")\n");
            //}
            //hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+" \n    PRIMARY KEY");
            if (classType=="LINK") {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append("("+column.getColumnName()+")");
                    }
                }
            }
            else if (classType=="RLNK") {
                hub.append("(");
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                     if (column.getColumnClass()=="BKEY") {
                         hub.append(column.getColumnName()+",");
                     }
                }
                hub.deleteCharAt(hub.length()-1);
                hub.append(")");
            }
            //hub.append(" USING INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table.replaceAll(" ","_")+ ";\n\n");
            //hub.append("GRANT SELECT ON "+schema+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            //hub.append("GRANT SELECT, INSERT ON "+schema+"."+table+" TO APP_ETL_ROLE;\n\n\n");
            //Create Foreign Key Constraints
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName().replaceAll(" ","_")+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName().replaceAll(" ","_") +"("+column.getForeignKeyColumnName()+") ;\n\n");
                }
            }
        }
        else if (classType=="SAT" || classType=="RSAT" || classType=="LSAT" || classType=="LRSAT" || classType=="MAS") {
            if (dvDataTablespace) { hub.append("\n    --tablespace "+dvDataTablespace); }
            if (dvLobTablespace) {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getDatatype()=="CLOB") {
                        hub.append("\n    LOB ("+column.getColumnName().replaceAll(" ","_")+") STORE AS SECUREFILE (tablespace "+dvLobTablespace+")");
                    }
                }
            }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+targetSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //Create Primary Key Constraints for Hub Hashkeys
            //hub.append("\n\nCREATE UNIQUE INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table.replaceAll(" ","_") + "\n");
            //hub.append("    ON ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append(table.replaceAll(" ","_")+"(");
            //for (var l=0; l<colArr.length; l++) {
            //    var column = colArr[l];
            //    if (classType=="SAT" || classType=="MAS") {
            //        if (column.getColumnClass()=="HKEY") {
            //            hub.append(column.getColumnName()+",");
            //        }
            //    }
            //    else if (classType=="RSAT" || classType=="LRSAT") {
            //        if (column.getColumnClass()=="BKEY") {
            //            hub.append(column.getColumnName()+",");
            //        }
            //    }
            //    else if (classType=="LSAT" ) {
            //        if (column.getColumnClass()=="LHKEY") {
            //            hub.append(column.getColumnName()+",");
            //        }
            //    }
            //}
            //for (var l=0; l<colArr.length; l++) {
            //    var column = colArr[l];
            //    if (column.getColumnClass()=="LDTS") {
            //        hub.append(column.getColumnName());
            //    }
            //}
            //for (var l=0; l<colArr.length; l++) {
            //    var column = colArr[l];
            //    if (column.getColumnClass()=="HDIFF"&&column.isPrimaryKeyFlag()==true) {
            //        hub.append(","+column.getColumnName());
            //    }
            //}
            //hub.append(")");
            ////hub.append("    (satHkPrefix"+table.replaceAll(" ","_")+"satHkSuffix, satLDTS)\n");
            //hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+" \n    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="SAT" || classType=="MAS") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="RSAT" || classType=="LRSAT") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="LSAT" ) {
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="LDTS") {
                    hub.append(column.getColumnName());
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="HDIFF"&&column.isPrimaryKeyFlag()==true) {
                    hub.append(","+column.getColumnName());
                }
            }
            hub.append(")");
            //hub.append("    (satHkPrefix"+table.replaceAll(" ","_")+"satHkSuffix, satLDTS)
            //hub.append(" USING INDEX ");
            //if (targetSchemaName) { 
            //    hub.append(targetSchemaName+".");
            //}
            //hub.append("IDX_"+table.replaceAll(" ","_")+ ";\n\n");
            //hub.append("GRANT SELECT ON "+schema+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            //hub.append("GRANT SELECT, INSERT ON "+schema+"."+table+" TO APP_ETL_ROLE;\n\n\n");
            // Create Foreign Key Constraints for Hub Satellites
            //Create Foreign Key Constraints
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName()+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName()+"("+column.getForeignKeyColumnName()+") ;\n\n");
                }
            }
        }
    } else 
    if (sqlType=="Snowflake") {
        // DDL Type Snowflake
        //Description Header and ANSI Settings
        hub.append("/****** Object:  Table "+targetSchemaName+"." + table.replaceAll(" ","_") + "   *****/\n");
        //Create Table Function For Each Designated Hub table
        hub.append("CREATE TABLE " +targetSchemaName+"."+table.replaceAll(" ","_") + " (\n");
        ////Create Derived Data Vault Columns
        for (var l=0; l<colArr.length; l++) {
            var column = colArr[l];
            var nullAble = "";
            if (column.isNullableFlag()==true) {
                nullAble = "NULL";
            } else {
                nullAble = "NOT NULL";
            }
            hub.append("    "+column.getColumnName() + " "+dvDataType(column)+" "+nullAble+",\n");
        }
        hub.deleteCharAt(hub.length()-2);
        hub.append("    );\n\n");
                //Create Primary Key Constraint
        if (classType=="STG") {
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { hub.append(targetSchemaName+"."); }
            hub.append(table.replaceAll(" ","_"));
            hub.append(" ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+"\n");
            hub.append("    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="SQN") {
                    hub.append(column.getColumnName());
                }
            }
            hub.append(");\n\n");
        } 
        else if (classType=="HUB" || classType=="RHUB") {
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_") + " ADD CONSTRAINT PK_"+table+"\n    PRIMARY KEY");
            if (classType=="RHUB") {
                hub.append("(");
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="HUB") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append("("+column.getColumnName()+")");
                    }
                }
                else if (classType=="RHUB") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
            }
            if (classType=="RHUB") {
                hub.deleteCharAt(hub.length()-1);
                hub.append(")");
            }
            hub.append(";\n\n");
        }
        else if (classType=="LINK" || classType=="RLNK") {
            //Create Primary Key Constraint
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+"\n    PRIMARY KEY");
            if (classType=="LINK") {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append("("+column.getColumnName()+");\n\n");
                    }
                }
            }
            else if (classType=="RLNK") {
                hub.append("(");
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                     if (column.getColumnClass()=="BKEY") {
                         hub.append(column.getColumnName()+",");
                     }
                }
                hub.deleteCharAt(hub.length()-1);
                hub.append(");\n\n");
            }
            //Create Foreign Key Constraints
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName().split(".")[1].replaceAll(" ","_")+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName().split(".")[1].replaceAll(" ","_") +"("+column.getColumnName()+");\n\n");
                }
            }
        }
        else if (classType=="SAT" || classType=="RSAT" || classType=="LSAT" || classType=="LRSAT" || classType=="MAS") {
            //Create Primary Key Constraints for Hub Hashkeys
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+"\n    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="SAT" || classType=="MAS") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="RSAT" || classType=="LRSAT") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="LSAT" ) {
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="LDTS") {
                    hub.append(column.getColumnName()+")");
                }
            }
            hub.append(";\n\n");
            // Create Foreign Key Constraints for Hub Satellites
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName().split(".")[1]+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName().split(".")[1]+"("+column.getColumnName()+");\n\n");
                }
            }
        }
    } else 
    if (sqlType=="Oracle") {
        // DDL Type Snowflake
        //Description Header and ANSI Settings
        hub.append("/****** Object:  Table ");
        hub.append(table.replaceAll(" ","_") + " ******/\n");
        if (liquibase!=false) { hub.append("--changeset${author}:create_table_"+table.replaceAll(" ","_")+"\n"); }
        //Create Table Function For Each Designated Hub table
        hub.append("CREATE TABLE ");
        if (targetSchemaName) { 
            hub.append(targetSchemaName+".");
        }
        hub.append(table.replaceAll(" ","_") + " (\n");
        //Append Fields
        for (var l=0; l<colArr.length; l++) {
            var column = colArr[l];
            var nullAble = "";
            if (column.isNullableFlag()==true) {
                nullAble = "NULL";
            } else {
                nullAble = "NOT NULL";
            }
            hub.append("    "+column.getColumnName() + " "+dvDataType(column)+" "+nullAble+",\n");
        }
        hub.deleteCharAt(hub.length()-2);
        hub.append("    )");
        
        //Create Primary Key Constraint
        if (classType=="STG") {
            if (dvDataTablespace) { hub.append(" tablespace "+stgDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+targetSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            hub.append("\nCREATE UNIQUE INDEX ");
            if (targetSchemaName) { hub.append(targetSchemaName+"."); }
            hub.append("IDX_"+table.replaceAll(" ","_")+"\n    ON ");
            if (targetSchemaName) { hub.append(targetSchemaName+"."); }
            hub.append(table.replaceAll(" ","_"));
            hub.append("(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="SQN") {
                    hub.append(column.getColumnName());
                }
            }
            hub.append(")\n");
            hub.append("    tablespace "+stgIndexTablespace+";\n\n");
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { hub.append(targetSchemaName+"."); }
            hub.append(table.replaceAll(" ","_"));
            hub.append(" ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+"\n");
            hub.append("    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="SQN") {
                    hub.append(column.getColumnName());
                }
            }
            hub.append(") USING INDEX ");
            if (targetSchemaName) { hub.append(targetSchemaName+"."); }
            hub.append("IDX_"+table.replaceAll(" ","_")+";\n\n");
            //hub.append("    tablespace "+stgIndexTablespace+";\n\n");
            hub.append("GRANT SELECT ON "+targetSchemaName+"."+table+" TO "+targetSchemaName+";\n");
            hub.append("GRANT SELECT, INSERT ON "+targetSchemaName+"."+table+" TO APP_ETL_ROLE;\n\n\n");
        } 
        else if (classType=="HUB" || classType=="RHUB") {
            if (dvDataTablespace) { hub.append(" tablespace "+dvDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+schema+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            hub.append("\nCREATE UNIQUE INDEX ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table.replaceAll(" ","")+ "\n");
            hub.append("    ON ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ",""));
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="HUB") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append("("+column.getColumnName()+")\n");
                    }
                }
                else if (classType=="RHUB") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append("("+column.getColumnName()+")\n");
                    }
                }
            }
            hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_") + " ADD CONSTRAINT PK_"+table+"\n    PRIMARY KEY");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="HUB") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append("("+column.getColumnName()+") USING INDEX ");
                    }
                }
                else if (classType=="RHUB") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append("("+column.getColumnName()+") USING INDEX ");
                    }
                }
            }
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table + ";\n\n");
            hub.append("GRANT SELECT ON "+targetSchemaName+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            hub.append("GRANT SELECT, INSERT ON "+targetSchemaName+"."+table+" TO APP_ETL_ROLE;\n\n\n");
        } 
        else if (classType=="LINK" || classType=="RLNK") {
            if (dvDataTablespace) { hub.append(" tablespace "+dvDataTablespace); }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+targetSchemaName+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //Create Primary Key Constraint
            hub.append("\nCREATE UNIQUE INDEX ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table.replaceAll(" ","_")+ "\n");
            hub.append("    ON ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_"));
            if (classType=="LINK") {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append("("+column.getColumnName()+")\n");
                    }
                }
            }
            else if (classType=="RLNK") {
                hub.append("(");
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                hub.deleteCharAt(hub.length()-1);
                hub.append(")\n");
            }
            hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+" \n    PRIMARY KEY");
            if (classType=="LINK") {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append("("+column.getColumnName()+")");
                    }
                }
            }
            else if (classType=="RLNK") {
                hub.append("(");
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                     if (column.getColumnClass()=="BKEY") {
                         hub.append(column.getColumnName()+",");
                     }
                }
                hub.deleteCharAt(hub.length()-1);
                hub.append(")");
            }
            hub.append(" USING INDEX ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table.replaceAll(" ","_")+ ";\n\n");
            hub.append("GRANT SELECT ON "+schema+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            hub.append("GRANT SELECT, INSERT ON "+schema+"."+table+" TO APP_ETL_ROLE;\n\n\n");
            //Create Foreign Key Constraints
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName().replaceAll(" ","_")+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName().replaceAll(" ","_") +"("+column.getForeignKeyColumnName()+") DISABLE;\n\n");
                }
            }
        }
        else if (classType=="SAT" || classType=="RSAT" || classType=="LSAT" || classType=="LRSAT" || classType=="MAS") {
            if (dvDataTablespace) { hub.append(" tablespace "+dvDataTablespace); }
            if (dvLobTablespace) {
                for (var l=0; l<colArr.length; l++) {
                    var column = colArr[l];
                    if (column.getDatatype()=="CLOB") {
                        hub.append("\n    LOB ("+column.getColumnName().replaceAll(" ","_")+") STORE AS SECUREFILE (tablespace "+dvLobTablespace+")");
                    }
                }
            }
            hub.append(";\n");
            if (liquibase!=false) { hub.append("    --rollback drop table "+schema+"."+table.replaceAll(" ","")+" cascade constraints\n"); }
            //Create Primary Key Constraints for Hub Hashkeys
            hub.append("\n\nCREATE UNIQUE INDEX ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table.replaceAll(" ","_") + "\n");
            hub.append("    ON ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+"(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="SAT" || classType=="MAS") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="RSAT" || classType=="LRSAT") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="LSAT" ) {
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="LDTS") {
                    hub.append(column.getColumnName());
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="HDIFF"&&column.isPrimaryKeyFlag()==true) {
                    hub.append(","+column.getColumnName());
                }
            }
            hub.append(")");
            //hub.append("    (satHkPrefix"+table.replaceAll(" ","_")+"satHkSuffix, satLDTS)\n");
            hub.append("    tablespace "+dvIndexTablespace+";\n\n");
            
            hub.append("ALTER TABLE ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append(table.replaceAll(" ","_")+ " ADD CONSTRAINT PK_"+table.replaceAll(" ","_")+" \n    PRIMARY KEY(");
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (classType=="SAT" || classType=="MAS") {
                    if (column.getColumnClass()=="HKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="RSAT" || classType=="LRSAT") {
                    if (column.getColumnClass()=="BKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
                else if (classType=="LSAT" ) {
                    if (column.getColumnClass()=="LHKEY") {
                        hub.append(column.getColumnName()+",");
                    }
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="LDTS") {
                    hub.append(column.getColumnName());
                }
            }
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getColumnClass()=="HDIFF"&&column.isPrimaryKeyFlag()==true) {
                    hub.append(","+column.getColumnName());
                }
            }
            hub.append(")");
            //hub.append("    (satHkPrefix"+table.replaceAll(" ","_")+"satHkSuffix, satLDTS)
            hub.append(" USING INDEX ");
            if (targetSchemaName) { 
                hub.append(targetSchemaName+".");
            }
            hub.append("IDX_"+table.replaceAll(" ","_")+ ";\n\n");
            hub.append("GRANT SELECT ON "+schema+"."+table+" TO "+reportSchema+" WITH GRANT OPTION;\n");
            hub.append("GRANT SELECT, INSERT ON "+schema+"."+table+" TO APP_ETL_ROLE;\n\n\n");
            // Create Foreign Key Constraints for Hub Satellites
            //Create Foreign Key Constraints
            for (var l=0; l<colArr.length; l++) {
                var column = colArr[l];
                if (column.getForeignKeyTableName()!=null&&column.getForeignKeyTableName()!="") {
                    fkt.append("ALTER TABLE ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(table.replaceAll(" ","_")+" ADD CONSTRAINT FK_"+table.replaceAll(" ","_")+"_"+column.getForeignKeyTableName()+"\n");
                    fkt.append("    FOREIGN KEY ("+column.getColumnName()+") REFERENCES ");
                    if (targetSchemaName) { 
                        fkt.append(targetSchemaName+".");
                    }
                    fkt.append(column.getForeignKeyTableName()+"("+column.getForeignKeyColumnName()+") DISABLE;\n\n");
                }
            }
        }
    } else 
    if (sqlType=="Teradata") {
        // DDL Type Snowflake
        //Description Header and ANSI Settings
        hub.append("/****** Object:  Table "+targetSchemaName+"." + hubTablePrefix+table.replaceAll(" ","_")+hubTableSuffix + "    Script Date: "+now+" ******/\n");
        //Create Table Function For Each Designated Hub table
        hub.append("CREATE MULTISET TABLE " + hubTablePrefix+table.replaceAll(" ","_")+hubTableSuffix + ",\n");
        hub.append("NO FALLBACK ,\n");
        hub.append("NO BEFORE JOURNAL,\n");
        hub.append("NO AFTER JOURNAL,\n");
        hub.append("CHECKSUM = DEFAULT,\n");
        hub.append("DEFAULT MERGEBLOCKRATIO\n");
        hub.append("(\n");
        //Create Derived Data Vault Columns
        if (hubHKEYType!="None") {
            hub.append("    "+hubHkPrefix+table.replaceAll(" ","_")+hubHkSuffix+" "+hubHkDataType+"("+hubHkLength+") NOT NULL,\n");
        }
        hub.append("    "+hubLDTS+" "+hubLDTSDataType+" NOT NULL,\n");
        hub.append("    "+hubRSRC+" "+hubRSRCDataType+"("+hubRSRCLength+") CHARACTER SET LATIN NOT NULL,\n");
        //MLTID
        if (hubMLTID) {
            hub.append("    "+hubMLTID+" "+hubMLTIDDataType+"("+hubMLTIDLength+") NOT NULL,\n");
        }
        //Append Hub Business Keys
        for (var l=0; l<colArr.length; l++) {
            var column = colArr[l];
            if ((bkeyFlag == true) && (column.getBusinessKeyFlag() == "Y" || column.getColumnClass() == "BKEY")) {
                hub.append("    " + column.getColumnName().replaceAll(" ","_") + " " + dvDataType(column) + " NOT NULL,\n");
            } else if (bkeyFlag == false && column.isPrimaryKeyFlag()===true) {
                hub.append("    " + column.getColumnName().replaceAll(" ","_") + " " + dvDataType(column) + " NOT NULL,\n");
            }
        }
        //BKCC
        if (hubBKCC) {
            hub.append("    "+hubBKCC+" "+hubBKCCDataType+"("+hubBKCCLength+") NOT NULL,\n");
        }
        //BWSC
        if (hubBWSC) {
            hub.append("    "+hubBWSC+" "+hubBWSCDataType+" NOT NULL,\n");
        }
        //hub.deleteCharAt(hub.length()-2);
        hub.append("    CONSTRAINT " + hubTablePrefix+table.replaceAll(" ","_")+hubTableSuffix + "_PK PRIMARY KEY ( "+hubHkPrefix+table.replaceAll(" ","_")+hubHkSuffix+" )\n");
        hub.append(")\n");
        hub.append("UNIQUE PRIMARY INDEX ( "+hubHkPrefix+table.replaceAll(" ","_")+hubHkSuffix+" );\n\n");
    }
}
