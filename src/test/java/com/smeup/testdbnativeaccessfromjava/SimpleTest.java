/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.smeup.testdbnativeaccessfromjava;

import com.smeup.dbnative.BigintType;
import com.smeup.dbnative.DBField;
import com.smeup.dbnative.DBMetadata;
import com.smeup.dbnative.SmallintType;
import com.smeup.dbnative.VarcharType;
import com.smeup.dbnative.file.DBFile;
import com.smeup.dbnative.file.Record;
import com.smeup.dbnative.file.RecordField;
import com.smeup.dbnative.file.Result;
import com.smeup.dbnative.sql.SQLDBMManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author marco.lanari
 */
public class SimpleTest {
    
    private static final String LIB = "LIBRARY_TEST";
    private static final String FILE = "FILE_TEST";
    private static DBMetadata DB_METADATA;
    
    @BeforeClass
    public static void createDbMatadata() {
        List<DBField> fields = new ArrayList<>();
        //definisco ID primary key
        fields.add(new DBField("ID", BigintType.INSTANCE, true, true, true));
        //definisco campo NAME che deve essere not null
        fields.add(new DBField("NAME", new VarcharType(50), false, false, true));
        //definisco campo RATING che puà essere null
        fields.add(new DBField("RATING", SmallintType.INSTANCE, false, false, false));
        DB_METADATA = new DBMetadata(FILE, fields);
    }
    
    @Test
    public void run() {
        SQLDBMManager dbmm = new SQLDBMManager();
        String url = "jdbc:hsqldb:mem:";
        dbmm.setConnectionData(url, "sa", "sa");
        if (!dbmm.existLib(LIB)) {
            dbmm.createLib(LIB);
        }
        if (!dbmm.existFile(LIB, FILE)) {
            dbmm.createFile(LIB, DB_METADATA);
        }
        DBFile dbFile = dbmm.openFile(LIB, FILE);
        
        //Creo il primo record
        Record r1 = new Record(
                new RecordField("ID", "1"), 
                new RecordField("NAME", "PAPERONE"), 
                new RecordField("RATING", "0")
        );
        //lo scrivo
        Result w1 = dbFile.write(r1);
        
        //Creo il secondo record
        Record r2 = new Record(
                new RecordField("ID", "2"), 
                new RecordField("NAME", "PIPPO"), 
                new RecordField("RATING", "5")
        );
        //lo scrivo
        Result w2 = dbFile.write(r2);
        
        Assert.assertTrue("Mi posiziono su record con ID=2", dbFile.setll(Arrays.asList(new RecordField("ID", "2"))));
        Assert.assertEquals("Leggo il record corrente", w2, dbFile.read());
        Assert.assertEquals("Leggo il precedente", w1, dbFile.readPrevious());
        dbmm.closeFile(LIB, FILE);
    }
    
}
