/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.smeup.testdbnativeaccess;

import com.smeup.dbnative.BigintType;
import com.smeup.dbnative.ConnectionConfig;
import com.smeup.dbnative.DBField;
import com.smeup.dbnative.DBNativeAccessConfig;
import com.smeup.dbnative.FileMetadata;
import com.smeup.dbnative.SmallintType;
import com.smeup.dbnative.VarcharType;
import com.smeup.dbnative.file.DBFile;
import com.smeup.dbnative.file.Record;
import com.smeup.dbnative.file.RecordField;
import com.smeup.dbnative.file.Result;
import com.smeup.dbnative.manager.DBFileFactory;
import com.smeup.dbnative.sql.SQLDBMManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author marco.lanari
 */
public class SimpleTest {

    private static final String FILE = "FILE_TEST";
    private SQLDBMManager manager;
    private DBFileFactory fileFactory;

    @Before
    public void setUp() {
        ConnectionConfig config
          = new ConnectionConfig(FILE, "jdbc:hsqldb:mem:TEST", "sa", "sa", new HashMap<>());
        //creo il manager perché devo creare il file
        manager = new SQLDBMManager(config);
        fileFactory = new DBFileFactory(new DBNativeAccessConfig(Arrays.asList(config)), fileName -> {
            return fileName.replaceAll("/", "\\.");
        });
        List<DBField> fields = new ArrayList<>();
        //definisco ID primary key
        fields.add(new DBField("ID", BigintType.INSTANCE, true, true, true));
        //definisco campo NAME che deve essere not null
        fields.add(new DBField("NAME", new VarcharType(50), false, false, true));
        //definisco campo RATING che puà essere null
        fields.add(new DBField("RATING", SmallintType.INSTANCE, false, false, false));
        FileMetadata metadata = new FileMetadata(FILE, fields);

        if (!manager.existFile(FILE)) {
            manager.createFile(metadata);
        }
    }

    @Test
    public void testWrite() {
        //Apro il file
        try (DBFile dbFile = fileFactory.open(FILE)) {
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
        }
    }
    
    @After
    public void tearDown() {
        if (manager != null) {
            manager.close();
        }
        if (fileFactory != null) {
            fileFactory.close();
        }
    }
    

}
