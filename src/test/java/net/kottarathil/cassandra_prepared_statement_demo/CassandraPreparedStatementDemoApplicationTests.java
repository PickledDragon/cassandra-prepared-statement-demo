package net.kottarathil.cassandra_prepared_statement_demo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.utility.MountableFile;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

@SpringBootTest
class CassandraPreparedStatementDemoApplicationTests {

	@Test
	void contextLoads() {
	}


    @Test
    void testDriverBehaviourCompressed() {

		//Test would fail on 3.11.19 (or any 3.x line) and passes on 4.1.8 (or 4.x line)
        final CassandraContainer cassandraContainer = new CassandraContainer("cassandra:3.11.19") 
                .withInitScript("init.cql");

        cassandraContainer.withCopyFileToContainer(
                MountableFile.forClasspathResource("init.cql"),
                "/docker-entrypoint-initdb.d/init.cql"
        );

        cassandraContainer.start();

        final CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(cassandraContainer.getContactPoint())
                .withLocalDatacenter(cassandraContainer.getLocalDatacenter())
                .withKeyspace("test_keyspace")
                .build();

        PreparedStatement ps1 = cqlSession.prepare("SELECT * FROM prepared_statement_test WHERE a = ?");
        ResultSet rows1 = cqlSession.execute(ps1.bind(1));

        Assertions.assertEquals(3, rows1.getColumnDefinitions().size());
        Assertions.assertTrue(ps1.getResultSetDefinitions().contains("b"));

        ResultSet resultSet =  cqlSession.execute("alter table prepared_statement_test drop b");
        Assertions.assertTrue(resultSet.wasApplied());

        rows1 = cqlSession.execute(ps1.bind(1));
        Row row1 = rows1.one();

        Assertions.assertFalse(ps1.getResultSetDefinitions().contains("b"));
        Assertions.assertEquals(2, rows1.getColumnDefinitions().size());

        Assertions.assertEquals(1,row1.getInt("a"));
        Assertions.assertEquals(1,row1.getInt("c"));
    }


}
