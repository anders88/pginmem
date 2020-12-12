package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class CreateTableTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun createWithDifferentTypes() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("""
                create table mytable(
                    id  text,
                    num integer,
                    success boolean,
                    description text)
                """.trimIndent())
            }
            conn.prepareStatement("""insert into "public"."mytable"("id","num","success",description) values (?,?,?,?)""").use {
                it.setString(1,"myid")
                it.setInt(2,42)
                it.setBoolean(3,true)
                it.setString(4,null)
                it.executeUpdate()
            }
            connection.prepareStatement("select * from mytable").executeQuery().use { resultSet ->
                assertThat(resultSet.next()).isTrue()
                assertThat(resultSet.getInt("num")).isEqualTo(42)
                assertThat(resultSet.getBoolean("success")).isTrue()
                assertThat(resultSet.getString("description")).isNull()
                assertThat(resultSet.next()).isFalse()
            }
        }
    }
}