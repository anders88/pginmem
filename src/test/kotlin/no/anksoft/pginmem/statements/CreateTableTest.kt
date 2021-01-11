package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.math.BigDecimal
import java.sql.SQLException

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
            conn.prepareStatement("""insert into "public"."mytable"("id","success","num") values (?,?,?)""").use {
                it.setString(1,"myid")
                it.setBoolean(2,true)
                it.setInt(3,42)
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

    @Test
    fun createWithDefaultvalue() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                create table if not exists mytable(
                    id  text,
                    created timestamp default now(),
                    installed integer
                    )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,installed) values (?,?)").use {
                it.setString(1,"myindex")
                it.setInt(2,42)
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getTimestamp("created")).isNotNull()
                    assertThat(it.getInt("installed")).isEqualTo(42)
                }
            }
        }
    }

    @Test
    fun notNullFlag() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute("""
                    create table mytable(
                        id  text not null,
                        description text)
                """.trimIndent())
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"myid")
                it.executeUpdate()
            }
            try {
                conn.prepareStatement("insert into mytable(description) values (?)").use {
                    it.setString(1,"a text")
                    it.executeUpdate()
                }
                fail("Expected sqlexception not null")
            } catch (s:SQLException) { }
        }
    }

    @Test
    fun differentDefaults() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        currency text default 'NOK'::text,
                        isused boolean default true
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"mykey")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("mykey")
                    assertThat(it.getString("currency")).isEqualTo("NOK")
                    assertThat(it.getBoolean("isused")).isTrue()
                }
            }
        }
    }

    @Test
    fun numericType() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        number numeric
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,number) values (?,?)").use {
                it.setString(1,"mykey")
                it.setDouble(2,3.14)
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("mykey")
                    assertThat(it.getBigDecimal("number")).isCloseTo(BigDecimal.valueOf(3.14), Offset.offset(BigDecimal.valueOf(0.0001)))
                }
            }
        }
    }

    @Test
    fun withByteA() {
        val byteArray:ByteArray = "This is fun ".toByteArray(Charsets.UTF_8)
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        content bytea
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,content) values (?,?)").use {
                it.setString(1,"mykey")
                it.setBytes(2,byteArray)
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("mykey")
                    assertThat(it.getBytes("content")).isEqualTo(byteArray)
                }
            }
        }
    }

    @Test
    fun createDropTable() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")

            conn.createStatement().execute("drop table mytable")

            conn.createStatement().execute("create table mytable(id text)")
        }
    }
}