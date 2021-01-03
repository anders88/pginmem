package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Timestamp
import java.time.LocalDateTime

class SelectStatementTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun selectWithLessThan() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create table mytable(
                    id  integer,
                    description text)
            """.trimIndent())
            val insertSql = "insert into mytable(id,description) values (?,?)"
            conn.prepareStatement(insertSql).use {
                it.setInt(1,1)
                it.setString(2,"one")
                it.executeUpdate()
            }
            conn.prepareStatement(insertSql).use {
                it.setInt(1,2)
                it.setString(2,"two")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable where id > ?").use { statement ->
                statement.setInt(1,1)
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("description")).isEqualTo("two")
                    assertThat(it.next()).isFalse()
                }
            }

        }
    }

    @Test
    fun selectWithColumns() {
        connection.use { conn ->
            conn.createStatement().execute(
                """
                    create table mytable(
                        id  integer,
                        description text,
                        dummy text)
                """.trimIndent()
            )
            val insertSql = "insert into mytable(id,description,dummy) values (?,?,?)"
            conn.prepareStatement(insertSql).use {
                it.setInt(1, 1)
                it.setString(2, "one")
                it.setString(3, "dummy")
                it.executeUpdate()
            }
            conn.prepareStatement("""select "description",id from mytable""").use { statement ->
                statement.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("one")
                    assertThat(it.getInt("id")).isEqualTo(1)
                }
            }

        }
    }

    @Test
    fun selectWithIsNull() {
        connection.use { conn ->
            conn.createStatement().execute(
                """
                create table mytable(
                    id  text,
                    description text)
            """.trimIndent()
            )
            val insertSql = "insert into mytable(id,description) values (?,?)"
            conn.prepareStatement(insertSql).use {
                it.setString(1, "one")
                it.setString(2, null)
                it.executeUpdate()
            }
            conn.prepareStatement(insertSql).use {
                it.setString(1, "two")
                it.setString(2, "something")
                it.executeUpdate()
            }
            conn.prepareStatement("select id from mytable where description is null").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("one")
                    assertThat(it.next()).isFalse()
                }
            }
            conn.prepareStatement("select id from mytable where description is not null").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("two")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun selectWithTwoTables() {
        connection.use { conn ->
            conn.createStatement().execute(
                """
                create table customer(
                    id  text,
                    name text)
            """.trimIndent()
            )
            conn.createStatement().execute(
                """
                create table purchase(
                    id  text,
                    customerid text,
                    price integer
                    )
            """.trimIndent()
            )
            conn.prepareStatement("insert into customer(id,name) values ('one','darth')").use { it.executeUpdate() }
            conn.prepareStatement("insert into customer(id,name) values ('two','luke')").use { it.executeUpdate() }
            conn.prepareStatement("insert into purchase(id,customerid,price) values ('p1','one',42)").use { it.executeUpdate() }

            conn.prepareStatement("select c.name, p.price from customer c, purchase p where c.id = p.customerid").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("name")).isEqualTo("darth")
                    assertThat(it.getInt("price")).isEqualTo(42)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun selectWithOrder() {
        connection.use { conn ->
            conn.createStatement().execute(
                """
                create table mytable(
                    id integer,
                    description  text,
                    created timestamp)
            """.trimIndent()
            )
            insertIntoTripleTable(conn, Triple(1,"a", LocalDateTime.of(2020,3,20,10,0,0)))
            insertIntoTripleTable(conn, Triple(2,"a", LocalDateTime.of(2020,2,20,10,0,0)))
            insertIntoTripleTable(conn, Triple(3,"b", LocalDateTime.of(2020,4,20,10,0,0)))
            conn.prepareStatement("select id from mytable order by description, created").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isEqualTo(2)
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isEqualTo(1)
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("id")).isEqualTo(3)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    private fun insertIntoTripleTable(conn:Connection,values:Triple<Int,String?,LocalDateTime?>) {
        conn.prepareStatement("insert into mytable(id,description,created) values (?,?,?)").use {
            it.setInt(1,values.first)
            it.setString(2,values.second)
            it.setTimestamp(3,values.third?.let { Timestamp.valueOf(it) })
            it.executeUpdate()
        }
    }
}