package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

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
}