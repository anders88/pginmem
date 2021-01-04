package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.sql.SQLException

class AlterTableTest {
    private val datasource = PgInMemDatasource()
    private val connection = datasource.connection

    @Test
    fun addNewColumnToTable() {
        connection.use {conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1, "mykey")
                it.executeUpdate()
            }
            conn.createStatement().use {
                it.execute("""
                    alter table mytable add column description text
                """.trimIndent())
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("description")).isNull()
                }
            }
            conn.prepareStatement("update mytable set description = ? where id = ?").use {
                it.setString(1,"A description")
                it.setString(2,"mykey")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("description")).isEqualTo("A description")
                }
            }
        }
    }

    @Test
    fun dropColumn() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        description text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,description) values (?,?)").use {
                it.setString(1, "mykey")
                it.setString(2,"something")
                it.executeUpdate()
            }
            conn.createStatement().use {
                it.execute("""
                    alter table mytable drop column description
                """.trimIndent())
            }
            conn.prepareStatement("select * from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    try {
                        it.getString("description")
                    } catch (s:SQLException) {}
                }
            }
        }
    }

    @Test
    fun renameColumn() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        description text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,description) values (?,?)").use {
                it.setString(1, "mykey")
                it.setString(2,"something")
                it.executeUpdate()
            }
            conn.createStatement().use {
                it.execute("""
                    alter table mytable rename column description to altdesc
                """.trimIndent())
            }
            conn.prepareStatement("select * from mytable where id = ?").use {
                it.setString(1,"mykey")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("altdesc")).isEqualTo("something")
                }
            }
        }
    }

    @Test
    fun renameTable() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        description text
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,description) values (?,?)").use {
                it.setString(1, "mykey")
                it.setString(2,"something")
                it.executeUpdate()
            }
            conn.createStatement().use {
                it.execute("""
                    alter table mytable rename to yourtable
                """.trimIndent())
            }
            conn.prepareStatement("select * from yourtable where id = ?").use {
                it.setString(1,"mykey")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("description")).isEqualTo("something")
                }
            }
        }
    }

    @Test
    public fun changeColumnTypeAdvanced() {
        connection.use { conn ->
            conn.createStatement().use {
                it.execute(
                    """
                    create table mytable(
                        id  text,
                        abool boolean default false
                        )
                """.trimIndent()
                )
            }
            conn.prepareStatement("insert into mytable(id,abool) values (?,?)").use {
                it.setString(1,"one")
                it.setBoolean(2,false)
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(id,abool) values (?,?)").use {
                it.setString(1,"two")
                it.setBoolean(2,true)
                it.executeUpdate()
            }
            conn.createStatement().use {
                it.execute("""
                    alter table mytable
                      alter column abool drop default,
                      alter column abool type int using abool::integer,
                      alter column abool set default 0
                """.trimIndent())
            }
            conn.prepareStatement("select * from mytable order by id").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("one")
                    assertThat(it.getInt("abool")).isEqualTo(0)
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("two")
                    assertThat(it.getInt("abool")).isEqualTo(1)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }
}