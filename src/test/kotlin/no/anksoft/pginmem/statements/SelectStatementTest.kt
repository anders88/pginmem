package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Test
import java.math.BigDecimal
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
                    assertThat(it.getString("Description")).isEqualTo("two")
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
            conn.prepareStatement("select id from mytable where description is distinct from 'something'").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("one")
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

    @Test
    fun selectWithAnd() {
        connection.use { conn ->
            conn.createStatement().execute(
                """
                create table mytable(
                    id text,
                    firstname text,
                    lastname text)
            """.trimIndent()
            )

            val insertAction:(Triple<String,String,String>)->Unit = {
                val (id,firstname,lastname) = it
                conn.prepareStatement("insert into mytable(id,firstname,lastname) values (?,?,?)").use {
                    it.setString(1,id)
                    it.setString(2,firstname)
                    it.setString(3,lastname)
                    it.executeUpdate()
                }
            }
            insertAction.invoke(Triple("luke","Luke","Skywalker"))
            insertAction.invoke(Triple("leia","Leia","Skywalker"))
            insertAction.invoke(Triple("han","Han","Solo"))

            conn.prepareStatement("select id from mytable where firstname = ? and lastname = ?").use {
                it.setString(1,"Luke")
                it.setString(2,"Skywalker")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("luke")
                    assertThat(it.next()).isFalse()
                }
            }
            conn.prepareStatement("select id from mytable where firstname = ? and firstname = ? and lastname = ?").use {
                it.setString(1,"Leia")
                it.setString(2,"Leia")
                it.setString(3,"Skywalker")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("leia")
                    assertThat(it.next()).isFalse()
                }
            }
            conn.prepareStatement("select firstname from mytable where lastname = 'Skywalker' order by firstname").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("firstname")).isEqualTo("Leia")
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("firstname")).isEqualTo("Luke")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun selectWithRenameWithAs() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")

            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"mykey")
                it.executeUpdate()
            }
            conn.prepareStatement("select id, 'hello' as greeting from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("greeting")).isEqualTo("hello")
                    assertThat(it.next()).isFalse()
                }
            }
        }

    }

    @Test
    fun aggregateFunctionsTest() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create table mytable(               
                    description text,
                    numvalue int
                )
            """.trimIndent())
            val valuesToInsert:List<Pair<String,Int>> = listOf(
                Pair("a",1),
                Pair("a",2),
                Pair("b",3),
            )
            valuesToInsert.forEach { valToInsert ->
                connection.prepareStatement("insert into mytable(description,numvalue) values (?,?)").use {
                    it.setString(1,valToInsert.first)
                    it.setInt(2,valToInsert.second)
                    it.executeUpdate()
                }
            }

            conn.prepareStatement("select description, max(numvalue) from mytable group by description").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("a")
                    assertThat(it.getInt(2)).isEqualTo(2)
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("b")
                    assertThat(it.getInt(2)).isEqualTo(3)
                    assertThat(it.next()).isFalse()

                }
            }


            conn.prepareStatement("select max(numvalue) as mymax, min(numvalue) as mymin from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(3)
                    assertThat(it.getInt("mymax")).isEqualTo(3)
                    assertThat(it.getInt(2)).isEqualTo(1)
                    assertThat(it.next()).isFalse()

                }
            }


        }

    }

    @Test
    fun testWitLowerAndOr() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text,description text)")

            conn.prepareStatement("insert into mytable(id,description) values (?,?)").use {
                it.setString(1,"myid")
                it.setString(2,"I have a Dog")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable where id = ? and (description = ? or lower(description) = ?)").use {
                it.setString(1,"myid")
                it.setString(2,"i have a dog")
                it.setString(3,"i have a dog")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                }
            }
            conn.prepareStatement("select * from mytable where  description = ? or lower(description) = ?").use {
                it.setString(1,"i have a dog")
                it.setString(2,"i have a dog")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                }
            }
            conn.prepareStatement("select * from mytable where lower(description) = ?").use {
                it.setString(1,"i have a dog")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                }
            }
        }
    }

    @Test
    fun selectWithLimit() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"last")
                it.executeUpdate()
            }
            conn.prepareStatement("insert into mytable(id) values (?)").use {
                it.setString(1,"first")
                it.executeUpdate()
            }
            conn.prepareStatement("select * from mytable order by id fetch first 1 rows only").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("first")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun aggregateWithSumAndCount() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text, price numeric)")
            listOf(Pair("a",BigDecimal.TEN), Pair("b", BigDecimal.valueOf(100L)),
                Pair("c", BigDecimal.ONE)
            ).forEach { pair ->
                conn.prepareStatement("insert into mytable(id,price) values (?,?)").use {
                    it.setString(1,pair.first)
                    it.setBigDecimal(2,pair.second)
                    it.executeUpdate()
                }
            }
            conn.prepareStatement("select sum(price),count(*) from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getBigDecimal(1)).isCloseTo(BigDecimal.valueOf(111L), Offset.offset(BigDecimal.valueOf(0.0001)))
                    assertThat(it.getInt(2)).isEqualTo(3)
                }

            }
        }
    }

    @Test
    fun selectDistinct() {
        connection.use { conn ->
            conn.createStatement().execute("""
                create table mytable(id text,description text)
            """.trimIndent())
            listOf(Pair("a","desca"),Pair("a","descb"),Pair("b","descb"),Pair("b","descb")).forEach { pair ->
                conn.prepareStatement("insert into mytable(id,description) values (?,?)").use {
                    it.setString(1,pair.first)
                    it.setString(2,pair.second)
                    it.executeUpdate()
                }
            }
            val res:MutableList<String> = mutableListOf()
            conn.prepareStatement("select distinct id from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    res.add(it.getString("id"))
                    assertThat(it.next()).isTrue()
                    res.add(it.getString("id"))
                    assertThat(it.next()).isFalse()
                }
            }
            assertThat(res).containsAll(listOf("a","b"))
        }
    }

}
