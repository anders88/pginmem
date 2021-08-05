package no.anksoft.pginmem.statements

import no.anksoft.pginmem.PgInMemDatasource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Offset
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Timestamp
import java.time.LocalDate
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

    @Test
    fun selectInWithBindings() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")
            listOf("a","b","c").forEach { value ->
                conn.prepareStatement("insert into mytable(id) values (?)").use {
                    it.setString(1,value)
                    it.executeUpdate()
                }
            }
            conn.prepareStatement("select * from mytable where id in (?,?)").use {
                it.setString(1,"a")
                it.setString(2,"b")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun aggregatesShouldReturnNullIfEmpty() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(mynum numeric)")
            conn.prepareStatement("select sum(mynum) as tablesum, min(mynum) as tablemin, count(*) as counttable from mytable").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getBigDecimal("tablesum")).isNull()
                    assertThat(it.getBigDecimal("tablemin")).isNull()
                    assertThat(it.getInt("counttable")).isEqualTo(0)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun notEqual() {
        connection.use {  conn ->
            conn.createStatement().execute("create table mytable(id text)")
            listOf("a","b").forEach { value ->
                conn.prepareStatement("insert into mytable(id) values (?)").use {
                    it.setString(1,value)
                    it.executeUpdate()
                }
            }
            conn.prepareStatement("select id from mytable where id <> ?").use {
                it.setString(1,"a")
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("id")).isEqualTo("b")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun selectAllFromOneTable() {
        connection.use { conn ->
            conn.createStatement().execute("create table tableone(oneid text)")
            conn.createStatement().execute("create table tabletwo(twoid text, numval integer)")
            conn.createStatement().execute("insert into tableone(oneid) values ('a')")
            conn.createStatement().execute("insert into tableone(oneid) values ('b')")
            conn.createStatement().execute("insert into tabletwo(twoid,numval) values ('c',42)")

            conn.prepareStatement("select distinct b.* from tableone a, tabletwo b").use {
                it.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("twoid")).isEqualTo("c")
                    assertThat(it.getInt("numval")).isEqualTo(42)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun columnValueAsSelect() {
        connection.use { conn ->
            conn.createStatement().execute("create table customer(id int,name text)")
            conn.createStatement().execute("create table custorder(customerid int, amount numeric)")
            val createCustomer:(Pair<Int,String>)->Int = { inp ->
                conn.prepareStatement("insert into customer(id,name) values (?,?)").use {
                    it.setInt(1,inp.first)
                    it.setString(2,inp.second)
                    it.executeUpdate()
                }
            }
            createCustomer.invoke(Pair(1,"Darth"))
            createCustomer.invoke(Pair(2,"Luke"))
            val createOrder:(Pair<Int,BigDecimal>)->Int = { inp ->
                conn.prepareStatement("insert into custorder(customerid,amount) values (?,?)").use {
                    it.setInt(1,inp.first)
                    it.setBigDecimal(2,inp.second)
                    it.executeUpdate()
                }
            }
            createOrder.invoke(Pair(1, BigDecimal(40.0)))
            createOrder.invoke(Pair(1,BigDecimal(2)))
            createOrder.invoke(Pair(2,BigDecimal(80)))
            val all:List<Pair<String,BigDecimal>> = conn.prepareStatement("select c.name, (select sum(o.amount) from custorder o where o.customerid = c.id) from customer c").use { statement ->
                val res:MutableList<Pair<String,BigDecimal>> = mutableListOf()
                statement.executeQuery().use {
                    while (it.next()) {
                        res.add(Pair(it.getString(1),it.getBigDecimal(2)))
                    }
                }
                res
            }
            assertThat(all).hasSize(2)
            assertThat(all.first { it.first == "Luke" }.second).isCloseTo(BigDecimal(80.0), Offset.offset(BigDecimal.valueOf(0.0001)))
            assertThat(all.first { it.first == "Darth" }.second).isCloseTo(BigDecimal(42.0), Offset.offset(BigDecimal.valueOf(0.0001)))
        }
    }

    @Test
    fun whereClauseWithInSelect() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")
            conn.createStatement().execute("create table totable(id text)")

            val insTable:(Pair<String,String>)->Unit = { pairval ->
                conn.prepareStatement("insert into ${pairval.first} (id) values (?)").use {
                    it.setString(1,pairval.second)
                    it.executeUpdate()
                }
            }
            insTable.invoke(Pair("mytable","one"))
            insTable.invoke(Pair("mytable","two"))
            insTable.invoke(Pair("mytable","three"))
            insTable.invoke(Pair("totable","one"))

            conn.prepareStatement("select id from mytable where id in (select id from totable)").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("one")
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun selectWithImplisitOneRow() {
        connection.use { conn ->
            conn.prepareStatement("select 'a' where 1 <> 1").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isFalse()
                }
            }
        }
        connection.use { conn ->
            conn.prepareStatement("select 'a' where 1 = 1").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("a")
                }
            }
        }
    }

    @Test
    fun selectWithExsists() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")
            conn.prepareStatement("select 'a' where not exists (select 1 from mytable)").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                }
            }
        }
    }

    @Test
    fun selectBindingsOutsideWhere() {
        connection.use { conn ->
            conn.prepareStatement("select ?").use { ps ->
                ps.setString(1,"answer")
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("answer")
                }
            }
            conn.createStatement().execute("create table mytable(id text)")
            val insStatement:(String)->Unit = { idval ->
                conn.prepareStatement("insert into mytable(id) values (?)").use {
                    it.setString(1, idval)
                    it.executeUpdate()
                }
            }
            insStatement.invoke("a")
            insStatement.invoke("b")
            conn.prepareStatement("select 'a' where not exists (select 1 from mytable where id = ?)").use { ps ->
                ps.setString(1,"c")
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                }
            }
            conn.prepareStatement("select (select a.id from mytable a where a.id = ?) as compval from mytable b").use { ps ->
                ps.setString(1,"b")
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString("compval")).isEqualTo("b")
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("b")
                    assertThat(it.next()).isFalse()

                }
            }
        }
    }

    @Test
    fun jsonConveringValue() {
        connection.use { conn ->
            conn.prepareStatement("""select null::json->'obje'->>'value'""").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isNull()
                }
            }

            conn.prepareStatement("""select '{"obje": {}}'::json->'obje'->>'value'""").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isNull()
                }
            }

            conn.prepareStatement("""select '{"obje": {"value":"42"}}'::json->'obje'->>'value'""").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getString(1)).isEqualTo("42")
                }

            }
        }
    }

    @Test
    fun anyTest() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(id text)")
            val insStatement:(String)->Unit = { idval ->
                conn.prepareStatement("insert into mytable(id) values (?)").use {
                    it.setString(1, idval)
                    it.executeUpdate()
                }
            }
            insStatement.invoke("a")
            insStatement.invoke("b")
            insStatement.invoke("c")
            insStatement.invoke("d")
            conn.prepareStatement("select * from mytable where id = any(?)").use { ps ->
                val inparray = ps.connection.createArrayOf("text",arrayOf("a","b"));
                ps.setArray(1, inparray)
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isTrue()
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun extractFromTest() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(name text,timeval timestamp)")
            conn.prepareStatement("insert into mytable(name,timeval) values (?,?)").use { ps ->
                ps.setString(1,"Luke")
                ps.setTimestamp(2,Timestamp.valueOf(LocalDateTime.of(2021,7,20,13,15)))
                ps.executeUpdate()
            }
            conn.prepareStatement("select extract(year from timeval) from mytable where name = ?").use { ps ->
                ps.setString(1,"Luke")
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(2021)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }

    @Test
    fun leftOuterJoin() {
        connection.use { conn ->
            conn.createStatement().execute("create table customer(id int,name text)")
            conn.createStatement().execute("create table sale(customerid int, amount numeric)")
            conn.prepareStatement("insert into customer(id,name) values (?,?)").use {
                it.setInt(1,1)
                it.setString(2,"Luke")
                it.executeUpdate()
            }
            val checkRes:(List<Pair<String,BigDecimal?>>)->Unit = { expectations ->
                conn.prepareStatement("select c.name,s.amount from customer c left outer join sale s on c.id = s.customerid").use { ps ->
                    ps.executeQuery().use { rs ->
                        for (expval in expectations) {
                            assertThat(rs.next()).isTrue()
                            assertThat(rs.getString("name")).isEqualTo(expval.first)
                            if (expval.second == null) {
                                assertThat(rs.getBigDecimal("amount")).isNull()
                            } else {
                                assertThat(rs.getBigDecimal("amount")).isCloseTo(expval.second, Offset.offset(BigDecimal.valueOf(0.001)))
                            }
                        }
                        assertThat(rs.next()).isFalse()
                    }
                }
            }
            checkRes.invoke(listOf(Pair("Luke",null)))
        }
    }

    @Test
    fun selectCountDistinct() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(name text)")
            val insertAct:(String)->Unit = { input ->
                conn.prepareStatement("insert into mytable(name) values (?)").use {
                    it.setString(1,input)
                    it.executeUpdate()
                }
            }
            insertAct.invoke("Luke")
            insertAct.invoke("Luke")
            insertAct.invoke("Darth")
            conn.prepareStatement("select count(distinct name) from mytable").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(2)
                }
            }
        }
    }

    @Test
    fun selectWithCase() {
        connection.use { conn ->
            conn.createStatement().execute("create table mytable(myvalue int)")
            val insact:(Int)->Unit = { input ->
                conn.prepareStatement("insert into mytable(myvalue) values (?)").use {
                    it.setInt(1,input)
                    it.executeUpdate()
                }
            }
            insact.invoke(10)
            insact.invoke(40)
            insact.invoke(80)

            conn.prepareStatement("select *, (case when myvalue < 20 then 'small' else 'large' end) as size from mytable").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt("myvalue")).isEqualTo(10)
                    assertThat(it.getString("size")).isEqualTo("small")
                }

            }
        }
    }

    @Test
    fun toNumber() {
        connection.use { conn ->
            conn.prepareStatement("select to_number('42','99999')").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(42)
                }
            }
        }
    }

    @Test
    fun toDate() {
        connection.use { conn ->
            conn.prepareStatement("select to_date('20150101','yyyymmdd')").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getTimestamp(1).toLocalDateTime().toLocalDate()).isEqualTo(LocalDate.of(2015,1,1))
                }
            }
        }
    }

    @Test
    fun orderByComputetCol() {
        connection.use { conn->
            conn.createStatement().execute("create table mytable(myvalue text)")
            val insact:(String)->Unit = { input ->
                conn.prepareStatement("insert into mytable(myvalue) values (?)").use {
                    it.setString(1,input)
                    it.executeUpdate()
                }
            }
            insact.invoke("42")
            insact.invoke("32")
            conn.prepareStatement("select to_number(myvalue,'99999') as compvalue from mytable order by compvalue").use { ps ->
                ps.executeQuery().use {
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(32)
                    assertThat(it.next()).isTrue()
                    assertThat(it.getInt(1)).isEqualTo(42)
                    assertThat(it.next()).isFalse()
                }
            }
        }
    }
}
