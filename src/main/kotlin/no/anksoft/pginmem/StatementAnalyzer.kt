package no.anksoft.pginmem

import no.anksoft.pginmem.clauses.IndexToUse
import no.anksoft.pginmem.clauses.WhereClause
import no.anksoft.pginmem.clauses.createWhereClause
import no.anksoft.pginmem.statements.select.*
import no.anksoft.pginmem.values.*
import org.jsonbuddy.JsonObject
import org.jsonbuddy.parse.JsonParser
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*

private fun splitStringToWords(sqlinp:String):List<String> {
    val lines = sqlinp.lines()
    val sql = lines.filter { !it.startsWith("--") }.fold("", { a,b ->
        a+" "+b
    })
    val result:MutableList<String> = mutableListOf()
    // Set to lovercase if not quoted
    val toTrimmed = StringBuilder()
    var inQuote = false
    for (char in sql.trim()) {
        if (char == '\'') {
            inQuote = !inQuote
        }
        toTrimmed.append(if (inQuote) char else char.toLowerCase())
    }
    val trimmed = toTrimmed.toString()
    var index = 0
    var previndex = 0
    var inSpecialCaseSequence:Boolean = false
    inQuote = false
    while (index < trimmed.length) {
        val charAtPos:Char = trimmed[index]
        if (inQuote) {
            index++
            if (charAtPos == '\'') {
                result.add(trimmed.substring(previndex, index))
                previndex = index
                inQuote = false
            }
            continue
        }
        if (charAtPos == '\'') {
            if (index > previndex) {
                result.add(trimmed.substring(previndex, index))
            }
            previndex = index
            index++
            inQuote = true
            continue
        }

        if (charAtPos.isWhitespace()) {
            inSpecialCaseSequence = false
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            index++
            previndex = index
            continue
        }
        if ("(),".indexOf(charAtPos) != -1) {
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            result.add(""+charAtPos)
            index++
            previndex = index
            inSpecialCaseSequence = false
            continue
        }
        if ("=<>:-".indexOf(charAtPos) != -1) {
            if (index > previndex && !inSpecialCaseSequence) {
                result.add(trimmed.substring(previndex,index))
                previndex = index
            }
            inSpecialCaseSequence = true;
            index++
            continue
        }
        if (inSpecialCaseSequence && index > previndex) {
            result.add(trimmed.substring(previndex,index))
            previndex = index
        }
        inSpecialCaseSequence = false
        index++
    }
    if (index > previndex) {
        result.add(trimmed.substring(previndex,index))
    }
    return result
}

interface ValueFromExpression {
    fun genereateValue(dbTransaction: DbTransaction,row: Row?):CellValue
    val column: ColumnInSelect?
    fun registerBinding(index:Int,value: CellValue):Boolean
}



class BindingValueFromExpression(indexToUse: IndexToUse?):ValueFromExpression {
    private val expectedIndex:Int = indexToUse?.takeInd()?:throw SQLException("Cannot use binding here")
    private var cellValue:CellValue? = null



    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        return cellValue?:throw SQLException("Binding not set for binding $expectedIndex")
    }

    override val column: Column? = null

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        if (index == expectedIndex) {
            cellValue = value
            return true
        }
        return false
    }
}

private fun readJsonProperty(input:String?):String {
    if (!(input?.startsWith("'") == true && input.endsWith("'") && input.length >= 3)) {
        throw SQLException("Illegal json property $input")
    }
    return input.substring(1,input.length-1)
}

private class ReadJsonProperty(val inputValue:ValueFromExpression,jsonPropertyText:String?):ValueFromExpression {
    private val jsonProperty:String = readJsonProperty(jsonPropertyText)


    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val startVal:CellValue = inputValue.genereateValue(dbTransaction,row)
        val jsonObject: JsonObject? = when {
            startVal is JsonCellValue -> startVal.myvalue
            startVal is StringCellValue -> JsonObject.parse(startVal.myValue)
            startVal is NullCellValue -> null
            else -> throw SQLException("Expected string as json")
        }
        return (jsonObject?.stringValue(jsonProperty)?.orElse(null))?.let { StringCellValue(it) }?:NullCellValue
    }

    override val column: Column? = null

    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

class FixedValueFromExpression(private val fixedValue:CellValue):ValueFromExpression {
    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue = fixedValue
    override val column: ColumnInSelect? = null
    override fun registerBinding(index: Int, value: CellValue): Boolean = false

}

private class CaseValueFromExpression(statementAnalyzer: StatementAnalyzer, dbTransaction: DbTransaction, indexToUse: IndexToUse, tables:Map<String,TableInSelect>):ValueFromExpression {
    private val conditions:List<Pair<WhereClause,ValueFromExpression>>
    private val elseCond:ValueFromExpression

    init {
        val conds:MutableList<Pair<WhereClause,ValueFromExpression>> = mutableListOf()
        while (statementAnalyzer.word() == "when") {
            val whereClause:WhereClause = createWhereClause(statementAnalyzer,tables,indexToUse,dbTransaction)
            if (statementAnalyzer.word() != "then") {
                throw SQLException("Expected then in case statement")
            }
            statementAnalyzer.addIndex()
            val valueFromExpression = statementAnalyzer.readValueFromExpression(dbTransaction,tables,indexToUse)
            statementAnalyzer.addIndex()
            conds.add(Pair(whereClause,valueFromExpression))
        }
        conditions = conds

        elseCond = if (statementAnalyzer.word() == "else") {
            statementAnalyzer.addIndex()
            val x = statementAnalyzer.readValueFromExpression(dbTransaction,tables,indexToUse)
            statementAnalyzer.addIndex()
            x
        } else {
            FixedValueFromExpression(NullCellValue)
        }
        if (statementAnalyzer.word() != "end") {
            throw SQLException("Expected end in case statement")
        }
        if (statementAnalyzer.word(1) == ")") {
            statementAnalyzer.addIndex(1)
        }
    }

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {

        for (casebranch in conditions) {
            if (casebranch.first.isMatch(row?.cells?: emptyList())) {
                val res = casebranch.second.genereateValue(dbTransaction,row)
                return res
            }
        }
        val elseval = elseCond.genereateValue(dbTransaction,row)
        return elseval
    }



    override val column: ColumnInSelect? = null

    override fun registerBinding(index: Int, value: CellValue): Boolean {
        TODO("Not yet implemented")
    }
}

private class ReadJsonObject(val inputValue:ValueFromExpression,jsonPropertyText:String?):ValueFromExpression {
    private val jsonProperty:String = readJsonProperty(jsonPropertyText)


    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val startVal:CellValue = inputValue.genereateValue(dbTransaction,row)
        val jsonObject:JsonObject? = when {
            startVal is JsonCellValue -> startVal.myvalue
            startVal is StringCellValue -> JsonObject.parse(startVal.myValue)
            startVal is NullCellValue -> null
            else -> throw SQLException("Expected string as json")
        }

        return jsonObject?.let { jo -> (jo.objectValue(jsonProperty).orElse(null))?.let { JsonCellValue(it) }}?:NullCellValue
    }

    override val column: Column? = null

    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

private class ConvertToColtype(val inputValue: ValueFromExpression,val columnType: ColumnType):ValueFromExpression {

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val startvalue:CellValue = inputValue.genereateValue(dbTransaction,row)
        val res = columnType.convertToMe(startvalue)
        return res
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

private class CoalesceValue(val coalvalues:List<ValueFromExpression>):ValueFromExpression {


    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        for (valueFromExp in coalvalues) {
            val valFromEx = valueFromExp.genereateValue(dbTransaction,row)
            if (valFromEx != NullCellValue) {
                return valFromEx
            }
        }
        return NullCellValue
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean {
        for (vfe in coalvalues) {
            if (vfe.registerBinding(index,value)) {
                return true
            }
        }
        return false
    }
}

private fun convertToJavaDateFormat(sqlFormat:String):DateTimeFormatter {
    val resFormat = StringBuilder()
    for (c in sqlFormat) {
        resFormat.append(when (c) {
            'Y' -> 'y'
            'D' -> 'd'
            'm' -> 'M'
            else -> c
        })
    }
    return DateTimeFormatter.ofPattern(resFormat.toString())
}

private class ToDateValue(val startValue:ValueFromExpression,dateformat:String):ValueFromExpression {

    private val dateTimeFormatter:DateTimeFormatter = convertToJavaDateFormat(dateformat)


    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val toConvert:CellValue = startValue.genereateValue(dbTransaction,row)
        return when {
            (toConvert == NullCellValue) -> NullCellValue
            (toConvert is StringCellValue) -> DateTimeCellValue(
                try {
                    LocalDateTime.parse(toConvert.myValue, dateTimeFormatter)
                } catch (e:DateTimeParseException) {
                    LocalDate.parse(toConvert.myValue, dateTimeFormatter).atStartOfDay()
                }
            )
            else -> throw SQLException("Can only convert to_date from string")
        }
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

private class DateTimeToDateValue(val startValue:ValueFromExpression):ValueFromExpression {

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val toConvert:CellValue = startValue.genereateValue(dbTransaction,row)
        return when {
            (toConvert == NullCellValue) -> NullCellValue
            (toConvert is DateCellValue) -> toConvert
            (toConvert is DateTimeCellValue) -> DateCellValue(toConvert.myValue.toLocalDate())
            else -> throw SQLException("Cannot use date function on $toConvert")
        }
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

private class DateTruncValue(val startValue: ValueFromExpression,val trunSpec:String):ValueFromExpression {

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val toConvert:CellValue = startValue.genereateValue(dbTransaction,row)
        if (toConvert == NullCellValue) {
            return NullCellValue
        }
        if (trunSpec != "'month'") {
            throw SQLException("Only supporting month in date trunc")
        }
        val truncedDate:LocalDateTime = toConvert.valueAsTimestamp().myValue.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
        return DateTimeCellValue(truncedDate)
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

private class ToNumberValue(val startValue:ValueFromExpression):ValueFromExpression {

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val toConvert:CellValue = startValue.genereateValue(dbTransaction,row)
        return if (toConvert == NullCellValue) {
            NullCellValue
        } else {
            toConvert.valueAsInteger()
        }

    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

private class LowerExpression(val startValue:ValueFromExpression):ValueFromExpression {


    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val startVal = startValue.genereateValue(dbTransaction,row)
        return if (startVal is StringCellValue) StringCellValue(startVal.myValue.toLowerCase()) else startVal
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)

}

private class CurrentTimeValueFromExpression:ValueFromExpression {
    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue = DateTimeCellValue(LocalDateTime.now())

    override val column: ColumnInSelect? = null

    override fun registerBinding(index: Int, value: CellValue): Boolean = false
}

class SequenceValueFromExpression(private val sequenceName:String):ValueFromExpression {
    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        return dbTransaction.sequence(sequenceName).nextVal()
    }

    override val column: ColumnInSelect? = null
    override fun registerBinding(index: Int, value: CellValue): Boolean = false
}

private class ExtractExpression(val startValue:ValueFromExpression):ValueFromExpression {

    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val startVal = startValue.genereateValue(dbTransaction,row)
        return when {
            (startVal is NullCellValue) -> NullCellValue
            (startVal is DateTimeCellValue) -> IntegerCellValue(startVal.myValue.year.toLong())
            (startVal is DateCellValue) -> IntegerCellValue(startVal.myValue.year.toLong())
            else -> throw SQLException("Illegal value to extract")
        }
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

class ColumnValueFromExpression constructor(override val column: ColumnInSelect):ValueFromExpression {
    override fun genereateValue(dbTransaction: DbTransaction, row: Row?): CellValue {
        val matches = (row?.cells?: emptyList()).filter{ it.column.name == column.name}
        if (matches.isEmpty()) {
            return NullCellValue
        }
        if (matches.size == 1) {
            return matches[0].value
        }
        val res:CellValue? = row?.cells?.firstOrNull { it.column.matches(column.tablename,column.name?:"") }?.value
        return res?:NullCellValue
    }


    override fun registerBinding(index: Int, value: CellValue): Boolean = false
}

class StatementAnalyzer {
    private val words:List<String>
    private var currentIndex:Int = 0

    constructor(sql:String) {
        this.words = splitStringToWords(sql)
    }

    private constructor(words:List<String>) {
        this.words = words
    }


    fun extractSelect(preappend:List<String>):StatementAnalyzer {
        val fromIndex = words.indexOf("from")
        val whereIndex = words.indexOf("where")

        if (fromIndex == -1 && whereIndex == -1) {
            return StatementAnalyzer(preappend)
        }
        if (fromIndex == -1 && whereIndex != -1) {
            val newWords:List<String> = preappend + words.subList(whereIndex,words.size)
            return StatementAnalyzer(newWords)
        }
        val newWords:List<String> = preappend + listOf(",") + words.subList(fromIndex+1,words.size)
        return StatementAnalyzer(newWords)
    }

    fun extractParantesStepForward():StatementAnalyzer? {
        return extractParantesStepForward(-1,true)?.first

    }

    fun extractParensFromOffset(offsetStart: Int):Pair<StatementAnalyzer,Int>? {
        return extractParantesStepForward(offsetStart-1,false)
    }

    private fun extractParantesStepForward(offsetStart:Int,adjustIndex:Boolean):Pair<StatementAnalyzer,Int>? {
        var offSet = offsetStart
        var parensCount = 0
        do {
            offSet++
            when (word(offSet)) {
                "(" -> parensCount++
                ")" -> parensCount --
                null -> throw SQLException("Unexpected end of statement")
            }
        } while (parensCount > 0)
        if (word(offSet+1) == "::") {
            return null
        }
        val newWords = words.subList(currentIndex+offsetStart+2,currentIndex+offSet)
        if (adjustIndex) {
            currentIndex = currentIndex + offSet
        }
        return Pair(StatementAnalyzer(newWords),offSet)
    }

    fun word(indexOffset:Int=0):String? {
        val readAt = currentIndex+indexOffset
        if (readAt < 0 || readAt >= words.size) {
            return null
        }
        return words[readAt]
    }

    fun addIndex(delta:Int=1):StatementAnalyzer {
        currentIndex+=delta
        return this
    }

    fun setIndex(index:Int):StatementAnalyzer {
        currentIndex = index
        return this
    }

    fun indexOf(word:String):Int {
        var resind = -1
        var parind = 0
        while (currentIndex+resind < words.size-1) {
            resind++
            val checking = words[currentIndex + resind]
            if (setOf("(",")").contains(word) && word == checking) {
                return 0
            }
            if (checking == "(") {
                parind++
                continue
            }
            if (checking == ")") {
                parind = Math.max(0,parind-1)
                continue
            }
            if (parind > 0) {
                continue
            }
            if (checking == word) {
                return resind
            }
        }
        return -1
    }

    fun wordAt(givenIndex:Int):String? = if (givenIndex+currentIndex >= 0 && givenIndex+currentIndex < words.size) words[givenIndex+currentIndex] else null

    val size: Int
        get() = words.size

    fun readValueFromExpression(dbTransaction: DbTransaction,tables: Map<String,TableInSelect>,indexToUse: IndexToUse?):ValueFromExpression {
        val aword:String = word()?:throw SQLException("Unexpected end of statement")
        var toReturn:ValueFromExpression = when {
                matchesWord(listOf("(","case","when")) -> {
                    addIndex(2)
                    CaseValueFromExpression(this,dbTransaction,indexToUse?: IndexToUse(),tables)
                }

                (aword == "now" && word(1) == "(" && word(2) == ")") -> {
                    addIndex(3)
                    CurrentTimeValueFromExpression()
                }
            (aword == "current_timestamp") ->
                CurrentTimeValueFromExpression()

            (aword == "uuid_in" && word(1) == "(") -> {
                var parind = 0
                do {
                    val currWord = addIndex().word()?:throw SQLException("Unexpected end of statement expected )")
                    if (currWord == "(") {
                        parind++
                    }
                    if (currWord == ")") {
                        parind--
                    }
                } while (parind > 0)
                addIndex()
                FixedValueFromExpression(StringCellValue(UUID.randomUUID().toString()))
            }
            (aword == "nextval" && word(1) == "(" && word(3) == ")") -> {
                val seqnamestr = word(2)
                if (!(seqnamestr?.startsWith("'") == true && seqnamestr.endsWith("'") == true)) {
                    throw SQLException("Expected sequence name in nextval")
                }
                addIndex(4)
                val seqname = seqnamestr.substring(1,seqnamestr.length-1).toLowerCase()
                dbTransaction.sequence(seqname)
                SequenceValueFromExpression(seqname)
            }
            ("true" == aword) -> FixedValueFromExpression(BooleanCellValue(true))
            ("false" == aword) -> FixedValueFromExpression(BooleanCellValue(false))
            ("null" == aword) -> FixedValueFromExpression(NullCellValue)

            (aword.toLongOrNull() != null) -> FixedValueFromExpression(IntegerCellValue(aword.toLong()))
            (aword.toBigDecimalOrNull() != null) -> FixedValueFromExpression(NumericCellValue(aword.toBigDecimal()))
            aword.startsWith("'") -> {
                val end = aword.indexOf("'",1)
                if (end == -1) {
                    throw SQLException("Illegal text $aword")
                }
                val text = aword.substring(1,end)
                FixedValueFromExpression(StringCellValue(text))
            }
            (aword == "(") -> {
                var toAdd:Int = -1
                var parcount  = 0
                do {
                    toAdd++
                    when (word(toAdd)) {
                        null -> throw SQLException("Unexpeced end of statement expected )")
                        "(" -> parcount++
                        ")" -> parcount--
                    }
                } while (parcount > 0)
                val parentsWords = words.subList(currentIndex+1,currentIndex+toAdd)
                currentIndex = currentIndex+toAdd
                StatementAnalyzer(parentsWords).readValueFromExpression(dbTransaction,tables,indexToUse)
            }
            aword == "coalesce" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( in coalesce")
                }
                val coalvalues:MutableList<ValueFromExpression> = mutableListOf()
                while (true) {
                    addIndex()
                    val nextVal = readValueFromExpression(dbTransaction,tables,indexToUse)
                    coalvalues.add(nextVal)
                    val nextWord = addIndex().word()
                    when (nextWord) {
                        ")" -> break
                        "," -> continue
                        else -> throw SQLException("Unexpedted end of coalesce")
                    }
                }
                CoalesceValue(coalvalues)
            }
            aword == "to_date" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( after to_date")
                }
                addIndex()
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ",") {
                    throw SQLException("Expected , after to_date")
                }
                val dateformat = addIndex().word()
                if (!(dateformat?.startsWith("'") == true && dateformat.endsWith("'") && dateformat.length >= 3)) {
                    throw SQLException("Expected dateformat after to_date")
                }
                if (addIndex().word() != ")") {
                    throw SQLException("Expected ) after to date")
                }
                ToDateValue(fromExpression,dateformat.substring(1,dateformat.length-1))
            }
            aword == "date" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( after to_date")
                }
                addIndex()
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ")") {
                    throw SQLException("Expected ) after date")
                }
                DateTimeToDateValue(fromExpression)
            }
            aword == "date_trunc" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( after to_date")
                }
                val truncspec:String? = addIndex().word()
                if (!(truncspec?.startsWith("'") == true && truncspec.endsWith("'") && truncspec.length >= 3)) {
                    throw SQLException("Expected truncspec after date_trunc")
                }
                if (addIndex().word() != ",") {
                    throw SQLException("Expecting , in date_trunc")
                }
                addIndex()
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ")") {
                    throw SQLException("Expected ) after date_trunc")
                }
                DateTruncValue(fromExpression,truncspec)
            }
            aword == "to_number" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( after to_date")
                }
                addIndex()
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ",") {
                    throw SQLException("Expected , after to_date")
                }
                while (word() != null && word() != ")") {
                    addIndex()
                }
                if (word() != ")") {
                    throw SQLException("Expecting ) after to_number")
                }
                ToNumberValue(fromExpression)
            }
            aword == "lower" -> {
                if (addIndex().word() != "(") {
                    throw SQLException("Expected ( after lower")
                }
                addIndex()
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ")") {
                    throw SQLException("Expected ) after lower")
                }
                LowerExpression(fromExpression)
            }
            aword == "extract" -> {
                if (!matchesWord(listOf("extract","(","year","from"))) {
                    throw SQLException("expected (year from after extract")
                }
                addIndex(4)
                val fromExpression:ValueFromExpression = readValueFromExpression(dbTransaction,tables,indexToUse)
                if (addIndex().word() != ")") {
                    throw SQLException("Expect ) ending extract")
                }
                ExtractExpression(fromExpression)
            }
            aword == "?" ->
                BindingValueFromExpression(indexToUse)
            else -> readColumnValue(tables,aword)
        }
        while (true) {
            toReturn = when(word(1)) {
                "::" -> {
                            /*if (word(2) == "json" && word(3) == "->>") {
                            addIndex(4)
                            return ReadJsonProperty(toReturn, word())
                        }*/
                    val coltypeToConvToText = word(2) ?: throw SQLException("Unexpected end of statement")
                    val columnType: ColumnType =
                        ColumnType.values().firstOrNull { it.matchesColumnType(coltypeToConvToText) }
                            ?: throw SQLException("Unknown convert to $$coltypeToConvToText")
                    addIndex(2)
                    ConvertToColtype(toReturn, columnType)
                }
                "->>" -> {
                    addIndex(2)
                    ReadJsonProperty(toReturn,word())
                }
                "->" -> {
                    addIndex(2)
                    ReadJsonObject(toReturn,word())
                }
                else -> break
            }
        }
        return toReturn
    }

    private fun myValuegen(column: ColumnInSelect,cells:List<Cell>?,tables: Map<String, TableInSelect>):CellValue {
        val matches = (cells?: emptyList()).filter{ it.column.name == column.name}
        if (matches.isEmpty()) {
            return NullCellValue
        }
        if (matches.size == 1) {
            return matches[0].value
        }
        val res:CellValue? = cells?.firstOrNull { it.column.matches(column.tablename,column.name?:"") }?.value
        return res?:NullCellValue
    }

    private fun readColumnValue(tables: Map<String, TableInSelect>, aword: String):ValueFromExpression {
        val column: ColumnInSelect = findColumnFromIdentifier(aword, tables, emptyList())
        return ColumnValueFromExpression(column)
    }

    fun findColumnFromIdentifier(
        aword: String,
        tables: Map<String, TableInSelect>,
        selectedColumns:List<SelectColumnProvider>
    ): ColumnInSelect {
        val ind = aword.indexOf(".")
        val column: ColumnInSelect = if (ind == -1) {
            val colinsel:ColumnInSelect? = tables.values.map { it.findColumn(aword) }.filterNotNull().firstOrNull()
            if (colinsel == null) {
                val selectColumnProvider = selectedColumns.firstOrNull { it.alias == aword } ?: throw SQLException("Unknown order by $aword")
                SelectColumnAsAColumn(
                    selectColumnProvider,
                    ""
                )
            } else {
                colinsel
            }
        } else {
            var tablename = aword.substring(0, ind)
            if (tablename.startsWith("\"") && tablename.endsWith("\"")) {
                tablename = tablename.substring(1, tablename.length - 1)
            }
            val table: TableInSelect? = tables[tablename]
            if (table == null) {
                throw SQLException("Unknown table $tablename")
            }
            val colname = aword.substring(ind + 1)
            table.findColumn(colname) ?: throw SQLException("Unknown column $colname")
        }
        return column
    }

    fun readValueOnRow():(SelectResultSet)->CellValue {
        val colname = word()?:throw SQLException("Unekpected end of statement")
        addIndex()
        if (colname.startsWith("'") && colname.endsWith("'")) {
            return ConstantValue(StringCellValue(colname.substring(1,colname.length-1)))
        }
        val resultTransformer:((CellValue)->CellValue)? = if (word() == "::") {
            if (addIndex().word() != "json") {
                throw SQLException("Unsupported konversion ${word()}")
            }
            if (addIndex().word() != "->>") {
                throw SQLException("Expected ->>")
            }
            addIndex()
            val jsonkey = word()
            if (!(jsonkey?.startsWith("'") == true && word()?.endsWith("'") == true)) {
                throw SQLException("Expected jsonkey got $jsonkey")
            }
            addIndex()
            JsonTransformer(jsonkey.substring(1,jsonkey.length-1))
        }  else null
        return ReadFromRow(colname,resultTransformer)
    }

    fun addIndexUntilNextCommaOrEnd() {
        var parentsIndex = 0
        while (word(1) != null && (parentsIndex > 0 || word(1) != ",")) {
            addIndex()
            if (word() == "(") {
                parentsIndex++
            }
            if (word() == ")") {
                parentsIndex--
            }
        }
    }

    fun matchesWord(expected:List<String>):Boolean {
        var indadd = 0
        for (w in expected) {
            if (word(indadd) != w) {
                return false
            }
            indadd++
        }
        return true
    }
}

private class ReadFromRow(val collabel:String,val resultTransformer:((CellValue)->CellValue)?):(SelectResultSet)->CellValue {
    override fun invoke(row: SelectResultSet): CellValue {
        var value:CellValue = row.readCell(collabel)
        if (resultTransformer != null) {
            value = resultTransformer.invoke(value)
        }
        return value
    }
}

private class JsonTransformer(val key:String):(CellValue)->CellValue {
    override fun invoke(inputValue: CellValue): CellValue {
        if (inputValue !is StringCellValue) {
            return NullCellValue
        }
        val jsonNode = JsonParser.parse(inputValue.myValue)
        if (jsonNode !is JsonObject) {
            throw SQLException("Expected jsonobject got $inputValue")
        }
        val readVal:String? = jsonNode.stringValue(key).orElse(null)
        return readVal?.let { StringCellValue(it) }?:NullCellValue
    }
}

private class ConstantValue(val value:CellValue):(SelectResultSet)->CellValue {
    override fun invoke(selectResultSet:SelectResultSet): CellValue {
        return value
    }

}