package no.anksoft.pginmem

import no.anksoft.pginmem.clauses.IndexToUse
import no.anksoft.pginmem.statements.select.ColumnInSelect
import no.anksoft.pginmem.statements.select.SelectResultSet
import no.anksoft.pginmem.statements.select.TableInSelect
import no.anksoft.pginmem.values.*
import org.jsonbuddy.JsonObject
import org.jsonbuddy.parse.JsonParser
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
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
    val valuegen: ((Pair<DbTransaction, Row?>) -> CellValue)
    val column: ColumnInSelect?
    fun registerBinding(index:Int,value: CellValue):Boolean
}


class BasicValueFromExpression(
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue,
    override val column: ColumnInSelect?
) :ValueFromExpression {
    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

class BindingValueFromExpression(indexToUse: IndexToUse?):ValueFromExpression {
    private val expectedIndex:Int = indexToUse?.takeInd()?:throw SQLException("Cannot use binding here")
    private var cellValue:CellValue? = null

    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        cellValue?:throw SQLException("Binding not set for binding $expectedIndex")
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

    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val startVal:CellValue = inputValue.valuegen.invoke(it)
        val jsonObject: JsonObject? = when {
            startVal is JsonCellValue -> startVal.myvalue
            startVal is StringCellValue -> JsonParser.parseToObject(startVal.myValue)
            startVal is NullCellValue -> null
            else -> throw SQLException("Expected string as json")
        }
        (jsonObject?.stringValue(jsonProperty)?.orElse(null))?.let { StringCellValue(it) }?:NullCellValue
    }

    override val column: Column? = null

    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

private class ReadJsonObject(val inputValue:ValueFromExpression,jsonPropertyText:String?):ValueFromExpression {
    private val jsonProperty:String = readJsonProperty(jsonPropertyText)

    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val startVal:CellValue = inputValue.valuegen.invoke(it)
        val jsonObject:JsonObject? = when {
            startVal is JsonCellValue -> startVal.myvalue
            startVal is StringCellValue -> JsonParser.parseToObject(startVal.myValue)
            startVal is NullCellValue -> null
            else -> throw SQLException("Expected string as json")
        }

        jsonObject?.let { jo -> (jo.objectValue(jsonProperty).orElse(null))?.let { JsonCellValue(it) }}?:NullCellValue
    }

    override val column: Column? = null

    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

private class ConvertToColtype(val inputValue: ValueFromExpression,val columnType: ColumnType):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val startvalue:CellValue = inputValue.valuegen.invoke(it)
        val res = columnType.convertToMe(startvalue)
        res
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = false
}

private class CoalesceValue(val coalvalues:List<ValueFromExpression>):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        var genvalue:CellValue = NullCellValue
        for (valueFromExp in coalvalues) {
            val valFromEx = valueFromExp.valuegen.invoke(it)
            if (valFromEx != NullCellValue) {
                genvalue = valFromEx
                break
            }
        }
        genvalue
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
            else -> c
        })
    }
    return DateTimeFormatter.ofPattern(resFormat.toString())
}

private class ToDateValue(val startValue:ValueFromExpression,dateformat:String):ValueFromExpression {

    private val dateTimeFormatter:DateTimeFormatter = convertToJavaDateFormat(dateformat)

    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val toConvert:CellValue = startValue.valuegen.invoke(it)
        if (toConvert == NullCellValue) {
            NullCellValue
        } else {
            toConvert.valueAsDate()
        }
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
}

private class LowerExpression(val startValue:ValueFromExpression):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val startVal = startValue.valuegen.invoke(it)
        if (startVal is StringCellValue) StringCellValue(startVal.myValue.toLowerCase()) else startVal
    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)

}

private class ExtractExpression(val startValue:ValueFromExpression):ValueFromExpression {
    override val valuegen: (Pair<DbTransaction, Row?>) -> CellValue = {
        val startVal = startValue.valuegen.invoke(it)
        when {
            (startVal is NullCellValue) -> NullCellValue
            (startVal is DateTimeCellValue) -> IntegerCellValue(startVal.myValue.year.toLong())
            (startVal is DateCellValue) -> IntegerCellValue(startVal.myValue.year.toLong())
            else -> throw SQLException("Illegal value to extract")
        }

    }

    override val column: Column? = null
    override fun registerBinding(index:Int,value: CellValue):Boolean = startValue.registerBinding(index,value)
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
            (aword == "now" && word(1) == "(" && word(2) == ")") -> {
                    addIndex(3)
                    BasicValueFromExpression({ DateTimeCellValue(LocalDateTime.now()) },null)
                }
            (aword == "current_timestamp") ->
                BasicValueFromExpression({ DateTimeCellValue(LocalDateTime.now()) },null)

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
                BasicValueFromExpression({ StringCellValue(UUID.randomUUID().toString()) },null)
            }
            (aword == "nextval" && word(1) == "(" && word(3) == ")") -> {
                val seqnamestr = word(2)
                if (!(seqnamestr?.startsWith("'") == true && seqnamestr.endsWith("'") == true)) {
                    throw SQLException("Expected sequence name in nextval")
                }
                addIndex(4)
                val seqname = seqnamestr.substring(1,seqnamestr.length-1).toLowerCase()
                dbTransaction.sequence(seqname)
                BasicValueFromExpression({ it.first.sequence(seqname).nextVal() },null)
            }
            ("true" == aword) -> BasicValueFromExpression({ BooleanCellValue(true) },null)
            ("false" == aword) -> BasicValueFromExpression({ BooleanCellValue(false) },null)
            ("null" == aword) -> BasicValueFromExpression({ NullCellValue },null)

            (aword.toLongOrNull() != null) -> BasicValueFromExpression({ IntegerCellValue(aword.toLong())},null)
            (aword.toBigDecimalOrNull() != null) -> BasicValueFromExpression({ NumericCellValue(aword.toBigDecimal()) },null)
            aword.startsWith("'") -> {
                val end = aword.indexOf("'",1)
                if (end == -1) {
                    throw SQLException("Illegal text $aword")
                }
                val text = aword.substring(1,end)
                BasicValueFromExpression({ StringCellValue(text) },null)
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
                ToDateValue(fromExpression,dateformat.substring(1,dateformat.length-1))
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


    private fun readColumnValue(tables: Map<String, TableInSelect>, aword: String):ValueFromExpression {
        val column: ColumnInSelect = findColumnFromIdentifier(aword, tables)
        val valuegen:((Pair<DbTransaction,Row?>)->CellValue) = { it.second?.cells?.firstOrNull { it.column.matches(column.tablename,column.name?:"") }?.value?:NullCellValue }
        return BasicValueFromExpression(valuegen,column)
    }

    fun findColumnFromIdentifier(
        aword: String,
        tables: Map<String, TableInSelect>
    ): ColumnInSelect {
        val ind = aword.indexOf(".")
        val column: ColumnInSelect = if (ind == -1) {
            tables.values.map { it.findColumn(aword) }.filterNotNull().firstOrNull()
                ?: throw SQLException("Unknown column $aword")
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