package no.anksoft.pginmem

import org.jsonbuddy.JsonObject
import org.jsonbuddy.parse.JsonParser
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime

private fun splitStringToWords(sql:String):List<String> {
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
        if (charAtPos == ',') {
            if (index > previndex) {
                result.add(trimmed.substring(previndex,index))
            }
            result.add(",")
            index++
            previndex = index
            inSpecialCaseSequence = false
            continue
        }
        if ("()=<>:-".indexOf(charAtPos) != -1) {
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


class StatementAnalyzer(val sql:String) {
    private val words:List<String> = splitStringToWords(sql)
    private var currentIndex:Int = 0

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
        var resind = 0
        while (currentIndex+resind < words.size) {
            if (words[currentIndex+resind] == word) {
                return resind
            }
            resind++
        }
        return -1
    }

    fun wordAt(givenIndex:Int):String? = if (givenIndex+currentIndex >= 0 && givenIndex+currentIndex < words.size) words[givenIndex+currentIndex] else null

    val size = words.size
    fun subList(fromIndex:Int,toIndex:Int) = words.subList(fromIndex,toIndex)

    fun readValueFromExpression(dbTransaction: DbTransaction,tables: List<Table>):((Pair<DbTransaction,Row?>)->Any?)? {
        val aword:String = word()?:return null
        when {
                (aword == "now" && word(1) == "()") -> {
                    addIndex(2)
                    return { Timestamp.valueOf(LocalDateTime.now()) }
                }
            (aword == "nextval" && word(1) == "(" && word(3) == ")") -> {
                val seqnamestr = word(2)
                if (!(seqnamestr?.startsWith("'") == true && seqnamestr.endsWith("'") == true)) {
                    throw SQLException("Expected sequence name in nextval")
                }
                addIndex(4)
                val seqname = seqnamestr.substring(1,seqnamestr.length-1)
                dbTransaction.sequence(seqname)
                return { it.first.sequence(seqname).nextVal() }

            }
            ("true" == aword) -> return { true }
            ("false" == aword) -> return { false }
            (aword.toLongOrNull() != null) -> return { aword.toLong()}
            (aword.toBigDecimalOrNull() != null) -> return { aword.toBigDecimal() }
            aword.startsWith("'") -> {
                val end = aword.indexOf("'",1)
                if (end == -1) {
                    throw SQLException("Illegal text $aword")
                }
                val text = aword.substring(1,end)
                return { text }
            }
            else -> return null
        }
    }

    fun readValueOnRow(tables:List<Table>):(Row)->Any? {
        val colname = word()?:throw SQLException("Unekpected end of statement")
        if (colname.startsWith("'") && colname.endsWith("'")) {
            addIndex()
            return ConstantValue(colname.substring(1,colname.length-1))
        }
        val column:Column = tables.map { it.findColumn(colname)}
            .filterNotNull()
            .firstOrNull()?:throw SQLException("Unknown column $colname")
        addIndex()
        val resultTransformer:((Any?)->Any?)? = if (word() == "::") {
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
        return ReadFromRow(column,resultTransformer)
    }
}

private class ReadFromRow(val column: Column,val resultTransformer:((Any?)->Any?)?):(Row)->Any? {
    override fun invoke(row: Row): Any? {
        val cell = row.cells.first { it.column == column }
        var value = cell.value
        if (resultTransformer != null) {
            value = resultTransformer.invoke(value)
        }
        return value
    }
}

private class JsonTransformer(val key:String):(Any?)->Any? {
    override fun invoke(inputValue: Any?): Any? {
        if (inputValue !is String) {
            return null
        }
        val jsonNode = JsonParser.parse(inputValue)
        if (jsonNode !is JsonObject) {
            throw SQLException("Expected jsonobject got $inputValue")
        }
        return jsonNode.stringValue(key).orElse(null)
    }
}

private class ConstantValue(val value:Any?):(Row)->Any? {
    override fun invoke(row: Row): Any? {
        return value
    }

}