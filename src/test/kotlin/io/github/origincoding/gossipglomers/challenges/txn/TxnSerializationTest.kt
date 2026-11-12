package io.github.origincoding.gossipglomers.challenges.txn

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.serialization.SerializationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

class TxnSerializationTest {
    @Test
    fun `request and response use three element arrays and preserve Long values`() {
        val request = TxnBody(listOf(Operation.Read(Long.MIN_VALUE), Operation.Write(Long.MAX_VALUE, Long.MIN_VALUE)))
        val requestJson = """{"txn":[["r",-9223372036854775808,null],["w",9223372036854775807,-9223372036854775808]]}"""
        assertEquals(requestJson, Json.encodeToString(request))
        assertEquals(request, Json.decodeFromString<TxnBody>(requestJson))

        val response = TxnOkBody(listOf(
            OperationResult.Read(1, null),
            OperationResult.Read(2, Long.MAX_VALUE),
            OperationResult.Write(2, Long.MAX_VALUE),
        ))
        val responseJson = """{"txn":[["r",1,null],["r",2,9223372036854775807],["w",2,9223372036854775807]]}"""
        assertEquals(responseJson, Json.encodeToString(response))
        assertEquals(response, Json.decodeFromString<TxnOkBody>(responseJson))
    }

    @Test
    fun `invalid requests reject missing fields unknown kinds and invalid numbers`() {
        for (operation in listOf(
            """["r",1]""",
            """["r",1,null,0]""",
            """["r",1,6]""",
            """["w",1,null]""",
            """["unknown",1,null]""",
            """["r","1",null]""",
            """["w",1,"6"]""",
            """["w",1,1.5]""",
            """["w",1,9223372036854775808]""",
        )) {
            assertFailsWith<SerializationException>(operation) {
                Json.decodeFromString<TxnBody>("""{"txn":[$operation]}""")
            }
        }
    }
}
