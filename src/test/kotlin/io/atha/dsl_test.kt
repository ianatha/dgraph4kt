package io.atha

import org.junit.Test
import kotlin.test.assertEquals

class QueryDSLTest {
    @Test
    fun testSimpleQuery() {
        assertEquals(
                """
                    { q(func: eq(name@en, "Steven Spielberg")) {
                      name@en
                    } }
                """.trimIndent(),
                q(func = eq("name@en", "Steven Spielberg")) {
                    + "name@en"
                }.toString()
        )
    }

    @Test
    fun testSimpleQueryWithAlias() {
        assertEquals(
                """
                    { q(func: eq(name@en, "Steven Spielberg")) {
                      name : name@en
                    } }
                """.trimIndent(),
                q(func = eq("name@en", "Steven Spielberg")) {
                    + "name@en" alias "name"
                }.toString()
        )
    }

    @Test
    fun testSimpleQueryWithSubgraphAlias() {
        assertEquals(
                """
                    { q(func: eq(name@en, "Steven Spielberg")) {
                      name : name@en
                      films : director.film {
                        name : name@en
                      }
                    } }
                """.trimIndent(),
                q(func = eq("name@en", "Steven Spielberg")) {
                    + "name@en" alias "name"
                    + "director.film" x {
                        + "name@en" alias "name"
                    } alias "films"
                }.toString()
        )
    }

    @Test
    fun testSimpleQueryWithSubgraph() {
        assertEquals(
                """
                    { q(func: eq(name@en, "Steven Spielberg")) {
                      name@en
                      director.film {
                        uid
                      }
                    } }
                """.trimIndent(),
                q(func = eq("name@en", "Steven Spielberg")) {
                    + "name@en"
                    + "director.film" x {
                        + "uid"
                    }
                }.toString()
        )
    }

    @Test
    fun testSimpleQueryWithUID() {
        assertEquals(
                """
                    { q(func: uid(0x0001)) {
                      director.film {
                        uid
                      }
                    } }
                """.trimIndent(),
                q(func = uid("0x0001")) {
                    + "director.film" x {
                        + "uid"
                    }
                }.toString()
        )
    }

    @Test
    fun testSimpleSet() {
        val newObj = MutationRef()

        assertEquals(
                """
                    { set {
                      _:0000 verb object .
                    } }
                """.trimIndent(),
                set {
                    nq(newObj, "verb", "object")
                }.toString()
        )
    }

    @Test
    fun testQueryWithRootFilter() {
        assertEquals(
                """
                    { q(func: has(director.film)) @filter(anyofterms(name@en, "Steven Theo")) {
                      name@en
                    } }
                """.trimIndent(),
                q(func = has("director.film"), filter = anyofterms("name@en", "Steven Theo")) {
                    + "name@en"
                }.toString()
        )
    }

    @Test
    fun testQueryWithPagination() {
        assertEquals(
                """
                    { q(func: has(director.film), first: 5) {
                      name@en
                    } }
                """.trimIndent(),
                q(func = has("director.film"), first = 5) {
                    + "name@en"
                }.toString()
        )
    }

    @Test
    fun testExploreQuery() {
        assertEquals(
                """
                    { q(func: has(director.film)) {
                      uid
                      _predicate_
                      expand(_all_)
                    } }
                """.trimIndent(),
                q(func = has("director.film")).explore().toString()
        )
    }

}
