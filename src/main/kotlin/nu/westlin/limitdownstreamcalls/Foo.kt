@file:Suppress("SpringFacetCodeInspection", "UNUSED_VARIABLE")

package nu.westlin.limitdownstreamcalls

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.getBean
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.stereotype.Repository
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange

@SpringBootApplication
class Foo


fun main() {
    val logger: Logger = LoggerFactory.getLogger("Main")

    val fooRepository = runApplication<LimitdownstreamcallsApplication>().getBean<FooRepository>()

    runBlocking {

        val futures = (1..20).map { index ->
            async {
                // Will limit number of concurrent requests to 5
                (1..3).map {
                    async { fooRepository.getPant("$index-$it") }
                    //fooRepository.getPant("$index-$it")
                }.awaitAll()
            }
        }

        val responses = futures.awaitAll()
        println(responses)
    }


/*
    data class Job(val id: Int) {
        suspend fun loadData(): String {
            logger.info("Loading data")
            delay(1000)
            return "foo"
        }
    }
        val mDownloadJobs = MutableList(20) {index -> Job(index)}


    runBlocking {
        val requestSemaphore = Semaphore(5)

        val mDownloadJobs = MutableList(20) {index -> Job(index)}

        val futures = mDownloadJobs.map {
            async {
                logger.info("${it.id} Före: ${requestSemaphore.availablePermits}")
                // Will limit number of concurrent requests to 5
                requestSemaphore.withPermit {
                    it.loadData()
                }
                logger.info("${it.id} Efter: ${requestSemaphore.availablePermits}")
            }
        }

        val responses = futures.awaitAll()
        println(responses)
    }
*/
}

@Repository
class FooRepository(private val webClient: WebClient) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)
    private val requestSemaphore = Semaphore(3)

    suspend fun getPant(inteckningsreferens: String): Pant {
        requestSemaphore.withPermit {
            logger.info("Hämtar panter för inteckningsreferens $inteckningsreferens")
            webClient.get()
                .uri("/Foo-inteckningsreferens:$inteckningsreferens/1000")
                .awaitExchange()
                .awaitBody<ApplicationDelay>()

            return Pant("$inteckningsreferens-1", inteckningsreferens)
        }
    }
}
