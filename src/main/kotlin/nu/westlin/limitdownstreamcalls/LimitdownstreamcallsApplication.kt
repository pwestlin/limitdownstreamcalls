@file:Suppress("UNUSED_VARIABLE", "unused", "SpringFacetCodeInspection")

package nu.westlin.limitdownstreamcalls

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.beans.factory.getBean
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.web.reactive.function.client.awaitExchange
import kotlin.system.measureTimeMillis

@SpringBootApplication
class LimitdownstreamcallsApplication

fun main(args: Array<String>) {
    val doShit = runApplication<LimitdownstreamcallsApplication>(*args).getBean<DoShit>()
    doShit.runShit()
}

@Configuration
class Configuration {

    @Bean
    fun webClient(@Value("\${repository.baseUrl}") baseUrl: String): WebClient = WebClient.create(baseUrl)
}

@Service
class DoShit(
    @Value("\${semaphore.main.size}") mainSemaphoreSize: Int,
    private val pantameraRepository: PantameraRepository,
    private val dominiumRepository: DominiumRepository,
    private val valvetRepository: ValvetRepository
) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    private val mainSemaphore = Semaphore(mainSemaphoreSize)

    fun runShit() {

        val execTime = measureTimeMillis {
            runBlocking {
                val uuidn = pantameraRepository.getFastighetsreferensen()
                uuidn.map { uuid ->
                    async {
                        logger.info("Arbetar med fastighetsreferens $uuid")

                        val antalInteckningar: Deferred<Int> =
                            async { pantameraRepository.getAntalInteckningar(uuid) }

                        val inteckningar = async {
                            dominiumRepository.getInteckningar(uuid)
                        }
                        val panter: List<Deferred<Pant>> = inteckningar.await().map {
                            async { valvetRepository.getPant(it.id) }
                        }

                        panter.awaitAll()
                        antalInteckningar.await()

                        logger.info("Klar med fastighetsreferens $uuid")
                    }
                }.awaitAll()
            }
        }

        logger.info("Total exekveringstid $execTime ms")
    }

}

@Repository
class PantameraRepository(
    @Value("\${semaphore.pantamera.size}") semaphoreSize: Int,
    private val webClient: WebClient) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    private val semaphore = Semaphore(semaphoreSize)

    private val fastighetsreferenser = (1..20).map { it.toString() }.toList()

    suspend fun getFastighetsreferensen(): List<String> {
        semaphore.withPermit {
            webClient.get()
                .uri("/Pantamera-uuidn/1")
                .awaitExchange()
                .awaitBody<ApplicationDelay>()

            return fastighetsreferenser
        }
    }

    suspend fun getAntalInteckningar(uuid: String): Int {
        semaphore.withPermit {
            webClient.get()
                .uri("/Pantamera-antalInteckningar/1")
                .awaitExchange()
                .awaitBody<ApplicationDelay>()

            return 3
        }
    }

}

@Repository
class DominiumRepository(
    @Value("\${semaphore.dominium.size}") semaphoreSize: Int,
    private val webClient: WebClient
) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    private val semaphore = Semaphore(semaphoreSize)

    suspend fun getInteckningar(fastighetsreferens: String): List<Inteckning> {
        semaphore.withPermit {
            webClient.get()
                .uri("/Dominium-inteckning-fastighetsreferens:$fastighetsreferens/100")
                .awaitExchange()
                .awaitBody<ApplicationDelay>()

            return listOf(
                Inteckning("$fastighetsreferens-1", fastighetsreferens),
                Inteckning("$fastighetsreferens-2", fastighetsreferens),
                Inteckning("$fastighetsreferens-3", fastighetsreferens)
            )
        }
    }
}

@Repository
class ValvetRepository(
    @Value("\${semaphore.valvet.size}") semaphoreSize: Int,
    private val webClient: WebClient
) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    private val semaphore = Semaphore(semaphoreSize)

    suspend fun getPant(inteckningsreferens: String): Pant {
        semaphore.withPermit {
            logger.info("Hämtar panter för inteckningsreferens $inteckningsreferens")

            webClient.get()
                .uri("/Valvet-inteckningsreferens:$inteckningsreferens/1000")
                .awaitExchange()
                .awaitBody<ApplicationDelay>()

            return Pant("$inteckningsreferens-1", inteckningsreferens)
        }
    }
}

data class Inteckning(val id: String, val fastighetsreferens: String)

data class ApplicationDelay(val applicationName: String, val actualDelayTime: Long)

data class Pant(val id: String, val inteckningsreferens: String)