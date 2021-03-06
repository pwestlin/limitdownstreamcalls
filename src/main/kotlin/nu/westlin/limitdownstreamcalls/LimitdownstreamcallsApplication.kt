package nu.westlin.limitdownstreamcalls

import kotlinx.coroutines.runBlocking
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
    private val pantameraRepository: PantameraRepository,
    private val dominiumRepository: DominiumRepository,
    private val valvetRepository: ValvetRepository
) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    fun runShit() {
        val execTime = measureTimeMillis {
            runBlocking {
                val uuidn = pantameraRepository.getFastighetsreferensen()
                uuidn.forEach { uuid ->
                    logger.info("Arbetar med fastighetsreferens $uuid")
                    val inteckningar = dominiumRepository.getInteckningar(uuid)
                    val panter: List<Pant> = inteckningar.map { valvetRepository.getPant(it.id) }
                    val antalInteckningar: Int = pantameraRepository.getAntalInteckningar(uuid)
                    logger.info("Klar med fastighetsreferens $uuid")
                }
            }
        }

        logger.info("Total exekveringstid $execTime ms")
    }

}

@Repository
class PantameraRepository(private val webClient: WebClient) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    private val fastighetsreferenser = (1..100).map { it.toString() }.toList()

    suspend fun getFastighetsreferensen(): List<String> {
        webClient.get()
            .uri("/Pantamera-uuidn/1")
            .awaitExchange()
            .awaitBody<ApplicationDelay>()

        return fastighetsreferenser
    }

    suspend fun getAntalInteckningar(uuid: String): Int {
        webClient.get()
            .uri("/Pantamera-antalInteckningar/1")
            .awaitExchange()
            .awaitBody<ApplicationDelay>()

        return 3
    }

}

@Repository
class DominiumRepository(private val webClient: WebClient) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    suspend fun getInteckningar(fastighetsreferens: String): List<Inteckning> {
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

@Repository
class ValvetRepository(private val webClient: WebClient) {

    private val logger: Logger = LoggerFactory.getLogger(this.javaClass)

    suspend fun getPant(inteckningsreferens: String): Pant {
        webClient.get()
            .uri("/Valvet-inteckningsreferens:$inteckningsreferens/100")
            .awaitExchange()
            .awaitBody<ApplicationDelay>()

        return Pant("$inteckningsreferens-1", inteckningsreferens)

    }
}

data class Inteckning(val id: String, val fastighetsreferens: String)

data class ApplicationDelay(val applicationName: String, val actualDelayTime: Long)

data class Pant(val id: String, val inteckningsreferens: String)