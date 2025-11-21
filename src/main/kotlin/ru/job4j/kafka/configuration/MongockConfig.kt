package ru.job4j.kafka.configuration

import com.mongodb.client.MongoClient
import io.mongock.runner.springboot.MongockSpringboot
import io.mongock.runner.core.executor.MongockRunner
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import io.mongock.driver.mongodb.sync.v4.driver.MongoSync4Driver
import org.springframework.boot.CommandLineRunner


@Configuration
class MongockConfig {

    @Bean
    fun mongockRunner(
        mongoClient: MongoClient,
        applicationContext: ApplicationContext
    ): MongockRunner {
        val driver = MongoSync4Driver.withDefaultLock(mongoClient, "kafka_course_db")
        return MongockSpringboot.builder()
            .setDriver(driver)
            .addMigrationScanPackage("ru.job4j.kafka.mongock.changelogs")
            .setSpringContext(applicationContext)
            .setTransactionEnabled(false)
            .buildRunner()
    }

    @Bean
    fun ctxPrinter(ctx: ApplicationContext): CommandLineRunner = CommandLineRunner {
        println("BEANS:")
        ctx.beanDefinitionNames
            .filter { it.contains("mongock", ignoreCase = true) }
            .forEach { println(it) }
    }

    @Bean
    fun forceMongock(runner: MongockRunner) = CommandLineRunner {
        println(">>> Форсируем Mongock")
        runner.execute()
    }
}