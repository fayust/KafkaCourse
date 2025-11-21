package ru.job4j.kafka.mongock.changelogs

import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ValidationAction
import com.mongodb.client.model.ValidationLevel
import com.mongodb.client.model.ValidationOptions
import io.mongock.api.annotations.ChangeUnit
import io.mongock.api.annotations.Execution
import io.mongock.api.annotations.RollbackExecution
import org.bson.Document
import org.springframework.data.mongodb.core.MongoTemplate

@ChangeUnit(id = "create-outbox-business-collection", order = "002", author = "fayust")
class CreateOutboxBusinessCollection {

    @Execution
    fun createCollection(mongoTemplate: MongoTemplate) {
        println("MONGOCK: Запуск миграции outbox_business")

        val db = mongoTemplate.db
        val collectionName = "outbox_business"

        if (!db.listCollectionNames().contains(collectionName)) {

            val validator = Document("\$jsonSchema",
                Document().apply {
                    append("bsonType", "object")
                    append("required", listOf("data", "created_at"))
                    append("properties", Document().apply {
                        append("id", Document().apply {
                            append("bsonType", listOf("string", "null"))
                            append("description", "MongoDB generated ObjectId as String")
                        })
                        append("data", Document().apply {
                            append("bsonType", "string")
                            append("description", "must be a non-empty string")
                            append("minLength", 1)
                        })
                        append("created_at", Document().apply {
                            append("bsonType", "date")
                            append("description", "must be a valid date")
                        })
                    })
                })

            val createOptions = CreateCollectionOptions().apply {
                validationOptions(
                    ValidationOptions()
                        .validator(validator)
                        .validationLevel(ValidationLevel.STRICT)
                        .validationAction(ValidationAction.ERROR)
                )
            }

            db.createCollection(collectionName, createOptions)
            println("Коллекция '$collectionName' создана с валидацией")
        }

        // Индекс по created_at — для сортировки и быстрого поиска по времени
        val collection = db.getCollection(collectionName)
        collection.createIndex(Indexes.descending("created_at"))
        println("Индекс для '$collectionName' создан")
    }

    @RollbackExecution
    fun rollback(mongoTemplate: MongoTemplate) {}
}