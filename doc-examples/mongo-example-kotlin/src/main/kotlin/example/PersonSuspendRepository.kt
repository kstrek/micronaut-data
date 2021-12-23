package example

import io.micronaut.data.document.mongodb.annotation.MongoRepository
import io.micronaut.data.repository.CrudRepository
import io.micronaut.data.repository.jpa.kotlin.CoroutineJpaSpecificationExecutor
import org.bson.types.ObjectId

@MongoRepository
interface PersonSuspendRepository : CrudRepository<Person, ObjectId>, CoroutineJpaSpecificationExecutor<Person>