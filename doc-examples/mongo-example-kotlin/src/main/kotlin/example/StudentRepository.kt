package example

import io.micronaut.core.annotation.NonNull
import io.micronaut.data.annotation.Join
import io.micronaut.data.annotation.repeatable.JoinSpecifications
import io.micronaut.data.document.mongodb.annotation.MongoRepository
import io.micronaut.data.repository.CrudRepository
import org.bson.types.ObjectId
import java.util.*

@MongoRepository
interface StudentRepository : CrudRepository<Student, ObjectId> {
    @Join("courses")
    override fun findById(@NonNull id: ObjectId?): Optional<Student>

    @JoinSpecifications(
            Join("courses"),
            Join("ratings"),
            Join("ratings.course"),
            Join("ratings.student")
    )
    fun queryById(id: ObjectId): Optional<Student>
}