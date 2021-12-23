package example

import io.micronaut.data.document.mongodb.annotation.MongoRepository
import io.micronaut.data.repository.CrudRepository
import org.bson.types.ObjectId

@MongoRepository
interface CourseRatingRepository : CrudRepository<CourseRating, ObjectId> {
}