
package example

import io.micronaut.data.annotation.Query
import io.micronaut.data.document.mongodb.annotation.MongoRepository
import io.micronaut.data.repository.CrudRepository
import org.bson.types.ObjectId
import javax.validation.constraints.NotNull

@MongoRepository
interface UserRepository : CrudRepository<User, ObjectId> { // <1>

    @Query("UPDATE user SET enabled = false WHERE id = :id") // <2>
    override fun deleteById(@NotNull id: ObjectId)

    @Query("SELECT * FROM user WHERE enabled = false") // <3>
    fun findDisabled(): List<User>
}
