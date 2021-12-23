
package example

import io.micronaut.data.annotation.Where
import org.bson.types.ObjectId
import javax.persistence.Entity
import javax.persistence.GeneratedValue
import javax.persistence.Id


@Entity
@Where("@.enabled = true") // <1>
data class User(
    @GeneratedValue
    @Id
    var id: ObjectId,
    val name: String,
    val enabled: Boolean // <2>
)