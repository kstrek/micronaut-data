package example

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.bson.types.ObjectId
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

@MicronautTest
class UserRepositorySpec (val userRepository: UserRepository) {

    @Test
    fun testSoftDelete() {
        val joe = User(ObjectId(), "Joe", true)
        val fred = User(ObjectId(), "Fred", true)
        val bob = User(ObjectId(), "Bob", true)
        userRepository.saveAll(listOf(fred, bob, joe))

        userRepository.deleteById(joe.id)

        assertEquals(2, userRepository.count())
        assertTrue(userRepository.existsById(fred.id))
        assertFalse(userRepository.existsById(joe.id))

        val disabled = userRepository.findDisabled()
        assertEquals(1, disabled.size)
        assertEquals("Joe", disabled.first().name)
    }
}
