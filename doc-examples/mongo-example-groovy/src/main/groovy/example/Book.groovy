
package example

import io.micronaut.data.annotation.GeneratedValue
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import org.bson.types.ObjectId

@MappedEntity
class Book {
    @Id
    @GeneratedValue
    ObjectId id
    private String title
    private int pages

    Book(String title, int pages) {
        this.title = title
        this.pages = pages
    }

    String getTitle() {
        return title
    }

    int getPages() {
        return pages
    }
}
