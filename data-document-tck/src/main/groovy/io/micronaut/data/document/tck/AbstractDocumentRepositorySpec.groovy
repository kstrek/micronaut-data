package io.micronaut.data.document.tck

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.data.document.tck.entities.Author
import io.micronaut.data.document.tck.entities.AuthorBooksDto
import io.micronaut.data.document.tck.entities.BasicTypes
import io.micronaut.data.document.tck.entities.Book
import io.micronaut.data.document.tck.entities.BookDto
import io.micronaut.data.document.tck.entities.Page
import io.micronaut.data.document.tck.entities.Person
import io.micronaut.data.document.tck.entities.Student
import io.micronaut.data.document.tck.repositories.AuthorRepository
import io.micronaut.data.document.tck.repositories.BasicTypesRepository
import io.micronaut.data.document.tck.repositories.BookRepository
import io.micronaut.data.document.tck.repositories.PersonRepository
import io.micronaut.data.document.tck.repositories.StudentRepository
import io.micronaut.data.exceptions.OptimisticLockException
import io.micronaut.data.model.Pageable
import io.micronaut.data.model.Sort
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class AbstractDocumentRepositorySpec extends Specification {

    abstract BasicTypesRepository getBasicTypeRepository()

    abstract PersonRepository getPersonRepository()

    abstract BookRepository getBookRepository()

    abstract AuthorRepository getAuthorRepository()

    abstract StudentRepository getStudentRepository()

    abstract Map<String, String> getProperties()

    @AutoCleanup
    @Shared
    ApplicationContext context = ApplicationContext.run(properties)

    protected void setupBooks() {
        // book without an author
        bookRepository.save(new Book(title: "Anonymous", totalPages: 400))

        // blank title
        bookRepository.save(new Book(title: "", totalPages: 0))

        saveSampleBooks()
    }

    protected void saveSampleBooks() {
        bookRepository.saveAuthorBooks([
                new AuthorBooksDto("Stephen King", Arrays.asList(
                        new BookDto("The Stand", 1000),
                        new BookDto("Pet Cemetery", 400)
                )),
                new AuthorBooksDto("James Patterson", Arrays.asList(
                        new BookDto("Along Came a Spider", 300),
                        new BookDto("Double Cross", 300)
                )),
                new AuthorBooksDto("Don Winslow", Arrays.asList(
                        new BookDto("The Power of the Dog", 600),
                        new BookDto("The Border", 700)
                ))])
    }

    protected void savePersons(List<String> names) {
        personRepository.saveAll(names.collect { new Person(name: it) })
    }

    protected void setup() {
        cleanupData()
    }

    protected void cleanupData() {
        personRepository.deleteAll()
        bookRepository.deleteAll()
        authorRepository.deleteAll()
    }

    void "test save and retrieve basic types"() {
        when: "we save a new saved"
            def saved = basicTypeRepository.save(new BasicTypes())

        then: "The ID is assigned"
            saved.myId != null

        when: "A saved is found"
            def retrieved = basicTypeRepository.findById(saved.myId).orElse(null)

        then: "The saved is correct"
            retrieved.uuid == saved.uuid
            retrieved.bigDecimal == saved.bigDecimal
            retrieved.byteArray == saved.byteArray
            retrieved.charSequence == saved.charSequence
            retrieved.charset == saved.charset
            retrieved.primitiveBoolean == saved.primitiveBoolean
            retrieved.primitiveByte == saved.primitiveByte
            retrieved.primitiveChar == saved.primitiveChar
            retrieved.primitiveDouble == saved.primitiveDouble
            retrieved.primitiveFloat == saved.primitiveFloat
            retrieved.primitiveInteger == saved.primitiveInteger
            retrieved.primitiveLong == saved.primitiveLong
            retrieved.primitiveShort == saved.primitiveShort
            retrieved.wrapperBoolean == saved.wrapperBoolean
            retrieved.wrapperByte == saved.wrapperByte
            retrieved.wrapperChar == saved.wrapperChar
            retrieved.wrapperDouble == saved.wrapperDouble
            retrieved.wrapperFloat == saved.wrapperFloat
            retrieved.wrapperInteger == saved.wrapperInteger
            retrieved.wrapperLong == saved.wrapperLong
            retrieved.uri == saved.uri
            retrieved.url == saved.url
//            retrieved.instant == saved.instant
//            retrieved.localDateTime == saved.localDateTime
//            retrieved.zonedDateTime == saved.zonedDateTime
//            retrieved.offsetDateTime == saved.offsetDateTime
//            retrieved.dateCreated == saved.dateCreated
//            retrieved.dateUpdated == saved.dateUpdated
            retrieved.date == saved.date
    }

    void "test save and fetch author with no books"() {
        given:
            def author = new Author(name: "Some Dude")
            authorRepository.save(author)

            author = authorRepository.queryByName("Some Dude")

        expect:
            author.books.size() == 0

        cleanup:
            authorRepository.deleteById(author.id)
    }

    void "test save one"() {
        given:
            savePersons(["Jeff", "James"])

        when: "one is saved"
            def person = new Person(name: "Fred")
            personRepository.save(person)

        then: "the instance is persisted"
            person.id != null
            personRepository.findById(person.id).isPresent()
            personRepository.get(person.id).name == 'Fred'
            personRepository.existsById(person.id)
            personRepository.count() == 3
            personRepository.count("Fred") == 1
            personRepository.findAll().size() == 3
    }

    void "test save many"() {
        given:
            savePersons(["Jeff", "James"])

        when: "many are saved"
            def p1 = personRepository.save("Frank", 0)
            def p2 = personRepository.save("Bob", 0)
            def people = [p1, p2]

        then: "all are saved"
            people.every { it.id != null }
            people.every { personRepository.findById(it.id).isPresent() }
            personRepository.findAll().size() == 4
            personRepository.count() == 4
            personRepository.count("Jeff") == 1

            personRepository.list(Pageable.from(1)).isEmpty()
            personRepository.list(Pageable.from(0, 1)).size() == 1
    }

    void "test update many"() {
        given:
            savePersons(["Jeff", "James"])

        when:
            def people = personRepository.findAll().toList()
            people.forEach() { it.name = it.name + " updated" }
            personRepository.updateAll(people)
            people = personRepository.findAll().toList()

        then:
            people.get(0).name.endsWith(" updated")
            people.get(1).name.endsWith(" updated")

        when:
            people = personRepository.findAll().toList()
            people.forEach() { it.name = it.name + " X" }
            def peopleUpdated = personRepository.updatePeople(people)
            people = personRepository.findAll().toList()

        then:
            peopleUpdated.size() == 2
            people.get(0).name.endsWith(" X")
            people.get(1).name.endsWith(" X")
            peopleUpdated.get(0).name.endsWith(" X")
            peopleUpdated.get(1).name.endsWith(" X")
    }

    void "test delete by id"() {
        given:
            savePersons(["Jeff", "James"])

        when: "an entity is retrieved"
            def person = personRepository.findByName("Jeff")

        then: "the person is not null"
            person != null
            person.name == 'Jeff'
            personRepository.findById(person.id).isPresent()

        when: "the person is deleted"
            personRepository.deleteById(person.id)

        then: "They are really deleted"
            !personRepository.findById(person.id).isPresent()
            old(personRepository.count()) - 1 == personRepository.count()
    }

    void "test delete by id and author id"() {
        given:
            setupBooks()
            def book = bookRepository.findByTitle("Pet Cemetery")
        when:
            int deleted = bookRepository.deleteByIdAndAuthorId(book.id, book.author.id)
        then:
            deleted == 1
            !bookRepository.findById(book.id).isPresent()
    }

    void "test delete by multiple ids"() {
        given:
            savePersons(["Jeff", "James"])

        when: "A search for some people"
            def people = personRepository.findByNameRegex(/^J/)

        then:
            people.size() == 2

        when: "the people are deleted"
            personRepository.deleteAll(people)

        then: "Only the correct people are deleted"
            old(personRepository.count()) - 2 == personRepository.count()
            people.every { !personRepository.findById(it.id).isPresent() }
    }

    void "test delete one"() {
        given:
            savePersons(["Bob"])

        when: "A specific person is found and deleted"
            def bob = personRepository.findByName("Bob")

        then: "The person is present"
            bob != null

        when: "The person is deleted"
            personRepository.delete(bob)

        then: "They are deleted"
            !personRepository.findById(bob.id).isPresent()
            old(personRepository.count()) - 1 == personRepository.count()
    }

    void "test update one"() {
        given:
            savePersons(["Jeff", "James"])

        when: "A person is retrieved"
            def fred = personRepository.findByName("Jeff")

        then: "The person is present"
            fred != null

        when: "The person is updated"
            personRepository.updatePerson(fred.id, "Jack")

        then: "the person is updated"
            personRepository.findByName("Jeff") == null
            personRepository.findByName("Jack") != null

        when: "an update is issued that returns a number"
            def updated = personRepository.updateByName("Jack", 20)

        then: "The result is correct"
            updated == 1
            personRepository.findByName("Jack").age == 20

        when: "A whole entity is updated"
            def jack = personRepository.findByName("Jack")
            jack.setName("Jeffrey")
            jack.setAge(30)
            personRepository.update(jack)

        then:
            personRepository.findByName("Jack") == null
            personRepository.findByName("Jeffrey").age == 30
    }

    void "test delete all"() {
        given:
            int personsWithG = personRepository.findByNameRegex("/^G/").size()

        when: "A new person is saved"
            personRepository.save("Greg", 30)
            personRepository.save("Groot", 300)

        then: "The count is "
            old(personRepository.count()) + 2 == personRepository.count()

        when: "batch delete occurs"
            def deleted = personRepository.deleteByNameRegex(/^G/)

        then: "The count is back to 1 and it entries were deleted"
            deleted == personsWithG + 2
            old(personRepository.count()) - (personsWithG + 2) == personRepository.count()

        when: "everything is deleted"
            personRepository.deleteAll()

        then: "data is gone"
            personRepository.count() == 0
    }

    void "test update method variations"() {
        when:
            def person = personRepository.save("Groot", 300)

        then:
            old(personRepository.count()) + 1 == personRepository.count()

        when:
            long result = personRepository.updatePersonCount(person.id, "Greg")

        then:
            personRepository.findByName("Greg")
            result == 1
    }

    void "test is null or empty"() {
        given:
            setupBooks()

        expect:
            bookRepository.count() == 8
            bookRepository.findByAuthorIsNull().size() == 2
            bookRepository.findByAuthorIsNotNull().size() == 6
            bookRepository.countByTitleIsEmpty() == 1
            bookRepository.countByTitleIsNotEmpty() == 7
    }

    void "test order by association"() {
        given:
            setupBooks()

        when:"Sorting by an assocation"
            def page = bookRepository.findAll(Pageable.from(Sort.of(
                    Sort.Order.desc("author.name")
            )))

        then:
            page.content
    }

    void "test project on single property"() {
        given:
            setupBooks()

            // cleanup invalid titles
            bookRepository.deleteByTitleIsEmptyOrTitleIsNull()

            personRepository.save(new Person(name: "Jeff", age: 40))
            personRepository.saveAll([
                    new Person(name: "Ivan", age: 30),
                    new Person(name: "James", age: 35)
            ])

        expect:
            bookRepository.findTop3OrderByTitle().size() == 3
            bookRepository.findTop3OrderByTitle()[0].title == 'Along Came a Spider'
            personRepository.countByAgeGreaterThan(33) == 2
            personRepository.countByAgeLessThan(33) == 1
            personRepository.findAgeByName("Jeff") == 40
            personRepository.findAgeByName("Ivan") == 30
//            personRepository.findMaxAgeByNameLike("J%") == 40
//            personRepository.findMinAgeByNameLike("J%") == 35
//            personRepository.getSumAgeByNameLike("J%") == 75
//            personRepository.getAvgAgeByNameLike("J%") == 37
//            personRepository.readAgeByNameLike("J%").sort() == [35,40]
//            personRepository.findByNameLikeOrderByAge("J%")*.age == [35,40]
//            personRepository.findByNameLikeOrderByAgeDesc("J%")*.age == [40,35]
    }

    void "test null argument handling" () {
        given:
            savePersons(["Jeff", "James"])
            setupBooks()

        when:
            personRepository.countByAgeGreaterThan(null) == 2

        then:
            def e = thrown(IllegalArgumentException)
            e.message == 'Argument [wrapper] value is null and the method parameter is not declared as nullable'

        when:
            def author = authorRepository.findByName("Stephen King")
            authorRepository.updateNickname(author.id, "SK")
            author = authorRepository.findByName("Stephen King")

        then:
            author.nickName == 'SK'

        when:
            authorRepository.updateNickname(author.id, null)
            author = authorRepository.findByName("Stephen King")

        then:
            author.nickName == null
    }

    void "test project on single ended association"() {
        given:
            setupBooks()

        expect:
            bookRepository.count() == 8
            bookRepository.queryTop3ByAuthorNameOrderByTitle("Stephen King")
                    .stream().findFirst().get().title == "Pet Cemetery"
            bookRepository.queryTop3ByAuthorNameOrderByTitle("Stephen King")
                    .size() == 2
            authorRepository.findByBooksTitle("The Stand").name == "Stephen King"
            authorRepository.findByBooksTitle("The Border").name == "Don Winslow"
            bookRepository.findByAuthorName("Stephen King").size() == 2
    }

    void "test join on single ended association"() {
        given:
            setupBooks()

        when:
            def book = bookRepository.findByTitle("Pet Cemetery")

        then:
            book != null
            book.title == "Pet Cemetery"
            book.author != null
            book.author.id != null
            book.author.name == "Stephen King"
    }

    void "test join on many ended association"() {
        given:
            saveSampleBooks()

        when:
            def author = authorRepository.searchByName("Stephen King")

        then:
            author != null
            author.books.size() == 2
            author.books.find { it.title == "The Stand"}
            author.books.find { it.title == "Pet Cemetery"}

        when:
            def allAuthors = CollectionUtils.iterableToList(authorRepository.findAll())

        then:
            allAuthors.size() == 3
            allAuthors.collect {it.books }.every { it.isEmpty() }
    }

    void "test one-to-many mappedBy"() {
        when:"a one-to-many is saved"
            def author = new Author()
            author.name = "author"

            def book1 = new Book()
            book1.title = "Book1"
            def page1 = new Page()
            page1.num = 1
            book1.getPages().add(page1)

            def book2 = new Book()
            book2.title = "Book2"
            def page21 = new Page()
            def page22 = new Page()
            page21.num = 21
            page22.num = 22
            book2.getPages().add(page21)
            book2.getPages().add(page22)

            def book3 = new Book()
            book3.title = "Book3"
            def page31 = new Page()
            def page32 = new Page()
            def page33 = new Page()
            page31.num = 31
            page32.num = 32
            page33.num = 33
            book3.getPages().add(page31)
            book3.getPages().add(page32)
            book3.getPages().add(page33)

            author.getBooks().add(book1)
            author.getBooks().add(book2)
            author.getBooks().add(book3)
            author = authorRepository.save(author)

        then: "They are saved correctly"
            author.id
            book1.prePersist == 1
            book1.postPersist == 1
            book2.prePersist == 1
            book2.postPersist == 1
            book3.prePersist == 1
            book3.postPersist == 1
            book3.preUpdate == 0
            book3.postUpdate == 0
            book3.preRemove == 0
            book3.postRemove == 0
            book3.postLoad == 0

        when:"retrieving an author"
            author = authorRepository.findById(author.id).orElse(null)

        then:"the associations are correct"
            author.getBooks().size() == 3
            author.getBooks()[0].postLoad == 1
            author.getBooks()[1].postLoad == 1
            author.getBooks()[2].postLoad == 1
            author.getBooks()[0].prePersist == 0
            author.getBooks()[0].postPersist == 0
            author.getBooks()[0].preUpdate == 0
            author.getBooks()[0].postUpdate == 0
            author.getBooks()[0].preRemove == 0
            author.getBooks()[0].postRemove == 0

//            def result1 = author.getBooks().find {book -> book.title == "Book1" }
//            result1.pages.size() == 1
//            result1.pages.find {page -> page.num = 1}
//
//            def result2 = author.getBooks().find {book -> book.title == "Book2" }
//            result2.pages.size() == 2
//            result2.pages.find {page -> page.num = 21}
//            result2.pages.find {page -> page.num = 22}
//
//            def result3 = author.getBooks().find {book -> book.title == "Book3" }
//            result3.pages.size() == 3
//            result3.pages.find {page -> page.num = 31}
//            result3.pages.find {page -> page.num = 32}
//            result3.pages.find {page -> page.num = 33}

        when:
            def newBook = new Book()
            newBook.title = "added"
            author.getBooks().add(newBook)
            authorRepository.update(author)

        then:
            newBook.id
            bookRepository.findById(newBook.id).isPresent()

        when:
            author = authorRepository.findById(author.id).get()

        then:
            author.getBooks().size() == 4

        when:
            authorRepository.delete(author)
        then:
            author.getBooks().size() == 4
            author.getBooks()[0].postLoad == 1
            author.getBooks()[0].prePersist == 0
            author.getBooks()[0].postPersist == 0
            author.getBooks()[0].preUpdate == 0
            author.getBooks()[0].postUpdate == 0
//     TODO: Consider whether to support cascade removes
//        author.getBooks()[0].preRemove == 1
//        author.getBooks()[0].postRemove == 1
    }

    void "test IN queries"() {
        given:
            setupBooks()
        when:
            def books1 = bookRepository.listByTitleIn(null as Collection)
        then:
            books1.size() == 0
        when:
            def books2 = bookRepository.listByTitleIn(["The Stand", "Along Came a Spider", "FFF"] as Collection)
        then:
            books2.size() == 2
        when:
            def books3 = bookRepository.listByTitleIn([] as Collection)
        then:
            books3.size() == 0
        when:
            def books4 = bookRepository.listByTitleIn(null as String[])
        then:
            books4.size() == 0
        when:
            def books5 = bookRepository.listByTitleIn(new String[] {"The Stand", "Along Came a Spider", "FFF"})
        then:
            books5.size() == 2
        when:
            def books6 = bookRepository.listByTitleIn(new String[0])
        then:
            books6.size() == 0
    }

    void "test string array data type"() {
        given:
            setupBooks()
        when:
            def books4 = bookRepository.listByTitleIn(["The Stand", "FFF"])
        then:
            books4.size() == 1
        when:
            def books5 = bookRepository.listByTitleIn(["Xyz", "FFF"])
        then:
            books5.size() == 0
        when:
            def books6 = bookRepository.listByTitleIn([])
        then:
            books6.size() == 0
        when:
            def books7 = bookRepository.listByTitleIn(null as Collection)
        then:
            books7.size() == 0
        when:
            def books8 = bookRepository.findByTitleIn(new String[] {"Xyz", "Ffff", "zzz"})
        then:
            books8.size() == 0
        when:
            def books9 = bookRepository.findByTitleIn(new String[] {})
        then:
            books9.size() == 0
        when:
            def books11 = bookRepository.findByTitleIn(null as String[])
        then:
            books11.size() == 0
        then:
            def books12 = bookRepository.findByTitleIn(new String[] {"The Stand"})
        then:
            books12.size() == 1
    }

    def "test optimistic locking"() {
        given:
            def student = new Student("Denis")
        when:
            studentRepository.save(student)
        then:
            student.version == 0
        when:
            student = studentRepository.findById(student.getId()).get()
        then:
            student.version == 0
        when:
            student.setVersion(5)
            student.setName("Xyz")
            studentRepository.update(student)
        then:
            def e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            studentRepository.updateByIdAndVersion(student.getId(), student.getVersion(), student.getName())
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            studentRepository.delete(student)
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            studentRepository.deleteByIdAndVersionAndName(student.getId(), student.getVersion(), student.getName())
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            studentRepository.deleteByIdAndVersion(student.getId(), student.getVersion())
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            studentRepository.deleteAll([student])
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 1 got: 0"
        when:
            student = studentRepository.findById(student.getId()).get()
        then:
            student.name == "Denis"
            student.version == 0
        when:
            student.setName("Abc")
            studentRepository.update(student)
            def student2 = studentRepository.findById(student.getId()).get()
        then:
            student.version == 1
            student2.name == "Abc"
            student2.version == 1
        when:
            studentRepository.updateByIdAndVersion(student2.getId(), student2.getVersion(), "Joe")
            def student3 = studentRepository.findById(student2.getId()).get()
        then:
            student3.name == "Joe"
            student3.version == 2
        when:
            studentRepository.updateById(student2.getId(), "Joe2")
            def student4 = studentRepository.findById(student2.getId()).get()
        then:
            student4.name == "Joe2"
            student4.version == 2
        when:
            studentRepository.deleteByIdAndVersionAndName(student4.getId(), student4.getVersion(), student4.getName())
            def student5 = studentRepository.findById(student2.getId())
        then:
            !student5.isPresent()
        when:
            student = new Student("Denis2")
            studentRepository.save(student)
            studentRepository.update(student)
            studentRepository.update(student)
            studentRepository.update(student)
        then:
            student.version == 3
        when:
            student = studentRepository.findById(student.getId()).orElseThrow()
        then:
            student.version == 3
        when:
            studentRepository.delete(student)
        then:
            !studentRepository.findById(student.getId()).isPresent()
        cleanup:
            studentRepository.deleteAll()
    }

    def "test batch optimistic locking"() {
        given:
            def student1 = new Student("Denis")
            def student2 = new Student("Frank")
        when:
            studentRepository.saveAll([student1, student2])
        then:
            student1.version == 0
            student2.version == 0
        when:
            student1 = studentRepository.findById(student1.getId()).get()
            student2 = studentRepository.findById(student2.getId()).get()
        then:
            student1.version == 0
            student2.version == 0
        when:
            studentRepository.updateAll([student1, student2])
            student1 = studentRepository.findById(student1.getId()).get()
            student2 = studentRepository.findById(student2.getId()).get()
        then:
            student1.version == 1
            student2.version == 1
        when:
            studentRepository.updateAll([student1, student2])
            student1 = studentRepository.findById(student1.getId()).get()
            student2 = studentRepository.findById(student2.getId()).get()
        then:
            student1.version == 2
            student2.version == 2
        when:
            student1.setVersion(5)
            student1.setName("Xyz")
            studentRepository.updateAll([student1, student2])
        then:
            def e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 2 got: 1"
        when:
            student1 = studentRepository.findById(student1.getId()).get()
            student2 = studentRepository.findById(student2.getId()).get()
            student1.setVersion(5)
            studentRepository.deleteAll([student1, student2])
        then:
            e = thrown(OptimisticLockException)
            e.message == "Execute update returned unexpected row count. Expected: 2 got: 1"
        cleanup:
            studentRepository.deleteAll()
    }

}
