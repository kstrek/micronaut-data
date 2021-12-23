
package example;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.micronaut.data.document.mongodb.database.MongoDatabaseFactory;
import io.micronaut.transaction.SynchronousTransactionManager;
import jakarta.inject.Singleton;

@Singleton
public class ProductManager {

    private final ClientSession clientSession;
    private final MongoDatabaseFactory mongoDatabaseFactory;
    private final SynchronousTransactionManager<ClientSession> transactionManager;

    public ProductManager(ClientSession clientSession,
                          MongoDatabaseFactory mongoDatabaseFactory,
                          SynchronousTransactionManager<ClientSession> transactionManager) { // <1>
        this.clientSession = clientSession;
        this.mongoDatabaseFactory = mongoDatabaseFactory;
        this.transactionManager = transactionManager;
    }

    Product save(String name, Manufacturer manufacturer) {
        return transactionManager.executeWrite(status -> { // <2>
            MongoDatabase database = mongoDatabaseFactory.getDatabase(Product.class);
            final Product product = new Product(name, manufacturer);
            database.getCollection("product", Product.class).insertOne(clientSession, product);
            return product;
        });
    }

    Product find(String name) {
        return transactionManager.executeRead(status -> { // <3>
            MongoDatabase database = mongoDatabaseFactory.getDatabase(Product.class);
            return database.getCollection("product", Product.class).find(clientSession, Filters.eq("name", name)).first();
        });
    }
}
