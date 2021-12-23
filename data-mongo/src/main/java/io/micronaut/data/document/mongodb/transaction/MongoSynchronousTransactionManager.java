package io.micronaut.data.document.mongodb.transaction;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import io.micronaut.context.annotation.EachBean;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.transaction.TransactionDefinition;
import io.micronaut.transaction.exceptions.CannotCreateTransactionException;
import io.micronaut.transaction.exceptions.NoTransactionException;
import io.micronaut.transaction.exceptions.TransactionException;
import io.micronaut.transaction.exceptions.TransactionSystemException;
import io.micronaut.transaction.support.AbstractSynchronousTransactionManager;
import io.micronaut.transaction.support.DefaultTransactionStatus;
import io.micronaut.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@EachBean(MongoClient.class)
@Internal
public class MongoSynchronousTransactionManager extends AbstractSynchronousTransactionManager<ClientSession> {

    private final MongoClient mongoClient;

    public MongoSynchronousTransactionManager(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Nullable
    public ClientSession findConnection() {
        return (ClientSession) TransactionSynchronizationManager.getResource(mongoClient);
    }

    public void closeClientSession() {
        ClientSession clientSession = (ClientSession) TransactionSynchronizationManager.unbindResource(mongoClient);
        if (clientSession != null) {
            clientSession.close();
        }
    }

    @Override
    public ClientSession getConnection() {
        ClientSession clientSession = findConnection();
        if (clientSession == null) {
            throw new NoTransactionException("No active Mongo client session!");
        }
        return clientSession;
    }

    @Override
    protected ClientSession getConnection(Object transaction) {
        return ((MongoTransaction) transaction).getClientSession();
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        ClientSession clientSession = (ClientSession) TransactionSynchronizationManager.getResource(mongoClient);
        return new MongoTransaction(clientSession);
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        MongoTransaction mongoTransaction = (MongoTransaction) transaction;
        return mongoTransaction.hasActiveTransaction();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        MongoTransaction mongoTransaction = (MongoTransaction) transaction;
        try {
            mongoTransaction.setName(definition.getName());
            if (!mongoTransaction.hasClientSession()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Acquired ClientSession for Mongo transaction [{}]", mongoTransaction);
                }
                ClientSession clientSession = mongoClient.startSession();
                mongoTransaction.setClientSessionHolder(clientSession, true);
            }

            TransactionOptions.Builder txOptionsBuilder = TransactionOptions.builder();
            Duration timeout = determineTimeout(definition);
            if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
                txOptionsBuilder = txOptionsBuilder.maxCommitTime(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Starting Mongo transaction [{}]", transaction);
            }
            mongoTransaction.beginTransaction(txOptionsBuilder.build());

            // Bind the client session holder to the thread.
            if (mongoTransaction.isNewClientSession()) {
                TransactionSynchronizationManager.bindResource(mongoClient, mongoTransaction.getClientSession());
            }
        } catch (Throwable ex) {
            mongoTransaction.close();
            throw new CannotCreateTransactionException("Could not open Mongo client session for transaction", ex);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        MongoTransaction transaction = (MongoTransaction) status.getTransaction();
        if (transaction.isRollbackOnly()) {
            throw new TransactionException("Transaction marked as rollback only!");
        }
        if (status.isDebug()) {
            logger.debug("Committing Mongo transaction [{}]", transaction);
        }
        try {
            transaction.commitTransaction();
        } catch (Exception ex) {
            throw new TransactionSystemException("Could not commit Mongo transaction", ex);
        }
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        MongoTransaction transaction = (MongoTransaction) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Rolling back Mongo transaction [{}]", transaction);
        }
        try {
            transaction.abortTransaction();
        } catch (Exception ex) {
            throw new TransactionSystemException("Could not roll back Mongo transaction", ex);
        }
    }

    @Override
    protected Object doSuspend(Object transaction) {
        return TransactionSynchronizationManager.unbindResource(mongoClient);
    }

    @Override
    protected void doResume(@Nullable Object transaction, Object suspendedResources) {
        TransactionSynchronizationManager.bindResource(mongoClient, suspendedResources);
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {
        MongoTransaction mongoTransaction = (MongoTransaction) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Setting Mongo transaction [{}] rollback-only", mongoTransaction);
        }
        mongoTransaction.setRollbackOnly();
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        MongoTransaction mongoTransaction = (MongoTransaction) transaction;
        // Remove the client session from the thread, if exposed.
        if (mongoTransaction.isNewClientSession()) {
            TransactionSynchronizationManager.unbindResource(mongoClient);
        }
        mongoTransaction.close();
    }
}
