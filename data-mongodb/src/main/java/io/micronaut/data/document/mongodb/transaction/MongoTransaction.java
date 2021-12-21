package io.micronaut.data.document.mongodb.transaction;

import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;

@Internal
final class MongoTransaction implements AutoCloseable {

    private ClientSession clientSession;
    private boolean newClientSession;
    private boolean rollbackOnly = false;

    public MongoTransaction(ClientSession clientSession) {
        this.clientSession = clientSession;
    }

    public boolean hasClientSession() {
        return clientSession != null;
    }

    public void setClientSessionHolder(ClientSession clientSession, boolean newClientSession) {
        this.clientSession = clientSession;
        this.newClientSession = newClientSession;
    }

    public void beginTransaction(TransactionOptions transactionOptions) {
        clientSession.startTransaction(transactionOptions);
    }

    public void commitTransaction() {
        clientSession.commitTransaction();
    }

    public void abortTransaction() {
        clientSession.abortTransaction();
    }

    public boolean hasActiveTransaction() {
        return clientSession != null && clientSession.hasActiveTransaction();
    }

    @Nullable
    public ClientSession getClientSession() {
        return clientSession;
    }

    public boolean isNewClientSession() {
        return newClientSession;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    public void setRollbackOnly() {
        this.rollbackOnly = true;
    }

    public void close() {
        if (newClientSession) {
            newClientSession = false;
            clientSession.close();
            clientSession = null;
        }
    }
}
