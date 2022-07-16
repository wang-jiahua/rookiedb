package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // if current type can substitute requested type, we've done
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        // current type cannot substitute requested type
        // if current type is IX and requested type is S, promote it to SIX
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            requestType = LockType.SIX;
            getIndent(parentContext, requestType);
            lockContext.promote(transaction, requestType);
            return;
        }
        // if current type is IS / IX / SIX, escalate it to S / X
        if (explicitLockType.isIntent()) {
            getIndent(parentContext, requestType);
            lockContext.escalate(transaction);
            return;
        }
        // if current type is NL / S / X, its children hold no lock.
        // acquire lock on it and corresponding indent lock on ancestors.
        getIndent(parentContext, requestType);
        getLock(lockContext, requestType);
    }

    // TODO(proj4_part2) add any helper methods you want

    /**
     * get corresponding indent lock of `requestType` on `lockContext` recursively
     * @param lockContext the given lock context
     * @param requestType the requested lock type
     */
    private static void getIndent(LockContext lockContext, LockType requestType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) {
            return;
        }
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        LockType indentLockType = LockType.parentLock(requestType);
        if (LockType.substitutable(explicitLockType, indentLockType)) {
            return;
        }
        LockContext parentContext = lockContext.parentContext();
        if (explicitLockType == LockType.S && indentLockType == LockType.IX) {
            indentLockType = LockType.SIX;
        }
        getIndent(parentContext, indentLockType);
        getLock(lockContext, indentLockType);
    }

    /**
     * get lock of `requestType` on `lockContext`
     * @param lockContext the given lock context
     * @param lockType the requested lock type
     */
    private static void getLock(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) {
            return;
        }
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else {
            lockContext.promote(transaction, lockType);
        }
    }
}
