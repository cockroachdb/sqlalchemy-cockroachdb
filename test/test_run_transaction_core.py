from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Table, Column, MetaData, select, testing, text
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer
import threading
from sqlalchemy.orm import sessionmaker, scoped_session


from sqlalchemy_cockroachdb import run_transaction
from sqlalchemy_cockroachdb.transaction import ChainTransaction

meta = MetaData()

account_table = Table(
    "account",
    meta,
    Column("acct", Integer, primary_key=True, autoincrement=False),
    Column("balance", Integer),
)


class BaseRunTransactionTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        with testing.db.begin() as conn:
            conn.execute(
                account_table.insert(), [dict(acct=1, balance=100), dict(acct=2, balance=100)]
            )

    def teardown_method(self, method):
        session = scoped_session(sessionmaker(bind=testing.db))
        session.query(account_table).delete()
        session.commit()

    def get_balances(self, conn):
        """Returns the balances of the two accounts as a list."""
        result = []
        query = (
            select(account_table.c.balance)
            .where(account_table.c.acct.in_((1, 2)))
            .order_by(account_table.c.acct)
        )
        for row in conn.execute(query):
            result.append(row.balance)
        if len(result) != 2:
            raise Exception("Expected two balances; got %d", len(result))
        return result

    def run_parallel_transactions(self, callback):
        """Runs the callback in two parallel transactions.

        A barrier function is passed to the callback and should be run
        after the transaction has performed its first read. This
        synchronizes the two transactions to ensure that at least one
        of them must restart.
        """
        cv = threading.Condition()
        wait_count = [2]

        def worker():
            iters = [0]

            def barrier():
                iters[0] += 1
                if iters[0] == 1:
                    # If this is the first iteration, wait for the other txn to also read.
                    with cv:
                        wait_count[0] -= 1
                        cv.notifyAll()
                        while wait_count[0] > 0:
                            cv.wait()

            callback(barrier)
            return iters[0]

        with ThreadPoolExecutor(2) as executor:
            future1 = executor.submit(worker)
            future2 = executor.submit(worker)
            iters1 = future1.result()
            iters2 = future2.result()

        assert (
            iters1 + iters2 > 2
        ), "expected at least one retry between the competing " "txns, got txn1=%d, txn2=%d" % (
            iters1,
            iters2,
        )
        balances = self.get_balances(testing.db.connect())
        assert balances == [100, 100], (
            "expected balances to be restored without error; " "got %s" % balances
        )


class RunTransactionCoreTest(BaseRunTransactionTest):
    __requires__ = ("sync_driver",)

    def perform_transfer(self, conn, balances):
        if balances[0] > balances[1]:
            conn.execute(
                account_table.update()
                .where(account_table.c.acct == 1)
                .values(balance=account_table.c.balance - 100)
            )
            conn.execute(
                account_table.update()
                .where(account_table.c.acct == 2)
                .values(balance=account_table.c.balance + 100)
            )
        else:
            conn.execute(
                account_table.update()
                .where(account_table.c.acct == 1)
                .values(balance=account_table.c.balance + 100)
            )
            conn.execute(
                account_table.update()
                .where(account_table.c.acct == 2)
                .values(balance=account_table.c.balance - 100)
            )

    def test_run_transaction(self):
        def callback(barrier):
            def txn_body(conn):
                balances = self.get_balances(conn)
                barrier()
                self.perform_transfer(conn, balances)

            with testing.db.connect() as conn:
                run_transaction(conn, txn_body)

        self.run_parallel_transactions(callback)

    def test_run_transaction_retry(self):
        def txn_body(conn):
            rs = conn.execute(text("select acct, balance from account where acct = 1"))
            conn.execute(text("select crdb_internal.force_retry('1s')"))
            return [r for r in rs]

        with testing.db.connect() as conn:
            rs = run_transaction(conn, txn_body)
            assert rs[0] == (1, 100)

    def test_run_transaction_retry_with_nested(self):
        def txn_body(conn):
            rs = conn.execute(text("select acct, balance from account where acct = 1"))
            conn.execute(text("select crdb_internal.force_retry('1s')"))
            return [r for r in rs]

        with testing.db.connect() as conn:
            rs = run_transaction(conn, txn_body, use_cockroach_restart=False)
            assert rs[0] == (1, 100)

    def test_run_chained_transaction(self):
        def txn_body(conn):
            # first transaction inserts
            conn.execute(account_table.insert(), [dict(acct=99, balance=100)])
            conn.execute(text("select crdb_internal.force_retry('1s')"))

            def _get_val(s):
                rs = s.execute(text("select acct, balance from account where acct = 99"))
                return [r for r in rs]

            # chain the get into a separate nested transaction, so that the value
            # in the previous nested transaction is flushed and available
            return ChainTransaction([lambda s: _get_val(s), lambda s: _get_val(s)])

        with testing.db.connect() as conn:
            rs = run_transaction(conn, txn_body, use_cockroach_restart=False)
            assert len(rs.results) == 2
            assert rs.results[0][0] == (99, 100)
            assert rs.results[1][0] == (99, 100)
