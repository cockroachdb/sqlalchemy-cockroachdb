from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Table, Column, MetaData, select, testing, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer
import threading

from sqlalchemy_cockroachdb import run_transaction

""" This file is named "test_aaa_run_transaction_session.py" to ensure that it is run before any
    other tests. Testing under SQLA 1.4 revealed that it ran by itself just fine, but if *any*
    other test ran before it (even the corresponding "core" test) then it would crash with

    sqlalchemy.exc.ArgumentError: Column expression or FROM clause expected,
    got <class 'test.test_run_transaction.Account'>.
"""
# TODO: Investigate SQLA_1.4 testing configuration to try and determine why this is happening.
#       (It didn't happen with the old name under SQLA_1.3.)

meta = MetaData()

account_table = Table(
    "account",
    meta,
    Column("acct", Integer, primary_key=True, autoincrement=False),
    Column("balance", Integer),
)


class Account(declarative_base()):
    __table__ = account_table


class BaseRunTransactionTest(fixtures.TestBase):
    def setup_method(self, method):
        meta.create_all(testing.db)
        with testing.db.begin() as conn:
            conn.execute(
                account_table.insert(), [dict(acct=1, balance=100), dict(acct=2, balance=100)]
            )

    def teardown_method(self, method):
        meta.drop_all(testing.db)

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


class RunTransactionSessionTest(BaseRunTransactionTest):
    def test_run_transaction(self):
        def callback(barrier):
            Session = sessionmaker(testing.db)

            def txn_body(session):
                accounts = list(
                    session.query(Account).filter(Account.acct.in_((1, 2))).order_by(Account.acct)
                )
                barrier()
                if accounts[0].balance > accounts[1].balance:
                    accounts[0].balance -= 100
                    accounts[1].balance += 100
                else:
                    accounts[0].balance += 100
                    accounts[1].balance -= 100

            run_transaction(Session, txn_body)

        self.run_parallel_transactions(callback)

    def test_run_transaction_retry(self):
        def txn_body(sess):
            rs = sess.execute(text("select acct, balance from account where acct = 1"))
            sess.execute(text("select crdb_internal.force_retry('1s')"))
            return [r for r in rs]

        Session = sessionmaker(testing.db)
        rs = run_transaction(Session, txn_body)
        assert rs[0] == (1, 100)
