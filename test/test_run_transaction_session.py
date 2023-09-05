from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Column, DateTime, func, Integer, select, testing, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.testing import fixtures
import threading

from sqlalchemy_cockroachdb import run_transaction


class BaseRunTransactionTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Account(Base):
            __tablename__ = "account"

            acct = Column(Integer, primary_key=True, autoincrement=False)
            balance = Column(Integer)

    @classmethod
    def insert_data(cls, connection):
        Account = cls.classes.Account

        session = Session(connection)
        session.add_all([Account(acct=1, balance=100), Account(acct=2, balance=100)])
        session.commit()

    def get_balances(self, conn):
        Account = self.classes.Account

        """Returns the balances of the two accounts as a list."""
        result = []
        query = select(Account.balance).where(Account.acct.in_((1, 2))).order_by(Account.acct)
        for row in conn.execute(query):
            result.append(row.balance)
        if len(result) != 2:
            raise Exception("Expected two balances; got %d", len(result))
        return result

    def run_parallel_transactions(self, callback, conn):
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
        balances = self.get_balances(conn)
        assert balances == [100, 100], (
            "expected balances to be restored without error; " "got %s" % balances
        )


class RunTransactionSessionTest(BaseRunTransactionTest):
    __requires__ = ("sync_driver",)

    def test_run_transaction(self, connection):
        Account = self.classes.Account

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

        self.run_parallel_transactions(callback, connection)

    def test_run_transaction_retry(self):
        def txn_body(sess):
            rs = sess.execute(text("select acct, balance from account where acct = 1"))
            sess.execute(text("select crdb_internal.force_retry('1s')"))
            return [r for r in rs]

        Session = sessionmaker(testing.db)
        rs = run_transaction(Session, txn_body)
        assert rs[0] == (1, 100)


class InsertReturningTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Item(Base):
            __tablename__ = "item"
            __mapper_args__ = {"eager_defaults": True}

            id = Column(Integer, primary_key=True, autoincrement=True)
            created = Column(DateTime, server_default=func.now())

    def test_insert_returning(self):
        # This test demonstrates the use of the INSERT RETURNING
        # clause with the ORM to return server-generated values from a
        # transaction. The expire_on_commit=False option is necessary
        # to make the objects valid after the transaction has
        # completed. The eager_defaults option (set above) is
        # necessary to handle fields other than the primary key (which
        # is always loaded eagerly)

        Item = self.classes.Item

        def txn_body(session):
            item = Item()
            session.add(item)
            return item

        Session = sessionmaker(testing.db, expire_on_commit=False)
        item = run_transaction(Session, txn_body)
        assert item.id is not None
        assert item.created is not None
