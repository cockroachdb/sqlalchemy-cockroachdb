from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import Table, Column, MetaData, select, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.types import Integer
import threading

from cockroachdb.sqlalchemy import run_transaction


class RunTransactionTest(fixtures.TestBase):
    def setup_method(self, method):
        self.meta = MetaData(testing.db)
        self.tbl = Table('t', self.meta,
                         Column('acct', Integer, primary_key=True, autoincrement=False),
                         Column('balance', Integer))
        self.meta.create_all()
        testing.db.execute(self.tbl.insert(), [dict(acct=1, balance=100),
                                               dict(acct=2, balance=100)])

    def teardown_method(self, method):
        self.meta.drop_all()

    def test_run_transaction(self):
        def get_balances(conn):
            result = []
            query = (select([self.tbl.c.balance])
                     .where(self.tbl.c.acct.in_((1, 2)))
                     .order_by(self.tbl.c.acct))
            for row in conn.execute(query):
                result.append(row.balance)
            if len(result) != 2:
                raise Exception("Expected two balances; got %d", len(result))
            return result

        cv = threading.Condition()
        wait_count = [2]

        def worker():
            iters = [0]

            def txn_body(conn):
                balances = get_balances(conn)

                iters[0] += 1
                if iters[0] == 1:
                    # If this is the first iteration, wait for the other txn to also read.
                    with cv:
                        wait_count[0] -= 1
                        cv.notifyAll()
                        while wait_count[0] > 0:
                            cv.wait()

                # Now, subtract from one account and give to the other.
                if balances[0] > balances[1]:
                    conn.execute(self.tbl.update().where(self.tbl.c.acct == 1)
                                 .values(balance=self.tbl.c.balance-100))
                    conn.execute(self.tbl.update().where(self.tbl.c.acct == 2)
                                 .values(balance=self.tbl.c.balance+100))
                else:
                    conn.execute(self.tbl.update().where(self.tbl.c.acct == 1)
                                 .values(balance=self.tbl.c.balance+100))
                    conn.execute(self.tbl.update().where(self.tbl.c.acct == 2)
                                 .values(balance=self.tbl.c.balance-100))
            with testing.db.connect() as conn:
                run_transaction(conn, txn_body)
            return iters[0]

        with ThreadPoolExecutor(2) as executor:
            future1 = executor.submit(worker)
            future2 = executor.submit(worker)
            iters1 = future1.result()
            iters2 = future2.result()

        assert iters1 + iters2 > 2, ("expected at least one retry between the competing "
                                     "txns, got txn1=%d, txn2=%d" % (iters1, iters2))

        balances = get_balances(testing.db)
        assert balances == [100, 100], ("expected balances to be restored without error; "
                                        "got %s" % balances)
