# Copyright 2023 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
from unittest import mock
from unittest.mock import call

from google.cloud.spanner_dbapi.exceptions import RetryAborted
from google.cloud.spanner_dbapi.checksum import ResultsChecksum
from google.cloud.spanner_dbapi.parsed_statement import Statement
from google.api_core.exceptions import Aborted
from google.cloud.spanner_dbapi import transaction_helper
from google.rpc.status_pb2 import Status
from google.rpc.code_pb2 import OK

from google.cloud.spanner_dbapi.transaction_helper import TransactionHelper


class TestTransactionHelper(unittest.TestCase):
    @mock.patch("google.cloud.spanner_dbapi.connection.Connection")
    def setUp(self, mock_connection):
        self._under_test = TransactionHelper(mock_connection)
        self._mock_connection = mock_connection

    def test_retry_transaction_checksum_mismatch(self):
        """
        Check retrying an aborted transaction with different result results in
        checksums mismatch and exception thrown.
        """

        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(statement)

        retried_row = ("field3", "field4")
        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.return_value = [retried_row]

        with self.assertRaises(RetryAborted):
            self._under_test.retry_transaction()

    def test_retry_aborted_retry(self):
        """
        Check that in case of a retried transaction aborted,
        it will be retried once again.
        """

        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(statement)

        metadata_mock = mock.Mock()
        metadata_mock.trailing_metadata.return_value = {}
        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.side_effect = [
            Aborted("Aborted", errors=[metadata_mock]),
            [row],
        ]

        self._under_test.retry_transaction()

        run_mock.assert_has_calls(
            (
                mock.call(statement),
                mock.call(statement),
            )
        )

    def test_retry_transaction_raise_max_internal_retries(self):
        """Check retrying raise an error of max internal retries."""

        transaction_helper.MAX_INTERNAL_RETRIES = 0
        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(statement)

        with self.assertRaises(Exception):
            self._under_test.retry_transaction()

        transaction_helper.MAX_INTERNAL_RETRIES = 50

    def test_retry_aborted_retry_without_delay(self):
        """
        Check that in case of a retried transaction failed,
        the connection will retry it once again.
        """

        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(statement)

        metadata_mock = mock.Mock()
        metadata_mock.trailing_metadata.return_value = {}
        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.side_effect = [
            Aborted("Aborted", errors=[metadata_mock]),
            [row],
        ]
        self._under_test._get_retry_delay = mock.Mock(return_value=False)

        self._under_test.retry_transaction()

        run_mock.assert_has_calls(
            (
                mock.call(statement),
                mock.call(statement),
            )
        )

    def test_retry_transaction_w_multiple_statement(self):
        """Check retrying an aborted transaction having multiple statements."""

        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        statement = Statement("SELECT 1", [], {}, checksum)
        statement1 = Statement("SELECT 2", [], {}, checksum)
        self._under_test._single_statements.append(statement)
        self._under_test._single_statements.append(statement1)
        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.return_value = [row]
        retried_checksum = checksum

        with mock.patch(
            "google.cloud.spanner_dbapi.transaction_helper._compare_checksums"
        ) as compare_mock:
            self._under_test.retry_transaction()

        compare_mock.assert_called_with(checksum, retried_checksum)
        run_mock.assert_has_calls([call(statement), call(statement1)])

    def test_retry_transaction_w_empty_response(self):
        """Check retrying an aborted transaction with empty response."""

        row = ()
        checksum = ResultsChecksum()
        statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(statement)
        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.return_value = [row]
        retried_checksum = ResultsChecksum()
        retried_checksum.consume_result(row)

        with mock.patch(
            "google.cloud.spanner_dbapi.transaction_helper._compare_checksums"
        ) as compare_mock:
            self._under_test.retry_transaction()

        compare_mock.assert_called_with(checksum, retried_checksum)
        run_mock.assert_called_with(statement)

    def test_retry_transaction_batch_statements_checksum_match(self):
        """
        Check retrying an aborted transaction with same result, results in
        checksums match.
        """

        res = (1, 1)
        checksum = ResultsChecksum()
        checksum.consume_result(res)
        checksum.consume_result(OK)
        statement1 = Statement("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)
        statement2 = Statement("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None)
        self._under_test._batch_statements_list.append(
            ([statement1, statement2], checksum)
        )

        mock_transaction = mock.MagicMock()
        self._under_test._connection.transaction_checkout = mock.Mock(
            return_value=mock_transaction
        )
        mock_transaction.batch_update = mock.Mock(return_value=(Status(code=OK), res))

        self._under_test.retry_transaction()

        mock_transaction.batch_update.assert_called_with(
            [
                (("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)),
                ("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None),
            ]
        )

    def test_retry_transaction_w_multiple_batch_statements(self):
        """Check retrying an aborted transaction having multiple statements."""

        res = (1, 1)
        checksum = ResultsChecksum()
        checksum.consume_result(res)
        checksum.consume_result(OK)
        statement1 = Statement("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)
        statement2 = Statement("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None)
        self._under_test._batch_statements_list.append(([statement1], checksum))
        self._under_test._batch_statements_list.append(([statement2], checksum))

        mock_transaction = mock.MagicMock()
        self._under_test._connection.transaction_checkout = mock.Mock(
            return_value=mock_transaction
        )
        mock_transaction.batch_update = mock.Mock(return_value=(Status(code=OK), res))

        self._under_test.retry_transaction()

        mock_transaction.batch_update.assert_has_calls(
            [
                call(
                    [
                        (("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)),
                    ]
                ),
                call(
                    [
                        ("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None),
                    ]
                ),
            ]
        )

    def test_retry_transaction_batch_statements_checksum_mismatch(self):
        """
        Check retrying an aborted transaction with different result, results in
        checksums mismatch and exception thrown.
        """

        res = (1, 1)
        checksum = ResultsChecksum()
        checksum.consume_result(res)
        checksum.consume_result(OK)
        statement1 = Statement("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)
        statement2 = Statement("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None)
        self._under_test._batch_statements_list.append(
            ([statement1, statement2], checksum)
        )

        retried_res = (2, 3)
        mock_transaction = mock.MagicMock()
        self._under_test._connection.transaction_checkout = mock.Mock(
            return_value=mock_transaction
        )
        mock_transaction.batch_update = mock.Mock(
            return_value=(Status(code=OK), retried_res)
        )

        with self.assertRaises(RetryAborted):
            self._under_test.retry_transaction()

    def test_batch_statements_retry_aborted_retry(self):
        """
        Check that in case of a retried transaction aborted,
        it will be retried once again.
        """

        res = 1
        checksum = ResultsChecksum()
        checksum.consume_result(res)
        checksum.consume_result(OK)
        statement1 = Statement("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)
        self._under_test._batch_statements_list.append(([statement1], checksum))

        metadata_mock = mock.Mock()
        metadata_mock.trailing_metadata.return_value = {}
        mock_transaction = mock.MagicMock()
        self._under_test._connection.transaction_checkout = mock.Mock(
            return_value=mock_transaction
        )
        mock_transaction.batch_update.side_effect = [
            Aborted("Aborted", errors=[metadata_mock]),
            (Status(code=OK), res),
        ]

        self._under_test.retry_transaction()

        statement_tuple = ("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)
        mock_transaction.batch_update.assert_has_calls(
            (
                mock.call([statement_tuple]),
                mock.call([statement_tuple]),
            )
        )

    def test_transaction_w_both_batch_and_non_batch_statements(self):
        """
        Check transaction having both batch and non batch type of statements and
        having same result in retry succeeds.
        """

        row = ("field1", "field2")
        checksum = ResultsChecksum()
        checksum.consume_result(row)
        single_statement = Statement("SELECT 1", [], {}, checksum)
        self._under_test._single_statements.append(single_statement)

        res = (1, 1)
        checksum = ResultsChecksum()
        checksum.consume_result(res)
        checksum.consume_result(OK)
        batch_statement_1 = Statement(
            "INSERT INTO T (f1, f2) VALUES (1, 2)", None, None
        )
        batch_statement_2 = Statement(
            "INSERT INTO T (f1, f2) VALUES (3, 4)", None, None
        )
        self._under_test._batch_statements_list.append(
            ([batch_statement_1, batch_statement_2], checksum)
        )

        run_mock = self._under_test._connection.run_statement = mock.Mock()
        run_mock.return_value = [row]
        mock_transaction = mock.MagicMock()
        self._under_test._connection.transaction_checkout = mock.Mock(
            return_value=mock_transaction
        )
        mock_transaction.batch_update = mock.Mock(return_value=(Status(code=OK), res))

        self._under_test.retry_transaction()

        run_mock.assert_called_with(single_statement)
        mock_transaction.batch_update.assert_called_with(
            [
                (("INSERT INTO T (f1, f2) VALUES (1, 2)", None, None)),
                ("INSERT INTO T (f1, f2) VALUES (3, 4)", None, None),
            ]
        )
