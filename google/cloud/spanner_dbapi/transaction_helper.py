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

from typing import TYPE_CHECKING, List

import time
from google.api_core.exceptions import Aborted

from google.cloud.spanner_dbapi.utils import PeekIterator

if TYPE_CHECKING:
    from google.cloud.spanner_dbapi import Connection
from google.cloud.spanner_dbapi.checksum import ResultsChecksum, _compare_checksums
from google.cloud.spanner_dbapi.parsed_statement import Statement
from google.cloud.spanner_v1.session import _get_retry_delay

from google.rpc.code_pb2 import ABORTED

MAX_INTERNAL_RETRIES = 50


class TransactionHelper:
    def __init__(self, connection: "Connection"):
        self._connection = connection
        # Non-Batch statements, which were executed within the current
        # transaction
        self._single_statements: List[Statement] = []
        # Batch statements, which were executed within the current transaction
        self._batch_statements_list: List[(List[Statement], ResultsChecksum)] = []

    def retry_transaction(self):
        """Retry the aborted transaction.

        All the statements executed in the original transaction
        will be re-executed in new one. Results checksums of the
        original statements and the retried ones will be compared.

        :raises: :class:`google.cloud.spanner_dbapi.exceptions.RetryAborted`
            If results checksum of the retried statement is
            not equal to the checksum of the original one.
        """
        attempt = 0
        while True:
            self._connection._spanner_transaction_started = False
            attempt += 1
            if attempt > MAX_INTERNAL_RETRIES:
                raise

            try:
                self._rerun_previous_statements()
                self._rerun_previous_batch_statements()
                break
            except Aborted as exc:
                delay = _get_retry_delay(exc.errors[0], attempt)
                if delay:
                    time.sleep(delay)

    def _rerun_previous_batch_statements(self):
        """
        Helper to run all the remembered statements from the last transaction.
        """
        for batch_statements, original_checksum in self._batch_statements_list:
            transaction = self._connection.transaction_checkout()
            statements_tuple = []
            for single_statement in batch_statements:
                statements_tuple.append(single_statement.get_tuple())
            status, res = transaction.batch_update(statements_tuple)
            if status.code == ABORTED:
                raise Aborted(status.details)

            retried_checksum = _get_batch_statements_result_checksum(res, status.code)
            _compare_checksums(original_checksum, retried_checksum)

    def _rerun_previous_statements(self):
        for single_statement in self._single_statements:
            res_iter = self._connection.run_statement(single_statement)
            retried_checksum = _get_single_statement_result_checksum(
                PeekIterator(res_iter)
            )
            _compare_checksums(single_statement.checksum, retried_checksum)


def _get_batch_statements_result_checksum(res, status_code):
    retried_checksum = ResultsChecksum()
    retried_checksum.consume_result(res)
    retried_checksum.consume_result(status_code)
    return retried_checksum


def _get_single_statement_result_checksum(res_iter):
    retried_checksum = ResultsChecksum()
    for res in res_iter:
        retried_checksum.consume_result(res)
    return retried_checksum
