# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Dtmf tools
"""
from collections import deque, OrderedDict
from ..apps import app
from ..marks import event_callback
from ..utils import get_logger


@app
class DtmfChecker(object):
    '''Play dtmf tones as defined by the iterable attr `sequence` with
    tone `duration`. Verify the rx sequence matches what was transmitted.

    For each session which is answered start a sequence check. For any session
    that fails digit matching store it locally in the `failed` attribute.
    '''
    def prepost(self):
        self.log = get_logger(self.__class__.__name__)
        self.sequence = list(map(str, range(1, 10))) + '0 * # A B C D'.split()
        self.duration = 200  # ms
        self.total_time = len(self.sequence) * self.duration / 1000.0
        self.incomplete = OrderedDict()
        self.failed = []

    @event_callback('CHANNEL_CREATE')
    def on_create(self, sess):
        if sess.is_outbound():
            self.incomplete[sess.call] = list(reversed(self.sequence))
            sess.vars['dtmf_checked'] = False

    @event_callback('CHANNEL_PARK')
    def on_park(self, sess):
        if sess.is_inbound():
            sess.answer()

    @event_callback('CHANNEL_ANSWER')
    def on_answer(self, sess):
        if sess.is_outbound():
            self.log.info(
                "Transmitting DTMF seq '{}' for session  '{}'"
                .format(self.sequence, sess.uuid)
            )
            sess.broadcast('playback::silence_stream://0')
            sess.send_dtmf(''.join(self.sequence), self.duration)

    @event_callback('DTMF')
    def on_digit(self, sess):
        digit = str(sess['DTMF-Digit'])
        self.log.debug(
            "handling dtmf digit '{}' for {} session '{}'".
            format(digit, sess['Call-Direction'], sess.uuid)
        )
        if sess.is_inbound():
            self.log.info(
                "rx dtmf digit '{}' for session '{}'".
                format(digit, sess.uuid)
            )
            # verify expected digit
            remaining = self.incomplete[sess.call]
            try:
                expected = remaining.pop()
            except IndexError:
                self.log.err("Received unexpected extra digit '{}' for"
                             " session '{}'".format(expected, digit))
                if sess not in self.failed:
                    # rx an extra digit that wasn't expected
                    self.failed.append(sess)

            if expected != digit:
                self.log.err("Expected digit '{}', instead received '{}' for"
                             " session '{}'".format(expected, digit))
                self.failed.append(sess)
            if not remaining:  # all digits have now arrived
                self.log.debug("session '{}' completed dtmf sequence match"
                               .format(sess.uuid))
                self.incomplete.pop(sess.call)  # sequence match success
                sess.vars['dtmf_checked'] = True
