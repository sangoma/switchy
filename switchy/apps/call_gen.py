# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Call generator app for load testing
"""
from __future__ import division
import time
import sched
import traceback
from itertools import cycle, chain
from collections import namedtuple, deque, Counter
from threading import Thread
import multiprocessing as mp
from .. import utils
from .. import marks
from ..observe import EventListener, Client
from ..distribute import SlavePool


def get_pool(contacts, **kwargs):
    """Construct and return a slave pool from a sequence of
    contact information
    """
    SlavePair = namedtuple("SlavePair", "client listener")
    pairs = deque()

    # instantiate all pairs
    for contact in contacts:
        if isinstance(contact, (basestring)):
            contact = (contact,)
        # create pairs
        listener = EventListener(*contact, **kwargs)
        client = Client(*contact, listener=listener)
        pairs.append(SlavePair(client, listener))

    return SlavePool(pairs)


def get_originator(contacts, *args, **kwargs):
    """Originator factory
    """
    if isinstance(contacts, str):
        contacts = (contacts,)

    # pop kwargs destined for the listener
    argname, kwargnames = utils.get_args(EventListener.__init__)
    lkwargs = {}
    for name in kwargnames:
        if name in kwargs:
            lkwargs[name] = kwargs.pop(name)

    slavepool = get_pool(contacts, **lkwargs)
    return Originator(slavepool, *args, **kwargs)


def limiter(pairs):
    """Yield slave pairs up until a slave has reached a number of calls
    less then or equal to it's predefined capacity limit
    """
    for pair in cycle(pairs):
        l = pair.listener
        if l.count_calls() > l.max_limit:
            continue
        yield pair


class WeightedIterator(object):
    """Pseudo weighted round robin iterator. Delivers items interleaved
    in weighted order.
    """
    def __init__(self, counter=None):
        self.weights = counter or Counter()
        self.counts = self.weights.copy()
        attr = '__getitem__'
        setattr(self.__class__, attr, getattr(self.weights, attr))

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, repr(self.weights))

    def __setitem__(self, key, value):
        self.weights[key] = value
        self.counts = self.weights.copy()

    def cycle(self):
        """Endlessly iterates the most up to date keys in `counts`.
        Allows for real-time weight updating from another thread.
        """
        while True:
            if not self.weights:
                raise StopIteration("no items in multiset")
            for item in self.counts:
                yield item

    def __iter__(self):
        """Iterate items in interleaved order until a particular item's weight
        has been exhausted
        """
        for item in self.cycle():
            if self.counts[item] > 0:
                yield item
                self.counts[item] -= 1

            # all counts have expired so reset
            if not any(self.counts.values()):
                self.counts = self.weights.copy()


class State(object):
    """Enumeration to represent the originator state machine
    """
    __slots__ = ['value']

    INITIAL = 0  # Originator is initialized and is awaiting start command
    ORIGINATING = 1  # Calls are currently being originated
    STOPPED = 2  # There are no more active calls/sessions and bgjobs

    def __init__(self, state=INITIAL):
        self.value = state

    def __str__(self):
        names, vals = zip(*self.__class__.__dict__.items())
        index = vals.index(self.value)
        return names[index]
        return names[vals.index(self.value)]


class Originator(object):
    """An automatic session generator
    """
    default_settings = {
        'rate': 30,  # call offer rate in cps
        'limit': 1,  # concurrent calls limit (i.e. erlangs)
        'max_offered': float('inf'),  # max offered calls
        'duration': 0,
        'period': 1,
        'uuid_gen': utils.uuid,
    }

    def __init__(self, slavepool, debug=False, auto_duration=True,
                 app_id=None, apps=None, **kwargs):
        '''
        Parameters
        ----------
        slavepool : SlavePool instance
            slave pool of (Client, EventListener) pairs to use for call
            generation
        debug : bool
            toggle debug logging on the slave servers
        auto_duration : bool
            determines whether to recalculate call hold times when adjusting
            rate or limit setting
        app_id : str
            id to use
        '''
        self.pool = slavepool
        self.iterslaves = limiter(slavepool.nodes)
        self.count_calls = self.pool.fast_count
        self.debug = debug
        self.auto_duration = auto_duration
        self.server = self.pool.evals('client.server')
        self.log = utils.get_logger(utils.pstr(self))
        self._thread = None
        self._start = mp.Event()
        self._exit = mp.Event()
        self._state = State()
        # load settings
        self._rate = None
        self._limit = None
        self._duration = None
        self._max_rate = 250  # a realistic hard cps limit
        self.duration_offset = 5  # calls must be at least 5 secs

        # attempt measurement capture setup
        try:
            from measure.metrics import new_array
        except ImportError:
            if not self.log.handlers:
                utils.log_to_stderr()
            self.log.warn(
                "Numpy is not installed; no call metrics will be collected"
            )
            self.metrics = None
        else:
            self.metrics = new_array()  # shared by whole cluster

        # don't worry so much about call state for load testing
        self.pool.evals('listener.unsubscribe("CALL_UPDATE")')
        self.pool.evals('listener.connect()')
        self.pool.evals('client.connect()')

        self.app_weights = WeightedIterator()
        if apps:
            self.load_app(apps, with_metrics=True)
        self.iterappids = iter(self.app_weights)

        # apply default load settings
        if len(kwargs):
            self.log.debug("kwargs contents : {}".format(kwargs))

        # assign instance vars
        for name, val in type(self).default_settings.iteritems():
            setattr(self, name, kwargs.pop(name, None) or val)

        if len(kwargs):
            raise TypeError("Unsupported kwargs: {}".format(kwargs))

        # burst loop scheduler
        self.sched = sched.scheduler(time.time, time.sleep)
        self.setup()
        # counters
        self._total_originated_sessions = 0

    # XXX: instead make this a `prepost` hook?
    def setup(self):
        """Apply load test settings on the slave server
        """
        # Raise the sps and max_sessions limit so they do not obstruct our
        # load settings
        self.pool.evals('client.api("fsctl sps {}")'.format(10000))
        self.pool.evals('client.api("fsctl max_sessions {}")'.format(10000))
        self.pool.evals('client.api("fsctl verbose_events true")')

        # Reduce logging level to avoid too much output in console/logfile
        if self.debug is True:
            self.log.info("setting debug logging on slaves!")
            self.pool.evals('client.api("fsctl loglevel debug")')
            self.pool.evals('client.api("console loglevel debug")')
        else:
            self.pool.evals('client.api("fsctl loglevel warning")')
            self.pool.evals('client.api("console loglevel warning")')

    def load_app(self, apps, app_id=None, weight=1, with_metrics=True):
        """Load app(s) for use across all slaves in the cluster
        """
        for app in apps:
            try:
                app, ppkwargs = app  # user can optionally pass doubles
            except TypeError:
                ppkwargs = {}

            # load each app under a common id
            app_id = self.pool.evals(
                'client.load_app(app, on_value=appid, **prepostkwargs)',
                app=app, appid=app_id, prepostkwargs=ppkwargs)[0]

        # always register our local callbacks for each app
        self.pool.evals('client.load_app(Originator, on_value=appid)',
                        Originator=self, appid=app_id)

        if with_metrics and self.metrics:
                from measure import Metrics
                self.pool.evals(
                    ('''client.load_app(
                            Metrics, on_value=appid, array=array, pool=pool
                    )'''),
                    Metrics=Metrics, array=self.metrics, appid=app_id,
                    pool=self.pool
                )

        self.app_weights[app_id] = weight

    def iterapps(self):
        """Iterable over all unique contained subapps
        """
        return set(
            app for app_map in chain.from_iterable(
                self.pool.evals('client._apps.values()')
            )
            for app in app_map.values()
        )

    @property
    def max_rate(self):
        """The maximum `rate` value which can be set.
        Setting `rate` any higher will simply clip to this value.
        """
        return self._max_rate

    def _get_rate(self):
        return self._rate

    def _set_rate(self, value):
        burst_rate = float(min(self.max_rate, value))
        # set the 'inter-burst-period' accounting for surrounding processing
        # latencies by a small %
        self.ibp = 1 / burst_rate * 0.90
        self._rate = value

        # update any sub-apps
        for app in self.iterapps():
            callback = getattr(app, '__setrate__', None)
            if callback:
                # XXX assumes equal delegation to all slaves
                callback(value / float(len(self.pool)))

        if self.auto_duration and self.limit:
            self.duration = self.limit / value + self.duration_offset

    rate = property(_get_rate, _set_rate, "Call rate (cps)")

    def _get_limit(self):
        return self._limit

    # TODO: auto_duration should be applied via decorator?
    def _set_limit(self, value):
        # update any sub-apps
        for app in self.iterapps():
            callback = getattr(app, '__setlimit__', None)
            if callback:
                callback(value)

        self._limit = value
        if self.auto_duration:
            self.duration = value / self.rate + self.duration_offset

    limit = property(_get_limit, _set_limit,
                     "Number of simultaneous calls allowed  (i.e. erlangs)")

    def _get_duration(self):
        return self._duration

    def _set_duration(self, value):
        # update any sub-apps
        for app in self.iterapps():
            callback = getattr(app, '__setduration__', None)
            if callback:
                callback(value)
        # apply the new duration
        self._duration = value

    duration = property(_get_duration, _set_duration, "Call duration (secs)")

    def __dir__(self):
        return utils.dirinfo(self)

    def __repr__(self):
        """Repr with [<state>] <load_settings> slapped in
        """
        props = "state total_originated_sessions rate limit max_offered duration".split()
        rep = type(self).__name__
        return "<{}: active-calls={} {}>".format(
            rep, self.pool.fast_count(),
            " ".join("{}={}".format(
                attr.replace('_', '-'), getattr(self, attr)) for attr in props)
        )

    def _stop_on_none(self):
        if self.pool.count_jobs() == 0 and self.pool.count_sessions() == 0:
            self.log.info('all sessions have ended...')
            self._change_state("STOPPED")

    @marks.event_callback("BACKGROUND_JOB")
    def _handle_bj(self, sess, job):
        '''Check for all jobs complete
        '''
        # if duration == 0 then never schedule hangup events
        if not sess.call.vars.get('noautohangup') and self.duration:
            self.log.debug("scheduling auto hangup for '{}'"
                           .format(sess.uuid))
            # schedule hangup
            sess.sched_hangup(self.duration - (sess.time - sess.create_time))

        # failed jobs and sessions should be popped in the listener's
        # default bg job handler
        self._stop_on_none()

    @marks.event_callback("CHANNEL_HANGUP")
    def _handle_hangup(self, *args):
        self._stop_on_none()

    @marks.event_callback("CHANNEL_ORIGINATE")
    def _handle_originate(self, sess):
        '''Set the call duration
        '''
        # use our tx for session commands
        # with sess(self.ctl._con):
        # sess.con = self.ctl._con

        # if max sessions are already up, stop
        self._total_originated_sessions += 1
        self._check_max()
        # remove our con from the session
        # sess.con = None

    def _check_max(self):
        if self._total_originated_sessions >= self.max_offered:
            self._change_state("STOPPED")
            self.log.info("'{}' sessions have been originated but"
                          " max allowed is '{}', exiting run loop..."
                          .format(self._total_originated_sessions,
                                  self.max_offered))

    @property
    def total_originated_sessions(self):
        return self._total_originated_sessions

    def _burst(self):
        '''Originate calls via a bgapi/originate call in a loop
        '''
        originated = 0
        count_calls = self.count_calls
        iterappids = self.iterappids
        num = min((self.limit - count_calls(), self.rate))
        self.log.debug("bursting num originates = {}".format(num))
        if num <= 0:
            self.log.debug(
                "maximum simultaneous sessions limit '{}' reached..."
                .format(self.limit))

        # TODO: need a proper traffic scheduling algo here!
        # try to launch 'rate' calls in a loop
        for _, slave in zip(range(num), self.iterslaves):
            if not self.check_state("ORIGINATING"):
                break
            if count_calls() >= self.limit:
                break
            self.log.debug("count calls = {}".format(count_calls()))
            # originate a call
            job = slave.client.originate(
                app_id=next(iterappids),
                uuid_func=self.uuid_gen
            )
            originated += 1
            # limit the max transmission rate
            time.sleep(self.ibp)

        if originated > 0:
            self.log.debug('Requested {} new sessions'
                           .format(originated))

    def _serve_forever(self):
        """Asynchronous mode process entry point and
        call burst loop. This method blocks until all calls
        have finished.
        A bg thread waits in initial state until started.
        """
        try:
            while not self._exit.is_set():
                # block until start cmd recieved
                self.log.info("Waiting for start command...")
                self._start.wait()
                # check again for exit event just after start trigger
                if self._exit.is_set():
                    continue
                if not self.originate_cmd[0]:
                    raise utils.ConfigurationError(
                        "you must first set an originate command")
                # if no pending tasks, insert a burst loop
                if self.sched.empty():
                    self.sched.enter(0, 1, self._burst, [])

                # task loop
                self._change_state("ORIGINATING")
                try:
                    while not self.check_state('STOPPED'):
                        prerun = time.time()
                        # NOTE: if we ever want to schedule other types
                        # of tasks we will need to move the enterabs below
                        # into _burst as it was previously.
                        # block until there are available tasks
                        self.sched.run()
                        # schedule the next re-entry
                        if self.check_state("ORIGINATING"):
                            self.log.debug('next burst loop re-entry is in {} '
                                           'seconds'.format(self.period))
                            self.sched.enterabs(prerun + self.period, 1,
                                                self._burst, ())
                except Exception:
                    self.log.error("exiting burst loop due to exception:\n{}"
                                   .format(traceback.format_exc()))
                    self._change_state("STOPPED")

                self.log.info("stopping burst loop...")

            # exit gracefully
            self.log.info("terminating originate thread...")
        except Exception:
            self.log.error("'{}' failed with:\n{}".format(
                mp.current_process().name, traceback.format_exc()))

    @property
    def state(self):
        """The current operating state as a string
        """
        return str(self._state)

    def _change_state(self, ident):
        init_state = self.state
        if not self.check_state(ident):
            self._state.value = getattr(State, ident)
            self.log.info("State Change: '{}' -> '{}'".format(
                          init_state, self.state))

    def check_state(self, ident):
        '''Compare current state to ident
        '''
        return self._state.value == getattr(State, ident)

    def stopped(self):
        '''Return bool indicating if in the stopped state.
        '''
        return self._state.value == State.STOPPED

    def start(self):
        """
        Start the engine by notifying the worker thread to call run.

        Change State INITIAL | STOPPED -> ORIGINATING
        """
        if not any(self.pool.evals('listener.is_alive()')):
            # Do listener(s) startup here so that additional apps
            # can be loaded just prior. Currently there is a restriction
            # since new event subscriptions (as required by most apps)
            # must be issued *before* starting the observer event loop
            # since the underlying connection is mutexed.
            self.pool.evals('listener.start()')

        if self._thread is None or not self._thread.is_alive():
            self.log.debug("starting burst loop thread")
            self._thread = Thread(target=self._serve_forever,
                                  name='burst-loop')
            self._thread.daemon = True  # die with parent
            self._thread.start()
            time.sleep(0.1)
        # trigger burst loop entry
        self._start.set()
        self._start.clear()

    def is_alive(self):
        """Indicate whether the call burst thread is up
        """
        return self._thread.is_alive() if self._thread else False

    def stop(self):
        '''
        Stop originate loop if currently originating sessions.
        Change state ORIGINATING -> STOPPED
        '''
        if not self.check_state("STOPPED"):
            self.log.info("Stopping sessions origination loop...")
            self._change_state("STOPPED")
        else:
            self.log.info("Originator in '{}'' state, nothing to stop..."
                          .format(self._state))

    def hupall(self):
        '''Send the 'hupall' command to hangup all active calls.
        '''
        self.log.warn("Stopping all calls with hupall!")
        # set stopped state - no further bursts will be scheduled
        self._change_state("STOPPED")
        self.pool.evals('client.hupall()')

    def hard_hupall(self):
        """Hangup all calls for all slaves, period, even if they weren't originated by
        this instance.
        """
        return self.pool.evals("client.cmd('hupall')")

    def shutdown(self):
        '''Shutdown this originator instance and hanging up all
        active calls and triggering the burst loop to exit.
        '''
        if self.pool.count_sessions():
            self.hupall()
        self._exit.set()  # trigger exit
        self._change_state("STOPPED")

    @property
    def originate_cmd(self):
        """Originate str used for making calls
        """
        return self.pool.evals('client.originate_cmd')
