"""Tests for the retry, politeness, clock, dedupe, and logging of step 7..

Nothing here waits for real. The policy's backoff is measured by replacing
`asyncio.sleep` with a recorder and its jitter by replacing `random.uniform` with
a script, which is why the retry module imports `random` itself: that module
object is the only seam the schedule needs. The one place a real
clock is read is the system time provider, which is the thing under test, and
the one real suspension is a 50 ms wait used to prove that clock advances.

The politeness, dedupe, and logging tests use the real classes against a
movable clock, so the behaviour is proved end to end rather than through a
double that could agree with a wrong implementation.
"""

import asyncio
import io
import logging
import time
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import datetime, timedelta, timezone
from typing import TypeVar

import pytest

from webcrawler.domain.retry_settings import RetrySettings
from webcrawler.infrastructure.politeness.no_op_politeness_policy import (
    NoOpPolitenessPolicy)
from webcrawler.infrastructure.retry import (
    exponential_backoff_retry_policy as retry_module)
from webcrawler.infrastructure.retry.exponential_backoff_retry_policy import (
    ExponentialBackoffRetryPolicy)
from webcrawler.infrastructure.time.system_time_provider import SystemTimeProvider
from webcrawler.ports.politeness_policy import PolitenessPolicy
from webcrawler.ports.request_deduplicator import RequestDeduplicator
from webcrawler.ports.retry_policy import RetryPolicy
from webcrawler.ports.time_provider import TimeProviderFactory
from webcrawler.utils.logger import (
    HANDLER_NAME,
    ROOT_LOGGER_NAME,
    configure_logging)
from webcrawler.utils.in_memory_request_id_deduplicator import (
    InMemoryRequestIdDeduplicator)

T = TypeVar("T")

NOW = datetime(2026, 9, 26, 11, 28, 0, tzinfo=timezone.utc)
PROBE_LOGGER_NAME = ROOT_LOGGER_NAME
EPOCH_FORMATTED = "1970-01-01 00:00:00"

def build_policy(
    max_attempts: int = 3,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 8.0,
    jitter_seconds: float = 0.5,
    timeout_seconds: float = 5.0) -> ExponentialBackoffRetryPolicy:
    """Build the policy the composition root builds, with scripted numbers.

    Args:
    max_attempts: The attempt budget the policy is given.
    base_delay_seconds: The delay before the first retry, doubled per attempt.
    max_delay_seconds: The cap on that exponential term.
    jitter_seconds: The width of the uniform spread added to each delay.
    timeout_seconds: The per-attempt deadline handed to `asyncio.wait_for`.

    Returns:
    ExponentialBackoffRetryPolicy: A policy reading these settings, with no
    clock read and nothing scheduled yet.
    """
    return ExponentialBackoffRetryPolicy(
        RetrySettings(
        max_attempts=max_attempts,
        base_delay_seconds=base_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        jitter_seconds=jitter_seconds,
        timeout_seconds=timeout_seconds)
        )

class RecordingSleep:
    """Stands in for `asyncio.sleep` and records the delay instead of waiting."""

    def __init__(self) -> None:
        """Start with no recorded delay.

        Args:
        None: Nothing to configure; the delays arrive by being called.
        """
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        """Record one delay the policy asked for.

        Args:
        delay: The number of seconds the policy would have slept.
        """
        self.delays.append(delay)

    class ScriptedJitter:
        """Stands in for `random.uniform` and returns spreads in script order.

        Args:
        values: The spread to return per call, counted from zero. The last value
        repeats once the script runs out, so a test that always fails only
        says so once.
        """

        def __init__(self, values: Sequence[float]) -> None:
            """Hold the scripted spreads.

            Args:
            values: One spread per expected call to the policy's jitter.
            """
            self._values = list(values)
            self.calls: list[tuple[float, float]] = []

        def __call__(self, low: float, high: float) -> float:
            """Record the bounds asked for and return the next scripted spread.

            Args:
            low: The lower bound the policy passed, which must always be 0.0.
            high: The upper bound the policy passed, which must be the
            configured jitter width.

            Returns:
            float: The next scripted spread, or the last one once they run out.
            """
            self.calls.append((low, high))
            return self._values[min(len(self.calls), len(self._values)) - 1]

        class RecordingWaitFor:
            """Stands in for `asyncio.wait_for` and records the deadline it is given.

            Args:
            real: The real `asyncio.wait_for`, captured at import time so the
            deadline still fires once the module attribute is replaced.
            """

            def __init__(self, real: Callable[..., Awaitable[T]] = asyncio.wait_for) -> None:
                """Keep the real function to delegate to.

                Args:
                real: The real `asyncio.wait_for`, captured before it is replaced.
                """
                self.timeouts: list[float | None] = []
                self.awaitables: list[Awaitable[T]] = []
                self._real = real

            async def __call__(self, awaitable: Awaitable[T], timeout: float | None) -> T:
                """Record one bounded await, then perform it for real.

                Args:
                awaitable: The awaitable the policy handed over, one per attempt.
                timeout: The per-attempt deadline the policy applied.

                Returns:
                T: Whatever the awaitable produced.

                Raises:
                asyncio.TimeoutError: If the real deadline fires, exactly as it would
                without this recorder in the way.
                """
                self.awaitables.append(awaitable)
                self.timeouts.append(timeout)
                return await self._real(awaitable, timeout)

            def install_recorders(
                monkeypatch: pytest.MonkeyPatch,
                sleep: RecordingSleep,
                jitter: ScriptedJitter | None = None) -> None:
                """Replace the policy's two patch points for the duration of a test.

                `asyncio.sleep` is patched on the module object the policy reaches it
                through, which is the `asyncio` module itself, so a test that patches it is
                a test that has already decided not to wait. `asyncio.wait_for` is left
                alone: the deadline has to be real for an overrunning attempt to be
                abandoned.

                Args:
                monkeypatch: pytest's patcher, undone after the test.
                sleep: The recorder installed in place of `asyncio.sleep`.
                jitter: The script installed in place of `random.uniform`, or None to
                leave the real jitter in place.
                """
                monkeypatch.setattr(retry_module.asyncio, "sleep", sleep)
                if jitter is not None:
                    monkeypatch.setattr(retry_module.random, "uniform", jitter)

                    class ScriptedOperation:
                        """The injected operation: one scripted outcome per attempt.

                        Args:
                        outcomes: What each attempt does in turn: a string to return, an
                        exception to raise, or None to suspend until the policy's deadline
                        abandons it. The last entry repeats once the script runs out.
                        """

                        def __init__(self, outcomes: Sequence[str | BaseException | None]) -> None:
                            """Hold the script and start with no attempts made.

                            Args:
                            outcomes: One outcome per expected attempt.
                            """
                            self._outcomes = list(outcomes)
                            self.attempts = 0
                            self.awaitables: list[Awaitable[str]] = []

                        async def __call__(self) -> str:
                            """Record one invocation and run that attempt in a fresh coroutine.

                            The awaitables are kept so a test can prove there was one per attempt: an
                            implementation that built the awaitable once and re-awaited it would
                            raise on the second attempt, and one that reused a coroutine would show
                            a single object here.

                            Returns:
                            str: The scripted result of this attempt.

                            Raises:
                            BaseException: Whichever error the script raised for this attempt.
                            asyncio.CancelledError: When the policy's deadline abandons a
                            hanging attempt.
                            """
                            self.attempts += 1
                            attempt = self._attempt
                            self.awaitables.append(attempt)
                            return await attempt

                        async def _attempt(self) -> str:
                            """Perform the scripted behaviour of the attempt that is running now.

                            Returns:
                            str: The scripted result.

                            Raises:
                            BaseException: Whichever error the script raised for this attempt.
                            asyncio.CancelledError: When the policy's deadline abandons a
                            hanging attempt.
                            """
                            outcome = self._outcomes[min(self.attempts, len(self._outcomes)) - 1]
                            if isinstance(outcome, BaseException):
                                raise outcome
                                if outcome is None:
                                    await asyncio.get_running_loop().create_future()
                                    return outcome

                                    class MovableClock(TimeProviderFactory):
                                        """A `TimeProviderFactory` whose instant the test moves by hand.

                                        Extends the clock port ABC, exactly as the shipped `SystemTimeProvider`
                                        does.

                                        Args:
                                        moment: The instant the clock reports until a test moves it.
                                        """

                                        def __init__(self, moment: datetime) -> None:
                                            """Start the clock at one instant.

                                            Args:
                                            moment: The instant `now` reports until `advance` is called.
                                            """
                                            self.moment = moment
                                            self.calls = 0

                                        def now(self) -> datetime:
                                            """Return the instant the clock is currently sitting at.

                                            Returns:
                                            datetime: The current instant, unchanged by the number of reads.
                                            """
                                            self.calls += 1
                                            return self.moment

                                        def advance(self, delta: timedelta) -> None:
                                            """Move the clock forward, which is what expires a dedupe entry.

                                            Args:
                                            delta: How far to move it, as a positive `timedelta`.
                                            """
                                            self.moment = self.moment + delta

                                        @pytest.fixture
                                        def restored_root_logging() -> Iterator[None]:
                                            """Restore the project logger after a test has configured logging.

                                            `configure_logging` adds a console handler to the `webcrawler` logger and
                                            pytest's own log capture sees records through it, so the handler and the
                                            level are undone here rather than leaking into the rest of the session.

                                            Args:
                                            None: The fixture carries no value; it only brackets the test.
                                            """
                                            project = logging.getLogger(ROOT_LOGGER_NAME)
                                            handlers = list(project.handlers())
                                            level = project.level
                                            yield
                                            project.handlers[:] = handlers
                                            project.setLevel(level)

                                        @pytest.mark.parametrize("max_attempts", [1, 3, 5])
                                        async def test_a_first_attempt_success_returns_without_retrying(
                                            monkeypatch: pytest.MonkeyPatch, max_attempts: int
                                            ) -> None:
                                            """A working operation is not retried and never sleeps: the budget above one
                                            attempt is not a licence to make them.

                                            Args:
                                            monkeypatch: pytest's patcher, used to install the recorder.
                                            max_attempts: The policy's attempt budget, above what this needs.
                                            """
                                            sleep = RecordingSleep()
                                            install_recorders(monkeypatch, sleep)
                                            operation = ScriptedOperation(["body"])

                                            fetched = await build_policy(max_attempts=max_attempts).execute(operation)

                                            assert fetched == "body"
                                            assert operation.attempts == 1
                                            assert sleep.delays == []

                                        @pytest.mark.parametrize("failures", [1, 2, 4])
                                        async def test_a_transient_failure_is_retried_until_it_succeeds(
                                            monkeypatch: pytest.MonkeyPatch, failures: int
                                            ) -> None:
                                            """A failure is the policy's business, so it starts a fresh attempt with a
                                            longer wait and returns the first success (`goal.md:17`).

                                            Args:
                                            monkeypatch: pytest's patcher, used to install the recorder.
                                            failures: How many attempts raise before one succeeds.
                                            """
                                            sleep = RecordingSleep()
                                            install_recorders(monkeypatch, sleep)
                                            operation = ScriptedOperation([OSError("busy")] * failures + ["body"])

                                            fetched = await build_policy(max_attempts=failures + 1).execute(operation)

                                            assert fetched == "body"
                                            assert operation.attempts == failures + 1
                                            assert len(sleep.delays) == failures

                                        @pytest.mark.parametrize("max_attempts", [1, 2, 3, 6])
                                        async def test_a_spent_budget_re_raises_the_last_error(
                                            monkeypatch: pytest.MonkeyPatch, max_attempts: int
                                            ) -> None:
                                            """The caller sees the real cause rather than a wrapper, and the last
                                            failure waits out no delay of its own.

                                            Args:
                                            monkeypatch: pytest's patcher, used to install the recorder.
                                            max_attempts: The policy's attempt budget.
                                            """
                                            sleep = RecordingSleep()
                                            install_recorders(monkeypatch, sleep)
                                            failure = OSError("the host is on fire")
                                            operation = ScriptedOperation([failure])

                                            with pytest.raises(OSError) as raised:
                                                await build_policy(max_attempts=max_attempts).execute(operation)

                                                assert raised.value is failure
                                                assert operation.attempts == max_attempts
                                                assert len(sleep.delays) == max_attempts - 1

                                                @pytest.mark.parametrize(
                                                    "error",
                                                    [
                                                    pytest.param(TimeoutError("the attempt overran its deadline"), id="timeout"),
                                                    pytest.param(ConnectionResetError("reset by peer"), id="connection_reset"),
                                                    pytest.param(ValueError("the page was not a page"), id="value"),
                                                    pytest.param(RuntimeError("the queue is closed"), id="runtime"),
                                                    ])
                                                async def test_whatever_the_operation_raises_is_the_error_the_caller_sees(
                                                    monkeypatch: pytest.MonkeyPatch, error: BaseException
                                                    ) -> None:
                                                    """The policy retries the exception rather than the type, so a caller cannot
                                                    be handed an error the operation never raised.

                                                    Args:
                                                    monkeypatch: pytest's patcher, used to install the recorder.
                                                    error: The error every attempt raises.
                                                    """
                                                    sleep = RecordingSleep()
                                                    install_recorders(monkeypatch, sleep)
                                                    operation = ScriptedOperation([error])

                                                    with pytest.raises(type(error)) as raised:
                                                        await build_policy(max_attempts=2).execute(operation)

                                                        assert raised.value is error
                                                        assert operation.attempts == 2

                                                        @pytest.mark.parametrize(
                                                            ("base_delay_seconds", "max_delay_seconds", "max_attempts", "expected"),
                                                            [
                                                            pytest.param(0.5, 8.0, 5, [0.5, 1.0, 2.0, 4.0], id="growth_under_the_cap"),
                                                            pytest.param(1.0, 4.0, 4, [1.0, 2.0, 4.0], id="cap_reached"),
                                                            pytest.param(0.25, 0.25, 4, [0.25, 0.25, 0.25], id="cap_below_the_base"),
                                                            pytest.param(2.0, 30.0, 1, [], id="a_single_attempt_never_sleeps"),
                                                            pytest.param(1.0, 3.0, 3, [1.0, 2.0], id="cap_never_reached"),
                                                            ])
                                                        async def test_delay_growth_is_bounded_by_max_delay_seconds(
                                                            monkeypatch: pytest.MonkeyPatch,
                                                            base_delay_seconds: float,
                                                            max_delay_seconds: float,
                                                            max_attempts: int,
                                                            expected: list[float]) -> None:
                                                            """`min(base_delay_seconds * 2 ** n, max_delay_seconds)` is the exact wait,
                                                            with the jitter scripted away so nothing hides a wrong term.

                                                            Args:
                                                            monkeypatch: pytest's patcher, used to install the recorders.
                                                            base_delay_seconds: The delay before the first retry.
                                                            max_delay_seconds: The cap on the exponential term.
                                                            max_attempts: The policy's attempt budget, so there are one fewer waits
                                                            than attempts.
                                                            expected: The exact exponential term per wait, attempt numbers from zero.
                                                            """
                                                            sleep = RecordingSleep()
                                                            jitter = ScriptedJitter([0.0])
                                                            install_recorders(monkeypatch, sleep, jitter)
                                                            operation = ScriptedOperation([OSError("nope")])
                                                            policy = build_policy(
                                                                max_attempts=max_attempts,
                                                                base_delay_seconds=base_delay_seconds,
                                                                max_delay_seconds=max_delay_seconds,
                                                                jitter_seconds=0.5)

                                                            with pytest.raises(OSError):
                                                                await policy.execute(operation)

                                                                assert sleep.delays == expected
                                                                assert len(jitter.calls) == len(expected)
                                                                for attempt, term in enumerate(expected):
                                                                    assert term == min(base_delay_seconds * 2**attempt, max_delay_seconds)
                                                                    assert all(delay <= max_delay_seconds for delay in sleep.delays)

                                                                    @pytest.mark.parametrize("timeout_seconds", [0.01, 0.05])
                                                                    async def test_an_attempt_over_its_deadline_is_abandoned_and_retried(
                                                                        monkeypatch: pytest.MonkeyPatch, timeout_seconds: float
                                                                        ) -> None:
                                                                        """A hung attempt is cancelled at the deadline and the next attempt starts
                                                                        fresh, so one stalled call cannot swallow the whole budget (`goal.md:17`).

                                                                        Args:
                                                                        monkeypatch: pytest's patcher, used to install the recorder.
                                                                        timeout_seconds: The per-attempt deadline the policy applies.
                                                                        """
                                                                        sleep = RecordingSleep()
                                                                        install_recorders(monkeypatch, sleep)
                                                                        operation = ScriptedOperation([None, "in time"])

                                                                        fetched = await build_policy(
                                                                            max_attempts=2, timeout_seconds=timeout_seconds
                                                                            ).execute(operation)

                                                                        assert fetched == "in time"
                                                                        assert operation.attempts == 2
                                                                        assert len(sleep.delays) == 1

                                                                    @pytest.mark.parametrize("timeout_seconds", [0.01, 0.05])
                                                                    async def test_a_deadline_that_keeps_firing_exhausts_the_attempts_with_a_timeout(
                                                                        monkeypatch: pytest.MonkeyPatch, timeout_seconds: float
                                                                        ) -> None:
                                                                        """Nothing ever completes, so the last error is the last deadline that
                                                                        fired, which is the case a caller has to be able to recognise.

                                                                        Args:
                                                                        monkeypatch: pytest's patcher, used to install the recorder.
                                                                        timeout_seconds: The per-attempt deadline the policy applies.
                                                                        """
                                                                        sleep = RecordingSleep()
                                                                        install_recorders(monkeypatch, sleep)
                                                                        operation = ScriptedOperation([None])

                                                                        with pytest.raises(asyncio.TimeoutError):
                                                                            await build_policy(
                                                                                max_attempts=3, timeout_seconds=timeout_seconds
                                                                                ).execute(operation)

                                                                            assert operation.attempts == 3
                                                                            assert len(sleep.delays) == 2

                                                                            @pytest.mark.parametrize("timeout_seconds", [0.5, 2.5, 30.0])
                                                                            async def test_every_attempt_is_bounded_by_the_configured_timeout(
                                                                                monkeypatch: pytest.MonkeyPatch, timeout_seconds: float
                                                                                ) -> None:
                                                                                """The deadline is `RetrySettings.timeout_seconds` on every attempt, and
                                                                                each attempt is a distinct awaitable (`goal.md:17`).

                                                                                Args:
                                                                                monkeypatch: pytest's patcher, used to install the recorders.
                                                                                timeout_seconds: The per-attempt deadline the policy applies.
                                                                                """
                                                                                sleep = RecordingSleep()
                                                                                install_recorders(monkeypatch, sleep)
                                                                                wait_for = RecordingWaitFor()
                                                                                monkeypatch.setattr(retry_module.asyncio, "wait_for", wait_for)
                                                                                operation = ScriptedOperation([OSError("nope")])

                                                                                with pytest.raises(OSError):
                                                                                    await build_policy(
                                                                                        max_attempts=3, timeout_seconds=timeout_seconds
                                                                                        ).execute(operation)

                                                                                    assert wait_for.timeouts == [timeout_seconds] * 3
                                                                                    assert len(wait_for.awaitables) == len(operation.awaitables) == 3
                                                                                    assert len({id(awaitable) for awaitable in wait_for.awaitables}) == 3
                                                                                    assert len({id(awaitable) for awaitable in operation.awaitables}) == 3

                                                                                    @pytest.mark.parametrize(
                                                                                        ("base_delay_seconds", "max_delay_seconds", "jitter_seconds", "spreads", "terms"),
                                                                                        [
                                                                                        pytest.param(1.0, 8.0, 0.25, [0.0, 0.25, 0.125], [1.0, 2.0, 4.0], id="sweep"),
                                                                                        pytest.param(2.0, 2.0, 0.5, [0.5, 0.0], [2.0, 2.0], id="capped_from_the_start"),
                                                                                        pytest.param(0.5, 4.0, 0.0, [0.0, 0.0, 0.0], [0.5, 1.0, 2.0], id="no_jitter"),
                                                                                        pytest.param(1.0, 1.0, 2.0, [0.0, 2.0], [1.0, 1.0], id="widest_jitter"),
                                                                                        ])
                                                                                    async def test_jitter_is_added_within_its_documented_bounds(
                                                                                        monkeypatch: pytest.MonkeyPatch,
                                                                                        base_delay_seconds: float,
                                                                                        max_delay_seconds: float,
                                                                                        jitter_seconds: float,
                                                                                        spreads: list[float],
                                                                                        terms: list[float]) -> None:
                                                                                        """With `random.uniform` patched, attempt `n`'s delay is
                                                                                        `min(base_delay_seconds * 2 ** n, max_delay_seconds)` plus a spread drawn
                                                                                        from `[0.0, jitter_seconds]`, so the total sits in
                                                                                        `[term, term + jitter_seconds]`.

                                                                                        Args:
                                                                                        monkeypatch: pytest's patcher, used to install the recorders.
                                                                                        base_delay_seconds: The delay before the first retry.
                                                                                        max_delay_seconds: The cap on the exponential term.
                                                                                        jitter_seconds: The width of the uniform spread, which is also the width
                                                                                        of the interval the delay must fall in.
                                                                                        spreads: The spread the scripted jitter returns per attempt, including
                                                                                        both ends of the interval.
                                                                                        terms: The exponential term per attempt, attempt numbers from zero.
                                                                                        """
                                                                                        sleep = RecordingSleep()
                                                                                        jitter = ScriptedJitter(spreads)
                                                                                        install_recorders(monkeypatch, sleep, jitter)
                                                                                        operation = ScriptedOperation([OSError("nope")])
                                                                                        policy = build_policy(
                                                                                            max_attempts=len(terms) + 1,
                                                                                            base_delay_seconds=base_delay_seconds,
                                                                                            max_delay_seconds=max_delay_seconds,
                                                                                            jitter_seconds=jitter_seconds)

                                                                                        with pytest.raises(OSError):
                                                                                            await policy.execute(operation)

                                                                                            assert jitter.calls == [(0.0, jitter_seconds)] * len(terms)
                                                                                            assert len(sleep.delays) == len(terms)
                                                                                            for attempt, (term, spread, delay) in enumerate(
                                                                                                zip(terms, spreads, sleep.delays, strict=True)
                                                                                                ):
                                                                                                assert term == min(base_delay_seconds * 2**attempt, max_delay_seconds)
                                                                                                assert term <= delay <= term + jitter_seconds
                                                                                                assert delay == pytest.approx(term + spread)

                                                                                                @pytest.mark.parametrize("max_attempts", [0, -1])
                                                                                                async def test_a_budget_that_never_attempts_is_reported_rather_than_succeeding(
                                                                                                    monkeypatch: pytest.MonkeyPatch, max_attempts: int
                                                                                                    ) -> None:
                                                                                                    """A misconfigured budget fails at the composition root's call instead of
                                                                                                    returning a result for work that was never done.

                                                                                                    Args:
                                                                                                    monkeypatch: pytest's patcher, used to install the recorder.
                                                                                                    max_attempts: The nonsensical attempt budget the policy is given.
                                                                                                    """
                                                                                                    sleep = RecordingSleep()
                                                                                                    install_recorders(monkeypatch, sleep)
                                                                                                    operation = ScriptedOperation(["body"])

                                                                                                    with pytest.raises(ValueError, match="max_attempts"):
                                                                                                        await build_policy(max_attempts=max_attempts).execute(operation)

                                                                                                        assert operation.attempts == 0
                                                                                                        assert sleep.delays == []

                                                                                                        @pytest.mark.parametrize(
                                                                                                            ("policy", "port"),
                                                                                                            [
                                                                                                            pytest.param(build_policy, RetryPolicy, id="exponential_backoff"),
                                                                                                            pytest.param(NoOpPolitenessPolicy, PolitenessPolicy, id="no_op_politeness"),
                                                                                                            ])
                                                                                                        def test_a_port_implementation_extends_its_port(
                                                                                                            policy: object, port: type[object]
                                                                                                            ) -> None:
                                                                                                            """The implementations named by are the only classes
                                                                                                            allowed a project base, and that base is their port (`goal.md:8`).

                                                                                                            Args:
                                                                                                            policy: The implementation under test.
                                                                                                            port: The `ports` ABC it must extend.
                                                                                                            """
                                                                                                            assert isinstance(policy, port)
                                                                                                            assert port in type(policy).__bases__

                                                                                                        @pytest.mark.parametrize(
                                                                                                            ("candidate", "port"),
                                                                                                            [
                                                                                                            pytest.param(SystemTimeProvider, TimeProviderFactory, id="system_time"),
                                                                                                            pytest.param(
                                                                                                            InMemoryRequestIdDeduplicator,
                                                                                                            RequestDeduplicator,
                                                                                                            id="request_id_deduplicator"),
                                                                                                            ])
                                                                                                        def test_a_port_implementation_extends_its_port_too(
                                                                                                            candidate: object, port: type[object]
                                                                                                            ) -> None:
                                                                                                            """The clock and dedupe ports are ABCs like the rest, so these
                                                                                                            implementations carry their port as a base class.

                                                                                                            Args:
                                                                                                            candidate: The implementation under test.
                                                                                                            port: The `ports` ABC it must extend.
                                                                                                            """
                                                                                                            assert isinstance(candidate, port)
                                                                                                            assert port in type(candidate).__bases__

                                                                                                        @pytest.mark.parametrize("sleep_threshold_ms", [0, 500, 2000])
                                                                                                        def test_the_no_op_politeness_policy_means_call_now(sleep_threshold_ms: int) -> None:
                                                                                                            """`0` is the port's "call now", so the worker sleeps its zero and crawls
                                                                                                            whatever its own threshold happens to be (`goal.md:140`).

                                                                                                            Args:
                                                                                                            sleep_threshold_ms: The worker's sleep threshold, which the answer must
                                                                                                            fall at or below for every configuration.
                                                                                                            """
                                                                                                            wait_ms = NoOpPolitenessPolicy.before_fetch

                                                                                                            assert wait_ms == 0 <= sleep_threshold_ms

                                                                                                        @pytest.mark.parametrize(
                                                                                                            ("request_id", "repeats"),
                                                                                                            [
                                                                                                            pytest.param("poll-check-1", 2, id="twice"),
                                                                                                            pytest.param("poll-check-2", 3, id="three_times"),
                                                                                                            pytest.param("api-request-1", 9, id="nine_times"),
                                                                                                            ])
                                                                                                        def test_a_repeated_id_is_skipped_and_an_unseen_one_is_not(
                                                                                                            request_id: str, repeats: int
                                                                                                            ) -> None:
                                                                                                            """`goal.md:100` asks for a repeat to be reported so a retried check does
                                                                                                            not queue the same work again, and the first sighting is never a repeat.

                                                                                                            Args:
                                                                                                            request_id: The poll-check or API request id the caller passes.
                                                                                                            repeats: How many times the caller checks the same id.
                                                                                                            """
                                                                                                            deduplicator = InMemoryRequestIdDeduplicator()

                                                                                                            results = [deduplicator.seen_and_record(request_id) for _ in range(repeats)]

                                                                                                            assert results == [False] + [True] * (repeats - 1)

                                                                                                        @pytest.mark.parametrize(
                                                                                                            "request_id",
                                                                                                            [
                                                                                                            pytest.param("poll-check-1", id="poll_check"),
                                                                                                            pytest.param("api-request-9", id="api_request"),
                                                                                                            pytest.param("0", id="numeric_id"),
                                                                                                            pytest.param("a" * 256, id="long_id"),
                                                                                                            ])
                                                                                                        def test_distinct_ids_never_suppress_each_other(request_id: str) -> None:
                                                                                                            """The dedupe key is the whole id, so two different checks are two different
                                                                                                            entries even inside one window.

                                                                                                            Args:
                                                                                                            request_id: The id under test, recorded alongside a second, different id.
                                                                                                            """
                                                                                                            deduplicator = InMemoryRequestIdDeduplicator()

                                                                                                            assert deduplicator.seen_and_record(request_id) is False
                                                                                                            assert deduplicator.seen_and_record(f"{request_id}-other") is False
                                                                                                            assert deduplicator.seen_and_record(request_id) is True

                                                                                                        @pytest.mark.parametrize("reads", [1, 2, 5])
                                                                                                        def test_the_system_clock_reports_aware_utc_instants(reads: int) -> None:
                                                                                                            """A naive instant would compare wrongly against the timezone-aware ones the
                                                                                                            repository binds, so the port's `datetime` is aware and in UTC
                                                                                                            (`goal.md:25`).

                                                                                                            Args:
                                                                                                            reads: How many successive instants to read from the provider.
                                                                                                            """
                                                                                                            provider = SystemTimeProvider()

                                                                                                            moments = [provider.now for _ in range(reads)]

                                                                                                            assert all(moment.tzinfo is not None for moment in moments)
                                                                                                            assert all(moment.utcoffset == timedelta(0) for moment in moments)
                                                                                                            assert moments == sorted(moments)

                                                                                                        @pytest.mark.parametrize("reads", [1, 3])
                                                                                                        def test_the_system_clock_reads_the_real_clock(reads: int) -> None:
                                                                                                            """The provider is not a fixed epoch: every read lands inside the window the
                                                                                                            real clock covered while the call was made.

                                                                                                            Args:
                                                                                                            reads: How many successive instants to bracket against the real clock.
                                                                                                            """
                                                                                                            provider = SystemTimeProvider()

                                                                                                            for _ in range(reads):
                                                                                                                before = datetime.now(timezone.utc())
                                                                                                                moment = provider.now
                                                                                                                after = datetime.now(timezone.utc())
                                                                                                                assert before <= moment <= after

                                                                                                                @pytest.mark.parametrize(
                                                                                                                    ("delay_seconds", "minimum_advance"),
                                                                                                                    [
                                                                                                                    pytest.param(0.0, timedelta(0), id="consecutive_reads"),
                                                                                                                    pytest.param(0.05, timedelta(milliseconds=25), id="after_a_real_wait"),
                                                                                                                    ])
                                                                                                                def test_the_system_clock_advances_with_the_real_clock(
                                                                                                                    delay_seconds: float, minimum_advance: timedelta
                                                                                                                    ) -> None:
                                                                                                                    """A frozen clock would make every crawl deadline unreachable, so two reads
                                                                                                                    around a wait must differ. The tolerance is half the wait, because the
                                                                                                                    system clock's resolution is not the test's to promise.

                                                                                                                    Args:
                                                                                                                    delay_seconds: How long the test waits between the two reads.
                                                                                                                    minimum_advance: The smallest difference the two reads must show.
                                                                                                                    """
                                                                                                                    provider = SystemTimeProvider()

                                                                                                                    first = provider.now
                                                                                                                    time.sleep(delay_seconds)
                                                                                                                    second = provider.now

                                                                                                                    assert second - first >= minimum_advance
                                                                                                                    assert first.tzinfo is not None and second.tzinfo is not None

                                                                                                                @pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG, logging.WARNING])
                                                                                                                def test_configuring_logging_installs_one_console_handler_and_returns_the_named_logger(
                                                                                                                    restored_root_logging: Iterator[None], level: int
                                                                                                                    ) -> None:
                                                                                                                    """`main.py` configures logging first and injects the logger it is given, so
                                                                                                                    the project logger has to reach the console through a single stream handler.

                                                                                                                    Args:
                                                                                                                    restored_root_logging: The fixture that undoes the configuration.
                                                                                                                    level: The level the project logger accepts.
                                                                                                                    """
                                                                                                                    logger = configure_logging(level)

                                                                                                                    project = logging.getLogger(ROOT_LOGGER_NAME)
                                                                                                                    assert logger is project
                                                                                                                    assert [handler.get_name for handler in project.handlers] == [HANDLER_NAME]
                                                                                                                    assert project.level == level

                                                                                                                @pytest.mark.parametrize("level", [logging.INFO, logging.DEBUG])
                                                                                                                def test_the_configured_formatter_renders_utc_timestamps(
                                                                                                                    restored_root_logging: Iterator[None], level: int
                                                                                                                    ) -> None:
                                                                                                                    """Every time in this project is UTC (`goal.md:25`), so a record rendered in
                                                                                                                    local time would not be comparable with a stored timestamp.

                                                                                                                    Args:
                                                                                                                    restored_root_logging: The fixture that undoes the configuration.
                                                                                                                    level: The level the configuration is asked for.
                                                                                                                    """
                                                                                                                    configure_logging(level)

                                                                                                                    handler = logging.getLogger(ROOT_LOGGER_NAME).handlers[0]
                                                                                                                    formatter = handler.formatter
                                                                                                                    assert formatter is not None
                                                                                                                    record = logging.LogRecord(
                                                                                                                        PROBE_LOGGER_NAME, logging.INFO, __file__, 1, "crawled %s", ("url"), None
                                                                                                                        )
                                                                                                                    record.created = 0.0

                                                                                                                    assert formatter.format(record).startswith(EPOCH_FORMATTED)
                                                                                                                    assert record.getMessage == "crawled url"

                                                                                                                @pytest.mark.parametrize(
                                                                                                                    "message",
                                                                                                                    [
                                                                                                                    pytest.param("visited https://crawlme.monzo.com/", id="visited"),
                                                                                                                    pytest.param("links: 3", id="links"),
                                                                                                                    ])
                                                                                                                def test_a_record_from_the_named_logger_reaches_the_configured_console_handler(
                                                                                                                    restored_root_logging: Iterator[None], message: str
                                                                                                                    ) -> None:
                                                                                                                    """The console output `goal.md:143` asks for is the logger's own record, so a
                                                                                                                    record from the injected logger has to arrive on the configured handler.

                                                                                                                    Args:
                                                                                                                    restored_root_logging: The fixture that undoes the configuration.
                                                                                                                    message: The message the test logs at INFO.
                                                                                                                    """
                                                                                                                    logger = configure_logging(logging.INFO())
                                                                                                                    handler = logging.getLogger(ROOT_LOGGER_NAME).handlers[0]
                                                                                                                    stream = io.StringIO
                                                                                                                    handler.setStream(stream)

                                                                                                                    logger.info(message)

                                                                                                                    assert message in stream.getvalue

                                                                                                                @pytest.mark.parametrize(
                                                                                                                    ("level", "printed", "expected"),
                                                                                                                    [
                                                                                                                    pytest.param(logging.INFO, "debug detail", False, id="debug_dropped_at_info"),
                                                                                                                    pytest.param(logging.DEBUG, "debug detail", True, id="debug_kept_at_debug"),
                                                                                                                    pytest.param(logging.INFO, "crawled a url", True, id="info_kept_at_info"),
                                                                                                                    pytest.param(logging.DEBUG, "crawled a url", True, id="info_kept_at_debug"),
                                                                                                                    ])
                                                                                                                def test_the_level_filters_which_records_reach_the_console(
                                                                                                                    restored_root_logging: Iterator[None],
                                                                                                                    level: int,
                                                                                                                    printed: str,
                                                                                                                    expected: bool) -> None:
                                                                                                                    """The only filter the project needs is the level, so DEBUG and INFO are
                                                                                                                    chosen at startup rather than at each call site.

                                                                                                                    Args:
                                                                                                                    restored_root_logging: The fixture that undoes the configuration.
                                                                                                                    level: The level the project logger is configured with.
                                                                                                                    printed: One of the two messages the test logs, at DEBUG or INFO.
                                                                                                                    expected: Whether that message may reach the console at this level.
                                                                                                                    """
                                                                                                                    logger = configure_logging(level)
                                                                                                                    handler = logging.getLogger(ROOT_LOGGER_NAME).handlers[0]
                                                                                                                    stream = io.StringIO
                                                                                                                    handler.setStream(stream)

                                                                                                                    logger.debug("debug detail")
                                                                                                                    logger.info("crawled a url")

                                                                                                                    assert (printed in stream.getvalue) is expected
