//! openraft [`AsyncRuntime`] on madsim's deterministic runtime.
//!
//! Simulation rig only (`--cfg madsim`). openraft's built-in `TokioRuntime`
//! is wired to the real `tokio` crate, which has no reactor inside a madsim
//! simulation, so Raft's tasks, timers, election-timeout randomness and
//! clock are routed through madsim here instead. Raft logic itself is
//! untouched: this only swaps the environment openraft runs on.

use std::fmt;
use std::future::Future;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use openraft::async_runtime::TokioOneShotSender;
use openraft::{AsyncRuntime, OptionalSend};

/// madsim-backed [`AsyncRuntime`] for openraft.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MadsimRuntime;

/// madsim's simulated clock as an [`openraft::Instant`].
///
/// A newtype because both the trait and `madsim::time::Instant` (a
/// re-export of `std::time::Instant`, virtualised by madsim) are foreign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MadsimInstant(pub madsim::time::Instant);

impl openraft::Instant for MadsimInstant {
    #[inline]
    fn now() -> Self {
        Self(madsim::time::Instant::now())
    }
}

impl Add<Duration> for MadsimInstant {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Duration) -> Self {
        Self(self.0 + rhs)
    }
}

impl AddAssign<Duration> for MadsimInstant {
    #[inline]
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}

impl Sub<Duration> for MadsimInstant {
    type Output = Self;
    #[inline]
    fn sub(self, rhs: Duration) -> Self {
        Self(self.0 - rhs)
    }
}

impl Sub<MadsimInstant> for MadsimInstant {
    type Output = Duration;
    #[inline]
    fn sub(self, rhs: MadsimInstant) -> Duration {
        self.0 - rhs.0
    }
}

impl SubAssign<Duration> for MadsimInstant {
    #[inline]
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 -= rhs;
    }
}

/// Error returned when a [`MadsimTimeout`] elapses.
pub type MadsimElapsed = madsim::time::error::Elapsed;

/// `future` raced against a madsim [`Sleep`](madsim::time::Sleep).
///
/// madsim's own `time::timeout` returns an unnameable `impl Future`, which
/// cannot fill openraft's `Timeout` associated type, hence this type. The
/// inner future is boxed so the wrapper is `Unpin` without unsafe pin
/// projection.
pub struct MadsimTimeout<F> {
    future: Pin<Box<F>>,
    sleep: madsim::time::Sleep,
}

impl<F> fmt::Debug for MadsimTimeout<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MadsimTimeout")
            .field("deadline", &self.sleep.deadline())
            .finish()
    }
}

impl<F: Future> Future for MadsimTimeout<F> {
    type Output = Result<F::Output, MadsimElapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(v) = self.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        match Pin::new(&mut self.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(madsim::time::error::Elapsed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRuntime for MadsimRuntime {
    type JoinError = madsim::task::JoinError;
    type JoinHandle<T: OptionalSend + 'static> = madsim::task::JoinHandle<T>;
    type Sleep = madsim::time::Sleep;
    type Instant = MadsimInstant;
    type TimeoutError = MadsimElapsed;
    type Timeout<R, T: Future<Output = R> + OptionalSend> = MadsimTimeout<T>;
    type ThreadLocalRng = madsim::rand::GlobalRng;
    // `tokio::sync::oneshot` is runtime-agnostic; reuse openraft's wrapper.
    type OneshotSender<T: OptionalSend> = TokioOneShotSender<T>;
    type OneshotReceiver<T: OptionalSend> = tokio::sync::oneshot::Receiver<T>;
    type OneshotReceiverError = tokio::sync::oneshot::error::RecvError;

    #[inline]
    fn spawn<T>(future: T) -> Self::JoinHandle<T::Output>
    where
        T: Future + OptionalSend + 'static,
        T::Output: OptionalSend + 'static,
    {
        madsim::task::spawn(future)
    }

    #[inline]
    fn sleep(duration: Duration) -> Self::Sleep {
        madsim::time::sleep(duration)
    }

    #[inline]
    fn sleep_until(deadline: Self::Instant) -> Self::Sleep {
        madsim::time::sleep_until(deadline.0)
    }

    #[inline]
    fn timeout<R, F: Future<Output = R> + OptionalSend>(
        duration: Duration,
        future: F,
    ) -> Self::Timeout<R, F> {
        MadsimTimeout {
            future: Box::pin(future),
            sleep: madsim::time::sleep(duration),
        }
    }

    #[inline]
    fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
        deadline: Self::Instant,
        future: F,
    ) -> Self::Timeout<R, F> {
        MadsimTimeout {
            future: Box::pin(future),
            sleep: madsim::time::sleep_until(deadline.0),
        }
    }

    #[inline]
    fn is_panic(join_error: &Self::JoinError) -> bool {
        join_error.is_panic()
    }

    #[inline]
    fn thread_rng() -> Self::ThreadLocalRng {
        madsim::rand::thread_rng()
    }

    #[inline]
    fn oneshot<T>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>)
    where
        T: OptionalSend,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (TokioOneShotSender(tx), rx)
    }
}
