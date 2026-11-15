//! What the runner asks of the process under test, wherever it runs: in
//! the process of the supervisor (the default, unchanged), or in a child
//! process driven by the control protocol (`child`).
//!
//! The runner chooses among the descriptions of the eligible choices and
//! names one by its index; it never holds a choice of the process itself.

use super::child::Child;
use super::collector::{Cause, Collector};
use super::process::{Books, Choice, Process, Stop, Written};
use crate::store::durable::DurableStore;

use mqttiumd_contract::ids::ConnectionGeneration;

/// The process under test.
pub enum Broker<S: DurableStore> {
    /// In the process of the supervisor, with the choices it offered last.
    Local {
        process: Box<Process<S>>,
        offered: Vec<Choice>,
    },
    /// In a child process.
    Child(Box<Child>),
}

impl<S: DurableStore> Broker<S> {
    /// The descriptions of the eligible choices, in order.
    pub fn choices(&mut self) -> Result<Vec<String>, Stop> {
        match self {
            Self::Local { process, offered } => {
                *offered = process.choices();
                Ok(offered.iter().map(Choice::describe).collect())
            }
            Self::Child(child) => Ok(child.choices().to_vec()),
        }
    }

    /// Performs the choice of `index` in the list [`Self::choices`] gave
    /// last, then settles: what the transport wrote to a peer, if anything.
    pub fn choose(
        &mut self,
        index: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<Written>, Stop> {
        match self {
            Self::Local { process, offered } => {
                let choice = offered
                    .get(index)
                    .cloned()
                    .ok_or_else(|| Stop::new(format!("no choice {index}")))?;
                let written = process.perform(&choice, collector, books)?;
                process.settle(collector, books)?;
                Ok(written)
            }
            Self::Child(child) => child.choose(index, collector, books),
        }
    }

    pub fn connect(
        &mut self,
        client: &str,
        generation: ConnectionGeneration,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => {
                process.connect(client, generation);
                Ok(())
            }
            Self::Child(child) => child.connect(client, generation, collector, books),
        }
    }

    pub fn receive(
        &mut self,
        generation: ConnectionGeneration,
        bytes: &[u8],
        sent: Cause,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => process.receive(generation, bytes, sent, collector),
            Self::Child(child) => child.receive(generation, bytes, sent, collector, books),
        }
    }

    /// Whether an effect still waits in the box, or an input for room.
    #[must_use]
    pub fn busy(&self) -> bool {
        match self {
            Self::Local { process, .. } => process.busy(),
            Self::Child(child) => child.state().busy,
        }
    }

    pub fn settle(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => process.settle(collector, books),
            Self::Child(child) => child.settle(collector, books),
        }
    }

    /// Fires the armed deadline due first: `None` when none is armed.
    pub fn fire_deadline(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<Option<()>, Stop> {
        match self {
            Self::Local { process, .. } => process.fire_deadline(collector, books),
            Self::Child(child) => child.fire_deadline(collector, books),
        }
    }

    /// Whether the scenario fault `fault` holds an item.
    #[must_use]
    pub fn holds(&self, books: &Books, fault: usize) -> bool {
        match self {
            Self::Local { .. } => books.held.contains_key(&fault),
            Self::Child(child) => child.state().held.contains(&fault),
        }
    }

    /// The command the scenario fault `fault` holds may enter the writer.
    pub fn release_command(
        &mut self,
        fault: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => {
                let id = books
                    .held
                    .get(&fault)
                    .copied()
                    .ok_or_else(|| Stop::new("the fault holds no command"))?;
                process.release_command(id, collector, books)
            }
            Self::Child(child) => child.release_command(fault, collector, books),
        }
    }

    /// The result the scenario fault `fault` holds is handed over.
    pub fn deliver_held_result(
        &mut self,
        fault: usize,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => {
                let id = books
                    .held
                    .get(&fault)
                    .copied()
                    .ok_or_else(|| Stop::new("the fault holds no result"))?;
                process.deliver_held_result(id, collector, books)
            }
            Self::Child(child) => child.deliver_held_result(fault, collector, books),
        }
    }

    /// Whether the store adapter is stopped at a barrier.
    #[must_use]
    pub fn halted(&self) -> bool {
        match self {
            Self::Local { process, .. } => process.halted(),
            Self::Child(child) => child.state().halted,
        }
    }

    pub fn leave_barrier(
        &mut self,
        collector: &mut Collector,
        books: &mut Books,
    ) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => process.leave_barrier(collector, books),
            Self::Child(child) => child.leave_barrier(collector, books),
        }
    }

    /// The deadlines armed and not fired.
    #[must_use]
    pub fn armed_deadlines(&self) -> usize {
        match self {
            Self::Local { process, .. } => process.armed_deadlines(),
            Self::Child(child) => child.state().armed,
        }
    }

    /// The epoch the process opened.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        match self {
            Self::Local { process, .. } => process.epoch().get(),
            Self::Child(child) => child.epoch(),
        }
    }

    pub fn recover(&mut self, collector: &mut Collector, books: &mut Books) -> Result<(), Stop> {
        match self {
            Self::Local { process, .. } => process.recover(collector, books),
            Self::Child(child) => child.recover(collector, books),
        }
    }
}
