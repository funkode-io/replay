use std::any::{Any, TypeId};
use std::fmt;

use urn::Urn;

use crate::{Aggregate, Metadata};

/// The erased `(StreamId, Command)` pair a [`Dispatch`] carries.
///
/// `Send` off wasm32, where a runner moves a dispatch between tasks; plain `Any` on
/// wasm32, where `Aggregate::Command` carries no `Send` bound to satisfy it.
#[cfg(not(target_arch = "wasm32"))]
type ErasedPair = Box<dyn Any + Send>;
#[cfg(target_arch = "wasm32")]
type ErasedPair = Box<dyn Any>;

/// A command a [`Policy`](crate::Policy) wants executed against an aggregate.
///
/// `Dispatch` is an *erased descriptor*: it remembers which aggregate type the
/// command targets ([`TypeId`]) and carries the `(StreamId, Command)` pair as an
/// opaque [`Any`] payload. A runner — which registered the
/// concrete aggregate — recovers the pair through [`Dispatch::into_parts`] and
/// executes it. It is not parameterised by target, so one reaction can address
/// several aggregate types; it names no store or runtime type, so it stays
/// WASM-ready.
///
/// It also carries, in the open, the identity a parked [dead letter] is read by:
/// the target stream's URN and the command's type name. A caller that knows the
/// target aggregate type recovers the pair itself through [`Dispatch::parts`],
/// which is what lets a `react` implementation be unit-tested.
///
/// [dead letter]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#dead-letter
pub struct Dispatch {
    target: TypeId,
    aggregate_name: &'static str,
    target_stream_id: Urn,
    command_name: &'static str,
    payload: ErasedPair,
    expected_version: Option<i64>,
    metadata: Option<Metadata>,
}

impl Dispatch {
    /// Builds a dispatch targeting aggregate `A`, identified by `id`, carrying
    /// `command`.
    ///
    /// ```rust,ignore
    /// Dispatch::to::<BankAccount>(account_id, BankAccountCommand::Freeze)
    /// ```
    pub fn to<A>(id: A::StreamId, command: A::Command) -> Self
    where
        A: Aggregate + 'static,
        A::StreamId: 'static,
        A::Command: 'static,
    {
        Dispatch {
            target: TypeId::of::<A>(),
            aggregate_name: std::any::type_name::<A>(),
            // Taken eagerly: a parked dead letter is read by code that does not
            // know `A` and so cannot call `parts` (funkode-io/replay#210).
            target_stream_id: id.clone().into(),
            command_name: std::any::type_name::<A::Command>(),
            payload: Box::new((id, command)),
            expected_version: None,
            metadata: None,
        }
    }

    /// Attach user-defined metadata to this dispatch.
    ///
    /// A runner merges this metadata with causation metadata before executing
    /// the command. Colliding top-level keys are rejected at runtime.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// The [`TypeId`] of the aggregate this dispatch targets.
    pub fn target(&self) -> TypeId {
        self.target
    }

    /// The Rust type name of the target aggregate (diagnostics only).
    pub fn aggregate_name(&self) -> &'static str {
        self.aggregate_name
    }

    /// The URN of the aggregate instance this command is addressed to.
    pub fn target_stream_id(&self) -> &Urn {
        &self.target_stream_id
    }

    /// The Rust type name of the command (diagnostics only).
    ///
    /// The type, not the variant: `Aggregate::Command` carries no `Debug` or
    /// `Serialize` bound, and adding one would break every consumer.
    pub fn command_name(&self) -> &'static str {
        self.command_name
    }

    /// Borrows the `(StreamId, Command)` pair this dispatch carries, when it
    /// targets `A`.
    ///
    /// The match is on the target aggregate, not on the payload's shape: a
    /// dispatch to another aggregate yields `None` even when that aggregate's
    /// `StreamId` and `Command` types coincide with `A`'s.
    ///
    /// ```rust,ignore
    /// let (id, command) = dispatch.parts::<FeeLedger>().unwrap();
    /// assert_eq!(command, &FeeLedgerCommand::ChargeFee { amount: 25 });
    /// ```
    pub fn parts<A>(&self) -> Option<(&A::StreamId, &A::Command)>
    where
        A: Aggregate + 'static,
        A::StreamId: 'static,
        A::Command: 'static,
    {
        if self.target != TypeId::of::<A>() {
            return None;
        }
        self.payload
            .downcast_ref::<(A::StreamId, A::Command)>()
            .map(|(id, command)| (id, command))
    }

    /// Takes the `(StreamId, Command)` pair out of this dispatch, when it targets `A`.
    ///
    /// The consuming counterpart to [`parts`](Self::parts), for an executor that
    /// *runs* the command rather than asserting on it: the pair is moved, not cloned,
    /// so `Command` needs no `Clone`. A dispatch to another aggregate is handed back
    /// unchanged, since the caller has no other way to recover it — which is also why
    /// the `Err` variant is the whole dispatch rather than a small error.
    #[allow(clippy::result_large_err)]
    pub fn into_parts<A>(self) -> Result<(A::StreamId, A::Command), Self>
    where
        A: Aggregate + 'static,
        A::StreamId: 'static,
        A::Command: 'static,
    {
        if self.target != TypeId::of::<A>() {
            return Err(self);
        }
        let Dispatch {
            target,
            aggregate_name,
            target_stream_id,
            command_name,
            payload,
            expected_version,
            metadata,
        } = self;
        match payload.downcast::<(A::StreamId, A::Command)>() {
            Ok(pair) => Ok(*pair),
            Err(payload) => Err(Dispatch {
                target,
                aggregate_name,
                target_stream_id,
                command_name,
                payload,
                expected_version,
                metadata,
            }),
        }
    }

    /// The optimistic-concurrency version the command is to be executed under.
    pub fn expected_version(&self) -> Option<i64> {
        self.expected_version
    }

    /// The user-defined metadata attached by [`Dispatch::with_metadata`], before
    /// a runner merges causation metadata into it.
    pub fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
}

impl fmt::Debug for Dispatch {
    /// Prints the identity a dead letter is read by. The payload is omitted for
    /// the reason given on [`Dispatch::command_name`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dispatch")
            .field("aggregate", &self.aggregate_name)
            .field("command", &self.command_name)
            .field("target_stream_id", &self.target_stream_id.to_string())
            .field("expected_version", &self.expected_version)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use urn::UrnBuilder;

    use crate::{Error, EventStream, Metadata, WithId};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum AccountEvent {
        Frozen,
    }

    impl crate::Event for AccountEvent {
        fn event_type(&self) -> String {
            "Frozen".to_string()
        }
    }

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct AccountUrn(Urn);

    impl From<AccountUrn> for Urn {
        fn from(urn: AccountUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for AccountUrn {
        type Error = Error;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(AccountUrn(urn))
        }
    }

    struct Account {
        id: AccountUrn,
    }

    impl WithId for Account {
        type StreamId = AccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            Account { id }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl EventStream for Account {
        type Event = AccountEvent;

        fn stream_type() -> String {
            "Account".to_string()
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    impl Aggregate for Account {
        type Command = String;
        type Error = Error;
        type Services = ();

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }
    }

    /// A second aggregate whose `StreamId` and `Command` types are *identical* to
    /// `Account`'s, so a payload downcast alone cannot tell the two apart.
    struct Shipment {
        id: AccountUrn,
    }

    impl WithId for Shipment {
        type StreamId = AccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            Shipment { id }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl EventStream for Shipment {
        type Event = AccountEvent;

        fn stream_type() -> String {
            "Shipment".to_string()
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    impl Aggregate for Shipment {
        type Command = String;
        type Error = Error;
        type Services = ();

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }
    }

    fn account() -> AccountUrn {
        AccountUrn(UrnBuilder::new("account", "42").build().unwrap())
    }

    /// The assertion a policy unit test exists to make: which command, with what
    /// payload, addressed to which instance.
    #[test]
    fn recovers_the_pair_a_dispatch_carries() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string());

        let (recovered_id, command) = dispatch
            .parts::<Account>()
            .expect("the target aggregate's own pair must be recoverable");

        assert_eq!(recovered_id, &account());
        assert_eq!(command, "freeze");
        assert_eq!(dispatch.expected_version(), None);
        assert!(dispatch.metadata().is_none());
    }

    /// What an executor does: move the pair out, since running the command consumes it.
    #[test]
    fn takes_the_pair_out_for_execution() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string());

        let (id, command) = dispatch
            .into_parts::<Account>()
            .expect("the target aggregate's own pair must be recoverable");

        assert_eq!(id, account());
        assert_eq!(command, "freeze");
    }

    /// A dispatch that targets another aggregate is handed back whole, so a caller
    /// trying one aggregate after another loses nothing.
    #[test]
    fn hands_back_a_dispatch_it_cannot_take_apart() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string());

        let returned = dispatch
            .into_parts::<Shipment>()
            .expect_err("a dispatch to another aggregate must not be taken apart");

        assert_eq!(returned.target(), TypeId::of::<Account>());
        assert_eq!(
            returned.parts::<Account>().map(|(_, command)| command),
            Some(&"freeze".to_string())
        );
    }

    /// Metadata attached at construction reads back through the accessor.
    #[test]
    fn exposes_attached_metadata() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string()).with_metadata(
            Metadata::from_json(serde_json::json!({ "correlation": "c-1" })),
        );

        assert_eq!(
            dispatch.metadata(),
            Some(&Metadata::from_json(
                serde_json::json!({ "correlation": "c-1" })
            ))
        );
    }

    /// The match is on the target aggregate, not the payload's shape: `Shipment`
    /// carries the same `(AccountUrn, String)` pair and still yields `None`.
    #[test]
    fn refuses_the_pair_to_a_different_aggregate() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string());

        assert!(dispatch.parts::<Shipment>().is_none());
    }

    /// `Debug` prints the dead-letter identity and nothing that would need a
    /// `Debug` bound on `Aggregate::Command`.
    #[test]
    fn debug_names_the_aggregate_the_command_and_the_stream() {
        let dispatch = Dispatch::to::<Account>(account(), "freeze".to_string());

        let rendered = format!("{dispatch:?}");

        assert!(rendered.contains("Account"), "{rendered}");
        assert!(rendered.contains("String"), "{rendered}");
        assert!(rendered.contains("urn:account:42"), "{rendered}");
        assert!(rendered.contains("expected_version"), "{rendered}");
        assert!(
            !rendered.contains("freeze"),
            "the payload must not be printed: {rendered}"
        );
    }
}
