//! The README's example domain, as far as the map reads it: `User` and `BankAccount` verbatim from "Defining the
//! aggregates", the `FeeLedger` the deposit fee is charged to (`PolicyFeeLedger` in
//! `persistence/examples/global_position.rs`), and the `FeePolicy` from "A policy that targets one aggregate".
//! `tests/readme.rs` checks that the README's diagram is what this generates. Never compiled: the map reads source.

define_aggregate! {
    User {
        // namespace auto-derives from the type name: `User` -> "user".
        state: {
            name: String,
        },
        commands: {
            Register { name: String },
        },
        events: {
            Registered { name: String },
        }
    }
}

impl EventStream for User {
    type Event = UserEvent;

    fn stream_type() -> String {
        "User".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            UserEvent::Registered { name } => self.name = name,
        }
    }
}

impl Aggregate for User {
    type Command = UserCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            UserCommand::Register { name } => Ok(vec![UserEvent::Registered { name }]),
        }
    }
}
define_aggregate! {
    BankAccount {
        // Override the default "bank-account" with the shorter "account",
        // so URNs read `urn:account:alice-checking`.
        namespace: "account",
        state: {
            owner: Option<UserUrn>,
            balance: f64,
        },
        commands: {
            OpenAccount { owner: UserUrn },
            Deposit { amount: f64 },
            Withdraw { amount: f64 },
            CloseMonth { month: chrono::NaiveDate },
        },
        events: {
            AccountOpened { owner: UserUrn },
            Deposited { amount: f64 },
            Withdrawn { amount: f64 },
            MonthlyClosed { month: chrono::NaiveDate, closing_balance: f64 },
        }
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { owner } => self.owner = Some(owner),
            BankAccountEvent::Deposited { amount } => self.balance += amount,
            BankAccountEvent::Withdrawn { amount } => self.balance -= amount,
            // A checkpoint replaces the running balance with the closing one, so a
            // compacted stream rehydrates to exactly the same state.
            BankAccountEvent::MonthlyClosed { closing_balance, .. } => {
                self.balance = closing_balance
            }
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::OpenAccount { owner } => {
                Ok(vec![BankAccountEvent::AccountOpened { owner }])
            }
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(replay::Error::business_rule_violation("Insufficient funds")
                        .with_operation("Withdraw")
                        .with_context("amount_tried", amount));
                }
                Ok(vec![BankAccountEvent::Withdrawn { amount }])
            }
            BankAccountCommand::CloseMonth { month } => Ok(vec![BankAccountEvent::MonthlyClosed {
                month,
                closing_balance: self.balance,
            }]),
        }
    }
}

define_aggregate! {
    FeeLedger {
        state: {
            balance: f64,
            applied_charge_keys: HashSet<String>,
        },
        commands: {
            Credit { amount: f64 },
            ChargeFee { amount: f64, charge_key: String },
        },
        events: {
            Credited { amount: f64 },
            FeeCharged { amount: f64, charge_key: String },
        }
    }
}

impl Aggregate for FeeLedger {
    type Command = FeeLedgerCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            FeeLedgerCommand::Credit { amount } => Ok(vec![FeeLedgerEvent::Credited { amount }]),
            FeeLedgerCommand::ChargeFee { amount, charge_key } => {
                if self.applied_charge_keys.contains(&charge_key) {
                    // A charge already applied under this key: absorb as no-op.
                    return Ok(Vec::new());
                }
                Ok(vec![FeeLedgerEvent::FeeCharged { amount, charge_key }])
            }
        }
    }
}

impl AggregatePolicy for FeePolicy {
    type Event = BankAccountEvent;
    type Target = FeeLedger;

    fn name(&self) -> &str {
        "deposit_fee"
    }

    fn react(
        &self,
        event: &ObservedEvent<BankAccountEvent>,
    ) -> Vec<(FeeLedgerUrn, FeeLedgerCommand)> {
        match &event.data {
            BankAccountEvent::Deposited { amount, reference } => vec![(
                self.ledger_id.clone(),
                FeeLedgerCommand::ChargeFee {
                    amount: amount * FEE_RATE,
                    charge_key: format!("{}#{reference}", event.stream_id),
                },
            )],
            _ => vec![],
        }
    }
}
