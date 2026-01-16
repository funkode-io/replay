use replay_macros::define_aggregate;

// This is needed to make the macro work inside tests
use replay::{Aggregate, EventStream};

define_aggregate! {
    BankAccount {
        state: {
            account_number: String,
            balance: f64
        },
        commands: {
            OpenAccount { account_number: String },
            Deposit { amount: f64 },
            Withdraw { amount: f64 }
        },
        events: {
            AccountOpened { account_number: String },
            Deposited { amount: f64 },
            Withdrawn { amount: f64 }
        }
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;
    type StreamId = BankAccountUrn;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { account_number } => {
                self.account_number = account_number;
            }
            BankAccountEvent::Deposited { amount } => {
                self.balance += amount;
            }
            BankAccountEvent::Withdrawn { amount } => {
                self.balance -= amount;
            }
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = BankAccountServices;

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::OpenAccount { account_number } => {
                Ok(vec![BankAccountEvent::AccountOpened { account_number }])
            }
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    Err(
                        replay::Error::business_rule_violation("Insufficient balance")
                            .with_operation("Withdraw")
                            .with_context("tried_amount", amount),
                    )
                } else {
                    Ok(vec![BankAccountEvent::Withdrawn { amount }])
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_generation() {
        // Test that the aggregate struct was created
        let account = BankAccount::default();
        assert_eq!(account.balance, 0.0);
        assert_eq!(account.account_number, "");

        // Test that command enum was created
        let cmd = BankAccountCommand::OpenAccount {
            account_number: "ACC001".to_string(),
        };
        assert!(matches!(cmd, BankAccountCommand::OpenAccount { .. }));

        // Test that event enum was created
        let evt = BankAccountEvent::Deposited { amount: 100.0 };
        assert!(matches!(evt, BankAccountEvent::Deposited { .. }));
    }

    #[test]
    fn test_event_type() {
        use replay::Event;

        let evt = BankAccountEvent::AccountOpened {
            account_number: "ACC001".to_string(),
        };
        assert_eq!(evt.event_type(), "AccountOpened");

        let evt = BankAccountEvent::Deposited { amount: 100.0 };
        assert_eq!(evt.event_type(), "Deposited");
    }

    #[test]
    fn test_stream_type() {
        use replay::EventStream;

        assert_eq!(BankAccount::stream_type(), "BankAccount");
    }

    #[test]
    fn test_lifecylce_of_aggregate() {
        let mut account = BankAccount::default();

        // Apply AccountOpened event
        let open_event = BankAccountEvent::AccountOpened {
            account_number: "ACC001".to_string(),
        };
        account.apply(open_event);
        assert_eq!(account.account_number, "ACC001");

        // Apply Deposited event
        let deposit_event = BankAccountEvent::Deposited { amount: 150.0 };
        account.apply(deposit_event);
        assert_eq!(account.balance, 150.0);

        // Apply Withdrawn event
        let withdraw_event = BankAccountEvent::Withdrawn { amount: 50.0 };
        account.apply(withdraw_event);
        assert_eq!(account.balance, 100.0);
    }
}
