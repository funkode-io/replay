use crate::persistence::PersistedEvent;

pub trait Query: Sync + Send {
    type Event: crate::Event;

    fn stream_filter(&self) -> crate::StreamFilter {
        crate::StreamFilter::all()
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>);
}
