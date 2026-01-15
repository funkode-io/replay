pub trait Query: Sync + Send {
    type Event: replay::Event;

    fn stream_filter(&self) -> crate::StreamFilter {
        crate::StreamFilter::all()
    }

    fn update(&mut self, event: Self::Event);
}
