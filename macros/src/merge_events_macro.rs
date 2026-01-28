use syn::{
    parse::{Parse, ParseStream},
    Ident, Token, Type,
};

// Struct to parse the query_events! macro input
pub struct QueryEventsDefinition {
    pub name: Ident,
    pub event_types: Vec<Type>,
}

impl Parse for QueryEventsDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![=>]>()?;

        let content;
        syn::bracketed!(content in input);

        let event_types: syn::punctuated::Punctuated<Type, Token![,]> =
            content.parse_terminated(Type::parse, Token![,])?;

        Ok(QueryEventsDefinition {
            name,
            event_types: event_types.into_iter().collect(),
        })
    }
}
