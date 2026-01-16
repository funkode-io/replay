use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    token::Brace,
    Data, DeriveInput, Field, Fields, Ident, Token, Type,
};

#[proc_macro_derive(Event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let event_type_impl = match input.data {
        Data::Enum(data_enum) => {
            let match_arms = data_enum.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let variant_str = variant_name.to_string();
                match &variant.fields {
                    Fields::Named(_) | Fields::Unnamed(_) | Fields::Unit => {
                        quote! {
                            #name::#variant_name { .. } => #variant_str.to_string(),
                        }
                    }
                }
            });

            quote! {
                impl replay::Event for #name {
                    fn event_type(&self) -> String {
                        match self {
                            #(#match_arms)*
                        }
                    }
                }
            }
        }
        // if it's an struct use the struct name
        Data::Struct(_data_struct) => {
            quote! {
                impl replay::Event for #name {
                    fn event_type(&self) -> String {
                        stringify!(#name).to_string()
                    }
                }
            }
        }
        _ => panic!("Event can only be derived for enums or structs"),
    };

    TokenStream::from(event_type_impl)
}

/*
Derive `Display`, `From<type> for Urn` and `FromStr` for a type encapsulating a Urn.

Example of implementation that need to be derived

    #[derive(SerializeDisplay, DeserializeFromStr)]
    pub struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    impl Display for BankAccountUrn {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl FromStr for BankAccountUrn {
        type Err = urn::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(Urn::from_str(s)?))
        }
    }

Example of usage:

#[derive(Urn, SerializeDisplay, DeserializeFromStr)]
pub struct BankAccountUrn(Urn);

*/
#[proc_macro_derive(Urn)]
pub fn derive_urn(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let urn_impl = quote! {

        impl From<#name> for urn::Urn {
            fn from(urn: #name) -> Self {
                urn.0
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for #name {
            type Err = urn::Error;

            fn from_str(s: &str) -> Result<Self, urn::Error> {
                Ok(Self(Urn::from_str(s)?))
            }
        }
    };

    TokenStream::from(urn_impl)
}

// Struct to parse the define_aggregate! macro input
struct AggregateDefinition {
    name: Ident,
    namespace: Option<syn::LitStr>,
    state_fields: Vec<Field>,
    commands: Vec<CommandVariant>,
    events: Vec<EventVariant>,
}

struct CommandVariant {
    name: Ident,
    fields: Vec<Field>,
}

struct EventVariant {
    name: Ident,
    fields: Vec<Field>,
}

impl Parse for AggregateDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        let content;
        syn::braced!(content in input);

        let mut namespace = None;
        let mut state_fields = Vec::new();
        let mut commands = Vec::new();
        let mut events = Vec::new();

        while !content.is_empty() {
            let section_name: Ident = content.parse()?;
            content.parse::<Token![:]>()?;

            match section_name.to_string().as_str() {
                "namespace" => {
                    namespace = Some(content.parse()?);
                }
                _ => {
                    let section_content;
                    syn::braced!(section_content in content);

                    match section_name.to_string().as_str() {
                        "state" => {
                            while !section_content.is_empty() {
                                let field_name: Ident = section_content.parse()?;
                                section_content.parse::<Token![:]>()?;
                                let field_type: Type = section_content.parse()?;

                                state_fields.push(Field {
                                    attrs: Vec::new(),
                                    vis: syn::Visibility::Public(syn::token::Pub::default()),
                                    mutability: syn::FieldMutability::None,
                                    ident: Some(field_name),
                                    colon_token: Some(Token![:](proc_macro2::Span::call_site())),
                                    ty: field_type,
                                });

                                if section_content.peek(Token![,]) {
                                    section_content.parse::<Token![,]>()?;
                                }
                            }
                        }
                        "commands" => {
                            while !section_content.is_empty() {
                                let variant_name: Ident = section_content.parse()?;

                                let variant_fields = if section_content.peek(Brace) {
                                    let fields_content;
                                    syn::braced!(fields_content in section_content);

                                    let mut fields = Vec::new();
                                    while !fields_content.is_empty() {
                                        let field_name: Ident = fields_content.parse()?;
                                        fields_content.parse::<Token![:]>()?;
                                        let field_type: Type = fields_content.parse()?;

                                        fields.push(Field {
                                            attrs: Vec::new(),
                                            vis: syn::Visibility::Inherited,
                                            mutability: syn::FieldMutability::None,
                                            ident: Some(field_name),
                                            colon_token: Some(Token![:](
                                                proc_macro2::Span::call_site(),
                                            )),
                                            ty: field_type,
                                        });

                                        if fields_content.peek(Token![,]) {
                                            fields_content.parse::<Token![,]>()?;
                                        }
                                    }
                                    fields
                                } else {
                                    Vec::new()
                                };

                                commands.push(CommandVariant {
                                    name: variant_name,
                                    fields: variant_fields,
                                });

                                if section_content.peek(Token![,]) {
                                    section_content.parse::<Token![,]>()?;
                                }
                            }
                        }
                        "events" => {
                            while !section_content.is_empty() {
                                let variant_name: Ident = section_content.parse()?;

                                let variant_fields = if section_content.peek(Brace) {
                                    let fields_content;
                                    syn::braced!(fields_content in section_content);

                                    let mut fields = Vec::new();
                                    while !fields_content.is_empty() {
                                        let field_name: Ident = fields_content.parse()?;
                                        fields_content.parse::<Token![:]>()?;
                                        let field_type: Type = fields_content.parse()?;

                                        fields.push(Field {
                                            attrs: Vec::new(),
                                            vis: syn::Visibility::Inherited,
                                            mutability: syn::FieldMutability::None,
                                            ident: Some(field_name),
                                            colon_token: Some(Token![:](
                                                proc_macro2::Span::call_site(),
                                            )),
                                            ty: field_type,
                                        });

                                        if fields_content.peek(Token![,]) {
                                            fields_content.parse::<Token![,]>()?;
                                        }
                                    }
                                    fields
                                } else {
                                    Vec::new()
                                };

                                events.push(EventVariant {
                                    name: variant_name,
                                    fields: variant_fields,
                                });

                                if section_content.peek(Token![,]) {
                                    section_content.parse::<Token![,]>()?;
                                }
                            }
                        }
                        _ => {
                            return Err(syn::Error::new_spanned(
                                section_name,
                                "Expected 'state', 'commands', or 'events'",
                            ));
                        }
                    }
                }
            }

            if content.peek(Token![,]) {
                content.parse::<Token![,]>()?;
            }
        }

        Ok(AggregateDefinition {
            name,
            namespace,
            state_fields,
            commands,
            events,
        })
    }
}

#[proc_macro]
pub fn define_aggregate(input: TokenStream) -> TokenStream {
    let aggregate_def = parse_macro_input!(input as AggregateDefinition);

    let name = &aggregate_def.name;
    let command_name = quote::format_ident!("{}Command", name);
    let event_name = quote::format_ident!("{}Event", name);
    let urn_name = quote::format_ident!("{}Urn", name);
    let services_name = quote::format_ident!("{}Services", name);

    // Generate state fields
    let state_fields = &aggregate_def.state_fields;

    // Generate command variants
    let command_variants = aggregate_def.commands.iter().map(|cmd| {
        let variant_name = &cmd.name;
        if cmd.fields.is_empty() {
            quote! { #variant_name }
        } else {
            let fields = &cmd.fields;
            quote! { #variant_name { #(#fields),* } }
        }
    });

    // Generate event variants
    let event_variants = aggregate_def.events.iter().map(|evt| {
        let variant_name = &evt.name;
        if evt.fields.is_empty() {
            quote! { #variant_name }
        } else {
            let fields = &evt.fields;
            quote! { #variant_name { #(#fields),* } }
        }
    });

    // Generate URN helper methods if namespace is provided
    let urn_impl_methods = if let Some(namespace) = &aggregate_def.namespace {
        quote! {
            impl #urn_name {
                /// Create a new URN with the configured namespace
                pub fn new(id: impl std::fmt::Display) -> Result<Self, urn::Error> {
                    let id_str = id.to_string();
                    Ok(Self(urn::UrnBuilder::new(#namespace, &id_str).build()?))
                }

                /// Get the namespace identifier (NID)
                pub fn namespace() -> &'static str {
                    #namespace
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        // Aggregate state struct
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, Default)]
        pub struct #name {
            #(#state_fields),*
        }

        // Command enum
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug)]
        pub enum #command_name {
            #(#command_variants),*
        }

        // Event enum with Event derive
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, replay_macros::Event)]
        pub enum #event_name {
            #(#event_variants),*
        }

        // URN type
        #[derive(Clone, PartialEq, Debug)]
        pub struct #urn_name(pub urn::Urn);

        impl From<#urn_name> for urn::Urn {
            fn from(urn: #urn_name) -> Self {
                urn.0
            }
        }

        impl std::fmt::Display for #urn_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        // URN helper methods (if namespace is configured)
        #urn_impl_methods

        // Services placeholder
        #[derive(Clone, Debug)]
        pub struct #services_name;
    };

    TokenStream::from(expanded)
}
