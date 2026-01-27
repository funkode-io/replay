use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    token::Brace,
    Data, DeriveInput, Field, Fields, FnArg, Ident, ReturnType, Token, Type,
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
    service_functions: Vec<ServiceFunction>,
}

struct CommandVariant {
    name: Ident,
    fields: Vec<Field>,
}

struct EventVariant {
    name: Ident,
    fields: Vec<Field>,
}

struct ServiceFunction {
    name: Ident,
    inputs: Vec<FnArg>,
    output: ReturnType,
    is_async: bool,
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
        let mut service_functions = Vec::new();

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
                        "service" => {
                            while !section_content.is_empty() {
                                // Parse optional "async"
                                let is_async = section_content.peek(Token![async]);
                                if is_async {
                                    section_content.parse::<Token![async]>()?;
                                }

                                // Parse "fn"
                                section_content.parse::<Token![fn]>()?;

                                // Parse function name
                                let fn_name: Ident = section_content.parse()?;

                                // Parse function parameters
                                let inputs_content;
                                syn::parenthesized!(inputs_content in section_content);
                                let inputs: syn::punctuated::Punctuated<FnArg, Token![,]> =
                                    inputs_content.parse_terminated(FnArg::parse, Token![,])?;

                                // Parse return type
                                let output: ReturnType = section_content.parse()?;

                                service_functions.push(ServiceFunction {
                                    name: fn_name,
                                    inputs: inputs.into_iter().collect(),
                                    output,
                                    is_async,
                                });

                                // Optional comma or semicolon
                                if section_content.peek(Token![,]) {
                                    section_content.parse::<Token![,]>()?;
                                } else if section_content.peek(Token![;]) {
                                    section_content.parse::<Token![;]>()?;
                                }
                            }
                        }
                        _ => {
                            return Err(syn::Error::new_spanned(
                                section_name,
                                "Expected 'state', 'commands', 'events', or 'service'",
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
            service_functions,
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

    // Derive namespace from aggregate name if not provided
    let namespace_str = aggregate_def
        .namespace
        .as_ref()
        .map(|lit| lit.value())
        .unwrap_or_else(|| {
            // Convert CamelCase to kebab-case with proper acronym handling
            // e.g., "HTTPConnection" -> "http-connection", "BankAccount" -> "bank-account"
            let name_str = name.to_string();
            let mut result = String::new();
            let chars: Vec<char> = name_str.chars().collect();

            for i in 0..chars.len() {
                let ch = chars[i];

                if ch.is_uppercase() {
                    // Add hyphen before uppercase if:
                    // 1. Not at the start (i > 0)
                    // 2. AND one of:
                    //    a. Previous char is lowercase (e.g., "myHTTP" -> "my-HTTP")
                    //    b. Next char exists and is lowercase (end of acronym: "HTTPConnection" -> "HTTP-Connection")
                    if i > 0 {
                        let prev_is_lower = chars[i - 1].is_lowercase();
                        let next_is_lower = i + 1 < chars.len() && chars[i + 1].is_lowercase();

                        if prev_is_lower || next_is_lower {
                            result.push('-');
                        }
                    }
                }

                result.push(ch.to_ascii_lowercase());
            }
            result
        });

    let namespace = syn::LitStr::new(&namespace_str, name.span());

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

    // Generate URN serialization implementations with namespace validation
    let urn_serde_impl = quote! {
        impl serde::Serialize for #urn_name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.0.to_string())
            }
        }

        impl<'de> serde::Deserialize<'de> for #urn_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                use std::str::FromStr;
                let s = String::deserialize(deserializer)?;
                let urn = urn::Urn::from_str(&s)
                    .map_err(|e| serde::de::Error::custom(format!("Invalid URN: {}", e)))?;

                if urn.nid() != #namespace {
                    return Err(serde::de::Error::custom(
                        format!("Invalid URN namespace: expected '{}', got '{}'", #namespace, urn.nid())
                    ));
                }

                Ok(Self(urn))
            }
        }
    };

    // Generate URN helper methods with namespace validation
    let urn_impl_methods = quote! {
        impl #urn_name {
            /// Create a new URN with the configured namespace
            pub fn new(id: impl std::fmt::Display) -> Result<Self, urn::Error> {
                let id_str = id.to_string();
                Ok(Self(urn::UrnBuilder::new(#namespace, &id_str).build()?))
            }

            /// Parse a URN string and validate the namespace
            pub fn parse(input: impl AsRef<str>) -> Result<Self, String> {
                use std::str::FromStr;
                let urn = urn::Urn::from_str(input.as_ref())
                    .map_err(|e| format!("Failed to parse URN: {}", e))?;

                if urn.nid() != #namespace {
                    return Err(format!(
                        "Invalid URN namespace: expected '{}', got '{}'",
                        #namespace,
                        urn.nid()
                    ));
                }

                Ok(Self(urn))
            }

            /// Get the namespace identifier (NID)
            pub fn namespace() -> &'static str {
                #namespace
            }

            /// Get a reference to the inner URN
            pub fn to_urn(&self) -> &urn::Urn {
                &self.0
            }

            /// Get the namespace identifier (NID) of this URN instance
            pub fn nid(&self) -> &str {
                self.0.nid()
            }

            /// Get the namespace specific string (NSS) - the ID part of the URN
            pub fn nss(&self) -> &str {
                self.0.nss()
            }

            /// Create a new URN with a random UUID v4 identifier
            pub fn new_random() -> Self {
                let id = uuid::Uuid::new_v4().to_string();
                Self(urn::UrnBuilder::new(#namespace, &id).build().expect("UUID-based URN should always be valid"))
            }
        }
    };

    // Extract field names for initialization
    let state_field_names = aggregate_def.state_fields.iter().map(|f| &f.ident);

    // Generate Services trait if service functions are defined
    let services_trait = if !aggregate_def.service_functions.is_empty() {
        // Check if any function is async
        let has_async = aggregate_def.service_functions.iter().any(|f| f.is_async);

        let trait_methods = aggregate_def.service_functions.iter().map(|func| {
            let func_name = &func.name;
            let inputs = &func.inputs;
            let output = &func.output;
            let async_token = if func.is_async {
                quote! { async }
            } else {
                quote! {}
            };
            quote! {
                #async_token fn #func_name(&self, #(#inputs),*) #output;
            }
        });

        // Add async_trait attributes conditionally based on target
        let async_trait_attr = if has_async {
            quote! {
                #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
                #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            }
        } else {
            quote! {}
        };

        quote! {
            #async_trait_attr
            pub trait #services_name: Send + Sync {
                #(#trait_methods)*
            }
        }
    } else {
        quote! {
            #[derive(Clone, Debug)]
            pub struct #services_name;
        }
    };

    let expanded = quote! {
        // Aggregate state struct
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug)]
        pub struct #name {
            pub id: #urn_name,
            #(#state_fields),*
        }

        // Implement WithId trait
        impl replay::WithId for #name {
            type StreamId = #urn_name;

            fn with_id(id: Self::StreamId) -> Self {
                Self {
                    id,
                    #(#state_field_names: Default::default()),*
                }
            }

            fn get_id(&self) -> &Self::StreamId {
                &self.id
            }
        }

        // Command enum
        #[derive(Debug)]
        pub enum #command_name {
            #(#command_variants),*
        }

        // Event enum with Event derive
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, replay_macros::Event)]
        pub enum #event_name {
            #(#event_variants),*
        }

        // URN type
        #[derive(Clone, PartialEq, Eq, Debug, Hash)]
        pub struct #urn_name(urn::Urn);

        #urn_serde_impl

        impl From<#urn_name> for urn::Urn {
            fn from(urn: #urn_name) -> Self {
                urn.0
            }
        }

        impl std::convert::TryFrom<urn::Urn> for #urn_name {
            type Error = String;

            fn try_from(urn: urn::Urn) -> Result<Self, Self::Error> {
                if urn.nid() == #namespace {
                    Ok(#urn_name(urn))
                } else {
                    Err(format!(
                        "Invalid URN namespace: expected '{}', got '{}'",
                        #namespace,
                        urn.nid()
                    ))
                }
            }
        }

        impl std::fmt::Display for #urn_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        // URN helper methods
        #urn_impl_methods

        // Generate Services trait
        #services_trait
    };

    TokenStream::from(expanded)
}
