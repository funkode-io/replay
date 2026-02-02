use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

mod define_aggregate_macro;
mod merge_events_macro;

use define_aggregate_macro::AggregateDefinition;
use merge_events_macro::QueryEventsDefinition;

#[proc_macro_derive(Event)]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

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
                impl #impl_generics replay::Event for #name #ty_generics #where_clause {
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
                impl #impl_generics replay::Event for #name #ty_generics #where_clause {
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

#[proc_macro]
pub fn define_aggregate(input: TokenStream) -> TokenStream {
    let aggregate_def = parse_macro_input!(input as AggregateDefinition);

    let name = &aggregate_def.name;
    let generics = &aggregate_def.generics;

    // Add required bounds to all generic type parameters
    let mut generics_with_bounds = generics.clone();
    for param in &mut generics_with_bounds.params {
        if let syn::GenericParam::Type(type_param) = param {
            // Add the required bounds: Clone + Default + Debug + Serialize + DeserializeOwned + Send + Sync
            // Note: PartialEq is NOT automatically added - it's only required if T is used in events,
            // which the Event derive macro will enforce naturally through its own where clauses
            type_param.bounds.push(syn::parse_quote!(Clone));
            type_param.bounds.push(syn::parse_quote!(Default));
            type_param.bounds.push(syn::parse_quote!(std::fmt::Debug));
            type_param.bounds.push(syn::parse_quote!(serde::Serialize));
            type_param
                .bounds
                .push(syn::parse_quote!(serde::de::DeserializeOwned));
            type_param.bounds.push(syn::parse_quote!(Send));
            type_param.bounds.push(syn::parse_quote!(Sync));
        }
    }

    let (impl_generics, ty_generics, where_clause) = generics_with_bounds.split_for_impl();
    // For type declarations, we need the params with bounds
    let type_params = &generics_with_bounds.params;

    // Analyze which type parameters are used in events using proper AST traversal
    let type_param_idents: Vec<_> = generics
        .params
        .iter()
        .filter_map(|param| {
            if let syn::GenericParam::Type(type_param) = param {
                Some(type_param.ident.clone())
            } else {
                None
            }
        })
        .collect();

    // Visitor to detect type parameter usage in types
    struct TypeParamVisitor<'a> {
        type_params: &'a [syn::Ident],
        found: std::collections::HashSet<syn::Ident>,
    }

    impl<'ast, 'a> syn::visit::Visit<'ast> for TypeParamVisitor<'a> {
        fn visit_path(&mut self, path: &'ast syn::Path) {
            // Check if this path is a single identifier matching one of our type parameters
            if path.segments.len() == 1 {
                let segment = &path.segments[0];
                if segment.arguments.is_empty() {
                    for type_param in self.type_params {
                        if segment.ident == *type_param {
                            self.found.insert(type_param.clone());
                            break;
                        }
                    }
                }
            }
            // Continue visiting nested types (e.g., Vec<T>, Option<T>)
            syn::visit::visit_path(self, path);
        }
    }

    // Check if a type is used in command fields
    let mut used_in_commands = std::collections::HashSet::new();
    for command in &aggregate_def.commands {
        for field in &command.fields {
            let mut visitor = TypeParamVisitor {
                type_params: &type_param_idents,
                found: std::collections::HashSet::new(),
            };
            syn::visit::visit_field(&mut visitor, field);
            used_in_commands.extend(visitor.found);
        }
    }

    // Check if a type is used in event fields
    let mut used_in_events = std::collections::HashSet::new();
    for event in &aggregate_def.events {
        for field in &event.fields {
            let mut visitor = TypeParamVisitor {
                type_params: &type_param_idents,
                found: std::collections::HashSet::new(),
            };
            syn::visit::visit_field(&mut visitor, field);
            used_in_events.extend(visitor.found);
        }
    }

    // Create generics for commands that only include used type parameters
    let mut command_generics = generics_with_bounds.clone();
    command_generics.params = command_generics
        .params
        .iter()
        .filter(|param| {
            if let syn::GenericParam::Type(type_param) = param {
                used_in_commands.contains(&type_param.ident)
            } else {
                true // Keep lifetime and const parameters
            }
        })
        .cloned()
        .collect();

    let (_command_impl_generics, _command_ty_generics, command_where_clause) =
        command_generics.split_for_impl();
    let command_type_params = &command_generics.params;

    // Create generics for events that only include used type parameters
    let mut event_generics = generics_with_bounds.clone();
    event_generics.params = event_generics
        .params
        .iter()
        .filter(|param| {
            if let syn::GenericParam::Type(type_param) = param {
                used_in_events.contains(&type_param.ident)
            } else {
                true // Keep lifetime and const parameters
            }
        })
        .cloned()
        .collect();

    let (_event_impl_generics, _event_ty_generics, event_where_clause) =
        event_generics.split_for_impl();
    let event_type_params = &event_generics.params;

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

    // Generate Services trait if service functions are defined or base traits are specified
    let services_trait = if !aggregate_def.service_functions.is_empty()
        || !aggregate_def.base_service_traits.is_empty()
    {
        // Check if any function is async
        let has_async = aggregate_def.service_functions.iter().any(|f| f.is_async);

        let trait_methods = aggregate_def.service_functions.iter().map(|func| {
            let func_name = &func.name;
            let lifetimes = &func.lifetimes;
            let inputs = &func.inputs;
            let output = &func.output;
            let async_token = if func.is_async {
                quote! { async }
            } else {
                quote! {}
            };

            let lifetime_params = if !lifetimes.is_empty() {
                quote! { <#(#lifetimes),*> }
            } else {
                quote! {}
            };

            quote! {
                #async_token fn #func_name #lifetime_params(&self, #(#inputs),*) #output;
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

        // Build the trait bounds (supertraits)
        let trait_bounds = if !aggregate_def.base_service_traits.is_empty() {
            let base_traits = &aggregate_def.base_service_traits;
            quote! { : #(#base_traits)+* + Send + Sync }
        } else {
            quote! { : Send + Sync }
        };

        quote! {
            #async_trait_attr
            pub trait #services_name #trait_bounds {
                #(#trait_methods)*
            }
        }
    } else {
        quote! {
            #[derive(Clone, Debug)]
            pub struct #services_name;
        }
    };

    // Generate serde(bound = "") to prevent serde from adding its own bounds
    let serde_bound_attr = if !generics.params.is_empty() {
        quote! { #[serde(bound = "")] }
    } else {
        quote! {}
    };

    let expanded = quote! {
        // Aggregate state struct
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        #serde_bound_attr
        pub struct #name <#type_params> #where_clause {
            pub id: #urn_name,
            #(#state_fields),*
        }

        // Implement WithId trait
        impl #impl_generics replay::WithId for #name #ty_generics #where_clause {
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

        // Implement PartialEq to compare aggregates by ID only
        impl #impl_generics PartialEq for #name #ty_generics #where_clause {
            fn eq(&self, other: &Self) -> bool {
                self.id == other.id
            }
        }

        // Command enum
        pub enum #command_name <#command_type_params> #command_where_clause {
            #(#command_variants),*
        }

        // Event enum with Event derive
        #[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Debug, replay_macros::Event)]
        #serde_bound_attr
        pub enum #event_name <#event_type_params> #event_where_clause {
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

/// Macro to generate a wrapper enum for multiple event types in queries.
///
/// Example usage:
/// ```ignore
/// query_events!(UserHistoryEvent => [UserEvent, CatalogEvent]);
/// ```
///
/// This will generate:
/// - An enum with variants for each event type
/// - From trait implementations for each event type
/// - Serialize/Deserialize implementations that delegate to inner types
/// - replay::Event trait implementation
/// - PartialEq, Display, and Debug implementations
#[proc_macro]
pub fn query_events(input: TokenStream) -> TokenStream {
    let query_def = parse_macro_input!(input as QueryEventsDefinition);

    let enum_name = &query_def.name;
    let event_types = &query_def.event_types;

    // Generate enum variants (e.g., UserEvent(UserEvent), CatalogEvent(CatalogEvent))
    let enum_variants = event_types.iter().map(|ty| {
        // Extract the last segment of the type path for the variant name
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            #variant_name(#ty)
        }
    });

    // Generate From trait implementations
    let from_impls = event_types.iter().map(|ty| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            impl From<#ty> for #enum_name {
                fn from(event: #ty) -> Self {
                    #enum_name::#variant_name(event)
                }
            }
        }
    });

    // Generate Deserialize match arms (try each type in order)
    let deserialize_attempts = event_types.iter().enumerate().map(|(i, ty)| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        if i < event_types.len() - 1 {
            // For all but the last, clone the value for retry
            quote! {
                if let Ok(event) = serde_json::from_value::<#ty>(value.clone()) {
                    return Ok(#enum_name::#variant_name(event));
                }
            }
        } else {
            // For the last one, no clone needed
            quote! {
                match serde_json::from_value::<#ty>(value) {
                    Ok(event) => Ok(#enum_name::#variant_name(event)),
                    Err(_) => Err(serde::de::Error::custom(
                        format!("Could not deserialize as any of the expected event types")
                    )),
                }
            }
        }
    });

    // Generate Serialize match arms
    let serialize_arms = event_types.iter().map(|ty| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            #enum_name::#variant_name(event) => event.serialize(serializer)
        }
    });

    // Generate replay::Event match arms
    let event_type_arms = event_types.iter().map(|ty| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            #enum_name::#variant_name(event) => event.event_type()
        }
    });

    // Generate PartialEq match arms
    let partial_eq_arms = event_types.iter().map(|ty| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            (#enum_name::#variant_name(e1), #enum_name::#variant_name(e2)) => e1 == e2
        }
    });

    // Generate Display match arms
    let display_arms = event_types.iter().map(|ty| {
        let variant_name = if let Type::Path(type_path) = ty {
            type_path.path.segments.last().unwrap().ident.clone()
        } else {
            panic!("Expected a type path");
        };

        quote! {
            #enum_name::#variant_name(event) => write!(f, "{:?}", event)
        }
    });

    let expanded = quote! {
        #[derive(Clone, Debug)]
        pub enum #enum_name {
            #(#enum_variants),*
        }

        // Generate From trait implementations
        #(#from_impls)*

        // Deserialize implementation
        impl<'de> serde::Deserialize<'de> for #enum_name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = serde_json::Value::deserialize(deserializer)?;

                #(#deserialize_attempts)*
            }
        }

        // Serialize implementation
        impl serde::Serialize for #enum_name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                match self {
                    #(#serialize_arms),*
                }
            }
        }

        // replay::Event implementation
        impl replay::Event for #enum_name {
            fn event_type(&self) -> String {
                match self {
                    #(#event_type_arms),*
                }
            }
        }

        // PartialEq implementation
        impl PartialEq for #enum_name {
            fn eq(&self, other: &Self) -> bool {
                match (self, other) {
                    #(#partial_eq_arms,)*
                    _ => false,
                }
            }
        }

        // Display implementation
        impl std::fmt::Display for #enum_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    #(#display_arms),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}
