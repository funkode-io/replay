use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

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
