use syn::{
    parse::{Parse, ParseStream},
    token::Brace,
    Field, FnArg, Ident, ReturnType, Token, Type,
};

// Struct to parse the define_aggregate! macro input
pub struct AggregateDefinition {
    pub name: Ident,
    pub namespace: Option<syn::LitStr>,
    pub state_fields: Vec<Field>,
    pub commands: Vec<CommandVariant>,
    pub events: Vec<EventVariant>,
    pub service_functions: Vec<ServiceFunction>,
}

pub struct CommandVariant {
    pub name: Ident,
    pub fields: Vec<Field>,
}

pub struct EventVariant {
    pub name: Ident,
    pub fields: Vec<Field>,
}

pub struct ServiceFunction {
    pub name: Ident,
    pub inputs: Vec<FnArg>,
    pub output: ReturnType,
    pub is_async: bool,
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
