extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Field, Fields, GenericArgument,
    PathArguments, Type, Visibility,
};

#[proc_macro_derive(FilterAndAggregation)]
pub fn derive_filter_and_agg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let filter_query = make_struct(&input, "FilterQuery", make_option_range);

    let agg_query = make_struct(&input, "AggregationQuery", make_vec_range);

    let agg_result = make_agg_result(&input);

    let froms = make_from_impl(&input);

    TokenStream::from(quote! {
        #filter_query
        #agg_query
        #agg_result
        #froms
    })
}

fn make_agg_result(input: &DeriveInput) -> TokenStream2 {
    let agg_result = make_struct(&input, "AggregationResult", make_vec);

    let name = format_ident!("{}AggregationResult", &input.ident);

    let tokens = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;

        quote_spanned! {field.span()=>
            for (idx, tally) in self.#name.iter_mut().enumerate() {
                *tally += other.#name[idx];
            }
        }
    });

    quote! {
        #agg_result

        impl #name {
            pub fn merge_same_size(&mut self, other: &Self) {
                #(#tokens;)*
            }
        }
    }
}

fn get_public_struct_fields(input: &DeriveInput) -> impl Iterator<Item = &Field> {
    match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => fields.named.iter().filter(|field| match &field.vis {
                Visibility::Public(_) => true,
                _ => false,
            }),
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

fn make_from_impl(input: &DeriveInput) -> TokenStream2 {
    let aq = format_ident!("{}AggregationQuery", &input.ident);
    let ar = format_ident!("{}AggregationResult", &input.ident);

    match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let declarations = fields
                    .named
                    .iter()
                    .filter(|field| match &field.vis {
                        Visibility::Public(_) => true,
                        _ => false,
                    })
                    .map(|field| {
                        let name = &field.ident;
                        quote_spanned! {
                            field.span()=>
                                #name: if src.#name.is_empty() {
                                    Vec::new()
                                } else {
                                    vec![0; src.#name.len()]
                                }
                        }
                    });

                quote! {
                    impl From<&#aq> for #ar {
                        fn from(src: &#aq) -> Self {
                            Self {
                                #(#declarations),*
                            }
                        }
                    }

                    impl From<#aq> for #ar {
                        fn from(src: #aq) -> Self {
                            #ar::from(&src)
                        }
                    }
                }
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

fn make_option_range(field: &Field) -> TokenStream2 {
    let name = &field.ident;
    let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);

    quote_spanned! { field.span()=>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub #name: Option<std::ops::Range<#ty>> }
}

fn make_vec(field: &Field) -> TokenStream2 {
    let name = &field.ident;
    quote_spanned! { field.span()=> pub #name: Vec<u32> }
}

fn make_vec_range(field: &Field) -> TokenStream2 {
    let name = &field.ident;
    let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);
    quote_spanned! { field.span()=> pub #name: Vec<std::ops::Range<#ty>> }
}

fn make_struct(input: &DeriveInput, name: &str, f: fn(&Field) -> TokenStream2) -> TokenStream2 {
    let struct_name = format_ident!("{}{}", input.ident, name);
    match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => {
                let struct_fields = fields
                    .named
                    .iter()
                    .filter(|field| match &field.vis {
                        Visibility::Public(_) => true,
                        _ => false,
                    })
                    .map(|field| {
                        let tokens = f(&field);
                        quote_spanned! {
                            field.span()=> #tokens
                        }
                    });
                quote! {
                    #[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
                    pub struct #struct_name {
                        #(#struct_fields),*
                    }
                }
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

fn extract_type_if_option(ty: &Type) -> Option<&Type> {
    if let Type::Path(tp) = ty {
        if tp.path.segments.len() == 1 && tp.path.segments.first().unwrap().ident == "Option" {
            if let Some(type_params) = tp.path.segments.first() {
                if let PathArguments::AngleBracketed(ref params) = type_params.arguments {
                    let generic_arg = params.args.first().unwrap();
                    if let GenericArgument::Type(ty) = generic_arg {
                        return Some(ty);
                    }
                }
            }
        }
    }
    None
}
