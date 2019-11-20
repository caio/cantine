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

    let filter_query = make_filter_query(&input);
    let agg_query = make_agg_query(&input);
    let agg_result = make_agg_result(&input);

    TokenStream::from(quote! {
        #filter_query
        #agg_query
        #agg_result
    })
}

fn make_filter_query(input: &DeriveInput) -> TokenStream2 {
    let name = format_ident!("{}FilterQuery", &input.ident);

    let fields = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);

        quote_spanned! { field.span()=>
            #[serde(skip_serializing_if = "Option::is_none")]
            pub #name: Option<std::ops::Range<#ty>>
        }
    });

    quote! {
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
        pub struct #name {
            #(#fields),*
        }
    }
}

fn make_agg_query(input: &DeriveInput) -> TokenStream2 {
    let name = format_ident!("{}AggregationQuery", &input.ident);

    let fields = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);
        quote_spanned! { field.span()=> pub #name: Vec<std::ops::Range<#ty>> }
    });

    quote! {
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
        pub struct #name {
            #(#fields),*
        }
    }
}

fn make_agg_result(input: &DeriveInput) -> TokenStream2 {
    let feature = &input.ident;
    let name = format_ident!("{}AggregationResult", &input.ident);

    let fields = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=> pub #name: Vec<u32> }
    });

    let merge_code = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            for (idx, tally) in self.#name.iter_mut().enumerate() {
                *tally += other.#name[idx];
            }
        }
    });

    let agg_query = format_ident!("{}AggregationQuery", &input.ident);
    let convert_code = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            #name:
                if src.#name.is_empty() {
                    Vec::new()
                } else {
                    vec![0; src.#name.len()]
                }
        }
    });

    let collect_code = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        if let Some(_type) = extract_type_if_option(&field.ty) {
            quote_spanned! { field.span()=>
                if let Some(feat) = feature.#name {
                    for (idx, range) in query.#name.iter().enumerate() {
                        if range.contains(&feat) {
                            self.#name[idx] += 1;
                        }
                    }
                }
            }
        } else {
            quote_spanned! { field.span()=>
                for (idx, range) in query.#name.iter().enumerate() {
                    if range.contains(&feature.#name) {
                        self.#name[idx] += 1;
                    }
                }
            }
        }
    });

    quote! {
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
        pub struct #name {
            #(#fields),*
        }

        impl #name {
            pub fn merge_same_size(&mut self, other: &Self) {
                #(#merge_code);*
            }

            pub fn collect(&mut self, query: &#agg_query, feature: &#feature) {
                #(#collect_code);*
            }
        }

        impl From<&#agg_query> for #name {
            fn from(src: &#agg_query) -> Self {
                Self {
                    #(#convert_code),*
                }
            }
        }

        impl From<#agg_query> for #name {
            fn from(src: #agg_query) -> Self {
                <#name>::from(&src)
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
