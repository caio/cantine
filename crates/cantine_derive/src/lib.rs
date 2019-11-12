extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Field, Fields, GenericArgument, Ident,
    PathArguments, Type, Visibility,
};

#[proc_macro_derive(FilterAndAggregation)]
pub fn derive_filter_and_agg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let filter_query = make_struct(&input.data, &input.ident, "FilterQuery", make_option_range);

    let agg_query = make_struct(
        &input.data,
        &input.ident,
        "AggregationQuery",
        make_vec_range,
    );

    let agg_result = make_struct(&input.data, &input.ident, "AggregationResult", make_vec);

    TokenStream::from(quote! {
        #filter_query
        #agg_query
        #agg_result
    })
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

fn make_struct(
    data: &Data,
    base: &Ident,
    name: &str,
    f: fn(&Field) -> TokenStream2,
) -> TokenStream2 {
    let struct_name = format_ident!("{}{}", base, name);
    match data {
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
