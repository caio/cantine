extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Field, Fields, GenericArgument,
    PathArguments, Type, Visibility,
};

// TODO split derives
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
    let feat = &input.ident;
    let name = format_ident!("{}FilterQuery", &input.ident);

    let fields: Vec<_> = get_public_struct_fields(&input).cloned().collect();

    let query_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);

        quote_spanned! { field.span()=>
            #[serde(skip_serializing_if = "Option::is_none")]
            pub #name: Option<std::ops::Range<#ty>>
        }
    });

    let index_name = format_ident!("{}FilterFields", &input.ident);
    let index_fields = fields.iter().map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            pub #name: tantivy::schema::Field
        }
    });

    let from_decls = fields.iter().map(|field| {
        let name = field.ident.as_ref().unwrap();
        let schema_name = format_ident!("_filter_{}", &name);
        let quoted = format!("\"{}\"", schema_name);
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);

        match get_field_type(&ty) {
            FieldType::UNSIGNED => quote_spanned! { field.span()=>
                #name: builder.add_u64_field(#quoted, tantivy::schema::INDEXED)
            },
            FieldType::SIGNED => quote_spanned! { field.span()=>
                #name: builder.add_i64_field(#quoted, tantivy::schema::INDEXED)
            },
            FieldType::FLOAT => quote_spanned! { field.span()=>
                #name: builder.add_f64_field(#quoted, tantivy::schema::INDEXED)
            },
        }
    });

    let try_from_decls = fields.iter().map(|field| {
        if let Some(name) = &field.ident {
            let schema_name = format_ident!("_filter_{}", &name);
            let err_msg = format!("Missing field for {} ({})", name, schema_name);
            let quoted = format!("\"{}\"", schema_name);
            quote_spanned! { field.span()=>
                #name: schema.get_field(#quoted).ok_or(#err_msg)?
            }
        } else {
            unreachable!();
        }
    });

    let interpret_code = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);
        let is_largest = is_largest_type(&ty);

        // FIXME The only difference between matches is the reference
        //       to their largest type, so the only "special" tokens
        //       are the largest type calls:
        //         * u64::from
        //         * RangeQuery::new_u64
        //       And the `is_largest` check is purely to avoid the
        //       identity_conversion clippy warning
        //       Shirley there's a better way, no?
        match get_field_type(&ty) {
            FieldType::UNSIGNED => {
                let range_code = if is_largest {
                    quote! {
                        let range = rr.clone();
                    }
                } else {
                    quote! {
                    let range = std::ops::Range {
                        start: u64::from(rr.start),
                        end: u64::from(rr.end),
                    };
                    }
                };
                quote_spanned! { field.span()=>
                    if let Some(ref rr) = query.#name {
                        #range_code
                        let query = tantivy::query::RangeQuery::new_u64(self.#name, range);
                        result.push(Box::new(query));
                    }
                }
            }
            FieldType::SIGNED => {
                let range_code = if is_largest {
                    quote! {
                        let range = rr.clone();
                    }
                } else {
                    quote! {
                    let range = std::ops::Range {
                        start: i64::from(rr.start),
                        end: i64::from(rr.end),
                    };
                    }
                };

                quote_spanned! { field.span()=>
                    if let Some(ref rr) = query.#name {
                        #range_code
                        let query = tantivy::query::RangeQuery::new_i64(self.#name, range);
                        result.push(Box::new(query));
                    }
                }
            }
            FieldType::FLOAT => {
                let range_code = if is_largest {
                    quote! {
                        let range = rr.clone();
                    }
                } else {
                    quote! {
                    let range = std::ops::Range {
                        start: f64::from(rr.start),
                        end: f64::from(rr.end),
                    };
                    }
                };
                quote_spanned! { field.span()=>
                    if let Some(ref rr) = query.#name {
                        #range_code
                        let query = tantivy::query::RangeQuery::new_f64(self.#name, range);
                        result.push(Box::new(query));
                    }
                }
            }
        }
    });

    let add_to_doc_code = fields.iter().map(|field| {
        let name = &field.ident;

        let opt_type = extract_type_if_option(&field.ty);
        let is_optional = opt_type.is_some();

        let ty = opt_type.unwrap_or(&field.ty);
        let is_largest = is_largest_type(&ty);

        let field_type = get_field_type(&ty);

        let convert_code = if is_largest {
            quote_spanned! { field.span()=>
                let value = value;
            }
        } else {
            match field_type {
                FieldType::UNSIGNED => quote_spanned! { field.span()=>
                    let value = u64::from(value);
                },
                FieldType::SIGNED => quote_spanned! { field.span()=>
                    let value = i64::from(value);
                },
                FieldType::FLOAT => quote_spanned! { field.span()=>
                    let value = f64::from(value);
                },
            }
        };

        let add_code = match field_type {
            FieldType::UNSIGNED => quote!(doc.add_u64(self.#name, value);),
            FieldType::SIGNED => quote!(doc.add_i64(self.#name, value);),
            FieldType::FLOAT => quote!(doc.add_f64(self.#name, value);),
        };

        if is_optional {
            quote_spanned! { field.span()=>
                if let Some(value) = feat.#name {
                    #convert_code
                    #add_code
                }
            }
        } else {
            quote_spanned! { field.span()=>
                let value = feat.#name;
                #convert_code
                #add_code
            }
        }
    });

    quote! {
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
        pub struct #name {
            #(#query_fields),*
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct #index_name {
            #(#index_fields),*
        }

        impl std::convert::TryFrom<&tantivy::schema::Schema> for #index_name {
            // TODO better errors
            type Error = &'static str;

            fn try_from(schema: &tantivy::schema::Schema) -> std::result::Result<Self, Self::Error> {
                Ok(Self {
                    #(#try_from_decls),*
                })
            }
        }

        impl From<&mut tantivy::schema::SchemaBuilder> for #index_name {
            fn from(builder: &mut tantivy::schema::SchemaBuilder) -> Self {
                Self {
                    #(#from_decls),*
                }
            }
        }

        impl #index_name {
            pub fn interpret(&self, query: &#name) -> Vec<Box<dyn tantivy::query::Query>> {
                let mut result : Vec<Box<dyn tantivy::query::Query>> = Vec::new();
                #(#interpret_code);*
                result
            }

            pub fn add_to_doc(&self, doc: &mut tantivy::Document, feat: &#feat) {
                #(#add_to_doc_code);*
            }
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
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
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
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone)]
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

enum FieldType {
    UNSIGNED,
    SIGNED,
    FLOAT,
}

const SUPPORTED_UNSIGNED: [&str; 4] = ["u8", "u16", "u32", "u64"];
const SUPPORTED_SIGNED: [&str; 4] = ["i8", "i16", "i32", "i64"];
const SUPPORTED_FLOAT: [&str; 2] = ["f32", "f64"];

const LARGEST_TYPE: [&str; 3] = ["u64", "i64", "f64"];

fn is_largest_type(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        if tp.path.segments.len() == 1 {
            let ident = &tp.path.segments.first().unwrap().ident;

            for name in LARGEST_TYPE.iter() {
                if ident == name {
                    return true;
                }
            }
        }
    }
    false
}

fn get_field_type(ty: &Type) -> FieldType {
    if let Type::Path(tp) = ty {
        if tp.path.segments.len() == 1 {
            let ident = &tp.path.segments.first().unwrap().ident;

            for name in SUPPORTED_SIGNED.iter() {
                if ident == name {
                    return FieldType::SIGNED;
                }
            }

            for name in SUPPORTED_UNSIGNED.iter() {
                if ident == name {
                    return FieldType::UNSIGNED;
                }
            }

            for name in SUPPORTED_FLOAT.iter() {
                if ident == name {
                    return FieldType::FLOAT;
                }
            }
        }
    }
    unimplemented!()
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
