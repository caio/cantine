use proc_macro::TokenStream;
use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use quote::{format_ident, quote, quote_spanned};
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Field, Fields, GenericArgument,
    PathArguments, Type, Visibility,
};

#[proc_macro_derive(Filterable)]
pub fn derive_filter_and_agg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    TokenStream::from(
        parse_public_fields(&input).map_or_else(render_error, |fields| {
            make_filter_query(&input.ident, &fields)
        }),
    )
}

#[proc_macro_derive(Aggregable)]
pub fn derive_agg(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    TokenStream::from(
        parse_public_fields(&input).map_or_else(render_error, |fields| {
            let agg_query = make_agg_query(&input.ident, &fields);
            let agg_result = make_agg_result(&input.ident, &fields);
            quote! {
                #agg_query
                #agg_result
            }
        }),
    )
}

fn parse_public_fields(input: &DeriveInput) -> Result<Vec<FieldInfo<'_>>, Error> {
    let fields = get_public_fields(input)?;

    if fields.is_empty() {
        Err(Error::BadInput)
    } else {
        let mut infos = Vec::with_capacity(fields.len());

        for field in fields.into_iter() {
            infos.push(FieldInfo::new(field)?);
        }

        Ok(infos)
    }
}

fn get_public_fields(input: &DeriveInput) -> Result<Vec<&Field>, Error> {
    match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => Ok(fields
                .named
                .iter()
                .filter(|field| match &field.vis {
                    Visibility::Public(_) => true,
                    _ => false,
                })
                .collect()),
            _ => Err(Error::BadInput),
        },
        _ => Err(Error::BadInput),
    }
}

struct FieldInfo<'a> {
    span: Span,

    ident: &'a Ident,
    ty: &'a Type,
    is_optional: bool,

    schema: FieldType,
    is_largest: bool,
}

impl<'a> FieldInfo<'a> {
    fn new(field: &'a Field) -> Result<Self, Error> {
        let span = field.span();
        let ident = field.ident.as_ref().ok_or(Error::BadField(span))?;

        let optional_type = extract_type_if_option(&field.ty);
        let is_optional = optional_type.is_some();
        let ty = optional_type.unwrap_or(&field.ty);

        let (schema, is_largest) = get_field_type(&ty).ok_or(Error::BadField(span))?;
        Ok(Self {
            span,
            ident,
            ty,
            is_optional,
            schema,
            is_largest,
        })
    }

    fn span(&self) -> Span {
        self.span
    }
}

fn make_filter_query(feat: &Ident, fields: &[FieldInfo]) -> TokenStream2 {
    let name = format_ident!("FilterableFilterQuery{}", &feat);

    let query_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;

        quote_spanned! { field.span() =>
            #[serde(skip_serializing_if = "Option::is_none")]
            pub #name: Option<std::ops::Range<#ty>>
        }
    });

    let index_name = format_ident!("FilterableFilterFields{}", &feat);
    let index_fields = fields.iter().map(|field| {
        let name = field.ident;
        quote_spanned! { field.span()=>
            pub #name: tantivy::schema::Field
        }
    });

    let from_decls = fields.iter().map(|field| {
        let name = field.ident;
        let field_name = format_ident!("Filterable_field_{}", name);
        let quoted = format!("{}", &field_name);

        let method = match field.schema {
            FieldType::UNSIGNED => quote!(add_u64_field),
            FieldType::SIGNED => quote!(add_i64_field),
            FieldType::FLOAT => quote!(add_f64_field),
        };

        quote_spanned! { field.span()=>
            #name: builder.#method(#quoted, flags.clone())
        }
    });

    let try_from_decls = fields.iter().map(|field| {
        let name = field.ident;
        let field_name = format_ident!("Filterable_field_{}", &name);
        let err_msg = format!("Missing field for {} ({})", name, field_name);
        let quoted = format!("{}", field_name);
        quote_spanned! { field.span()=>
            #name: schema.get_field(#quoted).ok_or_else(
                || tantivy::TantivyError::SchemaError(#err_msg.to_string()))?
        }
    });

    let interpret_code = fields.iter().map(|field| {
        let name = field.ident;

        let (from_code, query_code) = match field.schema {
            FieldType::UNSIGNED => (
                quote!(u64::from),
                quote!(tantivy::query::RangeQuery::new_u64),
            ),
            FieldType::SIGNED => (
                quote!(i64::from),
                quote!(tantivy::query::RangeQuery::new_i64),
            ),
            FieldType::FLOAT => (
                quote!(f64::from),
                quote!(tantivy::query::RangeQuery::new_f64),
            ),
        };

        let range_code = if field.is_largest {
            quote! {
                let range = rr.clone();
            }
        } else {
            quote! {
                let range = std::ops::Range {
                    start: #from_code(rr.start),
                    end: #from_code(rr.end),
                };
            }
        };

        quote_spanned! { field.span()=>
            if let Some(ref rr) = query.#name {
                #range_code
                let query = #query_code(self.#name, range);
                result.push(Box::new(query));
            }
        }
    });

    let add_to_doc_code = fields.iter().map(|field| {
        let name = field.ident;

        let convert_code = if field.is_largest {
            quote_spanned! { field.span()=>
                let value = value;
            }
        } else {
            match field.schema {
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

        let add_code = match field.schema {
            FieldType::UNSIGNED => quote!(doc.add_u64(self.#name, value);),
            FieldType::SIGNED => quote!(doc.add_i64(self.#name, value);),
            FieldType::FLOAT => quote!(doc.add_f64(self.#name, value);),
        };

        if field.is_optional {
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
        #[serde(deny_unknown_fields)]
        pub struct #name {
            #(#query_fields),*
        }

        #[derive(Clone, Debug, PartialEq)]
        pub struct #index_name {
            #(#index_fields),*
        }

        impl cantine_derive::Filterable for #feat {
            type Query = #name;
            type Schema = #index_name;

            fn load_schema(schema: &tantivy::schema::Schema) -> tantivy::Result<Self::Schema> {
                Self::Schema::try_from(schema)
            }

            fn create_schema<O: Into<tantivy::schema::IntOptions>>(
                builder: &mut tantivy::schema::SchemaBuilder,
                options: O,
            ) -> Self::Schema {
                Self::Schema::with_flags(builder, options)
            }
        }

        impl cantine_derive::FilterableSchema<#feat, #name> for #index_name {
            fn add_to_doc(&self, doc: &mut tantivy::Document, item: &#feat) {
                <#index_name>::add_to_doc(self, doc, item)
            }

            fn interpret(&self, query: &#name) -> Vec<Box<dyn tantivy::query::Query>> {
                <#index_name>::interpret(self, query)
            }
        }

        impl std::convert::TryFrom<&tantivy::schema::Schema> for #index_name {
            type Error = tantivy::TantivyError;

            fn try_from(schema: &tantivy::schema::Schema) -> Result<Self, Self::Error> {
                <#index_name>::try_from(schema)
            }
        }

        impl From<&mut tantivy::schema::SchemaBuilder> for #index_name {
            fn from(builder: &mut tantivy::schema::SchemaBuilder) -> Self {
                Self::with_flags(builder, tantivy::schema::INDEXED)
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

            pub fn with_flags<O: Into<tantivy::schema::IntOptions>>(
                builder: &mut tantivy::schema::SchemaBuilder,
                flags: O
            ) -> Self {
                let flags = flags.into();
                let new = Self {
                    #(#from_decls),*
                };

                if ! flags.is_indexed() {
                    panic!("Missing required INDEXED option");
                }

                new
            }

            pub fn try_from(schema: &tantivy::schema::Schema) -> tantivy::Result<Self> {
                Ok(Self {
                    #(#try_from_decls),*
                })
            }
        }
    }
}

fn make_agg_query(feat: &Ident, fields: &[FieldInfo]) -> TokenStream2 {
    let name = format_ident!("AggregableAggregationQuery{}", &feat);

    let query_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;
        quote_spanned! { field.span()=>
            #[serde(default = "Vec::new")]
            pub #name: Vec<std::ops::Range<#ty>>
        }
    });

    let full_range = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;
        quote_spanned! { field.span()=>
            #name: vec![std::#ty::MIN..std::#ty::MAX]
        }
    });

    quote! {
        #[derive(serde::Serialize, serde::Deserialize, Default, Debug, Clone, PartialEq)]
        #[serde(deny_unknown_fields)]
        pub struct #name {
            #(#query_fields),*
        }

        impl #name {
            pub fn full_range() -> Self {
                Self {
                    #(#full_range),*
                }
            }
        }
    }
}

fn make_agg_result(feature: &Ident, fields: &[FieldInfo]) -> TokenStream2 {
    let name = format_ident!("AggregableAggregationResult{}", &feature);

    let agg_fields = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = &field.ty;

        quote_spanned! { field.span()=>
            #[serde(skip_serializing_if = "Vec::is_empty")]
            pub #name: Vec<cantine_derive::RangeStats<#ty>>
        }
    });

    let merge_code = fields.iter().map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            for (idx, stats) in self.#name.iter_mut().enumerate() {
                stats.merge(&other.#name[idx]);
            }
        }
    });

    let agg_query = format_ident!("AggregableAggregationQuery{}", &feature);
    let convert_code = fields.iter().map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            #name:
                if src.#name.is_empty() {
                    Vec::new()
                } else {
                    src.#name.iter().map(From::from).collect()
                }
        }
    });

    let collect_code = fields.iter().map(|field| {
        let name = &field.ident;
        if field.is_optional {
            quote_spanned! { field.span()=>
                if let Some(feat) = feature.#name {
                    for (idx, range) in query.#name.iter().enumerate() {
                        if range.contains(&feat) {
                            self.#name[idx].collect(feat);
                        }
                    }
                }
            }
        } else {
            quote_spanned! { field.span()=>
                for (idx, range) in query.#name.iter().enumerate() {
                    if range.contains(&feature.#name) {
                        self.#name[idx].collect(feature.#name);
                    }
                }
            }
        }
    });

    quote! {
        #[derive(serde::Serialize, Default, Debug, Clone)]
        pub struct #name {
            #(#agg_fields),*
        }

        impl cantine_derive::Aggregable for #feature {
            type Query = #agg_query;
            type Agg = #name;
        }

        impl cantine_derive::Aggregator<#agg_query, #feature> for #name {
            fn merge_same_size(&mut self, other: &Self) {
                <#name>::merge_same_size(self, other);
            }

            fn collect(&mut self, query: &#agg_query, feature: &#feature) {
                <#name>::collect(self, query, feature);
            }

            fn from_query(query: &#agg_query) -> Self {
                <#name>::from(query)
            }
        }

        impl #name {
            fn merge_same_size(&mut self, other: &Self) {
                #(#merge_code);*
            }

            fn collect(&mut self, query: &#agg_query, feature: &#feature) {
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

fn extract_type_if_option(ty: &Type) -> Option<&Type> {
    match ty {
        Type::Path(tp) if tp.path.segments.first()?.ident == "Option" => {
            match tp.path.segments.first()?.arguments {
                PathArguments::AngleBracketed(ref params) => match params.args.first()? {
                    GenericArgument::Type(ty) => Some(ty),
                    _ => None,
                },
                _ => None,
            }
        }
        _ => None,
    }
}

enum FieldType {
    UNSIGNED,
    SIGNED,
    FLOAT,
}

fn get_field_type(ty: &Type) -> Option<(FieldType, bool)> {
    match ty {
        Type::Path(tp) if tp.path.segments.len() == 1 => {
            match tp.path.segments.first()?.ident.to_string().as_str() {
                "u64" => Some((FieldType::UNSIGNED, true)),
                "u8" | "u16" | "u32" => Some((FieldType::UNSIGNED, false)),

                "i64" => Some((FieldType::SIGNED, true)),
                "i8" | "i16" | "i32" => Some((FieldType::SIGNED, false)),

                "f64" => Some((FieldType::FLOAT, true)),
                "f32" => Some((FieldType::FLOAT, false)),
                _ => None,
            }
        }
        _ => None,
    }
}

enum Error {
    BadField(Span),
    BadInput,
}

fn render_error(err: Error) -> TokenStream2 {
    match err {
        Error::BadField(span) => {
            quote_spanned! { span =>
                compile_error!("Unsupported field");
            }
        }
        Error::BadInput => panic!("Only structs with public named fields are supported"),
    }
}
