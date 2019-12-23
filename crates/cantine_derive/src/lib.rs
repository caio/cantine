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
    let collector = make_collector(&input);

    TokenStream::from(quote! {
        #filter_query

        #agg_query
        #agg_result
        #collector
    })
}

fn make_collector(input: &DeriveInput) -> TokenStream2 {
    let meta = &input.ident;
    let agg = format_ident!("{}AggregationResult", meta);
    let query = format_ident!("{}AggregationQuery", meta);

    let collector = format_ident!("{}Collector", meta);
    let segment_collector = format_ident!("{}SegmentColletor", meta);

    quote! {
        pub struct #collector<F> {
            agg: #agg,
            query: #query,
            reader_factory: F,
        }

        impl<F, R> #collector<F>
        where
            F: 'static + Sync + Fn(&tantivy::SegmentReader) -> R,
            R: 'static + Fn(tantivy::DocId, &#query, &mut #agg),
        {
            pub fn new(query: #query, reader_factory: F) -> Self {
                let agg = <#agg>::from(&query);
                Self {
                    agg, query, reader_factory
                }
            }
        }

        pub struct #segment_collector<F> {
            agg: #agg,
            query: #query,
            reader: F,
        }

        impl<F, R> tantivy::collector::Collector for #collector<F>
        where
            F: 'static + Sync + Fn(&tantivy::SegmentReader) -> R,
            R: 'static + Fn(tantivy::DocId, &#query, &mut #agg),
        {
            type Fruit = #agg;
            type Child = #segment_collector<R>;

            fn for_segment(
                &self,
                _segment_id: tantivy::SegmentLocalId,
                segment_reader: &tantivy::SegmentReader,
            ) -> tantivy::Result<Self::Child> {
                Ok(#segment_collector {
                    agg: self.agg.clone(),
                    query: self.query.clone(),
                    reader: (self.reader_factory)(segment_reader),
                })
            }

            fn requires_scoring(&self) -> bool {
                false
            }

            fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
                let mut iter = fruits.into_iter();

                let mut first = iter.next().expect("Always at least one fruit");

                for fruit in iter {
                    first.merge_same_size(&fruit);
                }

                Ok(first)
            }
        }

        impl<F> tantivy::collector::SegmentCollector for #segment_collector<F>
        where
            F: 'static + Fn(tantivy::DocId, &#query, &mut #agg),
        {
            type Fruit = #agg;

            fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
                (self.reader)(doc, &self.query, &mut self.agg);
            }

            fn harvest(self) -> Self::Fruit {
                self.agg
            }
        }
    }
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
        let field_type = get_field_type(&ty);

        let method = match field_type {
            FieldType::UNSIGNED => quote!(add_u64_field),
            FieldType::SIGNED => quote!(add_i64_field),
            FieldType::FLOAT => quote!(add_f64_field),
        };

        quote_spanned! { field.span()=>
            #name: builder.#method(#quoted, tantivy::schema::INDEXED | tantivy::schema::FAST)
        }
    });

    let try_from_decls = fields.iter().map(|field| {
        if let Some(name) = &field.ident {
            let schema_name = format_ident!("_filter_{}", &name);
            let err_msg = format!("Missing field for {} ({})", name, schema_name);
            let quoted = format!("\"{}\"", schema_name);
            quote_spanned! { field.span()=>
                #name: schema.get_field(#quoted).ok_or_else(
                    || tantivy::TantivyError::SchemaError(#err_msg.to_string()))?
            }
        } else {
            unreachable!();
        }
    });

    let interpret_code = fields.iter().map(|field| {
        let name = &field.ident;
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);
        let is_largest = is_largest_type(&ty);
        let field_type = get_field_type(&ty);

        let (from_code, query_code) = match field_type {
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

        let range_code = if is_largest {
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
            type Error = tantivy::TantivyError;

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
        quote_spanned! { field.span()=>
            #[serde(default = "Vec::new")]
            pub #name: Vec<std::ops::Range<#ty>>
        }
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
        let ty = extract_type_if_option(&field.ty).unwrap_or(&field.ty);

        quote_spanned! { field.span()=>
            #[serde(skip_serializing_if = "Vec::is_empty")]
            pub #name: Vec<RangeStats<#ty>>
        }
    });

    let merge_code = get_public_struct_fields(&input).map(|field| {
        let name = &field.ident;
        quote_spanned! { field.span()=>
            for (idx, stats) in self.#name.iter_mut().enumerate() {
                stats.merge(&other.#name[idx]);
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
                    src.#name.iter().map(From::from).collect()
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
            #(#fields),*
        }

        // FIXME move outside crate
        #[derive(serde::Serialize, Debug, Clone)]
        pub struct RangeStats<T: serde::Serialize> {
            pub min: T,
            pub max: T,
            pub count: u64,
        }

        impl<T: PartialOrd + Copy + serde::Serialize> RangeStats<T> {
            pub fn collect(&mut self, value: T) {
                if self.min > value {
                    self.min = value;
                }

                if self.max < value {
                    self.max = value;
                }

                self.count += 1;
            }

            pub fn merge(&mut self, other: &Self) {
                if self.min > other.min {
                    self.min = other.min;
                }

                if self.max < other.max {
                    self.max = other.max;
                }

                self.count += other.count;
            }
        }

        impl<T: PartialOrd + Copy + serde::Serialize> From<&std::ops::Range<T>> for RangeStats<T> {
            fn from(src: &std::ops::Range<T>) -> Self {
                Self {
                    min: src.start,
                    max: src.start,
                    count: 0,
                }
            }
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
