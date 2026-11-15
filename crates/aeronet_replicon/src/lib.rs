#![cfg_attr(docsrs_aeronet, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//!
//! ## Feature flags
#![cfg_attr(feature = "document-features", doc = document_features::document_features!())]
#![no_std]

#[cfg(feature = "client")]
pub mod client;
pub mod convert;
pub mod lanes;
#[cfg(feature = "server")]
pub mod server;
