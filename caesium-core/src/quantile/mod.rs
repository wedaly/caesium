pub mod baseline;
mod compactor;
pub mod error;
pub mod kll;
mod minmax;
pub mod query;
mod sampler;

pub mod writable {
    #[cfg(not(feature = "baseline"))]
    pub use quantile::kll::KllSketch as WritableSketch;

    #[cfg(feature = "baseline")]
    pub use quantile::baseline::BaselineSketch as WritableSketch;
}

pub mod readable {
    #[cfg(not(feature = "baseline"))]
    pub use quantile::query::WeightedQuerySketch as ReadableSketch;

    #[cfg(feature = "baseline")]
    pub use quantile::query::UnweightedQuerySketch as ReadableSketch;
}

#[cfg(test)]
mod tests;
